//! MIGRATE's outgoing transport. Source snapshots and conditional deletion belong
//! to the caller; an acknowledged index is safe to consider even on partial error.
//!
//! Wire behavior follows https://redis.io/docs/latest/commands/migrate/ and
//! https://redis.io/docs/latest/commands/restore/. The timeout is an idle timeout,
//! not a deadline for the whole migration. No uncertain write is ever retried.

use std::{future::Future, io, time::Duration};

use bytes::Bytes;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::protocol::Resp;

const MAX_REPLY_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub struct MigrateOptions {
    pub host: String,
    pub port: u16,
    pub database: u32,
    pub timeout: Duration,
    pub copy: bool,
    pub replace: bool,
    /// AUTH arguments, excluding the command name: password, or username/password.
    pub auth: Option<Vec<Bytes>>,
    pub keys: Vec<Bytes>,
}

#[derive(Debug)]
pub struct MigrateItem {
    pub key: Bytes,
    pub dump: Bytes,
    /// Absolute Unix expiration in milliseconds; None means persistent.
    pub expires_at: Option<i64>,
}

#[derive(Debug)]
pub struct MigrateResult {
    /// Indices in the supplied item slice, never indices in options.keys.
    pub acknowledged: Vec<usize>,
    pub reply: Resp,
}

/// Parse the complete MIGRATE command, including its command name.
/// KEYS consumes every remaining argument, including words resembling options.
pub fn parse(args: &[Bytes]) -> Result<MigrateOptions, Resp> {
    if args.len() < 6 {
        return Err(Resp::Error(
            "ERR wrong number of arguments for 'migrate' command".into(),
        ));
    }
    if !args[0].eq_ignore_ascii_case(b"MIGRATE") {
        return Err(syntax_error());
    }
    let port = integer(&args[2])?;
    let port = u16::try_from(port)
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| Resp::Error("ERR Invalid target port".into()))?;
    let database = u32::try_from(integer(&args[4])?)
        .map_err(|_| Resp::Error("ERR DB index is out of range".into()))?;
    let timeout = integer(&args[5])?;
    // Redis normalizes nonpositive timeouts to one second, including zero.
    let timeout = Duration::from_millis(if timeout <= 0 { 1000 } else { timeout as u64 });
    let host =
        std::str::from_utf8(&args[1]).map_err(|_| Resp::Error("ERR Invalid target host".into()))?;
    if host.is_empty() || host.as_bytes().iter().any(|byte| byte.is_ascii_control()) {
        return Err(Resp::Error("ERR Invalid target host".into()));
    }
    let mut copy = false;
    let mut replace = false;
    let mut auth = None;
    let mut keys = None;
    let mut index = 6;
    while index < args.len() {
        let option = &args[index];
        if option.eq_ignore_ascii_case(b"COPY") {
            copy = true;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"AUTH") {
            if auth.is_some() || index + 1 >= args.len() {
                return Err(syntax_error());
            }
            auth = Some(vec![Bytes::copy_from_slice(&args[index + 1])]);
            index += 2;
        } else if option.eq_ignore_ascii_case(b"AUTH2") {
            if auth.is_some() || index + 2 >= args.len() {
                return Err(syntax_error());
            }
            auth = Some(vec![
                Bytes::copy_from_slice(&args[index + 1]),
                Bytes::copy_from_slice(&args[index + 2]),
            ]);
            index += 3;
        } else if option.eq_ignore_ascii_case(b"KEYS") {
            if !args[3].is_empty() {
                return Err(Resp::Error("ERR When using MIGRATE KEYS option, the key argument must be set to the empty string".into()));
            }
            if index + 1 >= args.len() {
                return Err(syntax_error());
            }
            keys = Some(
                args[index + 1..]
                    .iter()
                    .map(|key| Bytes::copy_from_slice(key))
                    .collect(),
            );
            break;
        } else {
            return Err(syntax_error());
        }
    }
    Ok(MigrateOptions {
        host: host.into(),
        port,
        database,
        timeout,
        copy,
        replace,
        auth,
        // Without KEYS, even the empty string is a legitimate single key name.
        keys: keys.unwrap_or_else(|| vec![Bytes::copy_from_slice(&args[3])]),
    })
}

/// Transfer already-captured, existing keys. No source state is modified here.
/// The caller must conditionally delete acknowledged, unchanged snapshots unless
/// COPY was requested, including when the final reply is an error.
pub async fn transfer(options: &MigrateOptions, items: &[MigrateItem]) -> MigrateResult {
    let mut acknowledged = Vec::new();
    if items.is_empty() {
        return MigrateResult {
            acknowledged,
            reply: Resp::Simple("NOKEY"),
        };
    }
    let reply = match transfer_inner(options, items, &mut acknowledged).await {
        Ok(Some(error)) => error,
        Ok(None) => Resp::Simple("OK"),
        Err(error) => Resp::Error(format!(
            "IOERR error or timeout communicating with target instance: {error}"
        )),
    };
    MigrateResult {
        acknowledged,
        reply,
    }
}

async fn transfer_inner(
    options: &MigrateOptions,
    items: &[MigrateItem],
    acknowledged: &mut Vec<usize>,
) -> io::Result<Option<Resp>> {
    let timeout = if options.timeout.is_zero() {
        Duration::from_millis(1000)
    } else {
        options.timeout
    };
    let socket = timed(
        timeout,
        TcpStream::connect((options.host.as_str(), options.port)),
    )
    .await?;
    socket.set_nodelay(true)?;
    let mut stream = BufReader::with_capacity(4096, socket);
    let mut reply = Vec::new();
    if let Some(auth) = &options.auth {
        match auth.as_slice() {
            [password] => write_command(&mut stream, timeout, &[b"AUTH", password]).await?,
            [username, password] => {
                write_command(&mut stream, timeout, &[b"AUTH", username, password]).await?
            }
            _ => return Err(invalid("invalid authentication arguments")),
        }
        if let Some(error) = read_reply(&mut stream, timeout, &mut reply).await? {
            return Ok(Some(error));
        }
    }
    let mut database = Decimal::new();
    write_command(
        &mut stream,
        timeout,
        &[b"SELECT", database.unsigned(u64::from(options.database))],
    )
    .await?;
    if let Some(error) = read_reply(&mut stream, timeout, &mut reply).await? {
        return Ok(Some(error));
    }
    let mut first_error = None;
    for (index, item) in items.iter().enumerate() {
        let mut ttl = Decimal::new();
        let ttl_bytes = ttl.signed(item.expires_at.unwrap_or(0));
        let mut command: [&[u8]; 6] = [b"RESTORE", &item.key, ttl_bytes, &item.dump, b"", b""];
        let mut length = 4;
        if options.replace {
            command[length] = b"REPLACE";
            length += 1;
        }
        if item.expires_at.is_some() {
            command[length] = b"ABSTTL";
            length += 1;
        }
        write_command(&mut stream, timeout, &command[..length]).await?;
        match read_reply(&mut stream, timeout, &mut reply).await? {
            Some(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            None => acknowledged.push(index),
        }
    }
    // Dropping the stream closes immediately, without waiting for the peer.
    Ok(first_error)
}

/// Write borrowed RESP chunks directly; the serialized value is never copied.
async fn write_command(
    stream: &mut BufReader<TcpStream>,
    timeout: Duration,
    args: &[&[u8]],
) -> io::Result<()> {
    let mut number = Decimal::new();
    write_all(stream, timeout, b"*").await?;
    write_all(stream, timeout, number.unsigned(args.len() as u64)).await?;
    write_all(stream, timeout, b"\r\n").await?;
    for argument in args {
        write_all(stream, timeout, b"$").await?;
        write_all(stream, timeout, number.unsigned(argument.len() as u64)).await?;
        write_all(stream, timeout, b"\r\n").await?;
        write_all(stream, timeout, argument).await?;
        write_all(stream, timeout, b"\r\n").await?;
    }
    Ok(())
}

async fn write_all(
    stream: &mut BufReader<TcpStream>,
    timeout: Duration,
    mut bytes: &[u8],
) -> io::Result<()> {
    while !bytes.is_empty() {
        let written = timed(timeout, stream.write(bytes)).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "target closed during write",
            ));
        }
        bytes = &bytes[written..];
    }
    Ok(())
}

/// Only +OK acknowledges a mutation. Errors remain target errors; malformed,
/// truncated, oversized, or unexpected frames are transport/protocol failures.
async fn read_reply(
    stream: &mut BufReader<TcpStream>,
    timeout: Duration,
    line: &mut Vec<u8>,
) -> io::Result<Option<Resp>> {
    line.clear();
    loop {
        let available = timed(timeout, stream.fill_buf()).await?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "target closed before acknowledgement",
            ));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let used = newline.map_or(available.len(), |index| index + 1);
        if used > MAX_REPLY_BYTES - line.len() {
            return Err(invalid("target reply exceeds 64 KiB"));
        }
        line.extend_from_slice(&available[..used]);
        stream.consume(used);
        if newline.is_some() {
            break;
        }
    }
    if !line.ends_with(b"\r\n") || line[..line.len() - 2].contains(&b'\r') {
        return Err(invalid("malformed target reply"));
    }
    if line.as_slice() == b"+OK\r\n" {
        return Ok(None);
    }
    if line.first() == Some(&b'-') {
        let message = String::from_utf8_lossy(&line[1..line.len() - 2]);
        return Ok(Some(Resp::Error(format!(
            "ERR Target instance replied with error: {message}"
        ))));
    }
    Err(invalid("unexpected target acknowledgement"))
}

async fn timed<T>(
    duration: Duration,
    future: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "migration idle timeout"))?
}

fn integer(bytes: &[u8]) -> Result<i64, Resp> {
    let error = || Resp::Error("ERR value is not an integer or out of range".into());
    if bytes == b"0" {
        return Ok(0);
    }
    let (negative, digits) = match bytes.first() {
        Some(b'-') => (true, &bytes[1..]),
        _ => (false, bytes),
    };
    if !digits
        .first()
        .is_some_and(|digit| matches!(*digit, b'1'..=b'9'))
    {
        return Err(error());
    }
    let mut value = 0i64;
    for digit in digits {
        if !digit.is_ascii_digit() {
            return Err(error());
        }
        value = value.checked_mul(10).ok_or_else(error)?;
        value = if negative {
            value.checked_sub(i64::from(digit - b'0'))
        } else {
            value.checked_add(i64::from(digit - b'0'))
        }
        .ok_or_else(error)?;
    }
    Ok(value)
}

fn syntax_error() -> Resp {
    Resp::Error("ERR syntax error".into())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Stack-only decimal formatting for RESP lengths and expiration timestamps.
struct Decimal {
    bytes: [u8; 21],
}
impl Decimal {
    fn new() -> Self {
        Self { bytes: [0; 21] }
    }
    fn unsigned(&mut self, mut value: u64) -> &[u8] {
        let mut index = self.bytes.len();
        loop {
            index -= 1;
            self.bytes[index] = b'0' + (value % 10) as u8;
            value /= 10;
            if value == 0 {
                break;
            }
        }
        &self.bytes[index..]
    }
    fn signed(&mut self, value: i64) -> &[u8] {
        let length = self.unsigned(value.unsigned_abs()).len();
        let index = self.bytes.len() - length;
        if value < 0 {
            self.bytes[index - 1] = b'-';
            &self.bytes[index - 1..]
        } else {
            &self.bytes[index..]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio::{io::AsyncReadExt, net::TcpListener};

    fn argv(args: &[&str]) -> Vec<Bytes> {
        args.iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect()
    }

    fn options() -> MigrateOptions {
        parse(&argv(&["MIGRATE", "127.0.0.1", "1", "key", "0", "100"])).unwrap()
    }

    #[test]
    fn parser_rejects_malformed_commands_and_numbers() {
        for args in [
            vec!["MIGRATE"],
            vec!["MIGRATE", "host", "0", "key", "0", "100"],
            vec!["MIGRATE", "host", "65536", "key", "0", "100"],
            vec!["MIGRATE", "host", "6379", "key", "-1", "100"],
            vec!["MIGRATE", "host", "6379", "key", "4294967296", "100"],
            vec!["MIGRATE", "host", "6379", "key", "0", "x"],
            vec!["MIGRATE", "host", "+6379", "key", "0", "100"],
            vec!["MIGRATE", "host", "06379", "key", "0", "100"],
            vec!["MIGRATE", "host", "6379", "key", "0", "100", "AUTH"],
            vec![
                "MIGRATE", "host", "6379", "key", "0", "100", "AUTH2", "user",
            ],
            vec![
                "MIGRATE", "host", "6379", "key", "0", "100", "AUTH", "p", "AUTH2", "u", "p",
            ],
            vec![
                "MIGRATE", "host", "6379", "key", "0", "100", "KEYS", "other",
            ],
            vec!["MIGRATE", "host", "6379", "", "0", "100", "KEYS"],
            vec!["MIGRATE", "host", "6379", "key", "0", "100", "UNKNOWN"],
        ] {
            assert!(parse(&argv(&args)).is_err(), "{args:?}");
        }
    }

    #[test]
    fn parser_handles_auth_and_keys_without_reinterpreting_key_names() {
        let options = parse(&argv(&[
            "migrate",
            "localhost",
            "65535",
            "",
            "4294967295",
            "0",
            "copy",
            "replace",
            "AUTH2",
            "user",
            "pass",
            "KEYS",
            "AUTH",
            "REPLACE",
            "",
        ]))
        .unwrap();
        assert_eq!(options.port, 65535);
        assert_eq!(options.database, u32::MAX);
        assert_eq!(options.timeout, Duration::from_millis(1000));
        assert!(options.copy && options.replace);
        assert_eq!(options.auth.unwrap(), argv(&["user", "pass"]));
        assert_eq!(options.keys, argv(&["AUTH", "REPLACE", ""]));
        assert_eq!(
            parse(&argv(&["MIGRATE", "host", "1", "", "0", "-1"]))
                .unwrap()
                .keys,
            argv(&[""])
        );
    }

    async fn command(stream: &mut TcpStream, input: &mut BytesMut) -> Vec<Bytes> {
        loop {
            if let Some(args) = crate::protocol::parse(input, Vec::with_capacity).unwrap() {
                return args;
            }
            assert_ne!(stream.read_buf(input).await.unwrap(), 0);
        }
    }

    #[tokio::test]
    async fn authenticates_selects_preserves_expiration_and_partial_acknowledgements() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut options = options();
        options.port = listener.local_addr().unwrap().port();
        options.database = 7;
        options.timeout = Duration::from_secs(2);
        options.auth = Some(argv(&["user", "pass"]));
        options.replace = true;
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut input = BytesMut::new();
            assert_eq!(
                command(&mut stream, &mut input).await,
                argv(&["AUTH", "user", "pass"])
            );
            stream.write_all(b"+OK\r\n").await.unwrap();
            assert_eq!(
                command(&mut stream, &mut input).await,
                argv(&["SELECT", "7"])
            );
            stream.write_all(b"+OK\r\n").await.unwrap();
            assert_eq!(
                command(&mut stream, &mut input).await,
                argv(&[
                    "RESTORE",
                    "first",
                    "1234567890000",
                    "dump\0\r\n",
                    "REPLACE",
                    "ABSTTL"
                ])
            );
            stream.write_all(b"+O").await.unwrap();
            stream.write_all(b"K\r\n").await.unwrap();
            assert_eq!(
                command(&mut stream, &mut input).await,
                argv(&["RESTORE", "second", "0", "payload", "REPLACE"])
            );
            stream
                .write_all(b"-BUSYKEY Target key name is busy.\r\n")
                .await
                .unwrap();
            assert_eq!(
                command(&mut stream, &mut input).await,
                argv(&["RESTORE", "third", "0", "payload", "REPLACE"])
            );
            stream.write_all(b"+OK\r\n").await.unwrap();
        });
        let items = [
            MigrateItem {
                key: Bytes::from_static(b"first"),
                dump: Bytes::from_static(b"dump\0\r\n"),
                expires_at: Some(1234567890000),
            },
            MigrateItem {
                key: Bytes::from_static(b"second"),
                dump: Bytes::from_static(b"payload"),
                expires_at: None,
            },
            MigrateItem {
                key: Bytes::from_static(b"third"),
                dump: Bytes::from_static(b"payload"),
                expires_at: None,
            },
        ];
        let result = transfer(&options, &items).await;
        assert_eq!(result.acknowledged, vec![0, 2]);
        assert!(
            matches!(result.reply, Resp::Error(message) if message.starts_with("ERR Target instance replied with error: BUSYKEY"))
        );
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn uncertain_acknowledgement_is_not_retried() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut options = options();
        options.port = listener.local_addr().unwrap().port();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut input = BytesMut::new();
            assert_eq!(command(&mut stream, &mut input).await[0], b"SELECT"[..]);
            stream.write_all(b"+OK\r\n").await.unwrap();
            assert_eq!(command(&mut stream, &mut input).await[0], b"RESTORE"[..]);
            // EOF after receiving RESTORE is uncertain, not a successful copy.
        });
        let item = MigrateItem {
            key: Bytes::from_static(b"key"),
            dump: Bytes::from_static(b"payload"),
            expires_at: None,
        };
        let result = transfer(&options, &[item]).await;
        assert!(result.acknowledged.is_empty());
        assert!(matches!(result.reply, Resp::Error(message) if message.starts_with("IOERR")));
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn auth_failure_stops_before_select_or_restore() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut options = options();
        options.port = listener.local_addr().unwrap().port();
        options.auth = Some(argv(&["secret"]));
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut input = BytesMut::new();
            assert_eq!(
                command(&mut stream, &mut input).await,
                argv(&["AUTH", "secret"])
            );
            stream
                .write_all(b"-WRONGPASS invalid credentials\r\n")
                .await
                .unwrap();
            let mut byte = [0];
            assert_eq!(
                timed(Duration::from_secs(2), stream.read(&mut byte))
                    .await
                    .unwrap(),
                0
            );
        });
        let item = MigrateItem {
            key: Bytes::from_static(b"key"),
            dump: Bytes::from_static(b"payload"),
            expires_at: None,
        };
        let result = transfer(&options, &[item]).await;
        assert!(result.acknowledged.is_empty());
        assert!(
            matches!(result.reply, Resp::Error(message) if message.starts_with("ERR Target instance replied with error: WRONGPASS"))
        );
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_and_non_ok_replies_cannot_acknowledge_keys() {
        for response in [
            vec![b'x'; MAX_REPLY_BYTES + 1],
            b"+PONG\r\n".to_vec(),
            b"+OK\n".to_vec(),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut options = options();
            options.port = listener.local_addr().unwrap().port();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut input = BytesMut::new();
                let _ = command(&mut stream, &mut input).await;
                stream.write_all(b"+OK\r\n").await.unwrap();
                let _ = command(&mut stream, &mut input).await;
                // The transport may close as soon as it sees a malformed frame.
                let _ = stream.write_all(&response).await;
            });
            let item = MigrateItem {
                key: Bytes::from_static(b"key"),
                dump: Bytes::from_static(b"payload"),
                expires_at: None,
            };
            let result = transfer(&options, &[item]).await;
            assert!(result.acknowledged.is_empty());
            assert!(matches!(result.reply, Resp::Error(message) if message.starts_with("IOERR")));
            peer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn idle_timeout_keeps_prior_acknowledgements_and_closes_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut options = options();
        options.port = listener.local_addr().unwrap().port();
        options.timeout = Duration::from_millis(100);
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut input = BytesMut::new();
            for _ in 0..2 {
                let _ = command(&mut stream, &mut input).await;
                stream.write_all(b"+OK\r\n").await.unwrap();
            }
            let _ = command(&mut stream, &mut input).await;
            // Withhold the second RESTORE acknowledgement until client timeout.
            let mut byte = [0];
            assert_eq!(
                timed(Duration::from_secs(2), stream.read(&mut byte))
                    .await
                    .unwrap(),
                0
            );
        });
        let items = [
            MigrateItem {
                key: Bytes::from_static(b"first"),
                dump: Bytes::from_static(b"payload"),
                expires_at: None,
            },
            MigrateItem {
                key: Bytes::from_static(b"second"),
                dump: Bytes::from_static(b"payload"),
                expires_at: None,
            },
        ];
        let result = transfer(&options, &items).await;
        assert_eq!(result.acknowledged, vec![0]);
        assert!(matches!(result.reply, Resp::Error(message) if message.starts_with("IOERR")));
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn empty_snapshots_do_not_connect() {
        let result = transfer(&options(), &[]).await;
        assert_eq!(result.reply, Resp::Simple("NOKEY"));
        assert!(result.acknowledged.is_empty());
    }
}
