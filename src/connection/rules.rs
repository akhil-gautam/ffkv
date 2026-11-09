//! Connection rules that need no server state, shared with the browser
//! playground's sessions (`src/wasm/session.rs`).
use bytes::Bytes;

use crate::protocol::{Resp, Version};

const MAX_QUEUED_COMMANDS: usize = 1024;
const MAX_QUEUED_BYTES: usize = 16 * 1024 * 1024;

fn error(message: &'static str) -> Resp {
    Resp::Error(message.into())
}

fn bulk(value: &'static str) -> Resp {
    Resp::Bulk(Some(Bytes::from_static(value.as_bytes())))
}

pub(crate) fn printable(value: &[u8]) -> bool {
    value.len() <= 256 && value.iter().all(|byte| (b'!'..=b'~').contains(byte))
}

pub(crate) fn invalid_client_name() -> Resp {
    error("ERR Client names must be printable ASCII without spaces and at most 256 bytes")
}

pub(crate) fn no_password_configured() -> Resp {
    error("ERR AUTH called without any password configured")
}

/// AUTH's `[username] password`; no username means the default user.
pub(crate) fn auth_credentials(args: &[Bytes]) -> Result<(Option<&[u8]>, &[u8]), Resp> {
    match args {
        [_, password] => Ok((None, password)),
        [_, username, password] => Ok((Some(username), password)),
        _ => Err(error("ERR wrong number of arguments for AUTH")),
    }
}

/// `CLIENT SETINFO <LIB-NAME|LIB-VER> <value>` (four arguments): whether the
/// field is the library name, and its value.
pub(crate) fn client_setinfo(args: &[Bytes]) -> Result<(bool, &Bytes), Resp> {
    if !printable(&args[3]) {
        return Err(error(
            "ERR client library metadata must be printable ASCII and at most 256 bytes",
        ));
    }
    let library = if args[2].eq_ignore_ascii_case(b"LIB-NAME") {
        true
    } else if args[2].eq_ignore_ascii_case(b"LIB-VER") {
        false
    } else {
        return Err(error("ERR unrecognized CLIENT SETINFO option"));
    };
    Ok((library, &args[3]))
}

/// Whether a RESP2 connection with active subscriptions may run `command`
/// (QUIT is always accepted before this check).
pub(crate) fn allowed_while_subscribed(command: &[u8]) -> bool {
    [
        b"SUBSCRIBE".as_slice(),
        b"PSUBSCRIBE",
        b"UNSUBSCRIBE",
        b"PUNSUBSCRIBE",
        b"PING",
        b"SSUBSCRIBE",
        b"SUNSUBSCRIBE",
        b"RESET",
    ]
    .iter()
    .any(|name| command.eq_ignore_ascii_case(name))
}

pub(crate) fn subscribed_mode_error() -> Resp {
    error(
        "ERR Can't execute command in subscribed mode: only SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE / PING / QUIT are allowed",
    )
}

pub(crate) fn is_subscription_command(command: &[u8]) -> bool {
    [
        b"SUBSCRIBE".as_slice(),
        b"PSUBSCRIBE",
        b"UNSUBSCRIBE",
        b"PUNSUBSCRIBE",
        b"SSUBSCRIBE",
        b"SUNSUBSCRIBE",
    ]
    .iter()
    .any(|name| command.eq_ignore_ascii_case(name))
}

/// PING's reply on a RESP2 connection with active subscriptions.
pub(crate) fn subscribed_ping(args: &[Bytes]) -> Resp {
    Resp::Array(Some(vec![
        bulk("pong"),
        Resp::Bulk(Some(args.get(1).cloned().unwrap_or_default())),
    ]))
}

/// A MULTI block's queued commands.
#[derive(Default)]
pub(crate) struct Transaction {
    pub commands: Vec<Vec<Bytes>>,
    pub bytes: usize,
    /// A command was rejected while queuing, so EXEC aborts.
    pub dirty: bool,
}

impl Transaction {
    /// Queued commands outlive the request, so only an accepted one is copied.
    pub(crate) fn queue(&mut self, args: &[Bytes]) -> Resp {
        let bytes: usize = args.iter().map(Bytes::len).sum();
        if self.commands.len() == MAX_QUEUED_COMMANDS
            || self
                .bytes
                .checked_add(bytes)
                .is_none_or(|bytes| bytes > MAX_QUEUED_BYTES)
        {
            self.dirty = true;
            return error("ERR transaction queue limit exceeded");
        }
        self.bytes += bytes;
        self.commands.push(args.to_vec());
        Resp::Simple("QUEUED")
    }
}

/// HELLO's requested connection changes, validated before any is applied.
pub(crate) struct Hello<U> {
    pub version: Version,
    pub name: Option<Bytes>,
    pub user: Option<U>,
}

/// Parses `HELLO [protover [AUTH username password] [SETNAME clientname]]`.
/// `credentials` authenticates an AUTH option where it appears.
pub(crate) fn parse_hello<U>(
    args: &[Bytes],
    current: Version,
    mut credentials: impl FnMut(&[u8], &[u8]) -> Result<U, Resp>,
) -> Result<Hello<U>, Resp> {
    let mut version = current;
    let mut name = None;
    let mut user = None;
    if let Some(protocol) = args.get(1) {
        version = match protocol.as_ref() {
            b"2" => Version::Resp2,
            b"3" => Version::Resp3,
            _ => return Err(error("NOPROTO unsupported protocol version")),
        };
    }
    let mut position = 2;
    while position < args.len() {
        if args[position].eq_ignore_ascii_case(b"SETNAME") && position + 1 < args.len() {
            let value = &args[position + 1];
            if !printable(value) {
                return Err(invalid_client_name());
            }
            name = Some(value.clone());
            position += 2;
        } else if args[position].eq_ignore_ascii_case(b"AUTH") && position + 2 < args.len() {
            if user.is_some() {
                return Err(error("ERR duplicate HELLO AUTH option"));
            }
            user = Some(credentials(&args[position + 1], &args[position + 2])?);
            position += 3;
        } else {
            return Err(error("ERR syntax error in HELLO option"));
        }
    }
    Ok(Hello {
        version,
        name,
        user,
    })
}

pub(crate) fn hello_reply(version: Version, id: u64, cluster: bool, role: &'static str) -> Resp {
    Resp::Map(vec![
        (bulk("server"), bulk("ffkv")),
        (bulk("version"), bulk(env!("CARGO_PKG_VERSION"))),
        (
            bulk("proto"),
            Resp::Integer(if version == Version::Resp3 { 3 } else { 2 }),
        ),
        (bulk("id"), Resp::Integer(id as i64)),
        (
            bulk("mode"),
            bulk(if cluster { "cluster" } else { "standalone" }),
        ),
        (bulk("role"), bulk(role)),
        (bulk("modules"), Resp::Array(Some(Vec::new()))),
    ])
}
