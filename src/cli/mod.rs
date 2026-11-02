mod format;
mod options;
mod tokenize;

use std::{
    collections::BTreeSet,
    io::{self, BufRead, IsTerminal, Read, Write},
};

use bytes::Bytes;
use clap::Parser;
use rustyline::{DefaultEditor, ExternalPrinter, error::ReadlineError};
use tokio::sync::mpsc;

use crate::client::{Client, Frame, validate_command};
use format::{Format, render};
use options::Options;

const REQUEST_LIMIT: usize = 8 * 1024 * 1024;

type Printer = Box<dyn FnMut(&[u8]) -> io::Result<()> + Send>;

enum Input {
    Line(Vec<u8>),
    Eof,
    Interrupted,
    Error,
}

enum Pending {
    Reply {
        quit: bool,
        sensitive: bool,
        hello: bool,
        reauthenticate: bool,
        reset: bool,
        debug_mode: Option<bool>,
    },
    Subscription {
        kind: Vec<u8>,
        remaining: usize,
    },
}

struct ReplyMode {
    enabled: bool,
    skip: bool,
}

impl ReplyMode {
    fn expect_reply(&mut self, args: &[Bytes]) -> bool {
        if args[0].eq_ignore_ascii_case(b"RESET") && args.len() == 1 {
            self.enabled = true;
            self.skip = false;
            return true;
        }
        if args.len() == 3
            && args[0].eq_ignore_ascii_case(b"CLIENT")
            && args[1].eq_ignore_ascii_case(b"REPLY")
        {
            if args[2].eq_ignore_ascii_case(b"ON") {
                self.enabled = true;
                self.skip = false;
                return true;
            }
            if args[2].eq_ignore_ascii_case(b"OFF") {
                self.enabled = false;
                self.skip = false;
                return false;
            }
            if args[2].eq_ignore_ascii_case(b"SKIP") {
                self.skip = true;
                return false;
            }
        }
        if self.skip {
            self.skip = false;
            false
        } else {
            self.enabled
        }
    }
}

#[derive(Default)]
struct Subscriptions {
    channels: BTreeSet<Vec<u8>>,
    patterns: BTreeSet<Vec<u8>>,
    shards: BTreeSet<Vec<u8>>,
    count: usize,
}

impl Subscriptions {
    fn pending(&self, args: &[Bytes]) -> Pending {
        let name = args[0].to_ascii_lowercase();
        let existing = match name.as_slice() {
            b"unsubscribe" => Some(self.channels.len()),
            b"punsubscribe" => Some(self.patterns.len()),
            b"sunsubscribe" => Some(self.shards.len()),
            b"subscribe" | b"psubscribe" | b"ssubscribe" => Some(0),
            _ => None,
        };
        match existing {
            Some(existing) if args.len() > 1 || name.ends_with(b"unsubscribe") => {
                Pending::Subscription {
                    kind: name,
                    remaining: if args.len() > 1 {
                        args.len() - 1
                    } else {
                        existing.max(1)
                    },
                }
            }
            // A subscribe command without arguments returns an ordinary error.
            _ => Pending::Reply {
                quit: name == b"quit",
                sensitive: tokenize::sensitive(args),
                hello: name == b"hello",
                reauthenticate: name == b"auth" || (name == b"hello" && hello_auth(args)),
                reset: name == b"reset",
                debug_mode: if args.len() == 3
                    && name == b"script"
                    && args[1].eq_ignore_ascii_case(b"debug")
                {
                    if args[2].eq_ignore_ascii_case(b"no") {
                        Some(false)
                    } else if args[2].eq_ignore_ascii_case(b"yes")
                        || args[2].eq_ignore_ascii_case(b"sync")
                    {
                        Some(true)
                    } else {
                        None
                    }
                } else {
                    None
                },
            },
        }
    }

    fn acknowledge<'a>(&mut self, frame: &'a Frame) -> Option<&'a [u8]> {
        let items = elements(frame)?;
        if items.len() != 3 {
            return None;
        }
        let kind = string(&items[0])?;
        let (set, add) = match kind {
            b"subscribe" => (&mut self.channels, true),
            b"unsubscribe" => (&mut self.channels, false),
            b"psubscribe" => (&mut self.patterns, true),
            b"punsubscribe" => (&mut self.patterns, false),
            b"ssubscribe" => (&mut self.shards, true),
            b"sunsubscribe" => (&mut self.shards, false),
            _ => return None,
        };
        let Frame::Integer(count) = &items[2] else {
            return None;
        };
        if *count < 0 {
            return None;
        }
        if let Some(channel) = string(&items[1]) {
            if add {
                set.insert(channel.to_vec());
            } else {
                set.remove(channel);
            }
        }
        self.count = self.channels.len() + self.patterns.len() + self.shards.len();
        Some(kind)
    }

    fn is_message(&self, frame: &Frame) -> bool {
        self.count > 0
            && elements(frame)
                .and_then(|items| items.first())
                .and_then(string)
                .is_some_and(|name| matches!(name, b"message" | b"pmessage" | b"smessage"))
    }
}

fn elements(frame: &Frame) -> Option<&[Frame]> {
    match frame {
        Frame::Array(items) | Frame::Push(items) => Some(items),
        Frame::Attribute(_, value) => elements(value),
        _ => None,
    }
}

fn string(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::Bulk(bytes) | Frame::Simple(bytes) => Some(bytes),
        _ => None,
    }
}

fn hello_auth(args: &[Bytes]) -> bool {
    let mut index = 2;
    while let Some(option) = args.get(index) {
        if option.eq_ignore_ascii_case(b"AUTH") {
            return true;
        }
        if !option.eq_ignore_ascii_case(b"SETNAME") {
            return false;
        }
        index += 2;
    }
    false
}

fn hello_protocol(frame: &Frame) -> Option<bool> {
    let protocol = |key: &Frame, value: &Frame| {
        if string(key) != Some(b"proto") {
            return None;
        }
        match value {
            Frame::Integer(2) => Some(false),
            Frame::Integer(3) => Some(true),
            _ => None,
        }
    };
    match frame {
        Frame::Map(entries) => entries.iter().find_map(|(key, value)| protocol(key, value)),
        Frame::Array(values) => values
            .as_chunks::<2>()
            .0
            .iter()
            .find_map(|pair| protocol(&pair[0], &pair[1])),
        Frame::Attribute(_, value) => hello_protocol(value),
        _ => None,
    }
}

pub async fn run() -> i32 {
    let options = match Options::try_parse() {
        Ok(options) => options,
        Err(error) => {
            let code = if error.use_stderr() { 2 } else { 0 };
            let _ = error.print();
            return code;
        }
    };
    let mut config = match options.config() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("ffkv-cli: {error}");
            return 2;
        }
    };
    if options.askpass {
        let interrupted = tokio::signal::ctrl_c();
        tokio::pin!(interrupted);
        if let std::task::Poll::Ready(result) = futures_util::poll!(&mut interrupted) {
            return if result.is_ok() { 130 } else { 2 };
        }
        let (sender, password) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let _ = sender.send(rpassword::prompt_password("Password: "));
        });
        config.password = Some(tokio::select! {
            _ = &mut interrupted => return 130,
            result = password => match result {
                Ok(Ok(password)) => password,
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => return 130,
                _ => { eprintln!("ffkv-cli: could not read password from terminal"); return 2; }
            }
        });
    }
    let format = if options.json {
        Format::Json
    } else if options.raw {
        Format::Raw
    } else {
        Format::Human
    };
    let mut command = options.command_bytes();
    if options.stdin_arg {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let mut data = Vec::new();
            let result = io::stdin()
                .lock()
                .take((REQUEST_LIMIT + 1) as u64)
                .read_to_end(&mut data)
                .and_then(|_| {
                    if data.len() <= REQUEST_LIMIT {
                        Ok(Bytes::from(data))
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "stdin exceeds request limit",
                        ))
                    }
                });
            let _ = sender.send(result);
        });
        let result = tokio::select! {
            _ = tokio::signal::ctrl_c() => return 130,
            result = receiver => result,
        };
        match result {
            Ok(Ok(data)) => command.push(data),
            _ => {
                eprintln!("ffkv-cli: could not read stdin within the 8 MiB request limit");
                return 2;
            }
        }
    }
    if !command.is_empty()
        && let Err(error) = validate_command(&command)
    {
        eprintln!("ffkv-cli: {error}");
        return 2;
    }
    let mut resp3 = config.protocol == 3;
    let mut client = match tokio::select! {
        _ = tokio::signal::ctrl_c() => return 130,
        result = Client::connect(config) => result,
    } {
        Ok(client) => client,
        Err(error) => {
            connection_error("connect", &error);
            return 2;
        }
    };
    let direct = !command.is_empty();
    let tty = !direct && io::stdin().is_terminal();
    let (mut receiver, mut print, resume) = if direct {
        let (_sender, receiver) = mpsc::channel(1);
        (receiver, stdout_printer(), None)
    } else {
        match input(tty, format) {
            Ok(input) => input,
            Err(_) => {
                eprintln!("ffkv-cli: could not initialize terminal input");
                return 2;
            }
        }
    };
    let mut subscriptions = Subscriptions::default();
    let mut pending = None;
    let mut queued = if direct { Some(command) } else { None };
    let mut eof = direct;
    let mut failed = false;
    let mut reply_mode = ReplyMode {
        enabled: true,
        skip: false,
    };
    let mut debug_enabled = false;
    let mut debug_session = false;
    let mut debug_paused = false;
    loop {
        if pending.is_none() || debug_paused {
            if let Some(args) = queued.take() {
                if let Err(error) = validate_command(&args) {
                    eprintln!("ffkv-cli: {error}");
                    failed = true;
                    if direct {
                        return 1;
                    }
                    resume_editor(&resume);
                    continue;
                }
                // Debugger commands belong to the outstanding EVAL, not to the
                // ordinary reply queue or CLIENT REPLY suppression state.
                let debugger_command = debug_paused;
                let expects_reply = if debugger_command {
                    false
                } else {
                    reply_mode.expect_reply(&args)
                };
                if !debugger_command {
                    if debug_enabled
                        && (args[0].eq_ignore_ascii_case(b"eval")
                            || args[0].eq_ignore_ascii_case(b"evalsha"))
                    {
                        debug_session = true;
                    }
                    if expects_reply {
                        pending = Some(subscriptions.pending(&args));
                    }
                }
                match tokio::select! {
                    _ = tokio::signal::ctrl_c() => return 130,
                    result = client.write_command(&args) => result,
                } {
                    Ok(()) => {}
                    Err(error) => {
                        connection_error("write command (outcome unknown; not retried)", &error);
                        return 2;
                    }
                }
                if debugger_command {
                    if args[0].first().is_some_and(|name| {
                        matches!(name.to_ascii_lowercase(), b's' | b'n' | b'c' | b'a')
                    }) {
                        debug_paused = false;
                    } else {
                        resume_editor(&resume);
                    }
                    continue;
                }
                if !expects_reply {
                    resume_editor(&resume);
                    continue;
                }
            } else if eof && (subscriptions.count == 0 || tty) {
                return if failed { 1 } else { 0 };
            }
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return 130,
            frame = client.read_frame() => {
                let mut frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => { connection_error("read reply (outcome unknown; not retried)", &error); return 2; }
                };
                if !frame.is_push() && frame.has_error()
                    && matches!(pending, Some(Pending::Reply { sensitive: true, .. }))
                {
                    frame = redacted_error(&frame);
                }
                if let Err(error) = print(&render(&frame, format)) {
                    connection_error("write output", &error);
                    return 2;
                }
                if debug_session {
                    if let Some(items) = elements(&frame) {
                        if items.len() == 1 && string(&items[0]) == Some(b"<endsession>".as_slice()) {
                            debug_session = false;
                            debug_paused = false;
                        } else if items.first().and_then(string).is_some_and(|line| line.starts_with(b"* Stopped at ")) {
                            debug_paused = true;
                            resume_editor(&resume);
                        }
                        // Console, stop and end events never finish EVAL.
                        continue;
                    }
                    // Invalid debugger input is an immediate error, not EVAL's
                    // final response (which follows the end-session event).
                    if frame.has_error() && debug_paused {
                        resume_editor(&resume);
                        continue;
                    }
                } else if debug_paused {
                    debug_paused = false;
                }
                let was_message = !resp3 && subscriptions.is_message(&frame);
                // Only pushes or a pending subscription can be acknowledgements:
                // an ordinary command may legitimately return an ack-shaped array.
                let ack = if frame.is_push() || matches!(pending, Some(Pending::Subscription { .. })) {
                    subscriptions.acknowledge(&frame).map(<[u8]>::to_vec)
                } else { None };
                if let Some(ack) = ack {
                    if let Some(Pending::Subscription { kind, remaining }) = &mut pending
                        && *kind == ack
                    {
                        *remaining -= 1;
                        if *remaining == 0 {
                            pending = None;
                            resume_editor(&resume);
                        }
                    }
                    continue;
                }
                if frame.is_push() || was_message { continue; }
                failed |= frame.has_error();
                if !frame.has_error()
                    && let Some(Pending::Reply { hello, reauthenticate, reset, debug_mode, .. }) = &pending
                {
                    if let Some(enabled) = debug_mode { debug_enabled = *enabled; }
                    if *hello && let Some(protocol) = hello_protocol(&frame) {
                        resp3 = protocol;
                    }
                    if *reauthenticate {
                        subscriptions = Subscriptions::default();
                    }
                    if *reset {
                        subscriptions = Subscriptions::default();
                        resp3 = false;
                        debug_enabled = false;
                    }
                }
                let quit = matches!(pending, Some(Pending::Reply { quit: true, .. }));
                pending = None;
                debug_session = false;
                debug_paused = false;
                if quit { return if failed { 1 } else { 0 }; }
                resume_editor(&resume);
            }
            event = receiver.recv(), if !eof && queued.is_none() => {
                match event {
                    Some(Input::Line(line)) => match tokenize::tokenize(&line) {
                        Ok(args) if !args.is_empty() => queued = Some(args),
                        Ok(_) => resume_editor(&resume),
                        Err(error) => {
                            eprintln!("ffkv-cli: input: {error}");
                            failed = true;
                            resume_editor(&resume);
                        }
                    },
                    Some(Input::Interrupted) => return 130,
                    Some(Input::Error) => { eprintln!("ffkv-cli: could not read input within the 8 MiB line limit"); return 2; }
                    Some(Input::Eof) | None => {
                        eof = true;
                        // EOF ends an interactive session even if subscribed;
                        // a pipe ending after SUBSCRIBE keeps receiving messages.
                        if tty && pending.is_none() && queued.is_none() { return if failed { 1 } else { 0 }; }
                    }
                }
            }
        }
    }
}

fn redacted_error(frame: &Frame) -> Frame {
    // Only preserve protocol-defined codes, never an arbitrary first token
    // supplied by a peer (that token may itself contain the credential).
    let code = match frame {
        Frame::Error(bytes) => bytes
            .split(|byte| byte.is_ascii_whitespace())
            .next()
            .unwrap_or(b""),
        Frame::Attribute(_, value) => return redacted_error(value),
        _ => b"".as_slice(),
    };
    let code = match code {
        b"ERR" | b"WRONGPASS" | b"NOPERM" | b"NOAUTH" | b"LOADING" | b"BUSY" | b"OOM"
        | b"READONLY" | b"WRONGTYPE" | b"EXECABORT" | b"NOSCRIPT" | b"NOTBUSY" | b"UNKILLABLE"
        | b"BUSYKEY" | b"NOKEY" | b"IOERR" | b"MISCONF" | b"CROSSSLOT" | b"MOVED" | b"ASK"
        | b"TRYAGAIN" | b"CLUSTERDOWN" | b"MASTERDOWN" => code,
        _ => b"ERR".as_slice(),
    };
    let mut redacted = Vec::with_capacity(code.len() + b" credential command failed".len());
    redacted.extend_from_slice(code);
    redacted.extend_from_slice(b" credential command failed");
    Frame::Error(Bytes::from(redacted))
}

fn connection_error(operation: &str, error: &io::Error) {
    // Error payloads are not included: a peer may echo a credential in an error.
    eprintln!("ffkv-cli: {operation} failed ({:?})", error.kind());
}

fn stdout_printer() -> Printer {
    Box::new(|bytes| {
        let mut stdout = io::stdout().lock();
        stdout.write_all(bytes)?;
        stdout.flush()
    })
}

type EditorResume = Option<std::sync::mpsc::Sender<()>>;

fn resume_editor(resume: &EditorResume) {
    if let Some(resume) = resume {
        let _ = resume.send(());
    }
}

fn input(tty: bool, format: Format) -> io::Result<(mpsc::Receiver<Input>, Printer, EditorResume)> {
    let (sender, receiver) = mpsc::channel(1);
    if tty {
        let mut editor = DefaultEditor::new().map_err(io::Error::other)?;
        let mut external = editor.create_external_printer().map_err(io::Error::other)?;
        let (resume, resumed) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            loop {
                let event = match editor.readline("ffkv> ") {
                    Ok(line) => {
                        if line.len() > REQUEST_LIMIT {
                            Input::Error
                        } else {
                            if let Ok(args) = tokenize::tokenize(line.as_bytes())
                                && !args.is_empty()
                                && !tokenize::sensitive(&args)
                            {
                                let _ = editor.add_history_entry(line.as_str());
                            }
                            Input::Line(line.into_bytes())
                        }
                    }
                    Err(ReadlineError::Interrupted) => Input::Interrupted,
                    Err(ReadlineError::Eof) => Input::Eof,
                    Err(_) => Input::Error,
                };
                let end = !matches!(event, Input::Line(_));
                if sender.blocking_send(event).is_err() || end {
                    break;
                }
                // Do not enter raw mode again until this command is complete.
                // Replies then print synchronously, including the final QUIT/EOF
                // reply, while idle subscription pushes use ExternalPrinter.
                if resumed.recv().is_err() {
                    break;
                }
            }
        });
        let print = match format {
            Format::Raw => stdout_printer(),
            _ => Box::new(move |bytes: &[u8]| {
                // Human and JSON output are ASCII/UTF-8 by construction.
                let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
                external.print(text.to_owned()).map_err(io::Error::other)
            }) as Printer,
        };
        Ok((receiver, print, Some(resume)))
    } else {
        std::thread::spawn(move || {
            let stdin = io::stdin();
            let mut stdin = stdin.lock();
            loop {
                let event = match bounded_line(&mut stdin) {
                    Ok(Some(line)) => Input::Line(line),
                    Ok(None) => Input::Eof,
                    Err(_) => Input::Error,
                };
                let end = !matches!(event, Input::Line(_));
                if sender.blocking_send(event).is_err() || end {
                    break;
                }
            }
        });
        Ok((receiver, stdout_printer(), None))
    }
}

fn bounded_line(reader: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let chunk = reader.fill_buf()?;
        if chunk.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        let take = chunk
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(chunk.len(), |index| index + 1);
        if line.len() + take > REQUEST_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "line too large",
            ));
        }
        let done = chunk[take - 1] == b'\n';
        line.extend_from_slice(&chunk[..take]);
        reader.consume(take);
        if done {
            return Ok(Some(line));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_redaction_preserves_only_safe_protocol_codes() {
        for (input, code) in [
            ("WRONGPASS supplied-password", "WRONGPASS"),
            ("NOPERM supplied-password", "NOPERM"),
            ("NOAUTH supplied-password", "NOAUTH"),
            ("supplied-password credential failure", "ERR"),
            ("ERR supplied-password", "ERR"),
        ] {
            let Frame::Error(redacted) = redacted_error(&Frame::Error(Bytes::from(input))) else {
                panic!("redaction must remain an error");
            };
            assert_eq!(
                redacted.as_ref(),
                format!("{code} credential command failed").as_bytes()
            );
            assert!(
                !redacted
                    .windows(b"supplied-password".len())
                    .any(|part| part == b"supplied-password")
            );
        }
    }

    #[test]
    fn suppressed_reply_modes_reenable_and_reset_without_consuming_extra_reply() {
        let mut mode = ReplyMode {
            enabled: true,
            skip: false,
        };
        let mut expects = |args: &[&'static [u8]]| {
            mode.expect_reply(
                &args
                    .iter()
                    .map(|arg| Bytes::from_static(arg))
                    .collect::<Vec<_>>(),
            )
        };
        assert!(!expects(&[b"CLIENT", b"REPLY", b"OFF"]));
        assert!(!expects(&[b"PING"]));
        assert!(!expects(&[b"CLIENT", b"REPLY", b"SKIP"]));
        assert!(!expects(&[b"PING"]));
        assert!(expects(&[b"CLIENT", b"REPLY", b"ON"]));
        assert!(!expects(&[b"CLIENT", b"REPLY", b"SKIP"]));
        assert!(!expects(&[b"PING"]));
        assert!(expects(&[b"PING"]));
        assert!(!expects(&[b"CLIENT", b"REPLY", b"OFF"]));
        assert!(expects(&[b"RESET"]));
        assert!(expects(&[b"PING"]));
    }
}
