use std::{ffi::OsString, path::PathBuf, time::Duration};

use bytes::Bytes;
use clap::Parser;
use percent_encoding::percent_decode_str;

use crate::client::{Config, TlsConfig};

#[derive(Parser)]
#[command(
    name = "ffkv-cli",
    version,
    disable_help_flag = true,
    trailing_var_arg = true,
    about = "Native ffkv command client",
    after_help = "Options must precede COMMAND. With no command, read quoted commands from the terminal or stdin.\nFFKV_URL supplies an optional redis:// or rediss:// URI; FFKVCLI_AUTH supplies a password.\nInteractive history is memory-only; credential commands are never added. Ctrl-C closes the session\nwith status 130, including while editing, to avoid reusing an interrupted connection.\nExit status: 0 success, 1 command/input error, 2 configuration/connection/protocol error, 130 interrupted.\nJSON strings represent UTF-8 Simple/Bulk values; other bytes use {\"type\":\"bytes\",\"base64\":\"...\"}.\nMaps become objects for unique UTF-8 string keys, otherwise {\"type\":\"map\",\"entries\":[[key,value],...]}.\nErrors use {\"type\":\"error\",\"value\":string-or-bytes}; sets/pushes use {\"type\":\"set\"|\"push\",\"values\":[...]}.\nRaw mode emits byte-string contents unchanged and a newline after each scalar."
)]
pub struct Options {
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,
    #[arg(short = 'h', long)]
    host: Option<String>,
    #[arg(short = 'p', long)]
    port: Option<u16>,
    #[arg(short = 'u', long)]
    uri: Option<String>,
    #[arg(short = 'n', long = "db")]
    database: Option<u32>,
    #[arg(short = '2', conflicts_with = "resp3")]
    resp2: bool,
    #[arg(short = '3')]
    resp3: bool,
    #[arg(long = "user")]
    username: Option<String>,
    #[arg(short = 'a', long = "pass")]
    password: Option<String>,
    #[arg(long)]
    pub askpass: bool,
    #[arg(long)]
    tls: bool,
    #[arg(long)]
    cacert: Option<PathBuf>,
    #[arg(long)]
    cert: Option<PathBuf>,
    #[arg(long)]
    key: Option<PathBuf>,
    #[arg(long)]
    sni: Option<String>,
    #[arg(long, default_value = "5", value_name = "SECONDS")]
    connect_timeout: f64,
    #[arg(long, conflicts_with = "json")]
    pub raw: bool,
    #[arg(long)]
    pub json: bool,
    #[arg(short = 'x')]
    pub stdin_arg: bool,
    #[arg(value_name = "COMMAND", num_args = 0.., allow_hyphen_values = true)]
    pub command: Vec<OsString>,
}

impl Options {
    pub fn config(&self) -> Result<Config, &'static str> {
        if self.stdin_arg && self.command.is_empty() {
            return Err("-x requires a command");
        }
        if !self.connect_timeout.is_finite()
            || self.connect_timeout <= 0.0
            || self.connect_timeout > 86_400.0
        {
            return Err("--connect-timeout must be greater than zero and at most 86400 seconds");
        }
        if self.cert.is_some() != self.key.is_some() {
            return Err("--cert and --key must be supplied together");
        }
        let mut host = "127.0.0.1".to_owned();
        let mut port = 6380;
        let mut username = None;
        let mut password = None;
        let mut database = 0;
        let mut tls = self.tls;
        let env_uri = std::env::var("FFKV_URL").ok();
        if let Some(uri) = self.uri.as_ref().or(env_uri.as_ref()) {
            let url = reqwest::Url::parse(uri).map_err(|_| "invalid connection URI")?;
            if !matches!(url.scheme(), "redis" | "rediss") {
                return Err("connection URI must use redis:// or rediss://");
            }
            if url.query().is_some() || url.fragment().is_some() {
                return Err("connection URI cannot contain a query or fragment");
            }
            host = url
                .host_str()
                .ok_or("connection URI requires a host")?
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned();
            port = url.port().unwrap_or(6380);
            tls |= url.scheme() == "rediss";
            if !url.username().is_empty() {
                username = Some(decode(url.username())?);
            }
            password = url.password().map(decode).transpose()?;
            let path = url.path().strip_prefix('/').unwrap_or(url.path());
            if !path.is_empty() {
                if !path.bytes().all(|b| b.is_ascii_digit()) {
                    return Err("connection URI database must be a nonnegative integer");
                }
                database = path
                    .parse()
                    .map_err(|_| "connection URI database is too large")?;
            }
        }
        if let Some(value) = &self.host {
            host.clone_from(value);
        }
        if let Some(value) = self.port {
            port = value;
        }
        if let Some(value) = self.database {
            database = value;
        }
        if self.username.is_some() {
            username.clone_from(&self.username);
        }
        if let Some(value) = std::env::var_os("FFKVCLI_AUTH") {
            password = Some(
                value
                    .into_string()
                    .map_err(|_| "FFKVCLI_AUTH must be UTF-8")?,
            );
        }
        if let Some(value) = &self.password {
            eprintln!(
                "Warning: supplying a password with --pass/-a may expose it in process arguments."
            );
            password = Some(value.clone());
        }
        tls |= self.cacert.is_some() || self.cert.is_some() || self.sni.is_some();
        Ok(Config {
            host,
            port,
            username,
            password,
            database,
            protocol: if self.resp3 { 3 } else { 2 },
            connect_timeout: Duration::from_secs_f64(self.connect_timeout),
            tls: tls.then(|| TlsConfig {
                ca_cert: self.cacert.clone(),
                server_name: self.sni.clone(),
                client_cert: self.cert.clone(),
                client_key: self.key.clone(),
            }),
        })
    }

    pub fn command_bytes(&self) -> Vec<Bytes> {
        self.command
            .iter()
            .map(|arg| {
                #[cfg(unix)]
                {
                    use std::os::unix::ffi::OsStrExt;
                    Bytes::copy_from_slice(arg.as_os_str().as_bytes())
                }
                #[cfg(not(unix))]
                {
                    Bytes::from(arg.to_string_lossy().into_owned())
                }
            })
            .collect()
    }
}

fn decode(value: &str) -> Result<String, &'static str> {
    // percent_decode_str deliberately accepts malformed '%' literally; reject it
    // rather than silently authenticating with a different credential.
    let bytes = value.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            return Err("invalid percent encoding in connection URI credentials");
        }
    }
    percent_decode_str(value)
        .decode_utf8()
        .map(|s| s.into_owned())
        .map_err(|_| "connection URI credentials must decode to UTF-8")
}
