use clap::Parser;
use serde::Deserialize;
use std::{collections::BTreeMap, io::Read, path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(
    name = "ffkv-mcp",
    version,
    about = "Native ffkv MCP server over stdio"
)]
pub struct Options {
    /// Operator-owned JSON configuration. Secrets may reference files or environment variables.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override the default target's host.
    #[arg(long)]
    host: Option<String>,
    /// Override the default target's RESP port.
    #[arg(long)]
    port: Option<u16>,
    /// Enable privileged ffkv-only child process administration.
    #[arg(long)]
    allow_process: bool,
    /// Fixed ffkv executable for enabled process operations (defaults to sibling ffkv).
    #[arg(long)]
    ffkv_binary: Option<PathBuf>,
    /// Existing directory under which explicit management downloads may create new files.
    #[arg(long)]
    file_root: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub targets: BTreeMap<String, Target>,
    pub default_target: String,
    pub limits: Limits,
    pub process: ProcessConfig,
    pub file_root: Option<PathBuf>,
    /// Absolute spelling of the configured root, retained alongside its canonical identity.
    #[serde(skip)]
    pub file_root_alias: Option<PathBuf>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            targets: BTreeMap::from([("local".into(), Target::default())]),
            default_target: "local".into(),
            limits: Limits::default(),
            process: ProcessConfig::default(),
            file_root: None,
            file_root_alias: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password_env: Option<String>,
    pub password_file: Option<PathBuf>,
    pub database: u32,
    pub protocol: u8,
    pub tls: Option<TargetTls>,
    pub management_url: Option<String>,
    pub management_token_env: Option<String>,
    pub management_token_file: Option<PathBuf>,
    pub management_ca_cert: Option<PathBuf>,
}
impl Default for Target {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 6379,
            username: None,
            password_env: None,
            password_file: None,
            database: 0,
            protocol: 3,
            tls: None,
            management_url: None,
            management_token_env: None,
            management_token_file: None,
            management_ca_cert: None,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TargetTls {
    pub ca_cert: Option<PathBuf>,
    pub server_name: Option<String>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_sessions: usize,
    pub max_batch_commands: usize,
    pub max_batch_bytes: usize,
    pub max_response_bytes: usize,
    pub operation_timeout_ms: u64,
    pub max_timeout_ms: u64,
    pub session_idle_ms: u64,
    pub max_results_bytes: usize,
    pub inline_result_bytes: usize,
    pub result_ttl_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_sessions: 64,
            max_batch_commands: 1024,
            max_batch_bytes: 16 * 1024 * 1024,
            max_response_bytes: 64 * 1024 * 1024,
            operation_timeout_ms: 10_000,
            max_timeout_ms: 300_000,
            session_idle_ms: 900_000,
            max_results_bytes: 128 * 1024 * 1024,
            inline_result_bytes: 64 * 1024,
            result_ttl_ms: 300_000,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProcessConfig {
    pub enabled: bool,
    pub binary: Option<PathBuf>,
    pub max_children: usize,
    pub max_log_bytes: usize,
}
impl Default for ProcessConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            binary: None,
            max_children: 8,
            max_log_bytes: 64 * 1024,
        }
    }
}

impl Config {
    pub fn load(options: Options) -> Result<Self, String> {
        let mut config: Self = if let Some(path) = options.config {
            let file = std::fs::File::open(path).map_err(|_| "cannot open MCP configuration")?;
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| "cannot read MCP configuration")?;
            if bytes.len() > 1024 * 1024 {
                return Err("MCP configuration exceeds 1 MiB".into());
            }
            serde_json::from_slice(&bytes).map_err(|e| format!("invalid MCP configuration: {e}"))?
        } else {
            Self::default()
        };
        let target = config
            .targets
            .get_mut(&config.default_target)
            .ok_or("default_target must name a configured target")?;
        if let Some(host) = options.host {
            target.host = host;
        }
        if let Some(port) = options.port {
            target.port = port;
        }
        if target.password_env.is_none()
            && target.password_file.is_none()
            && std::env::var_os("FFKV_MCP_AUTH").is_some()
        {
            target.password_env = Some("FFKV_MCP_AUTH".into());
        }
        config.process.enabled |= options.allow_process;
        if options.ffkv_binary.is_some() {
            config.process.binary = options.ffkv_binary;
        }
        if options.file_root.is_some() {
            config.file_root = options.file_root;
        }
        if let Some(root) = config.file_root.as_mut() {
            config.file_root_alias = Some(
                std::path::absolute(&*root).map_err(|_| "cannot resolve configured file_root")?,
            );
            *root = root
                .canonicalize()
                .map_err(|_| "file_root must be an existing directory")?;
            if !root.is_dir() {
                return Err("file_root must be an existing directory".into());
            }
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.targets.is_empty() || self.targets.len() > 64 {
            return Err("configure between 1 and 64 targets".into());
        }
        for (name, target) in &self.targets {
            if name.is_empty() || name.len() > 128 || target.host.is_empty() || target.port == 0 {
                return Err("invalid target name, host or port".into());
            }
            if !matches!(target.protocol, 2 | 3) {
                return Err("target protocol must be 2 or 3".into());
            }
            if target.password_env.is_some() && target.password_file.is_some() {
                return Err("choose password_env or password_file, not both".into());
            }
            if target.management_token_env.is_some() && target.management_token_file.is_some() {
                return Err(
                    "choose management_token_env or management_token_file, not both".into(),
                );
            }
            if let Some(tls) = &target.tls
                && tls.client_cert.is_some() != tls.client_key.is_some()
            {
                return Err("TLS client_cert and client_key must be provided together".into());
            }
            if let Some(url) = &target.management_url {
                let url = reqwest::Url::parse(url).map_err(|_| "invalid management_url")?;
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || url.path() != "/"
                {
                    return Err("management_url must be an HTTP(S) origin without credentials, query or path".into());
                }
            }
        }
        let l = &self.limits;
        if !(1..=1024).contains(&l.max_sessions)
            || !(1..=1024).contains(&l.max_batch_commands)
            || !(1..=16 * 1024 * 1024).contains(&l.max_batch_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&l.max_response_bytes)
            || l.operation_timeout_ms == 0
            || l.operation_timeout_ms > l.max_timeout_ms
            || l.max_timeout_ms > 3_600_000
            || l.session_idle_ms == 0
            || l.result_ttl_ms == 0
            || !(1024..=1024 * 1024).contains(&l.inline_result_bytes)
            || l.max_results_bytes < l.inline_result_bytes
            || l.max_results_bytes > 1024 * 1024 * 1024
        {
            return Err("invalid MCP limits (see ffkv MCP documentation)".into());
        }
        if !(1..=64).contains(&self.process.max_children)
            || !(1024..=1024 * 1024).contains(&self.process.max_log_bytes)
        {
            return Err("invalid process limits".into());
        }
        Ok(())
    }

    pub fn target(&self, name: Option<&str>) -> Result<(&str, &Target), String> {
        self.targets
            .get_key_value(name.unwrap_or(&self.default_target))
            .map(|(name, target)| (name.as_str(), target))
            .ok_or_else(|| "unknown target; only operator-configured targets are available".into())
    }
}

impl Target {
    pub fn client_config(&self) -> Result<crate::client::Config, String> {
        Ok(crate::client::Config {
            host: self.host.clone(),
            port: self.port,
            username: self.username.clone(),
            password: secret(&self.password_env, &self.password_file)?,
            database: self.database,
            protocol: self.protocol,
            tls: self.tls.as_ref().map(|t| crate::client::TlsConfig {
                ca_cert: t.ca_cert.clone(),
                server_name: t.server_name.clone(),
                client_cert: t.client_cert.clone(),
                client_key: t.client_key.clone(),
            }),
            connect_timeout: Duration::from_secs(5),
        })
    }
    pub fn management_token(&self) -> Result<Option<String>, String> {
        secret(&self.management_token_env, &self.management_token_file)
    }
}

fn secret(env: &Option<String>, file: &Option<PathBuf>) -> Result<Option<String>, String> {
    let value = if let Some(name) = env {
        Some(
            std::env::var(name)
                .map_err(|_| "configured credential environment variable is unavailable")?,
        )
    } else if let Some(path) = file {
        let input =
            std::fs::File::open(path).map_err(|_| "cannot open configured credential file")?;
        let mut value = String::new();
        input
            .take(65_537)
            .read_to_string(&mut value)
            .map_err(|_| "cannot read configured UTF-8 credential file")?;
        Some(value.trim_end_matches(['\r', '\n']).to_owned())
    } else {
        None
    };
    if value
        .as_ref()
        .is_some_and(|v| v.is_empty() || v.len() > 65_536 || v.contains(['\r', '\n', '\0']))
    {
        return Err(
            "configured credential is empty, oversized or contains a line break/NUL".into(),
        );
    }
    Ok(value)
}
