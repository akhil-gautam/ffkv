use crate::{management, replication, wal::Durability};
use clap::{Parser, Subcommand};
use std::{io, net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum StorageProfile {
    Memory,
    Hybrid,
}

#[derive(Subcommand)]
pub enum LocalCommand {
    /// Restore a verified backup into a NEW database directory (never overwrite).
    Restore {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[derive(Parser)]
#[command(version, about = "ffkv — freaking fast KV", args_override_self = true)]
pub struct Options {
    #[command(subcommand)]
    pub command: Option<LocalCommand>,
    /// RESP listener. Non-loopback requires both TLS and an ACL file.
    #[arg(long, default_value = "127.0.0.1:6379")]
    pub bind: SocketAddr,
    #[arg(long, default_value="1024", value_parser=clap::value_parser!(u32).range(1..))]
    pub max_connections: u32,
    #[arg(long, value_name = "DIRECTORY")]
    pub data_dir: Option<PathBuf>,
    #[arg(long, value_enum, requires = "data_dir")]
    pub durability: Option<Durability>,
    #[arg(long, default_value="4", value_parser=clap::value_parser!(u16).range(1..=256))]
    pub shards: u16,
    #[arg(long, value_enum, default_value = "memory")]
    pub storage: StorageProfile,
    #[arg(long, default_value = "1024")]
    pub inline_threshold: usize,
    #[arg(long, default_value = "67108864")]
    pub cache_bytes: usize,
    #[arg(long, default_value = "67108864")]
    pub segment_bytes: u64,
    /// Derived segment budget, including rebuild/compaction scratch; 0 unlimited.
    #[arg(long, default_value = "0")]
    pub disk_budget: u64,
    /// Soft process-RSS write circuit breaker; 0 disables it. Sampled every 1s:
    /// concurrent writes can overshoot. Not a hard memory cap or eviction policy.
    #[arg(long, default_value = "0")]
    pub max_rss_bytes: u64,
    /// Rewrite after this many WAL bytes beyond the last checkpoint; 0 disables automatic checkpoints.
    #[arg(long, default_value = "268435456")]
    pub wal_checkpoint_bytes: u64,
    #[arg(long)]
    pub acl_file: Option<PathBuf>,
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,
    #[arg(long)]
    pub tls_key: Option<PathBuf>,
    /// Separate management listener, disabled unless supplied.
    #[arg(long)]
    pub management_bind: Option<SocketAddr>,
    #[arg(long)]
    pub management_token_file: Option<PathBuf>,
    #[arg(long)]
    pub management_audit_file: Option<PathBuf>,
    #[arg(long)]
    pub management_tls_cert: Option<PathBuf>,
    #[arg(long)]
    pub management_tls_key: Option<PathBuf>,
    #[arg(long)]
    pub console_dir: Option<PathBuf>,
    #[arg(long)]
    pub management_origin: Option<String>,
    #[arg(long)]
    pub management_allow_sensitive_reveal: bool,
    #[arg(long)]
    pub management_oidc_issuer: Option<String>,
    #[arg(long)]
    pub management_oidc_audience: Option<String>,
    #[arg(long)]
    pub management_oidc_jwks_url: Option<String>,
    #[arg(long, default_value = "ffkv-admin")]
    pub management_oidc_admin_role: String,
    #[arg(long, default_value = "ffkv-viewer")]
    pub management_oidc_viewer_role: String,
    #[arg(long)]
    pub management_oidc_allow_loopback_http: bool,
    /// Opt in to admin-only raw full-snapshot export, not Redis replication/HA.
    #[arg(long)]
    pub replication_export: bool,
    /// Follow this management base URL using full-snapshot polling; dataset writes are disabled.
    #[arg(long)]
    pub replica_source: Option<String>,
    #[arg(long)]
    pub replica_token_file: Option<PathBuf>,
    #[arg(long)]
    pub replica_ca_file: Option<PathBuf>,
    #[arg(long, default_value="5000",value_parser=clap::value_parser!(u64).range(1..=86400000))]
    pub replica_interval_ms: u64,
    #[arg(long, default_value="120000",value_parser=clap::value_parser!(u64).range(1..=3600000))]
    pub replica_timeout_ms: u64,
    #[arg(long,default_value="1073741824",value_parser=clap::value_parser!(u64).range(1..=8589934591))]
    pub replica_max_snapshot_bytes: u64,
    #[arg(long)]
    pub replica_allow_loopback_http: bool,
    #[arg(long, default_value = "16", value_parser = clap::value_parser!(u32).range(1..=65536))]
    pub databases: u32,
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub appendonly: bool,
    #[arg(long)]
    pub cluster_enabled: bool,
    #[arg(long)]
    pub cluster_config_file: Option<PathBuf>,
    #[arg(long, default_value = "0")]
    pub cluster_bus_port: u16,
    #[arg(long)]
    pub cluster_announce_bus_port: Option<u16>,
    #[arg(long)]
    pub cluster_slot_stats_cpu: bool,
    #[arg(long)]
    pub cluster_slot_stats_memory: bool,
    #[arg(long)]
    pub cluster_slot_stats_network: bool,
    #[arg(long)]
    pub cluster_bus_secret: Option<String>,
    #[arg(long, default_value = "15000")]
    pub cluster_node_timeout_ms: u64,
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub cluster_require_full_coverage: bool,
    #[arg(long, default_value = "false", action = clap::ArgAction::Set)]
    pub cluster_allow_reads_when_down: bool,
    #[arg(long, default_value = "67108864")]
    pub replication_backlog_bytes: usize,
    #[arg(long)]
    pub native_replication_allow_loopback: bool,
    #[arg(long)]
    pub native_replica_user: Option<String>,
    #[arg(long)]
    pub native_replica_password_file: Option<PathBuf>,
    #[arg(long)]
    pub native_replica_ca_file: Option<PathBuf>,
    #[arg(long)]
    pub native_replica_tls_name: Option<String>,
    /// An explicit owned-fixture fsync gate; a present file holds durable replica ACKs.
    #[arg(long)]
    pub replication_fsync_gate: Option<PathBuf>,
    #[arg(long)]
    pub server_config_file: Option<PathBuf>,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

impl Options {
    pub fn validate(&self) -> io::Result<()> {
        if !self.cluster_enabled
            && (self.cluster_config_file.is_some()
                || self.cluster_bus_port != 0
                || self.cluster_announce_bus_port.is_some()
                || self.cluster_slot_stats_cpu
                || self.cluster_slot_stats_memory
                || self.cluster_slot_stats_network
                || self.cluster_bus_secret.is_some()
                || self.cluster_allow_reads_when_down
                || !self.cluster_require_full_coverage
                || self.cluster_node_timeout_ms != 15_000)
        {
            return Err(invalid("cluster settings require --cluster-enabled"));
        }
        if self.cluster_enabled && self.data_dir.is_none() {
            return Err(invalid(
                "--cluster-enabled requires --data-dir for durable topology",
            ));
        }
        if self.cluster_enabled && self.replica_source.is_some() {
            return Err(invalid(
                "native cluster replication cannot use development snapshot polling",
            ));
        }
        if self.cluster_node_timeout_ms == 0 || self.cluster_node_timeout_ms > i64::MAX as u64 / 16
        {
            return Err(invalid("cluster node timeout is out of range"));
        }
        if self.cluster_enabled
            && (self.cluster_announce_bus_port == Some(0)
                || (self.bind.port() != 0
                    && self.cluster_bus_port == 0
                    && self.bind.port().checked_add(10_000).is_none()))
        {
            return Err(invalid("invalid cluster bus port"));
        }
        if self.replication_backlog_bytes < 1024 {
            return Err(invalid("replication backlog must be at least 1024 bytes"));
        }
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err(invalid(
                "--tls-cert and --tls-key must be supplied together",
            ));
        }
        if self.management_tls_cert.is_some() != self.management_tls_key.is_some() {
            return Err(invalid(
                "management TLS certificate/key must be supplied together",
            ));
        }
        if !self.bind.ip().is_loopback() && (self.acl_file.is_none() || self.tls_cert.is_none()) {
            return Err(invalid("non-loopback RESP requires --acl-file and TLS"));
        }
        if self.storage == StorageProfile::Hybrid && self.data_dir.is_none() {
            return Err(invalid("--storage hybrid requires --data-dir"));
        }
        if self.management_bind.is_none()
            && (self.management_token_file.is_some()
                || self.management_audit_file.is_some()
                || self.management_tls_cert.is_some()
                || self.console_dir.is_some()
                || self.management_origin.is_some()
                || self.management_allow_sensitive_reveal
                || self.management_oidc_issuer.is_some()
                || self.management_oidc_audience.is_some()
                || self.management_oidc_jwks_url.is_some()
                || self.management_oidc_allow_loopback_http
                || self.replication_export)
        {
            return Err(invalid("management settings require --management-bind"));
        }
        if let Some(bind) = self.management_bind {
            if self.management_token_file.is_none() {
                return Err(invalid("management requires --management-token-file"));
            }
            if !bind.ip().is_loopback() && self.management_tls_cert.is_none() {
                return Err(invalid("non-loopback management requires TLS"));
            }
        }
        let oidc = [
            self.management_oidc_issuer.is_some(),
            self.management_oidc_audience.is_some(),
            self.management_oidc_jwks_url.is_some(),
        ];
        if oidc.iter().any(|value| *value) && !oidc.iter().all(|value| *value) {
            return Err(invalid(
                "OIDC requires issuer, audience and JWKS URL together",
            ));
        }
        if self.replication_export && (self.data_dir.is_none() || self.replica_source.is_some()) {
            return Err(invalid("replication export requires a durable master"));
        }
        if self.replica_source.is_some()
            && (self.data_dir.is_none() || self.replica_token_file.is_none())
        {
            return Err(invalid(
                "replica requires --data-dir and --replica-token-file",
            ));
        }
        if self.replica_source.is_none()
            && (self.replica_token_file.is_some()
                || self.replica_ca_file.is_some()
                || self.replica_allow_loopback_http)
        {
            return Err(invalid("replica options require --replica-source"));
        }
        Ok(())
    }

    pub fn management_config(&self) -> io::Result<Option<(SocketAddr, management::Config)>> {
        let Some(bind) = self.management_bind else {
            return Ok(None);
        };
        let token_file = self
            .management_token_file
            .as_ref()
            .ok_or_else(|| invalid("management token policy is required"))?
            .clone();
        let audit_file = self.management_audit_file.clone().unwrap_or_else(|| {
            self.data_dir
                .as_ref()
                .map(|directory| directory.join("management-audit.jsonl"))
                .unwrap_or_else(|| {
                    token_file
                        .parent()
                        .unwrap_or_else(|| std::path::Path::new("."))
                        .join("ffkv-audit.jsonl")
                })
        });
        let tls = self
            .management_tls_cert
            .as_ref()
            .zip(self.management_tls_key.as_ref())
            .map(|(cert, key)| management::TlsConfig {
                cert_file: cert.clone(),
                key_file: key.clone(),
            });
        let oidc = self
            .management_oidc_issuer
            .as_ref()
            .map(|issuer| management::OidcConfig {
                issuer: issuer.clone(),
                audience: self
                    .management_oidc_audience
                    .clone()
                    .expect("validated OIDC audience"),
                jwks_url: self
                    .management_oidc_jwks_url
                    .clone()
                    .expect("validated OIDC JWKS URL"),
                admin_role: self.management_oidc_admin_role.clone(),
                viewer_role: self.management_oidc_viewer_role.clone(),
                allow_loopback_http: self.management_oidc_allow_loopback_http,
            });
        Ok(Some((
            bind,
            management::Config {
                token_file,
                audit_file,
                ui_dir: self.console_dir.clone(),
                oidc,
                tls,
                allowed_origin: self.management_origin.clone(),
                allow_sensitive_reveal: self.management_allow_sensitive_reveal,
                replication_enabled: self.replication_export,
            },
        )))
    }

    pub fn replica_config(&self) -> io::Result<Option<replication::Config>> {
        let Some(source_url) = &self.replica_source else {
            return Ok(None);
        };
        Ok(Some(replication::Config {
            source_url: source_url.clone(),
            token_file: self
                .replica_token_file
                .clone()
                .ok_or_else(|| invalid("replica token file is required"))?,
            ca_file: self.replica_ca_file.clone(),
            interval: Duration::from_millis(self.replica_interval_ms),
            timeout: Duration::from_millis(self.replica_timeout_ms),
            max_snapshot_bytes: self.replica_max_snapshot_bytes,
            directory: self
                .data_dir
                .as_ref()
                .ok_or_else(|| invalid("replica data directory is required"))?
                .join("replication"),
            allow_loopback_http: self.replica_allow_loopback_http,
        }))
    }
}
