use std::{io, path::PathBuf, sync::Arc};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    protocol::{Resp, Version},
    watch::Token,
};

#[derive(Clone, Copy)]
pub enum Origin {
    Client(Version),
    Management,
}

impl Origin {
    pub fn reply_limit(self) -> usize {
        match self {
            Self::Client(_) => 64 * 1024 * 1024,
            Self::Management => 8 * 1024 * 1024,
        }
    }

    pub fn version(self) -> Version {
        match self {
            Self::Client(version) => version,
            Self::Management => Version::Resp2,
        }
    }
}

pub struct CommandRequest {
    pub args: Vec<Bytes>,
    pub reply: oneshot::Sender<Resp>,
    pub origin: Origin,
    pub user: Option<Arc<crate::auth::User>>,
    pub session: Option<Arc<crate::session::ClientSession>>,
    pub write_permit: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    pub cluster_client: Option<crate::cluster::ClusterClient>,
}

impl CommandRequest {
    /// Management has its own authenticated/authorized policy before this bus.
    pub fn internal(args: Vec<Bytes>, reply: oneshot::Sender<Resp>) -> Self {
        Self {
            args,
            reply,
            origin: Origin::Management,
            user: None,
            session: None,
            write_permit: None,
            cluster_client: None,
        }
    }
    pub fn client(
        args: Vec<Bytes>,
        reply: oneshot::Sender<Resp>,
        user: Option<Arc<crate::auth::User>>,
        version: Version,
        session: Option<Arc<crate::session::ClientSession>>,
    ) -> Self {
        Self {
            args,
            reply,
            origin: Origin::Client(version),
            user,
            session,
            write_permit: None,
            cluster_client: None,
        }
    }
}

pub enum Request {
    Command(CommandRequest),
    Watch {
        keys: Vec<Bytes>,
        reply: oneshot::Sender<io::Result<Vec<Token>>>,
        session: Option<Arc<crate::session::ClientSession>>,
    },
    Transaction {
        commands: Vec<Vec<Bytes>>,
        modules: Vec<(usize, crate::server_commands::extensions::Invocation)>,
        watched: Vec<Token>,
        user: Option<Arc<crate::auth::User>>,
        origin: Origin,
        session: Option<Arc<crate::session::ClientSession>>,
        write_permit: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
        cluster_client: Option<crate::cluster::ClusterClient>,
        reply: oneshot::Sender<Resp>,
    },
    InstallSnapshot {
        directory: PathBuf,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Module {
        invocation: crate::server_commands::extensions::Invocation,
        user: Option<Arc<crate::auth::User>>,
        origin: Origin,
        session: Option<Arc<crate::session::ClientSession>>,
        write_permit: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
        cluster_client: Option<crate::cluster::ClusterClient>,
        reply: oneshot::Sender<Resp>,
    },
    Native(crate::native_runtime::Control),
    Server(crate::server_runtime::Control),
    Unblock {
        client_id: u64,
        error: bool,
        reply: oneshot::Sender<Resp>,
    },
    Shutdown,
}
