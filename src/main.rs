mod admin;
mod auth;
mod backup;
mod blocking;
mod client_registry;
mod cluster;
mod connection;
mod engine;
mod executor;
mod frontend;
mod journal;
mod management;
mod metrics;
mod migrate;
mod native_replication;
mod native_runtime;
mod options;
mod output;
mod persistence;
mod pressure;
mod protocol;
mod pubsub;
mod queue;
mod rdb;
mod replication;
mod request;
mod scripting;
mod server_commands;
mod server_runtime;
mod session;
mod sharding;
mod slot;
mod state;
mod storage;
mod transport;
mod wal;
mod watch;
mod write_gate;

use std::{
    future::Future,
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use clap::Parser;
use connection::{Action, Connection};
use engine::Engine;
use options::{LocalCommand, Options, StorageProfile};
use output::{CommandRecord, Output};
use protocol::{Resp, Version};
use request::Request;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use transport::Stream;
use wal::{Durability, Wal};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut options = Options::parse();
    if let Some(LocalCommand::Restore { source, data_dir }) = options.command.take() {
        let restored = tokio::task::spawn_blocking(move || backup::restore(&source, &data_dir))
            .await
            .map_err(io::Error::other)??;
        let checksum = sha2::digest::Output::<sha2::Sha256>::from(restored.sha256);
        println!(
            "{}",
            serde_json::json!({"wal_bytes":restored.wal_bytes,"data_frames":restored.data_frames,"sha256":format!("{checksum:x}")})
        );
        return Ok(());
    }
    if options.server_config_file.is_none() {
        options.server_config_file = options
            .data_dir
            .as_ref()
            .map(|root| root.join("server-config.json"));
    }
    let startup = server_runtime::load_startup(&mut options)?;
    options.validate()?;
    let management_config = options.management_config()?;
    let replica_config = options.replica_config()?;
    let is_replica = replica_config.is_some();
    let acl = Some(Arc::new(match options.acl_file.as_ref() {
        Some(path) => auth::Acl::load(path)?,
        None => auth::Acl::new(),
    }));
    if let Some(acl) = acl.as_ref() {
        acl.validate_commands(&Engine::command_names(None))?;
    }
    let tls = options
        .tls_cert
        .as_ref()
        .zip(options.tls_key.as_ref())
        .map(|(cert, key)| transport::server_config(cert, key))
        .transpose()?;
    let mut storage_config = if options.storage == StorageProfile::Hybrid {
        let directory = options.data_dir.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "--storage hybrid requires --data-dir",
            )
        })?;
        Some(storage::Config {
            directory: directory.join("values"),
            inline_threshold: options.inline_threshold,
            cache_bytes: options.cache_bytes,
            segment_bytes: options.segment_bytes,
            disk_budget: options.disk_budget,
        })
    } else {
        None
    };
    let (engine, wal, mode) = match options.data_dir.clone() {
        Some(directory) => {
            let policy = options.durability.unwrap_or(Durability::Strict);
            let description = format!("WAL {policy:?}; data-dir={}", directory.display());
            let (engine, wal, recovery) = tokio::task::spawn_blocking(move || {
                let mut engine = Engine::new();
                // Wal::open holds the database LOCK before invoking recovery.
                let (wal, recovery) = Wal::open(&directory, policy, |batch| {
                    if let Some(config) = storage_config.take() {
                        engine.set_storage(storage::Store::open(config)?);
                    }
                    engine.replay(batch)
                })?;
                if let Some(config) = storage_config.take() {
                    engine.set_storage(storage::Store::open(config)?);
                }
                engine.finish_recovery()?;
                if let Some(store) = engine.storage() {
                    store.finish_recovery()?;
                }
                engine.set_recording(true);
                Ok::<_, io::Error>((engine, wal, recovery))
            })
            .await
            .map_err(io::Error::other)??;
            eprintln!(
                "WAL recovery: {} records; {} incomplete tail bytes discarded",
                recovery.records, recovery.truncated_bytes
            );
            (engine, Some(wal), description)
        }
        None => (
            Engine::new(),
            None,
            "in-memory; data is lost on exit".into(),
        ),
    };
    let durable = wal.is_some();
    let listener = TcpListener::bind(options.bind).await?;
    options.bind = listener.local_addr()?;
    eprintln!(
        "ffkv {} listening on {} ({mode})",
        env!("CARGO_PKG_VERSION"),
        listener.local_addr()?
    );

    let (tx, rx) = mpsc::channel::<Request>(1024);
    let storage = engine.storage().cloned();
    let state = Arc::new(state::State::new(
        listener.local_addr()?.port(),
        options.shards,
        durable,
    ));
    if is_replica {
        state.set_replica(true);
    }
    state
        .control_tx
        .set(tx.clone())
        .map_err(|_| io::Error::other("coordinator already initialized"))?;
    state.metrics.set_max_rss_bytes_soft(options.max_rss_bytes);
    let backup_directory = startup.backup_directory.clone().unwrap_or_else(|| {
        options
            .data_dir
            .as_ref()
            .map(|root| root.join("online-backup"))
            .unwrap_or_else(|| {
                std::env::temp_dir().join(format!(
                    "ffkv-backup-{}-{:032x}",
                    std::process::id(),
                    rand::random::<u128>()
                ))
            })
    });
    let backup =
        server_runtime::OnlineBackup::new(backup_directory.clone(), startup.backup_sealed_ttl);
    let admin =
        server_runtime::RuntimeState::new(backup, options.databases, options.max_connections);
    admin.load_metadata(options.data_dir.as_deref())?;
    let appendonly = options.appendonly && durable;
    let server_config = server_runtime::config_snapshot(
        &options,
        appendonly,
        &backup_directory,
        startup.backup_sealed_ttl,
        &startup,
    );
    let hooks = server_runtime::Hooks::new(Arc::downgrade(&state), admin.clone());
    state
        .server
        .set(Arc::new(server_commands::ServerRuntime::new(
            server_config,
            hooks,
        )))
        .map_err(|_| io::Error::other("server runtime already initialized"))?;
    state
        .admin_runtime
        .set(admin.clone())
        .map_err(|_| io::Error::other("administration runtime already initialized"))?;
    admin.publish(&state);
    let backend_config = native_runtime::Config::from_options(&options, appendonly)?;
    let backend = native_runtime::Runtime::new(&state, backend_config.clone());
    let native =
        native_replication::NativeReplication::new(backend.native_config(), backend.clone())?;
    state
        .native
        .set(native.clone())
        .map_err(|_| io::Error::other("native runtime already initialized"))?;
    let actor_config = blocking::Config {
        checkpoint_threshold: options.wal_checkpoint_bytes,
        aof_enabled: appendonly,
        durability: options.durability.unwrap_or(Durability::Strict),
        fsync_gate: backend_config.fsync_gate.clone(),
        databases: admin.databases.clone(),
    };
    let runtime = tokio::runtime::Handle::current();
    // Clients run on frontend runtimes, one single-worker runtime per worker
    // of this one, which keeps the coordinator and services. Built before the
    // coordinator starts, so a failure leaves nothing else to stop.
    let mut frontends = frontend::Frontends::new(runtime.metrics().num_workers()).await?;
    let actor_state = state.clone();
    let shard_count = usize::from(options.shards);
    let mut actor = tokio::task::spawn_blocking(move || {
        runtime.block_on(blocking::run(
            rx,
            engine,
            wal,
            shard_count,
            actor_state,
            actor_config,
        ))
    });
    tokio::select! {
        _ = state.wait_actor_ready() => {},
        result = &mut actor => {
            let _ = frontends.shutdown().await;
            return result.map_err(io::Error::other)?;
        }
    }
    let cluster_start = async {
        if let Some(config) = backend.cluster_config() {
            let cluster = cluster::ClusterRuntime::open(config, backend.clone()).await?;
            state
                .cluster
                .set(cluster.clone())
                .map_err(|_| io::Error::other("cluster runtime already initialized"))?;
            cluster.start().await?;
        }
        if !is_replica && !native.is_replica() {
            native_replication::Backend::replication_state(
                backend.as_ref(),
                native.generation(),
                false,
                true,
            )
            .await?;
        }
        Ok::<_, io::Error>(())
    }
    .await;
    if let Err(error) = cluster_start {
        if let Some(cluster) = state.cluster.get() {
            let _ = cluster.shutdown().await;
        }
        let _ = tx.send(Request::Shutdown).await;
        let _ = actor.await;
        let _ = frontends.shutdown().await;
        return Err(error);
    }
    let native_task = native.start();
    let fsync_task = backend.start_fsync();
    let mut server_shutdown = admin.shutdown_notifications();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let mut services = JoinSet::new();
    if let Some((bind, config)) = management_config {
        let management_listener = TcpListener::bind(bind).await?;
        eprintln!(
            "ffkv management listening on {}",
            management_listener.local_addr()?
        );
        services.spawn(management::serve(
            management_listener,
            config,
            tx.clone(),
            state.metrics.clone(),
            storage,
            stopped.clone(),
        ));
    }
    if let Some(config) = replica_config {
        services.spawn(replication::run(
            config,
            tx.clone(),
            state.metrics.clone(),
            stopped.clone(),
        ));
    }
    let mut clients = JoinSet::new();
    let mut actor_result = None;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let listener_result = loop {
        tokio::select! {
            result = server_shutdown.changed() => {
                if result.is_err() || *server_shutdown.borrow() { break Ok(()); }
            }
            signal = &mut shutdown => break signal,
            result = &mut actor => {
                actor_result = Some(result.map_err(io::Error::other).and_then(|result| result));
                break Ok(());
            }
            result = clients.join_next(), if !clients.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("client task failed: {error}");
                }
            }
            result = services.join_next(), if !services.is_empty() => {
                break match result {
                    Some(Ok(Ok(()))) => Err(io::Error::other("background service stopped unexpectedly")),
                    Some(Ok(Err(error))) => Err(error),
                    Some(Err(error)) => Err(io::Error::other(error)),
                    None => Err(io::Error::other("background services ended unexpectedly")),
                };
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                if clients.len() >= admin.max_connections.load(std::sync::atomic::Ordering::Acquire) as usize {
                    drop(stream);
                    continue;
                }
                // Leaves this runtime's reactor here; the frontend's takes it
                // in `serve_accepted`.
                let stream = match stream.into_std() {
                    Ok(stream) => stream,
                    Err(error) => {
                        eprintln!("client {peer}: {error}");
                        continue;
                    }
                };
                clients.spawn_on(
                    serve_accepted(
                        stream,
                        peer,
                        tls.clone(),
                        tx.clone(),
                        state.clone(),
                        acl.clone(),
                    ),
                    frontends.next_handle(),
                );
            }
        }
    };
    let _ = stop.send(true);
    // Aborting a client leaves its frontend running: a supervised commit or
    // administration job it started settles there during the drain below.
    clients.abort_all();
    while clients.join_next().await.is_some() {}
    native.shutdown();
    state.write_gate.resume();
    let cluster_result = match state.cluster.get() {
        Some(cluster) => cluster.shutdown().await,
        None => Ok(()),
    };
    fsync_task.abort();
    let _ = fsync_task.await;
    let _ = native_task.await;
    let admin_result = admin.shutdown().await;
    if let Some(server) = state.server.get() {
        server.extensions().shutdown().await;
    }
    let _ = tx.send(Request::Shutdown).await;
    drop(tx);
    let actor_result = match actor_result {
        Some(result) => result,
        None => actor
            .await
            .map_err(io::Error::other)
            .and_then(|result| result),
    };
    services.abort_all();
    while services.join_next().await.is_some() {}
    // Nothing a client started is left: the coordinator drained its commits
    // and administration settled its jobs.
    let frontend_result = frontends.shutdown().await;
    actor_result?;
    cluster_result?;
    admin_result?;
    listener_result?;
    frontend_result?;
    if durable {
        eprintln!("ffkv stopped; WAL synchronized");
    } else {
        eprintln!("ffkv stopped; in-memory data discarded");
    }
    Ok(())
}

async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

/// One accepted client, served on whichever runtime polls this future. The
/// accept loop hands over its socket already off the accepting reactor; it
/// registers with this runtime's here, before TLS and serving, so the
/// connection's IO, timers and inline commands all run on this runtime.
async fn serve_accepted(
    stream: std::net::TcpStream,
    peer: std::net::SocketAddr,
    tls: Option<tokio_rustls::TlsAcceptor>,
    tx: mpsc::Sender<Request>,
    state: Arc<state::State>,
    acl: Option<Arc<auth::Acl>>,
) {
    let result = async {
        let stream = tokio::net::TcpStream::from_std(stream)?;
        let stream = Stream::accept(stream, tls).await?;
        serve(stream, tx, state, acl).await
    }
    .await;
    if let Err(error) = result
        && !matches!(
            error.kind(),
            io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::UnexpectedEof
        )
    {
        eprintln!("client {peer}: {error}");
    }
}

async fn serve(
    mut stream: Stream,
    tx: mpsc::Sender<Request>,
    state: Arc<state::State>,
    acl: Option<Arc<auth::Acl>>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let peer = stream.peer_addr()?;
    let address = peer.to_string();
    let lease = state.register(peer, stream.local_addr()?);
    let mut connection = Connection::new(lease, state.clone(), acl);
    let auth_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut input = BytesMut::with_capacity(8192);
    let mut out = Output::new(&state, &address);
    let mut cancelled = connection.subscription.cancellation();
    let mut monitor = None;
    // A request was parsed since the last read, so a partial one after it may
    // take one read without waiting.
    let mut parsed = false;
    // Full ordinary reads grow the budget; genuinely short reads reset it.
    // The previous read size also prevents a shared buffer's small tail from
    // forcing another syscall without reserving a fresh 8 KiB for every SET.
    let mut read_limit = FIRST_READ_BYTES;
    let mut read_hint = FIRST_READ_BYTES;
    let run = async {
        loop {
            for _ in 0..16 {
                let Some(mut message) = connection.subscription.try_recv() else {
                    break;
                };
                out.publish(&mut stream, &mut message, connection.version)
                    .await?;
            }
            if !connection.authenticated() && tokio::time::Instant::now() >= auth_deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "authentication deadline elapsed",
                ));
            }
            let buffered_bytes = input.len();
            match protocol::parse(&mut input, |count| out.arguments(count)) {
                Ok(Some(args)) => {
                    parsed = true;
                    let input_bytes = buffered_bytes.saturating_sub(input.len()) as u64;
                    // Replies to buffered requests coalesce within the
                    // ledger's bounds. A command that reads or resets
                    // telemetry, controls clients or this connection, or runs
                    // nested commands first sees every earlier one delivered
                    // and observed, as when each reply left on its own.
                    // Bound once: routing, execution and every classification
                    // below reuse its registrations and validation.
                    let mut command = engine::BoundCommand::bind(&args);
                    // The registered root, case-folded at binding: equal to a
                    // builtin name exactly when `args[0]` names it in any case.
                    // Copied out so it outlives the command moved below.
                    let root = command.root_name();
                    if !out.admits(input_bytes) || settles_output(&command) {
                        // Authenticated, the deadline no longer applies and
                        // the settle is awaited without its wrapper.
                        if connection.authenticated() {
                            out.settle(&mut stream).await?;
                        } else {
                            auth_io(false, auth_deadline, out.settle(&mut stream)).await?;
                        }
                    }
                    let started = Instant::now();
                    let cpu_start = connection
                        .session
                        .cpu_ns
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let timer = out.timer(started);
                    let mut close = false;
                    let bypass_pause = root == Some("client")
                        && args.get(1).is_some_and(|sub| {
                            sub.eq_ignore_ascii_case(b"UNPAUSE")
                                || sub.eq_ignore_ascii_case(b"PAUSE")
                        });
                    // Execution time excludes a CLIENT PAUSE wait, which the
                    // request's latency includes. A command no pause may hold
                    // is admitted at that same check without waiting, so its
                    // execution starts with its request; with its cooperative
                    // budget spent it still takes the waiting path, which
                    // yields there before admission.
                    let mut execution_started = started;
                    if connection.authenticated() && !bypass_pause {
                        let write = command.is_pause_write();
                        if state.client_registry.may_hold(write)
                            || !tokio::task::coop::has_budget_remaining()
                        {
                            if response_or_disconnect(
                                &mut stream,
                                &mut input,
                                async {
                                    state.client_registry.wait_unpaused(write).await;
                                    Ok(())
                                },
                                &mut connection.subscription,
                                (&mut out, connection.version),
                                None,
                            )
                            .await?
                            .is_none()
                            {
                                return Ok(());
                            }
                            execution_started = Instant::now();
                        }
                    }
                    // The connection keeps the parsed arguments until their
                    // observation; routing and execution borrow them.
                    let mut record = CommandRecord {
                        timer,
                        started: execution_started,
                        cpu_start,
                        input_bytes,
                        builtin: command.exists(),
                    };
                    let action = connection.route(&mut command, record.started);
                    if !matches!(&action, Action::Command(_) | Action::Transaction { .. }) {
                        connection.cluster.asking = false;
                    }
                    let mut response = match action {
                        Action::Reply(reply) => reply,
                        Action::NoReply => {
                            out.record(record.complete(args, false, &connection, &state));
                            continue;
                        }
                        Action::Close => {
                            close = true;
                            Resp::Simple("OK")
                        }
                        Action::Unblock { id, error } => {
                            if state.client_registry.unblock_native(id, error) {
                                Resp::Integer(1)
                            } else {
                                let Some(reply) = dispatch(
                                    &mut stream,
                                    &mut input,
                                    &tx,
                                    &mut connection.subscription,
                                    (&mut out, connection.version),
                                    None,
                                    |reply| Request::Unblock {
                                        client_id: id,
                                        error,
                                        reply,
                                    },
                                )
                                .await?
                                else {
                                    return Ok(());
                                };
                                reply
                            }
                        }
                        Action::Command(module) => {
                            // Matched in place: an arm moves out only the
                            // reply or continuation it awaits with, never the
                            // whole entry or its `Ran`.
                            let reply = match enter(command, module, &connection, &state) {
                                Err(error) => return Err(error),
                                Ok(Entry::Reply(reply)) => SocketReply::Reply(reply),
                                Ok(Entry::Ran {
                                    ran: sharding::Ran::Done(reply),
                                    admitted,
                                }) => {
                                    charge_inline(admitted).await;
                                    SocketReply::Reply(reply)
                                }
                                Ok(Entry::Ran {
                                    ran: sharding::Ran::Committing(commit),
                                    admitted,
                                }) => {
                                    charge_inline(admitted).await;
                                    committed(
                                        commit,
                                        &mut connection,
                                        &mut stream,
                                        &mut input,
                                        &mut out,
                                    )
                                    .await?
                                }
                                Ok(Entry::Ran {
                                    ran: sharding::Ran::Escalate(command),
                                    admitted,
                                }) => {
                                    charge_inline(admitted).await;
                                    on_actor(
                                        command,
                                        &mut connection,
                                        &state,
                                        &tx,
                                        &mut stream,
                                        &mut input,
                                        &mut out,
                                    )
                                    .await?
                                }
                                Ok(Entry::Async {
                                    command,
                                    module,
                                    placement,
                                }) => {
                                    socket_command(
                                        command,
                                        module,
                                        placement,
                                        &mut connection,
                                        &state,
                                        &tx,
                                        &mut stream,
                                        &mut input,
                                        &mut out,
                                        &mut record.started,
                                        peer,
                                    )
                                    .await?
                                }
                            };
                            match reply {
                                SocketReply::Reply(reply) => reply,
                                SocketReply::Suppressed => {
                                    connection.cluster.asking = false;
                                    out.record(record.complete(args, false, &connection, &state));
                                    continue;
                                }
                                SocketReply::Closed => return Ok(()),
                            }
                        }
                        Action::Watch(keys) => {
                            let admission = match state.cluster.get() {
                                Some(cluster) => out
                                    .wait(
                                        &mut stream,
                                        cluster.admit(&keys, false, false, &mut connection.cluster),
                                    )
                                    .await?
                                    .map(|_| ()),
                                None => Ok(()),
                            };
                            if let Err(reply) = admission {
                                reply
                            } else {
                                let session = connection.session.clone();
                                let Some(tokens) = dispatch(
                                    &mut stream,
                                    &mut input,
                                    &tx,
                                    &mut connection.subscription,
                                    (&mut out, connection.version),
                                    None,
                                    |reply| Request::Watch {
                                        keys,
                                        session: Some(session),
                                        reply,
                                    },
                                )
                                .await?
                                else {
                                    return Ok(());
                                };
                                connection.watched(tokens?);
                                Resp::Simple("OK")
                            }
                        }
                        Action::Transaction {
                            commands,
                            watched,
                            user,
                        } => {
                            // Each queued command binds once for every
                            // admission classification below.
                            let server = state.server.get();
                            let cluster_keys = state.cluster.get().is_some();
                            let mut modules = Vec::new();
                            let mut keys = Vec::new();
                            let (mut write, mut pause_write) = (false, false);
                            let mut script = false;
                            for (index, args) in commands.iter().enumerate() {
                                let command = engine::BoundCommand::bind(args);
                                let root = command.root_name();
                                let spublish = root == Some("spublish");
                                script |= matches!(
                                    root,
                                    Some("eval" | "evalsha" | "eval_ro" | "evalsha_ro")
                                );
                                write |= command.is_dataset_write() || spublish;
                                pause_write |= command.is_pause_write();
                                if cluster_keys {
                                    if spublish {
                                        keys.extend(args.get(1).cloned());
                                    } else {
                                        keys.extend(command.accessed_keys());
                                    }
                                }
                                if !command.exists()
                                    && let Some(server) = server
                                    && let Some(Ok(invocation)) =
                                        server.extensions().prepare_sync(args)
                                {
                                    modules.push((index, invocation));
                                }
                            }
                            let module_write =
                                modules.iter().any(|(_, invocation)| invocation.is_write());
                            write |= module_write;
                            pause_write |= module_write;
                            if response_or_disconnect(
                                &mut stream,
                                &mut input,
                                async {
                                    state.client_registry.wait_unpaused(pause_write).await;
                                    Ok(())
                                },
                                &mut connection.subscription,
                                (&mut out, connection.version),
                                None,
                            )
                            .await?
                            .is_none()
                            {
                                return Ok(());
                            }
                            let write_permit = if write {
                                match response_or_disconnect(
                                    &mut stream,
                                    &mut input,
                                    async { Ok(state.write_gate.admit().await) },
                                    &mut connection.subscription,
                                    (&mut out, connection.version),
                                    None,
                                )
                                .await?
                                {
                                    Some(permit) => Some(permit),
                                    None => return Ok(()),
                                }
                            } else {
                                None
                            };
                            let cluster_client =
                                state.cluster.get().map(|_| cluster::ClusterClient {
                                    readonly: connection.cluster.readonly,
                                    asking: connection.cluster.asking,
                                });
                            let admission =
                                if let Some(cluster) = state.cluster.get() {
                                    keys.extend(modules.iter().flat_map(|(_, invocation)| {
                                        invocation.keys().iter().cloned()
                                    }));
                                    out.wait(
                                        &mut stream,
                                        cluster.admit(&keys, write, false, &mut connection.cluster),
                                    )
                                    .await?
                                    .map(|_| ())
                                } else {
                                    Ok(())
                                };
                            if let Err(reply) = admission {
                                reply
                            } else {
                                let session = connection.session.clone();
                                let version = connection.version;
                                let debugging = !modules.is_empty() || script;
                                let scripts = if debugging {
                                    state.scripts.get().map(|control| DebugClient {
                                        control: control.clone(),
                                        id: connection.lease.id,
                                        disconnected: connection.session.disconnected.clone(),
                                    })
                                } else {
                                    None
                                };
                                let Some(reply) = dispatch(
                                    &mut stream,
                                    &mut input,
                                    &tx,
                                    &mut connection.subscription,
                                    (&mut out, version),
                                    scripts,
                                    |reply| Request::Transaction {
                                        commands,
                                        modules,
                                        watched,
                                        user,
                                        origin: request::Origin::Client(version),
                                        session: Some(session),
                                        write_permit,
                                        cluster_client,
                                        reply,
                                    },
                                )
                                .await?
                                else {
                                    return Ok(());
                                };
                                reply
                            }
                        }
                    };
                    let failed = matches!(&response, Resp::Error(_));
                    connection
                        .control
                        .set_database(connection.session.database());
                    if !failed && root == Some("hello")
                        && let (Resp::Map(fields), Some(server)) = (&mut response, state.server.get())
                        && let Some(modules) = out.wait(&mut stream, server.extensions().execute(&[
                            bytes::Bytes::from_static(b"MODULE"), bytes::Bytes::from_static(b"LIST")
                        ])).await?
                        && let Some((_, value)) = fields.iter_mut().find(|(key, _)| matches!(key, Resp::Bulk(Some(name)) if name.as_ref() == b"modules")) {
                        *value = modules;
                    }
                    if failed || root != Some("asking") {
                        connection.cluster.asking = false;
                    }
                    if root == Some("monitor") && !failed && args.len() == 1 {
                        monitor = state.server.get().map(|server| server.monitor());
                    }
                    if root == Some("reset") {
                        monitor = None;
                    }
                    let done = record.complete(args, failed, &connection, &state);
                    let authenticated = connection.authenticated();
                    let close = close
                        || connection.close_after_reply()
                        || connection.authentication_exhausted();
                    if authenticated && !close {
                        // What `auth_io` awaits without a deadline: no settle,
                        // and a reply the buffer takes needs no future.
                        if !connection.take_reply_permission() {
                            out.record(done);
                        } else if out.buffers(&response) {
                            out.reply_buffered(response, connection.version, done);
                        } else {
                            out.reply(&mut stream, response, connection.version, done)
                                .await?;
                        }
                    } else {
                        auth_io(authenticated, auth_deadline, async {
                            if connection.take_reply_permission() {
                                out.reply(&mut stream, response, connection.version, done)
                                    .await?;
                            } else {
                                out.record(done);
                            }
                            // Nothing waits unauthenticated, and a closing
                            // connection delivers everything first.
                            out.settle(&mut stream).await?;
                            Ok::<_, io::Error>(())
                        })
                        .await?;
                        if close {
                            return stream.shutdown().await;
                        }
                    }
                }
                Ok(None) => {
                    // The last read ended inside a request while earlier
                    // replies await delivery. Bytes the client already sent
                    // are taken without waiting, so those replies leave with
                    // the ones they complete: one poll per parsed request, and
                    // only for a partial request shorter than a read that the
                    // ledger could still admit. It never waits, since a client
                    // may withhold the rest until it reads those replies. It
                    // stages its bytes rather than growing `input` for a poll
                    // that may find nothing.
                    let early = if std::mem::take(&mut parsed)
                        && (1..READY_READ_BYTES).contains(&input.len())
                        && !out.idle()
                        && out.admits(input.len() as u64)
                    {
                        let mut chunk = [0u8; READY_READ_BYTES];
                        match poll_once(stream.read(&mut chunk)).await {
                            Some(Ok(n)) if n > 0 => {
                                input.extend_from_slice(&chunk[..n]);
                                continue;
                            }
                            read => read,
                        }
                    } else {
                        None
                    };
                    // No complete request is buffered: deliver everything
                    // before waiting on the client, or before ending with the
                    // EOF or error already read.
                    if connection.authenticated() {
                        out.settle(&mut stream).await?;
                    } else {
                        auth_io(false, auth_deadline, out.settle(&mut stream)).await?;
                    }
                    let n = match early {
                        Some(read) => read?,
                        None => {
                            if input.capacity() - input.len() < read_hint {
                                input.reserve(read_limit);
                            }
                            let offered = (input.capacity() - input.len()).min(read_limit);
                            let authenticated = connection.authenticated();
                            tokio::select! {
                                message = connection.subscription.recv() => {
                                    let Some(mut message) = message else { return Ok(()); };
                                    out.publish(&mut stream, &mut message, connection.version).await?;
                                    continue;
                                }
                                message = monitor_message(&mut monitor), if monitor.is_some() => {
                                    match message {
                                        Ok(message) => out.push(&mut stream, Resp::Status(message), connection.version).await?,
                                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {},
                                        Err(tokio::sync::broadcast::error::RecvError::Closed) => monitor = None,
                                    }
                                    continue;
                                }
                                read = auth_io(authenticated, auth_deadline, read_ahead(&mut stream, &mut input, read_limit)) => {
                                    let n = read?;
                                    if n == read_limit {
                                        read_limit = (read_limit * 2).min(MAX_READ_BYTES);
                                    } else if n < offered && n < FIRST_READ_BYTES {
                                        read_limit = FIRST_READ_BYTES;
                                    }
                                    read_hint = n.min(FIRST_READ_BYTES);
                                    n
                                }
                            }
                        }
                    };
                    // An ordinary read appended its bytes into `input`.
                    if n == 0 {
                        return Ok(());
                    }
                }
                Err(error) => {
                    out.push(
                        &mut stream,
                        Resp::Error(format!("ERR Protocol error: {error}")),
                        connection.version,
                    )
                    .await?;
                    out.settle(&mut stream).await?;
                    return stream.shutdown().await;
                }
            }
        }
    };
    tokio::pin!(run);
    tokio::select! {
        biased;
        _ = cancelled.changed() => Ok(()),
        result = &mut run => result,
    }
}

/// A command that may read or reset command telemetry, control clients or this
/// connection, hand over its socket, or run nested commands sees every earlier
/// reply delivered and observed, as when each reply left on its own. The
/// catalog selects them: its control commands and those it keeps out of
/// scripts (script runners, administration, subscriptions). Module and unknown
/// commands have no catalog entry and settle too.
fn settles_output(command: &engine::BoundCommand<'_>) -> bool {
    command.is_control() || !command.script_allowed()
}

async fn monitor_message(
    monitor: &mut Option<tokio::sync::broadcast::Receiver<bytes::Bytes>>,
) -> Result<bytes::Bytes, tokio::sync::broadcast::error::RecvError> {
    match monitor {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

enum SocketReply {
    Reply(Resp),
    Suppressed,
    Closed,
}

const SCRIPT_BUSY: &str =
    "BUSY Redis is busy running a script. You can only call SCRIPT KILL or FUNCTION KILL.";

/// How `serve` begins a routed command.
enum Entry<'a> {
    /// Script control's reply or a busy script's refusal, which claim a command
    /// before any other interception or execution.
    Reply(Resp),
    /// Ran on its shard lease, taken with any write permit without waiting.
    Ran {
        ran: sharding::Ran<'a>,
        /// It took a write permit, whose cooperative unit `try_admit` did not
        /// charge.
        admitted: bool,
    },
    /// Untouched, with nothing taken or charged, for [`socket_command`].
    Async {
        command: engine::BoundCommand<'a>,
        module: Option<server_commands::extensions::Invocation>,
        placement: Placement,
    },
}

/// What [`enter`] learned from `Pool::inline_shard` about a command it
/// returned. That classification is fixed for the command, so
/// [`socket_command`] reuses it rather than route and hash its keys again.
#[derive(Clone, Copy)]
enum Placement {
    /// Returned before asking: script control, an interception, a module, a
    /// spent cooperative budget or a connection without a pool.
    Unasked,
    /// Placed on this shard.
    Shard(usize),
    /// Not to run inline.
    Refused,
}

impl Placement {
    /// `command`'s inline shard, asking `pool` only if [`enter`] did not.
    fn shard(
        self,
        pool: &sharding::Pool,
        command: &engine::BoundCommand<'_>,
        state: &state::State,
    ) -> Option<usize> {
        match self {
            Self::Unasked => pool.inline_shard(command, state),
            Self::Shard(shard) => Some(shard),
            Self::Refused => None,
        }
    }
}

/// Applies script control, then runs `command` on its shard lease when
/// [`socket_command`] would reach that same execution without waiting: no
/// module, script debugging, ACL, replication or server-runtime interception
/// claims it, `Pool::inline_shard` places it (refusing cluster engines, so no
/// cluster admission applies), cooperative budget remains, and its write
/// permit and lease are free now; a write whose commit needs the WAL or an
/// online backup then returns from the lease untouched, as from any holder.
/// Anything else returns it unexecuted, having taken nothing.
fn enter<'a>(
    command: engine::BoundCommand<'a>,
    module: Option<server_commands::extensions::Invocation>,
    connection: &Connection,
    state: &state::State,
) -> io::Result<Entry<'a>> {
    if let Some(scripts) = state.scripts.get() {
        if let Some(reply) = scripts.control(command.args()) {
            return Ok(Entry::Reply(reply));
        }
        if scripts.is_busy() {
            return Ok(Entry::Reply(Resp::Error(SCRIPT_BUSY.into())));
        }
    }
    let root = command.root_name();
    let intercepted = matches!(root, Some("script" | "acl"))
        || (state.native.get().is_some() && replication_command(root))
        || (command.server_command().is_some() && state.server.get().is_some());
    // An exhausted budget yields before write admission and execution, as
    // `socket_command`'s awaited admission does.
    let ready = module.is_none() && !intercepted && tokio::task::coop::has_budget_remaining();
    let (true, Some(pool)) = (ready, connection.pool.as_ref()) else {
        return Ok(Entry::Async {
            command,
            module,
            placement: Placement::Unasked,
        });
    };
    let Some(shard) = pool.inline_shard(&command, state) else {
        return Ok(Entry::Async {
            command,
            module,
            placement: Placement::Refused,
        });
    };
    let placed = Placement::Shard(shard);
    let write = takes_write_permit(&command);
    let write_permit = if write {
        let Some(permit) = state.write_gate.try_admit() else {
            return Ok(Entry::Async {
                command,
                module,
                placement: placed,
            });
        };
        Some(permit)
    } else {
        None
    };
    // A busy shard drops the permit before `socket_command` waits for both.
    let Some(lease) = pool.try_lease(shard) else {
        return Ok(Entry::Async {
            command,
            module,
            placement: placed,
        });
    };
    let command = request::SingleKey {
        command,
        origin: request::Origin::Client(connection.version),
        user: connection.user(),
        session: Some(connection.session.clone()),
        write_permit,
        // `inline_shard` placed it, so this engine has no cluster.
        cluster_client: None,
    };
    Ok(Entry::Ran {
        ran: lease.run(command, state)?,
        admitted: write,
    })
}

/// Whether `command` passes the write gate before it executes.
fn takes_write_permit(command: &engine::BoundCommand<'_>) -> bool {
    command.is_dataset_write() || command.root_name() == Some("spublish")
}

/// The cooperative units `socket_command` charges a command [`enter`] ran:
/// the write admission's, which `try_admit` did not spend, then inline
/// execution's.
async fn charge_inline(admitted: bool) {
    if admitted {
        tokio::task::coop::consume_budget().await;
    }
    tokio::task::coop::consume_budget().await;
}

/// Roots native replication handles itself while configured.
fn replication_command(root: Option<&str>) -> bool {
    matches!(
        root,
        Some(
            "psync"
                | "sync"
                | "replconf"
                | "replicaof"
                | "slaveof"
                | "wait"
                | "waitaof"
                | "failover"
        )
    )
}

/// The reply of an inline command whose commit continues on a supervised task.
/// That continuation owns the mutation; a disconnect only discards its reply.
async fn committed(
    commit: oneshot::Receiver<io::Result<Resp>>,
    connection: &mut Connection,
    stream: &mut Stream,
    input: &mut BytesMut,
    out: &mut Output<'_>,
) -> io::Result<SocketReply> {
    let version = connection.version;
    let reply = response_or_disconnect(
        stream,
        input,
        async { commit.await.map_err(|_| engine_stopped())? },
        &mut connection.subscription,
        (out, version),
        None,
    )
    .await?;
    Ok(reply.map_or(SocketReply::Closed, SocketReply::Reply))
}

/// Hands a single-key command to the coordinator actor; only a script takes a
/// debug client.
async fn on_actor(
    command: request::SingleKey<'_>,
    connection: &mut Connection,
    state: &state::State,
    tx: &mpsc::Sender<Request>,
    stream: &mut Stream,
    input: &mut BytesMut,
    out: &mut Output<'_>,
) -> io::Result<SocketReply> {
    let root = command.command.root_name();
    let scripts = if matches!(root, Some("eval" | "evalsha" | "eval_ro" | "evalsha_ro")) {
        state.scripts.get().map(|control| DebugClient {
            control: control.clone(),
            id: connection.lease.id,
            disconnected: connection.session.disconnected.clone(),
        })
    } else {
        None
    };
    let version = connection.version;
    let Some(reply) = dispatch(
        stream,
        input,
        tx,
        &mut connection.subscription,
        (out, version),
        scripts,
        |reply| Request::Command(command.into_request(reply)),
    )
    .await?
    else {
        return Ok(SocketReply::Closed);
    };
    Ok(SocketReply::Reply(reply))
}

/// [`enter`] applied script control first and learned `placement`.
#[expect(
    clippy::too_many_arguments,
    reason = "Dispatch borrows the connection buffers, actor channel, and peer identity for one socket request."
)]
async fn socket_command(
    bound: engine::BoundCommand<'_>,
    module: Option<server_commands::extensions::Invocation>,
    placement: Placement,
    connection: &mut Connection,
    state: &Arc<state::State>,
    tx: &mpsc::Sender<Request>,
    stream: &mut Stream,
    input: &mut BytesMut,
    out: &mut Output<'_>,
    execution_started: &mut Instant,
    peer: std::net::SocketAddr,
) -> io::Result<SocketReply> {
    let args = bound.args();
    // Case-folded at binding; read before `bound` moves into the request.
    let root = bound.root_name();
    let version = connection.version;
    let cluster_client = state.cluster.get().map(|_| cluster::ClusterClient {
        readonly: connection.cluster.readonly,
        asking: connection.cluster.asking,
    });
    if root == Some("script")
        && args
            .get(1)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(b"DEBUG"))
    {
        let reply = match args
            .get(2)
            .and_then(|value| scripting::ScriptRunner::parse_debug_mode(value))
        {
            Some(mode) if args.len() == 3 => {
                let mode = match mode {
                    scripting::DebugMode::No => 0,
                    scripting::DebugMode::Yes => 1,
                    scripting::DebugMode::Sync => 2,
                };
                connection
                    .session
                    .script_debug_mode
                    .store(mode, std::sync::atomic::Ordering::Release);
                Resp::Simple("OK")
            }
            _ => Resp::Error("ERR Use SCRIPT DEBUG YES/SYNC/NO".into()),
        };
        return Ok(SocketReply::Reply(reply));
    }
    if root == Some("acl") {
        let Some(acl) = connection.acl() else {
            return Ok(SocketReply::Reply(Resp::Error(
                "ERR ACL runtime is not initialized".into(),
            )));
        };
        let metadata = match state.server.get() {
            Some(server) => {
                out.wait(stream, server.extensions().command_metadata())
                    .await?
            }
            None => Vec::new(),
        };
        let user = connection.user();
        let reply = acl.execute(
            args,
            user.as_ref().map_or("default", |user| user.name()),
            |command, user| {
                if let Some(spec) = command.first().and_then(|name| {
                    metadata
                        .iter()
                        .find(|spec| name.eq_ignore_ascii_case(spec.name.as_bytes()))
                }) {
                    let count = command.len() as i64;
                    if (spec.arity > 0 && count != spec.arity)
                        || (spec.arity < 0 && count < -spec.arity)
                    {
                        return Err(Resp::Error(
                            "ERR wrong number of arguments for module command".into(),
                        ));
                    }
                    user.authorize(
                        spec.name.as_bytes(),
                        &spec.categories,
                        &spec.keys(command),
                        false,
                    )
                    .map_err(|reason| Resp::Error(format!("NOPERM {reason}")))
                } else {
                    let mut command = engine::BoundCommand::bind(command);
                    command.check().and_then(|()| command.authorize(user, None))
                }
            },
            |category| {
                let mut commands = Engine::command_names(category);
                commands.extend(
                    metadata
                        .iter()
                        .filter(|spec| {
                            category.is_none_or(|category| {
                                spec.categories
                                    .iter()
                                    .any(|name| name.eq_ignore_ascii_case(category))
                            })
                        })
                        .map(|spec| spec.name.clone()),
                );
                commands
            },
        );
        return Ok(SocketReply::Reply(reply));
    }
    if let Some(invocation) = module {
        let user = connection.user();
        if let Some(user) = user.as_ref()
            && let Err(reason) = user.authorize_logged(
                invocation.name(),
                invocation.categories(),
                invocation.keys(),
                invocation.global_keyspace(),
                connection.audit().as_ref(),
            )
        {
            return Ok(SocketReply::Reply(Resp::Error(format!("NOPERM {reason}"))));
        }
        let write = invocation.is_write();
        if response_or_disconnect(
            stream,
            input,
            async {
                state.client_registry.wait_unpaused(write).await;
                Ok(())
            },
            &mut connection.subscription,
            (&mut *out, version),
            None,
        )
        .await?
        .is_none()
        {
            return Ok(SocketReply::Closed);
        }
        let write_permit = if write {
            match response_or_disconnect(
                stream,
                input,
                async { Ok(state.write_gate.admit().await) },
                &mut connection.subscription,
                (&mut *out, version),
                None,
            )
            .await?
            {
                Some(permit) => Some(permit),
                None => return Ok(SocketReply::Closed),
            }
        } else {
            None
        };
        if let Some(cluster) = state.cluster.get()
            && let Err(reply) = out
                .wait(
                    stream,
                    cluster.admit(invocation.keys(), write, false, &mut connection.cluster),
                )
                .await?
        {
            return Ok(SocketReply::Reply(reply));
        }
        let session = connection.session.clone();
        let scripts = state.scripts.get().map(|control| DebugClient {
            control: control.clone(),
            id: connection.lease.id,
            disconnected: connection.session.disconnected.clone(),
        });
        let Some(reply) = dispatch(
            stream,
            input,
            tx,
            &mut connection.subscription,
            (&mut *out, version),
            scripts,
            |reply| Request::Module {
                invocation,
                user,
                origin: request::Origin::Client(version),
                session: Some(session),
                write_permit,
                cluster_client,
                reply,
            },
        )
        .await?
        else {
            return Ok(SocketReply::Closed);
        };
        return Ok(SocketReply::Reply(reply));
    }
    if let Some(cluster) = state.cluster.get()
        && matches!(root, Some("asking" | "readonly" | "readwrite"))
        && let Some(reply) = out
            .wait(stream, cluster.command(args, &mut connection.cluster))
            .await?
    {
        return Ok(SocketReply::Reply(reply));
    }
    let write = takes_write_permit(&bound);
    // Transfer the permit into the actor request after final owner admission.
    let write_permit = if write {
        match response_or_disconnect(
            stream,
            input,
            async { Ok(state.write_gate.admit().await) },
            &mut connection.subscription,
            (&mut *out, version),
            None,
        )
        .await?
        {
            Some(permit) => Some(permit),
            None => return Ok(SocketReply::Closed),
        }
    } else {
        None
    };
    if let Some(cluster) = state.cluster.get() {
        let keys = match root {
            Some("ssubscribe") => args[1..].to_vec(),
            Some("spublish") if args.len() > 1 => vec![args[1].clone()],
            _ => bound.accessed_keys().into_vec(),
        };
        let admitted = if root == Some("migrate") {
            out.wait(
                stream,
                cluster.admit_migrate(&keys, &mut connection.cluster),
            )
            .await?
        } else if root == Some("ssubscribe") {
            let mut subscriber = cluster::ClusterClient {
                readonly: true,
                asking: connection.cluster.asking,
            };
            let admitted = out
                .wait(stream, cluster.admit(&keys, false, false, &mut subscriber))
                .await?;
            connection.cluster.asking = subscriber.asking;
            admitted
        } else {
            let asking = root == Some("restore-asking");
            out.wait(
                stream,
                cluster.admit(&keys, write, asking, &mut connection.cluster),
            )
            .await?
        };
        if let Err(reply) = admitted {
            return Ok(SocketReply::Reply(reply));
        }
        if root == Some("ssubscribe") {
            return Ok(match connection.subscription.command(args) {
                Ok(()) => SocketReply::Suppressed,
                Err(reply) => SocketReply::Reply(reply),
            });
        }
        if matches!(root, Some("publish" | "spublish")) && args.len() == 3 {
            let sharded = root == Some("spublish");
            let cpu = session::ClientSession::cpu_window(
                Some(&*connection.session),
                state.cpu_observed(),
            );
            let publication = if sharded {
                state.pubsub.publish_shard(&args[1], &args[2])
            } else {
                state.pubsub.publish(&args[1], &args[2])
            };
            connection.session.charge_cpu(cpu);
            let count = publication.map_err(io::Error::other)?;
            // Publication controls settle earlier output before admission.
            // Finish forwarding the applied publication independently of socket I/O.
            if sharded {
                cluster.forward_shard(&args[1], &args[2]).await?;
            } else {
                cluster.forward_publish(&args[1], &args[2]).await?;
            }
            return Ok(SocketReply::Reply(Resp::Integer(count)));
        }
    }
    if let Some(native) = state.native.get()
        && replication_command(root)
    {
        let watermark = connection
            .session
            .last_write_offset
            .load(std::sync::atomic::Ordering::Acquire);
        let wait =
            matches!(root, Some("wait" | "waitaof")).then(|| connection.control.native_wait());
        let unblock = wait.as_ref().map(|wait| wait.receiver());
        let result = response_or_disconnect(
            stream,
            input,
            async {
                Ok(native
                    .handle_command(
                        args,
                        &mut connection.native,
                        watermark,
                        false,
                        state.cluster.get().is_some(),
                        unblock,
                    )
                    .await)
            },
            &mut connection.subscription,
            (&mut *out, version),
            None,
        )
        .await?;
        drop(wait);
        let Some(result) = result else {
            return Ok(SocketReply::Closed);
        };
        return match result {
            native_replication::CommandResult::Reply(reply) => Ok(SocketReply::Reply(reply)),
            native_replication::CommandResult::NoReply => Ok(SocketReply::Suppressed),
            native_replication::CommandResult::Downstream => {
                connection
                    .control
                    .set_kind(client_registry::ClientKind::Replica);
                out.settle(stream).await?;
                native
                    .serve_downstream(
                        stream,
                        std::mem::take(input),
                        args,
                        connection.native.clone(),
                        peer.ip(),
                    )
                    .await?;
                Ok(SocketReply::Closed)
            }
        };
    }
    if bound.server_command().is_some()
        && let Some(server) = state.server.get()
    {
        let context = server_commands::CommandContext {
            database: connection.session.database(),
        };
        match response_or_disconnect(
            stream,
            input,
            async { Ok(server.execute(&bound, context).await) },
            &mut connection.subscription,
            (&mut *out, version),
            None,
        )
        .await?
        {
            Some(Some(reply)) => return Ok(SocketReply::Reply(reply)),
            Some(None) => {}
            None => return Ok(SocketReply::Closed),
        }
    }
    let mut command = request::SingleKey {
        command: bound,
        origin: request::Origin::Client(version),
        user: connection.user(),
        session: Some(connection.session.clone()),
        write_permit,
        cluster_client,
    };
    // `enter` already placed or refused most commands. A write whose commit
    // needs the WAL or an online backup would only come back untouched from
    // the cell, so it neither waits nor queues for it.
    if let Some(pool) = connection.pool.as_ref()
        && let Some(shard) = placement.shard(pool, &command.command, state)
        && pool.commits_inline(&command.command)
    {
        // Waiting for the shard runs nothing until its worker claims the
        // command: a disconnect before then drops it unexecuted, as while
        // waiting for actor queue capacity. A brief spin first spares the
        // flush or hand-off below whenever another thread frees the shard
        // within it.
        let ran = loop {
            if let Some(lease) = pool.try_lease_spinning(shard) {
                let ran = lease.run(command, state)?;
                // Inline execution never parks on its own, so charge it to this
                // task's cooperative budget; a pipelining client yields its
                // worker in turn.
                tokio::task::coop::consume_budget().await;
                break ran;
            }
            if out.idle() {
                // The shard's worker runs it, so no lease passes between
                // runtimes per waiting command.
                match served_or_disconnect(stream, input, pool.contend(shard, command)).await? {
                    Some(sharding::Contention::Done(reply)) => break sharding::Ran::Done(reply),
                    Some(sharding::Contention::Escalate(mut untouched)) => {
                        // Its worker released the write permit: a write is
                        // admitted again for the coordinator.
                        if write {
                            match served_or_disconnect(stream, input, async {
                                Ok(state.write_gate.admit().await)
                            })
                            .await?
                            {
                                Some(permit) => untouched.write_permit = Some(permit),
                                None => return Ok(SocketReply::Closed),
                            }
                        }
                        break sharding::Ran::Escalate(untouched);
                    }
                    None => return Ok(SocketReply::Closed),
                }
            }
            // As before any wait for the shard, earlier output is delivered
            // first, holding neither a queued command nor a write permit, which
            // would hold off write pauses; then CLIENT PAUSE is checked again,
            // the permit taken again, and the command retried.
            let permit = command.write_permit.take();
            let readmit = permit.is_some();
            drop(permit);
            let delivery_started = Instant::now();
            out.settle(stream).await?;
            if response_or_disconnect(
                stream,
                input,
                async {
                    state
                        .client_registry
                        .wait_unpaused(command.command.is_pause_write())
                        .await;
                    Ok(())
                },
                &mut connection.subscription,
                (&mut *out, version),
                None,
            )
            .await?
            .is_none()
            {
                return Ok(SocketReply::Closed);
            }
            // This delivery and renewed CLIENT PAUSE admission precede execution;
            // request wall latency still includes them, unlike recorded execution time.
            *execution_started += delivery_started.elapsed();
            if readmit {
                match response_or_disconnect(
                    stream,
                    input,
                    async { Ok(state.write_gate.admit().await) },
                    &mut connection.subscription,
                    (&mut *out, version),
                    None,
                )
                .await?
                {
                    Some(permit) => command.write_permit = Some(permit),
                    None => return Ok(SocketReply::Closed),
                }
            }
        };
        match ran {
            sharding::Ran::Done(reply) => return Ok(SocketReply::Reply(reply)),
            sharding::Ran::Committing(commit) => {
                return committed(commit, connection, stream, input, out).await;
            }
            sharding::Ran::Escalate(untouched) => command = untouched,
        }
    }
    on_actor(command, connection, state, tx, stream, input, out).await
}

async fn dispatch<T: Send>(
    stream: &mut Stream,
    input: &mut BytesMut,
    tx: &mpsc::Sender<Request>,
    subscription: &mut pubsub::Subscription,
    output: (&mut Output<'_>, Version),
    scripts: Option<DebugClient>,
    build: impl FnOnce(oneshot::Sender<T>) -> Request + Send,
) -> io::Result<Option<T>> {
    let (reply, response) = oneshot::channel();
    response_or_disconnect(
        stream,
        input,
        async {
            // Observe transport cancellation while waiting for actor queue capacity,
            // not only after the request has already entered the queue.
            tx.send(build(reply)).await.map_err(|_| engine_stopped())?;
            response.await.map_err(|_| engine_stopped())
        },
        subscription,
        output,
        scripts,
    )
    .await
}

fn engine_stopped() -> io::Error {
    io::Error::other("engine stopped")
}

/// Pipelined bytes a waiting connection may buffer before TCP backpressure.
const MAX_BUFFERED_BYTES: usize = 8 * 1024 * 1024;
/// Largest single read while waiting for a response.
const READ_AHEAD_BYTES: usize = 8192;
/// First limit of an ordinary request read.
const FIRST_READ_BYTES: usize = 8192;
/// Largest ordinary request read, reached by doubling after full reads.
const MAX_READ_BYTES: usize = 64 * 1024;
/// Largest read a ready-only continuation takes in one poll.
const READY_READ_BYTES: usize = 8192;

async fn response_or_disconnect<T>(
    stream: &mut Stream,
    input: &mut BytesMut,
    response: impl Future<Output = io::Result<T>>,
    subscription: &mut pubsub::Subscription,
    output: (&mut Output<'_>, Version),
    scripts: Option<DebugClient>,
) -> io::Result<Option<T>> {
    let (out, version) = output;
    let mut debug_connection = DebugConnection(scripts.clone());
    tokio::pin!(response);
    // Only a debugging script forwards events on a timer.
    let mut debug_tick = scripts
        .as_ref()
        .map(|_| tokio::time::interval(Duration::from_millis(10)));
    // Earlier replies are delivered first: this response may wait on the
    // client. Only then read ahead, to detect EOF and retain pipelined bytes;
    // a blocked connection cannot execute its next command until this reply
    // is delivered. Stop reading at the request-buffer cap, applying TCP
    // backpressure. Reading only with nothing left to deliver also keeps
    // `input` from reallocating under the arguments of undelivered commands.
    loop {
        if let Some(control) = scripts.as_ref() {
            for event in control.take_debug_events() {
                out.push(stream, event, version).await?;
            }
            if control.debugger_paused()
                && let Some(args) =
                    protocol::parse(input, Vec::with_capacity).map_err(io::Error::other)?
                && let Some(reply) = control.debug_command(&args)
                && matches!(&reply, Resp::Error(_))
            {
                out.push(stream, reply, version).await?;
            }
        }
        let delivering = !out.idle();
        let room = if delivering {
            0
        } else {
            read_ahead_room(input)
        };
        tokio::select! {
            biased;
            reply = &mut response => {
                if let Some(control) = scripts.as_ref() {
                    for event in control.take_debug_events() {
                        out.push(stream, event, version).await?;
                    }
                }
                debug_connection.0 = None;
                return reply.map(Some);
            }
            () = next_debug_tick(&mut debug_tick), if debug_tick.is_some() => {}
            message = subscription.recv(), if version == Version::Resp3 => {
                let Some(mut message) = message else { return Ok(None); };
                out.publish(stream, &mut message, version).await?;
            }
            open = async {
                if delivering {
                    out.deliver(stream).await.map(|()| true)
                } else {
                    read_ahead(stream, input, room).await.map(|read| read > 0)
                }
            }, if delivering || room > 0 => {
                if !open? {
                    // Dropping the receiver cancels an unfulfilled blocking pop.
                    return Ok(None);
                }
            }
        }
    }
}

/// Waits for a contended command's hand-off to its shard worker and its
/// answer, or for a write permit, reading ahead only to notice EOF and keep
/// pipelined bytes; EOF drops the wait, and with it a command its worker has
/// not claimed. It never writes, so whatever the awaited future holds or is
/// granted, a write permit or a queue slot, never waits behind an unread
/// socket or an undelivered publication. Pushes stay queued; the serve loop
/// writes them after this command's reply.
async fn served_or_disconnect<R: tokio::io::AsyncRead + Unpin, T>(
    stream: &mut R,
    input: &mut BytesMut,
    served: impl Future<Output = io::Result<T>>,
) -> io::Result<Option<T>> {
    tokio::pin!(served);
    loop {
        let room = read_ahead_room(input);
        tokio::select! {
            biased;
            answer = &mut served => return answer.map(Some),
            read = read_ahead(stream, input, room), if room > 0 => {
                if read? == 0 {
                    return Ok(None);
                }
            }
        }
    }
}

/// Read-ahead space left before the request-buffer cap applies TCP backpressure.
fn read_ahead_room(input: &BytesMut) -> usize {
    MAX_BUFFERED_BYTES
        .saturating_sub(input.len())
        .min(READ_AHEAD_BYTES)
}

async fn next_debug_tick(tick: &mut Option<tokio::time::Interval>) {
    match tick {
        Some(tick) => {
            tick.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// Append at most `limit` bytes from `stream` straight into `input`'s spare
/// capacity: no zeroed staging buffer, no second copy. Cancel-safe: a dropped
/// read has consumed nothing.
async fn read_ahead<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
    input: &mut BytesMut,
    limit: usize,
) -> io::Result<usize> {
    // A full buffer would otherwise offer the socket only 64 bytes. Any spare
    // room is used as is, so only a full buffer ever grows here.
    if input.capacity() == input.len() {
        input.reserve(limit);
    }
    stream.take(limit as u64).read_buf(input).await
}

/// `future`'s output if its first poll is ready, `None` if it would wait. The
/// poll runs in the calling task's context: it registers that task's waker and
/// spends its cooperative budget, so an exhausted budget also reads as `None`.
/// A pending future is dropped, so it must be cancel-safe.
async fn poll_once<F: Future>(future: F) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        std::task::Poll::Ready(match future.as_mut().poll(cx) {
            std::task::Poll::Ready(output) => Some(output),
            std::task::Poll::Pending => None,
        })
    })
    .await
}

#[derive(Clone)]
struct DebugClient {
    control: Arc<scripting::ExecutionControl>,
    id: u64,
    disconnected: Arc<std::sync::atomic::AtomicBool>,
}
impl DebugClient {
    fn take_debug_events(&self) -> Vec<Resp> {
        self.control.take_debug_events_for(self.id)
    }
    fn debugger_paused(&self) -> bool {
        self.control.debugger_paused_for(self.id)
    }
    fn debug_command(&self, args: &[bytes::Bytes]) -> Option<Resp> {
        self.control.debug_command_for(self.id, args)
    }
    fn abort_debug(&self) {
        self.disconnected
            .store(true, std::sync::atomic::Ordering::Release);
        self.control.abort_debug_for(self.id);
    }
}

struct DebugConnection(Option<DebugClient>);
impl Drop for DebugConnection {
    fn drop(&mut self) {
        if let Some(control) = self.0.as_ref() {
            control.abort_debug();
        }
    }
}

async fn auth_io<T>(
    authenticated: bool,
    deadline: tokio::time::Instant,
    future: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    if authenticated {
        future.await
    } else {
        tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "authentication deadline elapsed")
            })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    fn frame(args: &[&[u8]]) -> Vec<u8> {
        let mut frame = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            frame.extend_from_slice(arg);
            frame.extend_from_slice(b"\r\n");
        }
        frame
    }

    fn pipeline(commands: &[&[&[u8]]]) -> Vec<u8> {
        commands.iter().flat_map(|args| frame(args)).collect()
    }

    /// Deliver `bytes` in uneven fragments so frames straddle socket reads.
    async fn send_fragmented(client: &mut tokio::net::TcpStream, mut bytes: &[u8]) {
        for size in [1, 2, 3, 5, 8, 13].into_iter().cycle() {
            if bytes.is_empty() {
                return;
            }
            let (piece, rest) = bytes.split_at(size.min(bytes.len()));
            client.write_all(piece).await.unwrap();
            tokio::task::yield_now().await;
            bytes = rest;
        }
    }

    /// Read replies until `done` accepts everything received so far.
    async fn read_until(
        client: &mut tokio::net::TcpStream,
        done: impl Fn(&[u8]) -> bool,
    ) -> Vec<u8> {
        let mut received = Vec::<u8>::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done(received.as_slice()) {
                let read = client.read_buf(&mut received).await.unwrap();
                assert!(read > 0, "closed after {:?}", Bytes::from(received.clone()));
            }
        })
        .await
        .expect("replies within the deadline");
        received
    }

    async fn expect_replies(client: &mut tokio::net::TcpStream, expected: &[u8]) {
        let received = read_until(client, |received| received.len() >= expected.len()).await;
        assert_eq!(Bytes::from(received), Bytes::copy_from_slice(expected));
    }

    async fn loopback() -> (tokio::net::TcpStream, Stream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        client.set_nodelay(true).unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, Stream::Plain(server))
    }

    /// The real coordinator actor and shard pool of a memory-engine server,
    /// with loopback clients served by `serve` as `main` serves them, here or,
    /// through `connect_on`, on a frontend runtime.
    struct Harness {
        state: Arc<state::State>,
        tx: mpsc::Sender<Request>,
        actor: tokio::task::JoinHandle<io::Result<()>>,
        listener: TcpListener,
        clients: JoinSet<io::Result<()>>,
    }

    impl Harness {
        async fn start(shards: u16) -> Self {
            let state = Arc::new(state::State::new(0, shards, false));
            Self::launch(state, shards, None, false, mpsc::channel(1024)).await
        }

        /// As `start`, journaling to `wal` with AOF off, as `--data-dir`
        /// without `--appendonly` starts.
        async fn start_journaled(shards: u16, wal: Wal) -> Self {
            let state = Arc::new(state::State::new(0, shards, true));
            Self::launch(state, shards, Some(wal), false, mpsc::channel(1024)).await
        }

        /// As `start_journaled`, with AOF on, as `--appendonly` starts.
        async fn start_appendonly(shards: u16, wal: Wal) -> Self {
            let state = Arc::new(state::State::new(0, shards, true));
            Self::launch(state, shards, Some(wal), true, mpsc::channel(1024)).await
        }

        /// As `start`, with native replication wired as `main` wires a server
        /// started without `--data-dir`; its snapshots reach the coordinator.
        async fn start_native(shards: u16) -> Self {
            let options = Options::try_parse_from(["ffkv"]).unwrap();
            let state = Arc::new(state::State::new(0, shards, false));
            let (tx, rx) = mpsc::channel(1024);
            assert!(state.control_tx.set(tx.clone()).is_ok());
            let backend = native_runtime::Runtime::new(
                &state,
                native_runtime::Config::from_options(&options, false).unwrap(),
            );
            let native =
                native_replication::NativeReplication::new(backend.native_config(), backend)
                    .unwrap();
            assert!(state.native.set(native).is_ok());
            Self::launch(state, shards, None, false, (tx, rx)).await
        }

        async fn launch(
            state: Arc<state::State>,
            shards: u16,
            wal: Option<Wal>,
            aof_enabled: bool,
            (tx, rx): (mpsc::Sender<Request>, mpsc::Receiver<Request>),
        ) -> Self {
            let config = blocking::Config {
                checkpoint_threshold: u64::MAX,
                aof_enabled,
                durability: Durability::Strict,
                fsync_gate: None,
                databases: Arc::new(std::sync::atomic::AtomicU32::new(16)),
            };
            let runtime = tokio::runtime::Handle::current();
            let actor_state = state.clone();
            let actor = tokio::task::spawn_blocking(move || {
                runtime.block_on(blocking::run(
                    rx,
                    Engine::new(),
                    wal,
                    usize::from(shards),
                    actor_state,
                    config,
                ))
            });
            tokio::time::timeout(Duration::from_secs(10), state.wait_actor_ready())
                .await
                .expect("actor ready");
            Self {
                state,
                tx,
                actor,
                listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
                clients: JoinSet::new(),
            }
        }

        fn pool(&self) -> Arc<sharding::Pool> {
            self.state
                .pool
                .get()
                .and_then(std::sync::Weak::upgrade)
                .expect("the actor attached its pool")
        }

        /// Observe commands as the server runtime does, logging every one as slow.
        fn observe(&self) {
            struct NoHooks;
            impl server_commands::ServerHooks for NoHooks {
                fn run<'a>(
                    &'a self,
                    action: server_commands::ServerAction,
                ) -> server_commands::HookFuture<'a> {
                    let error = format!("no server hooks in tests: {action:?}");
                    Box::pin(std::future::ready(Err(error)))
                }
            }
            let mut config = server_commands::ConfigSnapshot::default();
            for (name, value) in [
                ("slowlog-log-slower-than", "0"),
                ("slowlog-max-len", "1024"),
            ] {
                config
                    .parameters
                    .get_mut(name)
                    .expect("a default parameter")
                    .value = Bytes::from_static(value.as_bytes());
            }
            let server = server_commands::ServerRuntime::new(config, Arc::new(NoHooks));
            assert!(self.state.server.set(Arc::new(server)).is_ok());
        }

        async fn connect(&mut self) -> tokio::net::TcpStream {
            let client = tokio::net::TcpStream::connect(self.listener.local_addr().unwrap())
                .await
                .unwrap();
            client.set_nodelay(true).unwrap();
            let (server, _) = self.listener.accept().await.unwrap();
            self.clients.spawn(serve(
                Stream::Plain(server),
                self.tx.clone(),
                self.state.clone(),
                None,
            ));
            client
        }

        /// As `connect`, handing the accepted client to `frontend` as `main`
        /// does; its task joins `clients`.
        async fn connect_on(
            &self,
            frontend: &tokio::runtime::Handle,
            clients: &mut JoinSet<()>,
        ) -> tokio::net::TcpStream {
            let client = tokio::net::TcpStream::connect(self.listener.local_addr().unwrap())
                .await
                .unwrap();
            client.set_nodelay(true).unwrap();
            let (server, peer) = self.listener.accept().await.unwrap();
            clients.spawn_on(
                serve_accepted(
                    server.into_std().unwrap(),
                    peer,
                    None,
                    self.tx.clone(),
                    self.state.clone(),
                    None,
                ),
                frontend,
            );
            client
        }

        /// As `main` does: connections end first, then the actor drains.
        async fn shutdown(self) {
            tokio::time::timeout(Duration::from_secs(10), async move {
                let Self {
                    tx,
                    actor,
                    mut clients,
                    ..
                } = self;
                while let Some(served) = clients.join_next().await {
                    served.unwrap().unwrap();
                }
                tx.send(Request::Shutdown).await.unwrap();
                drop(tx);
                actor.await.unwrap().unwrap();
            })
            .await
            .expect("clean shutdown");
        }
    }

    /// Polls a connection that must stay open: it may only park.
    fn park(connection: Pin<&mut impl Future<Output = io::Result<()>>>, cx: &mut Context<'_>) {
        if let Poll::Ready(served) = connection.poll(cx) {
            panic!("the connection ended early: {served:?}");
        }
    }

    /// Runs `work` while driving a connection that only this task polls.
    async fn alongside<T>(
        mut connection: Pin<&mut impl Future<Output = io::Result<()>>>,
        work: impl Future<Output = T>,
    ) -> T {
        let mut work = std::pin::pin!(work);
        std::future::poll_fn(|cx| {
            park(connection.as_mut(), cx);
            work.as_mut().poll(cx)
        })
        .await
    }

    /// Appends to `received` until it holds `len` bytes; cancel-safe.
    async fn fill(client: &mut tokio::net::TcpStream, received: &mut Vec<u8>, len: usize) {
        while received.len() < len {
            let read = client.read_buf(received).await.unwrap();
            assert!(read > 0, "closed after {:?}", Bytes::from(received.clone()));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fragmented_pipelines_reply_in_order_across_inline_and_coordinated_commands() {
        let mut server = Harness::start(4).await;
        let binary: &[u8] = b"bin\r\n\0\xff";
        let mut client = server.connect().await;
        send_fragmented(
            &mut client,
            &pipeline(&[
                &[b"SET", b"a", b"1"],
                &[b"MSET", b"a", b"2", b"b", binary],
                &[b"GET", b"a"],
                &[b"INCR", b"a"],
                &[b"GET", b"b"],
                &[b"DBSIZE"],
                &[b"DEL", b"a"],
                &[b"GET", b"a"],
                &[b"EXISTS", b"a", b"b"],
            ]),
        )
        .await;
        let binary_reply = [format!("${}\r\n", binary.len()).as_bytes(), binary, b"\r\n"].concat();
        let expected = [
            b"+OK\r\n+OK\r\n$1\r\n2\r\n:3\r\n".as_slice(),
            binary_reply.as_slice(),
            b":2\r\n:1\r\n$-1\r\n:1\r\n",
        ]
        .concat();
        expect_replies(&mut client, &expected).await;
        // Inline replies take each connection's own protocol.
        let mut resp3 = server.connect().await;
        send_fragmented(
            &mut resp3,
            &pipeline(&[
                &[b"HELLO", b"3"],
                &[b"GET", b"missing"],
                &[b"GET", b"b"],
                &[b"ECHO", b"end"],
            ]),
        )
        .await;
        let tail = [
            b"_\r\n".as_slice(),
            binary_reply.as_slice(),
            b"$3\r\nend\r\n",
        ]
        .concat();
        read_until(&mut resp3, |received| received.ends_with(&tail)).await;
        drop((client, resp3));
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_gone_while_its_shard_is_busy_never_runs_its_command() {
        let mut server = Harness::start(2).await;
        let pool = server.pool();
        let set = Vec::from([b"SET".as_slice(), b"held", b"lost"].map(Bytes::copy_from_slice));
        let shard = pool
            .inline_shard(&engine::BoundCommand::bind(&set), &server.state)
            .expect("a memory-backed SET runs inline");
        // Maintenance pauses may hold the shard briefly; wait our turn.
        let busy = tokio::time::timeout(Duration::from_secs(10), pool.lease(shard))
            .await
            .expect("the shard frees up")
            .unwrap();
        let mut client = server.connect().await;
        client
            .write_all(&frame(&[b"SET", b"held", b"lost"]))
            .await
            .unwrap();
        drop(client);
        // Waiting for the busy shard, the connection observes the disconnect and ends.
        let served = tokio::time::timeout(Duration::from_secs(10), server.clients.join_next())
            .await
            .expect("the waiting connection ends");
        served.expect("one connection").unwrap().unwrap();
        drop(busy);
        let mut client = server.connect().await;
        client
            .write_all(&pipeline(&[
                &[b"GET", b"held"],
                &[b"SET", b"held", b"kept"],
                &[b"GET", b"held"],
            ]))
            .await
            .unwrap();
        expect_replies(&mut client, b"$-1\r\n+OK\r\n$4\r\nkept\r\n").await;
        drop((client, pool));
        server.shutdown().await;
    }

    /// A directory removed with everything in it when dropped.
    struct Directory(std::path::PathBuf);

    impl Directory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("ffkv-frontend-{:032x}", rand::random::<u128>()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A client handed to a frontend registers with that runtime's reactor,
    /// so it is still served once the runtime that accepted it, and with it
    /// that runtime's reactor, has ended.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_frontend_serves_its_client_after_the_accepting_runtime_ends() {
        let server = Harness::start(2).await;
        let mut frontends = frontend::Frontends::new(1).await.unwrap();
        let frontend = frontends.next_handle().clone();
        let (tx, state) = (server.tx.clone(), server.state.clone());
        let (client, mut clients) = tokio::task::spawn_blocking(move || {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            // The kernel completes the handshake before the accept.
            let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let accepting = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut clients = JoinSet::new();
            accepting.block_on(async {
                let listener = TcpListener::from_std(listener).unwrap();
                let (stream, peer) = listener.accept().await.unwrap();
                clients.spawn_on(
                    serve_accepted(stream.into_std().unwrap(), peer, None, tx, state, None),
                    &frontend,
                );
            });
            drop(accepting);
            (client, clients)
        })
        .await
        .unwrap();
        client.set_nonblocking(true).unwrap();
        let mut client = tokio::net::TcpStream::from_std(client).unwrap();
        client.set_nodelay(true).unwrap();
        client
            .write_all(&pipeline(&[&[b"SET", b"k", b"v"], &[b"GET", b"k"]]))
            .await
            .unwrap();
        expect_replies(&mut client, b"+OK\r\n$1\r\nv\r\n").await;
        drop(client);
        while let Some(served) = clients.join_next().await {
            served.unwrap();
        }
        server.shutdown().await;
        frontends.shutdown().await.unwrap();
    }

    /// Aborting a frontend client, as shutdown does, leaves a commit it
    /// already started to its supervised task on that frontend, which keeps
    /// running: the write commits exactly once and its shard stays healthy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborting_a_frontend_client_never_cancels_its_admitted_supervised_commit() {
        let directory = Directory::new();
        let path = directory.0.join("wal");
        let (wal, _) =
            tokio::task::spawn_blocking(move || Wal::open(&path, Durability::Strict, |_| Ok(())))
                .await
                .unwrap()
                .unwrap();
        let mut server = Harness::start_journaled(1, wal).await;
        let persistence = server
            .state
            .persistence
            .get()
            .expect("the actor published its persistence")
            .clone();
        // Any WATCH makes every commit capture its effects.
        let _watch = persistence
            .watches
            .register(0, Bytes::from_static(b"watched"), None);
        let revision = persistence.status().await.revision;
        let mut frontends = frontend::Frontends::new(1).await.unwrap();
        let frontend = frontends.next_handle().clone();
        let mut clients = JoinSet::new();
        let mut client = server.connect_on(&frontend, &mut clients).await;
        // An exclusive writer holds the commit gate until released, so the
        // SET cannot commit on its connection and settles on its own task.
        let (held, holding) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let writer = {
            let persistence = persistence.clone();
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let mut held = Some(held);
                runtime.block_on(persistence.checkpoint(std::iter::from_fn(move || {
                    if let Some(held) = held.take() {
                        let _ = held.send(());
                        let _ = released.recv();
                    }
                    None::<io::Result<journal::Batch>>
                })))
            })
        };
        holding.await.unwrap();
        client
            .write_all(&frame(&[b"SET", b"k", b"v"]))
            .await
            .unwrap();
        // Besides the connection, the frontend's only task is that commit.
        tokio::time::timeout(Duration::from_secs(10), async {
            while frontend.metrics().num_alive_tasks() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the SET settles on a supervised task");
        clients.abort_all();
        while clients.join_next().await.is_some() {}
        drop(client);
        release.send(()).unwrap();
        writer.await.unwrap().unwrap();
        let pool = server.pool();
        drop(
            tokio::time::timeout(Duration::from_secs(10), pool.lease(0))
                .await
                .expect("the commit releases its shard")
                .unwrap(),
        );
        assert_eq!(persistence.status().await.revision, revision + 1);
        let mut reader = server.connect().await;
        reader.write_all(&frame(&[b"GET", b"k"])).await.unwrap();
        expect_replies(&mut reader, b"$1\r\nv\r\n").await;
        drop((reader, pool));
        server.shutdown().await;
        frontends.shutdown().await.unwrap();
    }

    /// With AOF on, a write runs through the coordinator whether its shard
    /// was free when it arrived, so its inline lease returned it untouched, or
    /// busy, so it never waited or queued for the shard: either commits to the
    /// WAL exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn appendonly_writes_commit_once_whether_their_shard_was_free_or_busy() {
        let directory = Directory::new();
        let path = directory.0.join("wal");
        let (wal, _) =
            tokio::task::spawn_blocking(move || Wal::open(&path, Durability::Strict, |_| Ok(())))
                .await
                .unwrap()
                .unwrap();
        let mut server = Harness::start_appendonly(1, wal).await;
        let persistence = server
            .state
            .persistence
            .get()
            .expect("the actor published its persistence")
            .clone();
        let before = persistence.status().await;
        let mut client = server.connect().await;
        client.write_all(&frame(&[b"INCR", b"n"])).await.unwrap();
        expect_replies(&mut client, b":1\r\n").await;
        // Held here, the shard is busy when the next write arrives.
        let pool = server.pool();
        let held = tokio::time::timeout(Duration::from_secs(10), pool.lease(0))
            .await
            .expect("maintenance releases the shard")
            .unwrap();
        client
            .write_all(&frame(&[b"INCRBY", b"n", b"2"]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !String::from_utf8_lossy(&server.state.client_registry.list(None, None))
                .contains(" cmd=incrby ")
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the connection routes the INCRBY while its shard is busy");
        drop(held);
        expect_replies(&mut client, b":3\r\n").await;
        let after = persistence.status().await;
        assert_eq!(after.revision, before.revision + 2);
        assert!(after.bytes > before.bytes, "both writes reached the WAL");
        client.write_all(&frame(&[b"GET", b"n"])).await.unwrap();
        expect_replies(&mut client, b"$1\r\n3\r\n").await;
        drop((client, pool, persistence));
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_waiting_out_a_write_pause_runs_once_after_it() {
        let mut server = Harness::start(2).await;
        let mut reader = server.connect().await;
        // The writer's connection runs only when this test polls it, so it
        // parks exactly where its write waits.
        let mut writer = tokio::net::TcpStream::connect(server.listener.local_addr().unwrap())
            .await
            .unwrap();
        writer.set_nodelay(true).unwrap();
        let (accepted, _) = server.listener.accept().await.unwrap();
        let mut connection = Box::pin(tokio::task::coop::unconstrained(serve(
            Stream::Plain(accepted),
            server.tx.clone(),
            server.state.clone(),
            None,
        )));
        server.state.write_gate.pause().await.unwrap();
        writer.write_all(&frame(&[b"INCR", b"held"])).await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            std::future::poll_fn(|cx| {
                park(connection.as_mut(), cx);
                let clients = server.state.client_registry.list(None, None);
                if String::from_utf8_lossy(&clients).contains(" cmd=incr ") {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("the INCR waits for the write gate");
        // Parked at the paused gate, it has run nothing.
        reader.write_all(&frame(&[b"GET", b"held"])).await.unwrap();
        expect_replies(&mut reader, b"$-1\r\n").await;
        // Admitted once the pause ends, it runs exactly once.
        server.state.write_gate.resume();
        alongside(connection.as_mut(), expect_replies(&mut writer, b":1\r\n")).await;
        reader.write_all(&frame(&[b"GET", b"held"])).await.unwrap();
        expect_replies(&mut reader, b"$1\r\n1\r\n").await;
        drop(connection);
        drop((writer, reader));
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_subscriber_not_reading_its_pushes_never_keeps_its_shard_from_other_clients() {
        let mut server = Harness::start(1).await;
        let pool = server.pool();
        let get = Vec::from([b"GET".as_slice(), b"k"].map(Bytes::copy_from_slice));
        let shard = pool
            .inline_shard(&engine::BoundCommand::bind(&get), &server.state)
            .expect("a memory-backed GET runs inline");
        let mut other = server.connect().await;
        // The subscriber's connection runs only when this test polls it, so each
        // phase below is exact. A fixed receive window lets one push fill the
        // socket while the subscriber reads nothing.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(64 * 1024).unwrap();
        let mut subscriber = socket
            .connect(server.listener.local_addr().unwrap())
            .await
            .unwrap();
        subscriber.set_nodelay(true).unwrap();
        let (accepted, _) = server.listener.accept().await.unwrap();
        let mut connection = Box::pin(tokio::task::coop::unconstrained(serve(
            Stream::Plain(accepted),
            server.tx.clone(),
            server.state.clone(),
            None,
        )));
        subscriber
            .write_all(&pipeline(&[&[b"HELLO", b"3"], &[b"SUBSCRIBE", b"ch"]]))
            .await
            .unwrap();
        let subscribed: &[u8] = b">3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n";
        alongside(
            connection.as_mut(),
            read_until(&mut subscriber, |received| received.ends_with(subscribed)),
        )
        .await;
        let busy = tokio::time::timeout(Duration::from_secs(10), pool.lease(shard))
            .await
            .expect("the shard frees up")
            .unwrap();
        subscriber.write_all(&frame(&[b"GET", b"k"])).await.unwrap();
        // Parked after reading its GET, the connection is queued for the shard.
        tokio::time::timeout(
            Duration::from_secs(10),
            std::future::poll_fn(|cx| {
                park(connection.as_mut(), cx);
                let clients = server.state.client_registry.list(None, None);
                if String::from_utf8_lossy(&clients).contains(" cmd=get ") {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("the GET waits for its shard");
        // A push staged during that wait: a wait that writes pushes takes it
        // now and parks on its delivery; a shard wait must leave it queued.
        let payload = Bytes::from(vec![b'p'; 12 << 20]);
        let publication = server
            .state
            .pubsub
            .stage(&Bytes::from_static(b"ch"), &payload)
            .unwrap();
        assert_eq!(publication.count(), 1);
        std::future::poll_fn(|cx| {
            park(connection.as_mut(), cx);
            Poll::Ready(())
        })
        .await;
        // The shard is released and a push larger than the unread socket can
        // buffer is delivered. Another client's GET on that shard and a
        // coordinated DBSIZE must finish while the subscriber reads nothing.
        drop(busy);
        publication.deliver();
        other
            .write_all(&pipeline(&[&[b"GET", b"k"], &[b"DBSIZE"]]))
            .await
            .unwrap();
        let answer: &'static [u8] = b"$-1\r\n:0\r\n";
        let mut answered = Vec::new();
        let prompt = alongside(
            connection.as_mut(),
            tokio::time::timeout(
                Duration::from_secs(5),
                fill(&mut other, &mut answered, answer.len()),
            ),
        )
        .await
        .is_ok();
        // Closing the subscriber frees whatever it holds; everything finishes
        // before asserting.
        drop(connection);
        drop(subscriber);
        tokio::time::timeout(
            Duration::from_secs(10),
            fill(&mut other, &mut answered, answer.len()),
        )
        .await
        .expect("the other client is answered");
        drop((other, pool));
        server.shutdown().await;
        assert!(
            prompt,
            "another client waited on a subscriber that read nothing: the released \
             shard stayed reserved for it behind an unread push"
        );
        assert_eq!(Bytes::from(answered), Bytes::from_static(answer));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_push_wakes_a_consumer_already_blocked_on_its_key() {
        let mut server = Harness::start(2).await;
        let mut consumer = server.connect().await;
        consumer
            .write_all(&frame(&[b"BLPOP", b"jobs", b"0"]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while server.state.metrics.snapshot().blocked_clients != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the consumer blocks");
        let mut producer = server.connect().await;
        producer
            .write_all(&pipeline(&[
                &[b"RPUSH", b"jobs", b"first", b"second"],
                &[b"LRANGE", b"jobs", b"0", b"-1"],
            ]))
            .await
            .unwrap();
        expect_replies(&mut producer, b":2\r\n*1\r\n$6\r\nsecond\r\n").await;
        expect_replies(&mut consumer, b"*2\r\n$4\r\njobs\r\n$5\r\nfirst\r\n").await;
        drop((consumer, producer));
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replica_traffic_keeps_the_replica_replies() {
        let mut server = Harness::start(2).await;
        server.state.set_replica(true);
        let mut client = server.connect().await;
        client
            .write_all(&pipeline(&[&[b"SET", b"k", b"v"], &[b"GET", b"k"]]))
            .await
            .unwrap();
        let replies = read_until(&mut client, |received| {
            received.iter().filter(|&&byte| byte == b'\n').count() >= 2
        })
        .await;
        let replies = String::from_utf8(replies).unwrap();
        let mut lines = replies.lines();
        assert!(
            lines
                .next()
                .is_some_and(|line| line.starts_with("-READONLY ")),
            "{replies}"
        );
        assert!(
            lines
                .next()
                .is_some_and(|line| line.starts_with("-LOADING ")),
            "{replies}"
        );
        drop(client);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn waiting_read_ahead_stops_at_the_request_cap_and_keeps_every_byte_in_order() {
        fn pattern(index: usize) -> u8 {
            (index % 251) as u8
        }
        let total = MAX_BUFFERED_BYTES + 3 * READ_AHEAD_BYTES + 7;
        let (mut client, mut stream) = loopback().await;
        let writer = tokio::spawn(async move {
            let data: Vec<u8> = (0..total).map(pattern).collect();
            // Odd-sized writes straddle every read boundary.
            for piece in data.chunks(4093) {
                client.write_all(piece).await.unwrap();
            }
            client.shutdown().await.unwrap();
            client
        });
        let mut subscription = Arc::new(pubsub::Hub::default()).attach(1);
        let state = state::State::new(0, 1, false);
        let (mut input, mut out) = (BytesMut::new(), Output::new(&state, "test"));
        // No response arrives, so only the cap ends the read-ahead, and a full
        // buffer never reaches the EOF queued behind it.
        for attempt in 0.. {
            assert!(
                attempt < 1000,
                "read-ahead stalled at {} bytes",
                input.len()
            );
            let waited = tokio::time::timeout(
                Duration::from_millis(10),
                response_or_disconnect(
                    &mut stream,
                    &mut input,
                    std::future::pending::<io::Result<()>>(),
                    &mut subscription,
                    (&mut out, Version::Resp2),
                    None,
                ),
            )
            .await;
            assert!(waited.is_err(), "read past the request cap");
            assert!(input.len() <= MAX_BUFFERED_BYTES);
            if input.len() == MAX_BUFFERED_BYTES {
                break;
            }
        }
        assert!(
            input
                .iter()
                .enumerate()
                .all(|(index, byte)| *byte == pattern(index))
        );
        // Once the buffered commands are consumed the rest arrives intact.
        input.clear();
        let closed = tokio::time::timeout(
            Duration::from_secs(10),
            response_or_disconnect(
                &mut stream,
                &mut input,
                std::future::pending::<io::Result<()>>(),
                &mut subscription,
                (&mut out, Version::Resp2),
                None,
            ),
        )
        .await
        .expect("EOF once drained")
        .unwrap();
        assert!(closed.is_none());
        assert_eq!(input.len(), total - MAX_BUFFERED_BYTES);
        assert!(
            input
                .iter()
                .enumerate()
                .all(|(index, byte)| *byte == pattern(MAX_BUFFERED_BYTES + index))
        );
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn peer_eof_while_waiting_drops_the_response_and_keeps_pipelined_bytes() {
        let (mut client, mut stream) = loopback().await;
        let pipelined = pipeline(&[&[b"GET", b"k"], &[b"SET", b"k", b"\r\n"]]);
        let mut subscription = Arc::new(pubsub::Hub::default()).attach(1);
        let state = state::State::new(0, 1, false);
        let (mut input, mut out) = (BytesMut::new(), Output::new(&state, "test"));
        let (held, released) = oneshot::channel::<()>();
        let sender = async {
            send_fragmented(&mut client, &pipelined).await;
            client.shutdown().await.unwrap();
        };
        let waiting = response_or_disconnect(
            &mut stream,
            &mut input,
            async move {
                let _held = held;
                std::future::pending::<io::Result<()>>().await
            },
            &mut subscription,
            (&mut out, Version::Resp3),
            None,
        );
        let ((), closed) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(sender, waiting)
        })
        .await
        .expect("EOF observed");
        assert!(closed.unwrap().is_none());
        assert!(
            released.await.is_err(),
            "a disconnect drops the waiting response"
        );
        assert_eq!(&input[..], &pipelined[..]);
    }

    /// A reply as these tests read it: maps flatten to keys and values, and
    /// every other aggregate to its members.
    #[derive(Clone, Debug, PartialEq)]
    enum Value {
        Line(Vec<u8>),
        Bulk(Option<Vec<u8>>),
        Many(Vec<Value>),
    }

    /// The complete RESP2 or RESP3 reply at the front of `bytes`, and its length.
    fn value(bytes: &[u8]) -> Option<(Value, usize)> {
        let end = bytes.windows(2).position(|pair| pair == b"\r\n")?;
        let (marker, line, mut at) = (bytes[0], &bytes[1..end], end + 2);
        let length = || std::str::from_utf8(line).ok()?.parse::<i64>().ok();
        match marker {
            b'$' | b'=' => match usize::try_from(length()?) {
                Ok(length) => {
                    let data = bytes.get(at..at + length + 2)?;
                    Some((Value::Bulk(Some(data[..length].to_vec())), at + length + 2))
                }
                Err(_) => Some((Value::Bulk(None), at)),
            },
            b'*' | b'~' | b'>' | b'%' => {
                let Ok(count) = usize::try_from(length()?) else {
                    return Some((Value::Bulk(None), at));
                };
                let count = if marker == b'%' { 2 * count } else { count };
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    let (member, used) = value(&bytes[at..])?;
                    members.push(member);
                    at += used;
                }
                Some((Value::Many(members), at))
            }
            _ => Some((Value::Line(line.to_vec()), at)),
        }
    }

    /// Every reply in `bytes`, or `None` while the last one is incomplete.
    fn values(mut bytes: &[u8]) -> Option<Vec<Value>> {
        let mut replies = Vec::new();
        while !bytes.is_empty() {
            let (reply, used) = value(bytes)?;
            replies.push(reply);
            bytes = &bytes[used..];
        }
        Some(replies)
    }

    async fn read_values(client: &mut tokio::net::TcpStream, count: usize) -> Vec<Value> {
        let received = read_until(client, |received| {
            values(received).is_some_and(|replies| replies.len() >= count)
        })
        .await;
        values(&received).expect("complete replies")
    }

    /// One request's arguments.
    fn request<'a>(args: &[&'a [u8]]) -> Vec<&'a [u8]> {
        args.to_vec()
    }

    fn requests(all: &[Vec<&[u8]>]) -> Vec<u8> {
        all.iter().flat_map(|args| frame(args)).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn earlier_replies_reach_the_client_before_a_following_blpop_waits() {
        let mut server = Harness::start(2).await;
        let mut consumer = server.connect().await;
        consumer
            .write_all(&pipeline(&[
                &[b"SET", b"k", b"v"],
                &[b"GET", b"k"],
                &[b"BLPOP", b"jobs", b"0"],
            ]))
            .await
            .unwrap();
        // Delivered while BLPOP waits for another client, not held behind it.
        expect_replies(&mut consumer, b"+OK\r\n$1\r\nv\r\n").await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while server.state.metrics.snapshot().blocked_clients != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the consumer blocks");
        let mut producer = server.connect().await;
        producer
            .write_all(&frame(&[b"RPUSH", b"jobs", b"x"]))
            .await
            .unwrap();
        expect_replies(&mut producer, b":1\r\n").await;
        expect_replies(&mut consumer, b"*2\r\n$4\r\njobs\r\n$1\r\nx\r\n").await;
        drop((consumer, producer));
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_partial_next_request_never_holds_back_earlier_replies() {
        let mut server = Harness::start(2).await;
        let mut client = server.connect().await;
        let mut sent = pipeline(&[&[b"SET", b"a", b"1"], &[b"GET", b"a"], &[b"GET", b"a"]]);
        // The last request still lacks its final argument.
        let rest = sent.split_off(sent.len() - 3);
        client.write_all(&sent).await.unwrap();
        expect_replies(&mut client, b"+OK\r\n$1\r\n1\r\n").await;
        client.write_all(&rest).await.unwrap();
        expect_replies(&mut client, b"$1\r\n1\r\n").await;
        // Nor does one spanning many reads, whose rest the client sends only
        // once the earlier reply arrives.
        let large = vec![b'l'; 1024 * 1024];
        let mut sent = pipeline(&[&[b"SET", b"a", b"2"], &[b"SET", b"large", large.as_slice()]]);
        let rest = sent.split_off(100 * 1024);
        client.write_all(&sent).await.unwrap();
        expect_replies(&mut client, b"+OK\r\n").await;
        client.write_all(&rest).await.unwrap();
        expect_replies(&mut client, b"+OK\r\n").await;
        drop(client);
        // A client that half-closes inside a request still receives every
        // earlier reply before the connection ends. Exactly one read's worth
        // is sent, so the read after the first request may find the EOF.
        let mut client = server.connect().await;
        let value = vec![b'v'; 5000];
        let mut sent = pipeline(&[
            &[b"SET", b"b", value.as_slice()],
            &[b"SET", b"c", value.as_slice()],
        ]);
        sent.truncate(8192);
        client.write_all(&sent).await.unwrap();
        client.shutdown().await.unwrap();
        expect_replies(&mut client, b"+OK\r\n").await;
        let mut unexpected = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut unexpected))
            .await
            .expect("EOF within the deadline")
            .unwrap();
        assert!(
            unexpected.is_empty(),
            "replied to an incomplete request: {:?}",
            Bytes::from(unexpected)
        );
        drop(client);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipelined_requests_straddling_reads_keep_their_own_arguments() {
        let mut server = Harness::start(2).await;
        let mut client = server.connect().await;
        let keys: Vec<Vec<u8>> = (0..40).map(|i| format!("key-{i}").into_bytes()).collect();
        let values: Vec<Vec<u8>> = (0..40)
            .map(|i| format!("{i:04}").repeat(1024).into_bytes())
            .collect();
        // About half a read each, so most end in a later read than they start
        // while replies to earlier ones are still undelivered.
        let sets: Vec<Vec<&[u8]>> = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| request(&[b"SET", key.as_slice(), value.as_slice()]))
            .collect();
        client.write_all(&requests(&sets)).await.unwrap();
        expect_replies(&mut client, &b"+OK\r\n".repeat(keys.len())).await;
        // Requests with fewer arguments keep only their own.
        let gets: Vec<Vec<&[u8]>> = keys
            .iter()
            .map(|key| request(&[b"GET", key.as_slice()]))
            .collect();
        client.write_all(&requests(&gets)).await.unwrap();
        let expected: Vec<u8> = values
            .iter()
            .flat_map(|value| {
                [
                    format!("${}\r\n", value.len()).as_bytes(),
                    value.as_slice(),
                    b"\r\n",
                ]
                .concat()
            })
            .collect();
        expect_replies(&mut client, &expected).await;
        drop(client);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipelined_replies_match_unpipelined_ones_across_buffer_boundaries() {
        let mut server = Harness::start(4).await;
        let elements: Vec<Vec<u8>> = (0..3000)
            .map(|i| format!("element-{i}").into_bytes())
            .collect();
        let big = vec![b'b'; 2 * output::OUTPUT_BUFFER_BYTES];
        let edge = vec![b'e'; output::BULK_COPY_BYTES - 40];
        let mut writes: Vec<Vec<&[u8]>> = elements
            .chunks(1000)
            .map(|chunk| {
                let mut args = request(&[b"RPUSH", b"list"]);
                args.extend(chunk.iter().map(Vec::as_slice));
                args
            })
            .collect();
        writes.extend([
            request(&[b"SET", b"big", &big]),
            request(&[b"SET", b"edge", &edge]),
            request(&[b"SET", b"small", b"s"]),
            request(&[b"HSET", b"h", b"field", b"value"]),
            request(&[b"ZADD", b"z", b"1", b"a", b"2.5", b"b"]),
        ]);
        let mut setup = server.connect().await;
        setup.write_all(&requests(&writes)).await.unwrap();
        read_values(&mut setup, writes.len()).await;
        // Aggregates of small members, payloads written straight from their
        // `Bytes`, and replies ending near the buffer edge, in one pipeline.
        let reads = [
            request(&[b"GET", b"edge"]),
            request(&[b"LRANGE", b"list", b"0", b"-1"]),
            request(&[b"GET", b"big"]),
            request(&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]),
            request(&[b"HGETALL", b"h"]),
            request(&[b"MGET", b"big", b"missing", b"small"]),
            request(&[b"GET", b"small"]),
            request(&[b"LRANGE", b"list", b"0", b"99"]),
            request(&[b"EXISTS", b"small", b"big", b"missing"]),
        ];
        for resp3 in [false, true] {
            let mut unpipelined = server.connect().await;
            let mut pipelined = server.connect().await;
            if resp3 {
                for client in [&mut unpipelined, &mut pipelined] {
                    client.write_all(&frame(&[b"HELLO", b"3"])).await.unwrap();
                    read_values(client, 1).await;
                }
            }
            let mut expected = Vec::new();
            for args in &reads {
                unpipelined.write_all(&frame(args)).await.unwrap();
                expected.extend(
                    read_until(&mut unpipelined, |received| {
                        values(received).is_some_and(|replies| replies.len() == 1)
                    })
                    .await,
                );
            }
            assert!(expected.len() > 4 * output::OUTPUT_BUFFER_BYTES);
            pipelined.write_all(&requests(&reads)).await.unwrap();
            expect_replies(&mut pipelined, &expected).await;
            drop((unpipelined, pipelined));
        }
        drop(setup);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipelined_commands_are_observed_with_their_own_database_and_name() {
        let mut server = Harness::start(2).await;
        server.observe();
        let mut monitor = server.connect().await;
        monitor.write_all(&frame(&[b"MONITOR"])).await.unwrap();
        expect_replies(&mut monitor, b"+OK\r\n").await;
        let mut client = server.connect().await;
        let address = client.local_addr().unwrap().to_string();
        client
            .write_all(&pipeline(&[
                &[b"SET", b"a", b"1"],
                &[b"CLIENT", b"SETNAME", b"first"],
                &[b"GET", b"a"],
                &[b"SELECT", b"1"],
                &[b"SET", b"a", b"2"],
                &[b"CLIENT", b"SETNAME", b"second"],
                &[b"GET", b"a"],
                &[b"PING"],
            ]))
            .await
            .unwrap();
        // PING runs only once every earlier command was delivered and observed.
        expect_replies(
            &mut client,
            b"+OK\r\n+OK\r\n$1\r\n1\r\n+OK\r\n+OK\r\n+OK\r\n$1\r\n2\r\n+PONG\r\n",
        )
        .await;
        let monitored = [
            format!("[0 {address}] \"SET\" \"a\" \"1\""),
            format!("[0 {address}] \"GET\" \"a\""),
            format!("[1 {address}] \"SET\" \"a\" \"2\""),
            format!("[1 {address}] \"GET\" \"a\""),
        ];
        let lines = read_until(&mut monitor, |received| {
            String::from_utf8_lossy(received).contains(&monitored[3])
        })
        .await;
        let lines: Vec<String> = String::from_utf8(lines)
            .unwrap()
            .lines()
            .filter(|line| line.contains(" \"GET\" ") || line.contains(" \"SET\" "))
            .map(|line| line.split_once(' ').expect("a timestamp").1.to_owned())
            .collect();
        assert_eq!(lines, monitored);
        let mut reader = server.connect().await;
        reader
            .write_all(&frame(&[b"SLOWLOG", b"GET", b"-1"]))
            .await
            .unwrap();
        let log = read_values(&mut reader, 1).await;
        let [Value::Many(entries)] = log.as_slice() else {
            panic!("a slow log: {log:?}");
        };
        let observed: Vec<(String, String)> = entries
            .iter()
            .filter_map(|entry| {
                let Value::Many(fields) = entry else {
                    return None;
                };
                let [
                    _,
                    _,
                    _,
                    Value::Many(args),
                    Value::Bulk(Some(from)),
                    Value::Bulk(Some(name)),
                ] = fields.as_slice()
                else {
                    return None;
                };
                let args: Vec<String> = args
                    .iter()
                    .map(|arg| match arg {
                        Value::Bulk(Some(arg)) => String::from_utf8_lossy(arg).into_owned(),
                        other => panic!("an argument: {other:?}"),
                    })
                    .collect();
                (from.as_slice() == address.as_bytes() && args[0] != "PING")
                    .then(|| (args.join(" "), String::from_utf8_lossy(name).into_owned()))
            })
            .collect();
        // Newest first.
        let expected = [
            ("GET a", "second"),
            ("CLIENT SETNAME second", "second"),
            ("SET a 2", "first"),
            ("SELECT 1", "first"),
            ("GET a", "first"),
            ("CLIENT SETNAME first", "first"),
            ("SET a 1", ""),
        ];
        assert_eq!(
            observed,
            expected.map(|(args, name)| (args.to_owned(), name.to_owned()))
        );
        // Other observed commands can still be queued on MONITOR. Drain the
        // server's orderly close instead of resetting a socket with unread data.
        monitor.write_all(&frame(&[b"QUIT"])).await.unwrap();
        let mut closing = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), monitor.read_to_end(&mut closing))
            .await
            .expect("monitor closes after QUIT")
            .unwrap();
        assert!(closing.ends_with(b"+OK\r\n"));
        drop((monitor, client, reader));
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipelined_commands_report_their_own_bytes_and_cpu() {
        use std::collections::HashMap;
        let mut server = Harness::start(2).await;
        server.observe();
        let mut client = server.connect().await;
        client
            .write_all(&pipeline(&[&[b"HELLO", b"3"], &[b"SUBSCRIBE", b"news"]]))
            .await
            .unwrap();
        read_values(&mut client, 2).await;
        let alpha = vec![b'a'; 20_000];
        let elements: Vec<Vec<u8>> = (0..50_000).map(|i| i.to_string().into_bytes()).collect();
        let mut writes: Vec<Vec<&[u8]>> = elements
            .chunks(1000)
            .map(|chunk| {
                let mut args = request(&[b"RPUSH", b"heavy"]);
                args.extend(chunk.iter().map(Vec::as_slice));
                args
            })
            .collect();
        let start: &[&[u8]] = &[b"HOTKEYS", b"START", b"METRICS", b"2", b"CPU", b"NET"];
        writes.extend([
            request(&[b"SET", b"alpha", &alpha]),
            request(&[b"SET", b"light", b"x"]),
            request(start),
        ]);
        client.write_all(&requests(&writes)).await.unwrap();
        read_values(&mut client, writes.len()).await;
        // A push lands among these replies; it is no command's output.
        let measured = [
            request(&[b"GET", b"alpha"]),
            request(&[b"PUBLISH", b"news", b"hello"]),
            request(&[b"GET", b"alpha"]),
            request(&[b"GET", b"light"]),
            request(&[b"LPOS", b"heavy", b"missing"]),
            request(&[b"GET", b"light"]),
        ];
        client.write_all(&requests(&measured)).await.unwrap();
        let mut replies = read_values(&mut client, measured.len() + 1).await;
        let message = Value::Many(
            [b"message".as_slice(), b"news", b"hello"]
                .map(|part| Value::Bulk(Some(part.to_vec())))
                .to_vec(),
        );
        let pushed = replies
            .iter()
            .position(|reply| *reply == message)
            .expect("the publication");
        assert!((1..=2).contains(&pushed), "{replies:?}");
        replies.remove(pushed);
        let (alpha_value, light) = (
            Value::Bulk(Some(alpha.clone())),
            Value::Bulk(Some(b"x".to_vec())),
        );
        assert_eq!(
            replies,
            [
                alpha_value.clone(),
                Value::Line(b"1".to_vec()),
                alpha_value,
                light.clone(),
                Value::Line(Vec::new()),
                light,
            ]
        );
        client
            .write_all(&frame(&[b"HOTKEYS", b"GET"]))
            .await
            .unwrap();
        let report = read_values(&mut client, 1).await;
        let [Value::Many(sessions)] = report.as_slice() else {
            panic!("a hotkeys report: {report:?}");
        };
        let [Value::Many(fields)] = sessions.as_slice() else {
            panic!("one hotkeys session: {report:?}");
        };
        let field = |name: &str| {
            let at = fields
                .iter()
                .position(|field| *field == Value::Bulk(Some(name.as_bytes().to_vec())))
                .unwrap_or_else(|| panic!("no {name} in {report:?}"));
            &fields[at + 1]
        };
        let number = |value: &Value| match value {
            Value::Line(digits) => String::from_utf8_lossy(digits).parse::<u64>().unwrap(),
            other => panic!("a number: {other:?}"),
        };
        let per_key = |name: &str| -> HashMap<Vec<u8>, u64> {
            let Value::Many(pairs) = field(name) else {
                panic!("{name} lists keys");
            };
            pairs
                .chunks(2)
                .map(|pair| match pair {
                    [Value::Bulk(Some(key)), count] => (key.clone(), number(count)),
                    other => panic!("a key and its count: {other:?}"),
                })
                .collect()
        };
        // Each command counts its own request and reply bytes exactly.
        let size = |args: &[&[u8]], reply: usize| (frame(args).len() + reply) as u64;
        let alpha_reply = format!("${}\r\n", alpha.len()).len() + alpha.len() + 2;
        let net = HashMap::from([
            (
                b"alpha".to_vec(),
                2 * size(&[b"GET", b"alpha"], alpha_reply),
            ),
            (b"light".to_vec(), 2 * size(&[b"GET", b"light"], 7)),
            (b"heavy".to_vec(), size(&[b"LPOS", b"heavy", b"missing"], 3)),
        ]);
        assert_eq!(per_key("by-net-bytes"), net);
        let keyless = size(&[b"PUBLISH", b"news", b"hello"], 4) + size(start, 5);
        assert_eq!(
            number(field("net-bytes-all-commands-all-slots")),
            net.values().sum::<u64>() + keyless
        );
        // CPU is read before the next command runs: the light reads never
        // absorb the scan pipelined between them.
        let cpu = per_key("by-cpu-time-us");
        assert!(
            4 * cpu[b"light".as_slice()] < cpu[b"heavy".as_slice()],
            "{cpu:?}"
        );
        drop(client);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_hotkeys_session_after_stop_and_reset_still_reports_cpu() {
        let mut server = Harness::start(2).await;
        server.observe();
        let mut client = server.connect().await;
        client.write_all(&frame(&[b"HELLO", b"3"])).await.unwrap();
        read_values(&mut client, 1).await;
        let elements: Vec<Vec<u8>> = (0..50_000).map(|i| i.to_string().into_bytes()).collect();
        let mut writes: Vec<Vec<&[u8]>> = elements
            .chunks(1000)
            .map(|chunk| {
                let mut args = request(&[b"RPUSH", b"heavy"]);
                args.extend(chunk.iter().map(Vec::as_slice));
                args
            })
            .collect();
        // Each ended session leaves nothing reporting CPU; the next one
        // measures the commands it tracks again.
        let start: &[&[u8]] = &[b"HOTKEYS", b"START", b"METRICS", b"1", b"CPU"];
        writes.extend([
            request(start),
            request(&[b"HOTKEYS", b"STOP"]),
            request(start),
            request(&[b"HOTKEYS", b"RESET"]),
            request(start),
            request(&[b"LPOS", b"heavy", b"missing"]),
        ]);
        client.write_all(&requests(&writes)).await.unwrap();
        let replies = read_values(&mut client, writes.len()).await;
        assert_eq!(
            replies.last(),
            Some(&Value::Line(Vec::new())),
            "{replies:?}"
        );
        client
            .write_all(&frame(&[b"HOTKEYS", b"GET"]))
            .await
            .unwrap();
        let report = read_values(&mut client, 1).await;
        let [Value::Many(sessions)] = report.as_slice() else {
            panic!("a hotkeys report: {report:?}");
        };
        let [Value::Many(fields)] = sessions.as_slice() else {
            panic!("one hotkeys session: {report:?}");
        };
        let field = |name: &str| {
            let at = fields
                .iter()
                .position(|field| *field == Value::Bulk(Some(name.as_bytes().to_vec())))
                .unwrap_or_else(|| panic!("no {name} in {report:?}"));
            &fields[at + 1]
        };
        let number = |value: &Value| match value {
            Value::Line(digits) => String::from_utf8_lossy(digits).parse::<u64>().unwrap(),
            other => panic!("a number: {other:?}"),
        };
        assert!(number(field("all-commands-all-slots-us")) > 0, "{report:?}");
        let Value::Many(pairs) = field("by-cpu-time-us") else {
            panic!("by-cpu-time-us lists keys: {report:?}");
        };
        let [Value::Bulk(Some(key)), cpu] = pairs.as_slice() else {
            panic!("only the tracked key: {report:?}");
        };
        assert_eq!(key.as_slice(), b"heavy");
        assert!(number(cpu) > 0, "{report:?}");
        drop(client);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn earlier_replies_arrive_while_a_later_command_waits_for_its_shard() {
        let mut server = Harness::start(2).await;
        let pool = server.pool();
        let shard = |key: &[u8]| {
            let args = [
                Bytes::from_static(b"SET"),
                Bytes::copy_from_slice(key),
                Bytes::from_static(b"v"),
            ];
            pool.inline_shard(&engine::BoundCommand::bind(&args), &server.state)
                .expect("a memory-backed SET runs inline")
        };
        // Maintenance acquires cells in ascending order. Hold the first cell so
        // a maintenance pause cannot hold the free cell while waiting for us.
        let busy = (0..)
            .map(|i| format!("busy{i}").into_bytes())
            .find(|key| shard(key) == 0)
            .unwrap();
        let free = (0..)
            .map(|i| format!("free{i}").into_bytes())
            .find(|key| shard(key) != 0)
            .unwrap();
        let held = tokio::time::timeout(Duration::from_secs(10), pool.lease(0))
            .await
            .expect("the shard frees up")
            .unwrap();
        let mut client = server.connect().await;
        // The write passes the write gate before it finds its shard busy.
        client
            .write_all(&pipeline(&[
                &[b"GET", &free],
                &[b"SET", &busy, b"v"],
                &[b"GET", &busy],
            ]))
            .await
            .unwrap();
        expect_replies(&mut client, b"$-1\r\n").await;
        drop(held);
        expect_replies(&mut client, b"+OK\r\n$1\r\nv\r\n").await;
        drop((client, pool));
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pause_begun_while_a_busy_shard_command_delivers_earlier_replies_holds_it() {
        let shard = |key: &[u8]| Engine::shard_for(&Bytes::copy_from_slice(key), 2);
        // Maintenance acquires cells in ascending order. Holding the first cell
        // keeps a maintenance pause from holding the free cell while it waits.
        let busy = (0..)
            .map(|i| format!("busy{i}").into_bytes())
            .find(|key| shard(key) == 0)
            .unwrap();
        let pad = (0..)
            .map(|i| format!("pad{i}").into_bytes())
            .find(|key| shard(key) != 0)
            .unwrap();
        // About the largest reply encoded without a socket write: it is still
        // undelivered when the next command runs.
        let padding = vec![b'p'; output::BULK_COPY_BYTES - 64];
        let padded = [
            format!("${}\r\n", padding.len()).as_bytes(),
            padding.as_slice(),
            b"\r\n",
        ]
        .concat();
        let earlier = request(&[b"GETRANGE", &pad, b"0", b"-1"]);
        // The CLIENT PAUSE mode, the commands it must hold, and their replies
        // when they run after a write made under the released shard while paused.
        for (mode, paused, unpaused) in [
            (
                b"WRITE".as_slice(),
                vec![request(&[b"SET", &busy, b"v"]), request(&[b"GET", &busy])],
                b"+OK\r\n$1\r\nv\r\n".as_slice(),
            ),
            (
                b"ALL".as_slice(),
                vec![request(&[b"GET", &busy])],
                b"$6\r\nmarker\r\n".as_slice(),
            ),
        ] {
            let mut server = Harness::start(2).await;
            let pool = server.pool();
            let inline = |args: &[&[u8]]| {
                let args: Vec<Bytes> = args.iter().copied().map(Bytes::copy_from_slice).collect();
                pool.inline_shard(&engine::BoundCommand::bind(&args), &server.state)
            };
            assert_eq!(inline(&earlier), Some(shard(&pad)));
            assert_eq!(inline(&paused[0]), Some(0));
            let mut setup = server.connect().await;
            setup
                .write_all(&frame(&[b"SET", &pad, &padding]))
                .await
                .unwrap();
            expect_replies(&mut setup, b"+OK\r\n").await;
            // Joined, the setup connection has completed every command it ran.
            drop(setup);
            let served = tokio::time::timeout(Duration::from_secs(10), server.clients.join_next())
                .await
                .expect("the setup connection ends");
            served.expect("one connection").unwrap().unwrap();
            let mut pauser = server.connect().await;
            let held = tokio::time::timeout(Duration::from_secs(10), pool.lease(0))
                .await
                .expect("the shard frees up")
                .unwrap();
            // This connection runs only when polled here, so each phase is exact.
            // A small fixed receive window bounds what the kernel still accepts
            // once the path to the unread client is full.
            let listener = tokio::net::TcpSocket::new_v4().unwrap();
            listener.set_send_buffer_size(4096).unwrap();
            listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let listener = listener.listen(1).unwrap();
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.set_recv_buffer_size(4096).unwrap();
            let mut client = socket
                .connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            client.set_nodelay(true).unwrap();
            let (accepted, _) = listener.accept().await.unwrap();
            accepted.writable().await.unwrap();
            let filler = vec![b'f'; 64 * 1024];
            let mut filled = 0;
            loop {
                loop {
                    match accepted.try_write(&filler) {
                        Ok(written) => {
                            assert!(written > 0, "the live socket accepts a nonempty write");
                            filled += written;
                            assert!(
                                filled <= 16 * 1024 * 1024,
                                "the socket becomes backpressured"
                            );
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => panic!("filling the socket: {error}"),
                    }
                }
                // The first WouldBlock can exhaust only the sender's buffer:
                // TCP acknowledgements still open space in the unread peer's
                // receive window. Fill that window before driving the server.
                match tokio::time::timeout(Duration::from_millis(250), accepted.writable()).await {
                    Ok(ready) => ready.unwrap(),
                    Err(_) => break,
                }
            }
            let mut connection = Box::pin(tokio::task::coop::unconstrained(serve(
                Stream::Plain(accepted),
                server.tx.clone(),
                server.state.clone(),
                None,
            )));
            let completed = server.state.metrics.snapshot().commands_total;
            let mut commands = vec![earlier.clone()];
            commands.extend(paused.iter().cloned());
            client.write_all(&requests(&commands)).await.unwrap();
            // Admitted before any pause, the command finds its shard busy behind
            // the earlier reply, and delivering that reply waits for the client.
            let name = String::from_utf8_lossy(paused[0][0]).to_ascii_lowercase();
            let running = format!(" cmd={name} ");
            tokio::time::timeout(
                Duration::from_secs(10),
                std::future::poll_fn(|cx| {
                    park(connection.as_mut(), cx);
                    let clients = server.state.client_registry.list(None, None);
                    if String::from_utf8_lossy(&clients).contains(&running) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
            )
            .await
            .expect("the command reaches its busy shard");
            assert_eq!(
                server.state.metrics.snapshot().commands_total,
                completed,
                "the {name} waits on its busy shard with the reply before it undelivered; \
                 filled={filled}, clients={}",
                String::from_utf8_lossy(&server.state.client_registry.list(None, None))
            );
            // The pause begins during that delivery; then the client reads it all.
            pauser
                .write_all(&frame(&[b"CLIENT", b"PAUSE", b"600000", mode]))
                .await
                .unwrap();
            expect_replies(&mut pauser, b"+OK\r\n").await;
            let mut received = Vec::new();
            alongside(
                connection.as_mut(),
                tokio::time::timeout(
                    Duration::from_secs(10),
                    fill(&mut client, &mut received, filled + padded.len()),
                ),
            )
            .await
            .expect("the earlier reply is delivered");
            assert!(received[..filled].iter().all(|&byte| byte == b'f'));
            assert!(
                received[filled..] == padded[..],
                "only the earlier reply follows"
            );
            // Queued before the shard is released, this write runs after any
            // command already waiting for the shard, and before any later one.
            // The wait borrows `pool`, so it ends with this block, before the
            // cleanup below drops `pool`.
            let lease = {
                let queued = tokio::time::timeout(Duration::from_secs(10), pool.lease(0));
                let mut queued = std::pin::pin!(queued);
                std::future::poll_fn(|cx| {
                    assert!(queued.as_mut().poll(cx).is_pending(), "the shard is held");
                    Poll::Ready(())
                })
                .await;
                drop(held);
                alongside(connection.as_mut(), queued)
                    .await
                    .expect("the shard frees up")
                    .unwrap()
            };
            let marker =
                [b"SET".as_slice(), busy.as_slice(), b"marker"].map(Bytes::copy_from_slice);
            let write = request::SingleKey {
                command: engine::BoundCommand::bind(&marker),
                origin: request::Origin::Client(Version::Resp2),
                user: None,
                session: None,
                write_permit: None,
                cluster_client: None,
            };
            let sharding::Ran::Done(marked) = lease.run(write, &server.state).unwrap() else {
                panic!("a memory-backed SET commits on its lease");
            };
            assert_eq!(marked, Resp::Simple("OK"));
            pauser
                .write_all(&frame(&[b"CLIENT", b"UNPAUSE"]))
                .await
                .unwrap();
            expect_replies(&mut pauser, b"+OK\r\n").await;
            let replies = values(unpaused).expect("complete replies").len();
            let received = alongside(
                connection.as_mut(),
                read_until(&mut client, |received| {
                    values(received).is_some_and(|got| got.len() >= replies)
                }),
            )
            .await;
            assert_eq!(
                Bytes::from(received),
                Bytes::copy_from_slice(unpaused),
                "CLIENT PAUSE {} let a command run once it delivered the replies \
                 before it and found its shard free",
                String::from_utf8_lossy(mode)
            );
            drop(connection);
            drop((client, pauser, pool));
            server.shutdown().await;
        }
    }

    /// A server started without `--data-dir`, served exactly as `main` serves
    /// it, through its real coordinator snapshot barrier.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fresh_memory_server_starts_native_history_at_its_first_replica() {
        /// Takes one CRLF line, leaving any bytes after it buffered.
        async fn line(client: &mut tokio::net::TcpStream, buffer: &mut Vec<u8>) -> String {
            loop {
                if let Some(end) = buffer.windows(2).position(|pair| pair == b"\r\n") {
                    let line: Vec<u8> = buffer.drain(..end + 2).collect();
                    return String::from_utf8(line[..end].to_vec()).unwrap();
                }
                let wanted = buffer.len() + 1;
                fill(client, buffer, wanted).await;
            }
        }
        async fn take(
            client: &mut tokio::net::TcpStream,
            buffer: &mut Vec<u8>,
            len: usize,
        ) -> Vec<u8> {
            fill(client, buffer, len).await;
            buffer.drain(..len).collect()
        }
        /// Sends PSYNC and returns its header and, for a full resync, the
        /// snapshot loaded into a fresh engine with its auxiliary fields.
        async fn psync(
            client: &mut tokio::net::TcpStream,
            buffer: &mut Vec<u8>,
            history: &str,
            next: u64,
        ) -> (Vec<String>, Option<(Engine, Vec<(Bytes, Bytes)>)>) {
            let next = next.to_string();
            client
                .write_all(&frame(&[b"PSYNC", history.as_bytes(), next.as_bytes()]))
                .await
                .unwrap();
            let header: Vec<String> = line(client, buffer)
                .await
                .split(' ')
                .map(str::to_owned)
                .collect();
            if header[0] != "+FULLRESYNC" {
                return (header, None);
            }
            let length = line(client, buffer).await;
            let length = length.strip_prefix('$').unwrap().parse().unwrap();
            let rdb = take(client, buffer, length).await;
            let mut image = Engine::new();
            let metadata = image
                .install_rdb(&mut std::io::Cursor::new(rdb), Engine::now())
                .unwrap();
            (header, Some((image, metadata.auxiliary)))
        }
        fn get(image: &mut Engine, key: &'static [u8]) -> Option<Bytes> {
            match image
                .execute(&mut crate::engine::BoundCommand::bind(&[
                    Bytes::from_static(b"GET"),
                    Bytes::from_static(key),
                ]))
                .unwrap()
            {
                engine::Execution::Reply(Resp::Bulk(value)) => value,
                _ => panic!("GET did not return a bulk value"),
            }
        }
        fn auxiliary<'a>(fields: &'a [(Bytes, Bytes)], key: &[u8]) -> &'a [u8] {
            fields
                .iter()
                .rev()
                .find(|(name, _)| name.as_ref() == key)
                .map(|(_, value)| value.as_ref())
                .unwrap()
        }
        fn commands(stream: &[u8]) -> Vec<Vec<Bytes>> {
            native_replication::decode_commands(Bytes::copy_from_slice(stream)).unwrap()
        }
        fn sets(commands: &[Vec<Bytes>], key: &[u8]) -> bool {
            commands.iter().any(|command| {
                command.len() == 3
                    && command[0].eq_ignore_ascii_case(b"SET")
                    && command[1].as_ref() == key
            })
        }
        async fn until(mut done: impl FnMut() -> bool) {
            while !done() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        let mut server = Harness::start_native(2).await;
        let native = server.state.native.get().unwrap().clone();
        tokio::time::timeout(Duration::from_secs(30), async move {
            let preactivation = native.status().history;
            assert!(!native.history_active());
            let mut writer = server.connect().await;
            writer
                .write_all(&pipeline(&[
                    &[b"SET", b"early", b"1"],
                    &[b"PUBLISH", b"events", b"payload"],
                    &[b"WAIT", b"1", b"10"],
                ]))
                .await
                .unwrap();
            expect_replies(&mut writer, b"+OK\r\n:0\r\n:0\r\n").await;
            // Neither the write, its publication nor WAIT's GETACK started
            // a stream under the preactivation identifier.
            assert!(!native.history_active());
            let inactive = native.status();
            assert_eq!(inactive.history, preactivation);
            assert_eq!(inactive.offset, 0);

            // Its holder must full-sync, and the first snapshot carries the
            // earlier write at the new history's offset zero.
            let mut first = server.connect().await;
            let mut first_stream = Vec::new();
            let (header, image) = psync(&mut first, &mut first_stream, &preactivation, 1).await;
            assert_eq!(
                header[0], "+FULLRESYNC",
                "preactivation identifier continued"
            );
            let history = header[1].clone();
            assert_ne!(history, preactivation);
            assert_eq!(header[2], "0");
            assert!(native.history_active());
            assert_eq!(native.status().history, history);
            let (mut image, metadata) = image.unwrap();
            assert_eq!(auxiliary(&metadata, b"repl-id"), history.as_bytes());
            assert_eq!(auxiliary(&metadata, b"repl-offset"), b"0");
            assert_eq!(get(&mut image, b"early"), Some(Bytes::from_static(b"1")));

            // `early` left the writer at watermark zero: a registered replica
            // meets it only after acknowledging its loaded snapshot.
            until(|| {
                let replicas = native.status().replicas;
                replicas.len() == 1 && !replicas[0].online
            })
            .await;
            writer
                .write_all(&frame(&[b"WAIT", b"1", b"50"]))
                .await
                .unwrap();
            expect_replies(&mut writer, b":0\r\n").await;
            first
                .write_all(&frame(&[b"REPLCONF", b"ACK", b"0"]))
                .await
                .unwrap();
            until(|| {
                native
                    .status()
                    .replicas
                    .iter()
                    .any(|replica| replica.online)
            })
            .await;
            writer
                .write_all(&frame(&[b"WAIT", b"1", b"0"]))
                .await
                .unwrap();
            expect_replies(&mut writer, b":1\r\n").await;
            // Processing a zero-offset snapshot proves no fsync: this replica
            // sent ACK, not FACK, for the earlier write.
            writer
                .write_all(&frame(&[b"WAITAOF", b"0", b"1", b"50"]))
                .await
                .unwrap();
            expect_replies(&mut writer, b"*2\r\n:0\r\n:0\r\n").await;

            // Later writes stream after the snapshot offset.
            writer
                .write_all(&frame(&[b"SET", b"late", b"2"]))
                .await
                .unwrap();
            expect_replies(&mut writer, b"+OK\r\n").await;
            let end = native.status().offset;
            let streamed = take(&mut first, &mut first_stream, end as usize).await;
            let streamed_commands = commands(&streamed);
            assert!(sets(&streamed_commands, b"late"));
            assert!(!sets(&streamed_commands, b"early"));
            writer
                .write_all(&frame(&[b"WAIT", b"1", b"50"]))
                .await
                .unwrap();
            expect_replies(&mut writer, b":0\r\n").await;
            let acknowledged = end.to_string();
            first
                .write_all(&frame(&[b"REPLCONF", b"ACK", acknowledged.as_bytes()]))
                .await
                .unwrap();
            writer
                .write_all(&frame(&[b"WAIT", b"1", b"0"]))
                .await
                .unwrap();
            expect_replies(&mut writer, b":1\r\n").await;

            // A replica of the activated history resumes from its first byte.
            let mut resumed = server.connect().await;
            let mut resumed_stream = Vec::new();
            let (header, image) = psync(&mut resumed, &mut resumed_stream, &history, 1).await;
            assert_eq!(header, ["+CONTINUE"]);
            assert!(image.is_none());
            let replayed = take(&mut resumed, &mut resumed_stream, end as usize).await;
            assert_eq!(Bytes::from(replayed), Bytes::from(streamed));

            // The preactivation identifier never continues, even now.
            let mut late = server.connect().await;
            let mut late_stream = Vec::new();
            let (header, image) = psync(&mut late, &mut late_stream, &preactivation, 1).await;
            assert_eq!(header[0], "+FULLRESYNC");
            assert_eq!(header[1], history);
            let (mut image, _) = image.unwrap();
            assert_eq!(get(&mut image, b"early"), Some(Bytes::from_static(b"1")));
            assert_eq!(get(&mut image, b"late"), Some(Bytes::from_static(b"2")));
            assert!(native.history_active());

            // Stop the downstream sessions before their sockets close. Each
            // already streamed bytes from its loop, so it observes the stop.
            writer
                .write_all(&frame(&[b"SET", b"final", b"3"]))
                .await
                .unwrap();
            expect_replies(&mut writer, b"+OK\r\n").await;
            let streamed_after_snapshot = late_stream.len() + 1;
            fill(&mut late, &mut late_stream, streamed_after_snapshot).await;
            native.shutdown();
            until(|| native.status().replicas.is_empty()).await;
            drop((writer, first, resumed, late));
            server.shutdown().await;
        })
        .await
        .expect("lazy native history regression within the deadline");
    }
}
