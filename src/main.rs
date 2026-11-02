mod admin;
mod auth;
mod backup;
mod blocking;
mod client_registry;
mod cluster;
mod connection;
mod engine;
mod executor;
mod journal;
mod management;
mod metrics;
mod migrate;
mod native_replication;
mod native_runtime;
mod options;
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
mod state;
mod storage;
mod transport;
mod wal;
mod watch;
mod write_gate;

use std::{
    future::Future,
    io,
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{BufMut, BytesMut};
use clap::Parser;
use connection::{Action, Connection};
use engine::Engine;
use options::{LocalCommand, Options, StorageProfile};
use protocol::{Resp, Version};
use request::{CommandRequest, Request};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
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
        result = &mut actor => return result.map_err(io::Error::other)?,
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
                let tx = tx.clone();
                let state = state.clone();
                let tls = tls.clone();
                let acl = acl.clone();
                clients.spawn(async move {
                    let result = async {
                        let stream = Stream::accept(stream,tls).await?;
                        serve(stream,tx,state,acl).await
                    }.await;
                    if let Err(error) = result
                        && !matches!(error.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof)
                    {
                        eprintln!("client {peer}: {error}");
                    }
                });
            }
        }
    };
    let _ = stop.send(true);
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
    actor_result?;
    cluster_result?;
    admin_result?;
    listener_result?;
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
    let metrics = state.metrics.clone();
    let mut connection = Connection::new(lease, state.clone(), acl);
    let auth_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut input = BytesMut::with_capacity(8192);
    let mut header = BytesMut::with_capacity(128);
    let mut pending = Vec::with_capacity(16);
    let mut cancelled = connection.subscription.cancellation();
    let mut monitor = None;
    let run = async {
        loop {
            for _ in 0..16 {
                let Some(mut message) = connection.subscription.try_recv() else {
                    break;
                };
                write_pubsub_message(
                    &mut stream,
                    &mut message,
                    &mut header,
                    &mut pending,
                    connection.version,
                )
                .await?;
            }
            if !connection.authenticated() && tokio::time::Instant::now() >= auth_deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "authentication deadline elapsed",
                ));
            }
            let buffered_bytes = input.len();
            match protocol::parse(&mut input) {
                Ok(Some(args)) => {
                    let observed = args.clone();
                    let started = Instant::now();
                    let cpu_start = connection
                        .session
                        .cpu_ns
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let mut timer = RequestTimer {
                        metrics: &metrics,
                        started,
                        error: true,
                    };
                    let input_bytes = buffered_bytes.saturating_sub(input.len()) as u64;
                    let mut close = false;
                    let bypass_pause = args
                        .first()
                        .is_some_and(|name| name.eq_ignore_ascii_case(b"CLIENT"))
                        && args.get(1).is_some_and(|sub| {
                            sub.eq_ignore_ascii_case(b"UNPAUSE")
                                || sub.eq_ignore_ascii_case(b"PAUSE")
                        });
                    if connection.authenticated() && !bypass_pause {
                        let write = Engine::is_pause_write(&args);
                        if response_or_disconnect(
                            &mut stream,
                            &mut input,
                            async {
                                state.client_registry.wait_unpaused(write).await;
                                Ok(())
                            },
                            &mut connection.subscription,
                            (&mut header, &mut pending, connection.version),
                            None,
                        )
                        .await?
                        .is_none()
                        {
                            return Ok(());
                        }
                    }
                    let mut record = CommandRecord {
                        args: &observed,
                        started: Instant::now(),
                        elapsed: None,
                        cpu_start,
                        input_bytes,
                    };
                    let action = connection.route(args);
                    if !matches!(&action, Action::Command(_, _) | Action::Transaction { .. }) {
                        connection.cluster.asking = false;
                    }
                    let mut response = match action {
                        Action::Reply(reply) => reply,
                        Action::NoReply => {
                            timer.error = false;
                            record.finish(&state, &connection, &address, 0, false);
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
                                    (&mut header, &mut pending, connection.version),
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
                        Action::Command(args, module) => {
                            match socket_command(
                                args,
                                module,
                                &mut connection,
                                &state,
                                &tx,
                                &mut stream,
                                &mut input,
                                &mut header,
                                &mut pending,
                                peer,
                            )
                            .await?
                            {
                                SocketReply::Reply(reply) => reply,
                                SocketReply::Suppressed => {
                                    connection.cluster.asking = false;
                                    timer.error = false;
                                    record.finish(&state, &connection, &address, 0, false);
                                    continue;
                                }
                                SocketReply::Closed => return Ok(()),
                            }
                        }
                        Action::Watch(keys) => {
                            let admission = match state.cluster.get() {
                                Some(cluster) => cluster
                                    .admit(&keys, false, false, &mut connection.cluster)
                                    .await
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
                                    (&mut header, &mut pending, connection.version),
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
                            let modules = if let Some(server) = state.server.get() {
                                commands
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(index, args)| {
                                        if args
                                            .first()
                                            .is_some_and(|name| Engine::command_exists(name))
                                        {
                                            return None;
                                        }
                                        match server.extensions().prepare_sync(args) {
                                            Some(Ok(invocation)) => Some((index, invocation)),
                                            _ => None,
                                        }
                                    })
                                    .collect::<Vec<_>>()
                            } else {
                                Vec::new()
                            };
                            let write =
                                commands.iter().any(|args| {
                                    Engine::is_dataset_write(args)
                                        || args.first().is_some_and(|name| {
                                            name.eq_ignore_ascii_case(b"SPUBLISH")
                                        })
                                }) || modules.iter().any(|(_, invocation)| invocation.is_write());
                            let pause_write =
                                commands.iter().any(|args| Engine::is_pause_write(args))
                                    || modules.iter().any(|(_, invocation)| invocation.is_write());
                            if response_or_disconnect(
                                &mut stream,
                                &mut input,
                                async {
                                    state.client_registry.wait_unpaused(pause_write).await;
                                    Ok(())
                                },
                                &mut connection.subscription,
                                (&mut header, &mut pending, connection.version),
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
                                    (&mut header, &mut pending, connection.version),
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
                                    let mut keys = commands
                                        .iter()
                                        .flat_map(|args| {
                                            if args.first().is_some_and(|name| {
                                                name.eq_ignore_ascii_case(b"SPUBLISH")
                                            }) {
                                                args.get(1).cloned().into_iter().collect::<Vec<_>>()
                                            } else {
                                                Engine::accessed_keys(args)
                                            }
                                        })
                                        .collect::<Vec<_>>();
                                    keys.extend(modules.iter().flat_map(|(_, invocation)| {
                                        invocation.keys().iter().cloned()
                                    }));
                                    cluster
                                        .admit(&keys, write, false, &mut connection.cluster)
                                        .await
                                        .map(|_| ())
                                } else {
                                    Ok(())
                                };
                            if let Err(reply) = admission {
                                reply
                            } else {
                                let session = connection.session.clone();
                                let version = connection.version;
                                let debugging = !modules.is_empty()
                                    || commands.iter().any(|args| {
                                        args.first().is_some_and(|name| {
                                            [
                                                b"EVAL".as_slice(),
                                                b"EVALSHA",
                                                b"EVAL_RO",
                                                b"EVALSHA_RO",
                                            ]
                                            .iter()
                                            .any(|command| name.eq_ignore_ascii_case(command))
                                        })
                                    });
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
                                    (&mut header, &mut pending, version),
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
                    timer.error = matches!(&response, Resp::Error(_));
                    connection
                        .control
                        .set_database(connection.session.database());
                    if !timer.error && observed.first().is_some_and(|name| name.eq_ignore_ascii_case(b"HELLO"))
                        && let (Resp::Map(fields), Some(server)) = (&mut response, state.server.get())
                        && let Some(modules) = server.extensions().execute(&[
                            bytes::Bytes::from_static(b"MODULE"), bytes::Bytes::from_static(b"LIST")
                        ]).await
                        && let Some((_, value)) = fields.iter_mut().find(|(key, _)| matches!(key, Resp::Bulk(Some(name)) if name.as_ref() == b"modules")) {
                        *value = modules;
                    }
                    if timer.error
                        || observed
                            .first()
                            .is_none_or(|name| !name.eq_ignore_ascii_case(b"ASKING"))
                    {
                        connection.cluster.asking = false;
                    }
                    if observed
                        .first()
                        .is_some_and(|name| name.eq_ignore_ascii_case(b"MONITOR"))
                        && !timer.error
                        && observed.len() == 1
                    {
                        monitor = state.server.get().map(|server| server.monitor());
                    }
                    if observed
                        .first()
                        .is_some_and(|name| name.eq_ignore_ascii_case(b"RESET"))
                    {
                        monitor = None;
                    }
                    record.executed();
                    let output_bytes = if connection.take_reply_permission() {
                        let result = auth_io(
                            connection.authenticated(),
                            auth_deadline,
                            write_response(
                                &mut stream,
                                response,
                                &mut header,
                                &mut pending,
                                connection.version,
                            ),
                        )
                        .await;
                        timer.error |= result.is_err();
                        result?
                    } else {
                        0
                    };
                    record.finish(&state, &connection, &address, output_bytes, timer.error);
                    if close
                        || connection.close_after_reply()
                        || connection.authentication_exhausted()
                    {
                        return stream.shutdown().await;
                    }
                }
                Ok(None) => {
                    let mut chunk = [0u8; 8192];
                    let authenticated = connection.authenticated();
                    let n = tokio::select! {
                        message = connection.subscription.recv() => {
                            let Some(mut message) = message else { return Ok(()); };
                            write_pubsub_message(&mut stream, &mut message, &mut header, &mut pending, connection.version).await?;
                            continue;
                        }
                        message = monitor_message(&mut monitor), if monitor.is_some() => {
                            match message {
                                Ok(message) => { write_response(&mut stream, Resp::Status(message), &mut header, &mut pending, connection.version).await?; },
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {},
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => monitor = None,
                            }
                            continue;
                        }
                        read = auth_io(authenticated, auth_deadline, stream.read(&mut chunk)) => read?,
                    };
                    if n == 0 {
                        return Ok(());
                    }
                    input.extend_from_slice(&chunk[..n]);
                }
                Err(error) => {
                    write_response(
                        &mut stream,
                        Resp::Error(format!("ERR Protocol error: {error}")),
                        &mut header,
                        &mut pending,
                        connection.version,
                    )
                    .await?;
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
struct CommandRecord<'a> {
    args: &'a [bytes::Bytes],
    started: Instant,
    elapsed: Option<Duration>,
    cpu_start: u64,
    input_bytes: u64,
}

impl CommandRecord<'_> {
    fn executed(&mut self) {
        self.elapsed = Some(self.started.elapsed());
    }

    fn finish(
        self,
        state: &state::State,
        connection: &Connection,
        address: &str,
        output_bytes: u64,
        failed: bool,
    ) {
        let keys = if self
            .args
            .first()
            .is_some_and(|name| Engine::command_exists(name))
        {
            Engine::accessed_keys(self.args)
        } else {
            state
                .server
                .get()
                .and_then(|server| server.extensions().prepare_sync(self.args))
                .and_then(Result::ok)
                .map_or_else(Vec::new, |invocation| invocation.keys().to_vec())
        };
        let cpu_ns = connection
            .session
            .cpu_ns
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(self.cpu_start);
        let slot_key = if self.args.first().is_some_and(|name| {
            name.eq_ignore_ascii_case(b"SPUBLISH") || name.eq_ignore_ascii_case(b"SSUBSCRIBE")
        }) {
            self.args.get(1)
        } else {
            keys.first()
        };
        if let (Some(cluster), Some(key)) = (state.cluster.get(), slot_key) {
            cluster.record(
                cluster::key_slot(key),
                cpu_ns / 1000,
                self.input_bytes,
                output_bytes,
            );
        }
        if let Some(server) = state.server.get() {
            let name = connection.lease.name().unwrap_or_default();
            server.observe(server_commands::Observation {
                args: self.args,
                keys: &keys,
                database: connection.session.database(),
                client_address: address,
                client_name: &String::from_utf8_lossy(&name),
                elapsed: self.elapsed.unwrap_or_else(|| self.started.elapsed()),
                cpu_time: Duration::from_nanos(cpu_ns),
                input_bytes: self.input_bytes,
                output_bytes,
                failed,
            });
        }
    }
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

#[expect(
    clippy::too_many_arguments,
    reason = "Dispatch borrows the connection buffers, actor channel, and peer identity for one socket request."
)]
async fn socket_command(
    args: Vec<bytes::Bytes>,
    module: Option<server_commands::extensions::Invocation>,
    connection: &mut Connection,
    state: &Arc<state::State>,
    tx: &mpsc::Sender<Request>,
    stream: &mut Stream,
    input: &mut BytesMut,
    header: &mut BytesMut,
    pending: &mut Vec<Resp>,
    peer: std::net::SocketAddr,
) -> io::Result<SocketReply> {
    let name = &args[0];
    let version = connection.version;
    let cluster_client = state.cluster.get().map(|_| cluster::ClusterClient {
        readonly: connection.cluster.readonly,
        asking: connection.cluster.asking,
    });
    if let Some(scripts) = state.scripts.get() {
        if let Some(reply) = scripts.control(&args) {
            return Ok(SocketReply::Reply(reply));
        }
        if scripts.is_busy() {
            return Ok(SocketReply::Reply(Resp::Error("BUSY Redis is busy running a script. You can only call SCRIPT KILL or FUNCTION KILL.".into())));
        }
    }
    if name.eq_ignore_ascii_case(b"SCRIPT")
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
    if name.eq_ignore_ascii_case(b"ACL") {
        let Some(acl) = connection.acl() else {
            return Ok(SocketReply::Reply(Resp::Error(
                "ERR ACL runtime is not initialized".into(),
            )));
        };
        let metadata = match state.server.get() {
            Some(server) => server.extensions().command_metadata().await,
            None => Vec::new(),
        };
        let user = connection.user();
        let reply = acl.execute(
            &args,
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
                    Engine::authorize(command, user)
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
            (header, pending, version),
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
                (header, pending, version),
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
            && let Err(reply) = cluster
                .admit(invocation.keys(), write, false, &mut connection.cluster)
                .await
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
            (header, pending, version),
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
        && [b"ASKING".as_slice(), b"READONLY", b"READWRITE"]
            .iter()
            .any(|command| name.eq_ignore_ascii_case(command))
        && let Some(reply) = cluster.command(&args, &mut connection.cluster).await
    {
        return Ok(SocketReply::Reply(reply));
    }
    let write = Engine::is_dataset_write(&args) || name.eq_ignore_ascii_case(b"SPUBLISH");
    // Transfer the permit into the actor request after final owner admission.
    let write_permit = if write {
        match response_or_disconnect(
            stream,
            input,
            async { Ok(state.write_gate.admit().await) },
            &mut connection.subscription,
            (header, pending, version),
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
        let keys = if name.eq_ignore_ascii_case(b"SSUBSCRIBE") {
            args[1..].to_vec()
        } else if name.eq_ignore_ascii_case(b"SPUBLISH") && args.len() > 1 {
            vec![args[1].clone()]
        } else {
            Engine::accessed_keys(&args)
        };
        let admitted = if name.eq_ignore_ascii_case(b"MIGRATE") {
            cluster.admit_migrate(&keys, &mut connection.cluster).await
        } else if name.eq_ignore_ascii_case(b"SSUBSCRIBE") {
            let mut subscriber = cluster::ClusterClient {
                readonly: true,
                asking: connection.cluster.asking,
            };
            let admitted = cluster.admit(&keys, false, false, &mut subscriber).await;
            connection.cluster.asking = subscriber.asking;
            admitted
        } else {
            cluster
                .admit(
                    &keys,
                    write,
                    name.eq_ignore_ascii_case(b"RESTORE-ASKING"),
                    &mut connection.cluster,
                )
                .await
        };
        if let Err(reply) = admitted {
            return Ok(SocketReply::Reply(reply));
        }
        if name.eq_ignore_ascii_case(b"SSUBSCRIBE") {
            return Ok(match connection.subscription.command(&args) {
                Ok(()) => SocketReply::Suppressed,
                Err(reply) => SocketReply::Reply(reply),
            });
        }
        if (name.eq_ignore_ascii_case(b"PUBLISH") || name.eq_ignore_ascii_case(b"SPUBLISH"))
            && args.len() == 3
        {
            let sharded = name.eq_ignore_ascii_case(b"SPUBLISH");
            let cpu_started = server_commands::thread_cpu_time();
            let publication = if sharded {
                state.pubsub.publish_shard(&args[1], &args[2])
            } else {
                state.pubsub.publish(&args[1], &args[2])
            };
            let cpu_ns = server_commands::thread_cpu_time()
                .saturating_sub(cpu_started)
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64;
            connection
                .session
                .cpu_ns
                .fetch_add(cpu_ns, std::sync::atomic::Ordering::Relaxed);
            let count = publication.map_err(io::Error::other)?;
            if sharded {
                cluster.forward_shard(&args[1], &args[2]).await?;
            } else {
                cluster.forward_publish(&args[1], &args[2]).await?;
            }
            return Ok(SocketReply::Reply(Resp::Integer(count)));
        }
    }
    if let Some(native) = state.native.get()
        && [
            b"PSYNC".as_slice(),
            b"SYNC",
            b"REPLCONF",
            b"REPLICAOF",
            b"SLAVEOF",
            b"WAIT",
            b"WAITAOF",
            b"FAILOVER",
        ]
        .iter()
        .any(|command| name.eq_ignore_ascii_case(command))
    {
        let watermark = connection
            .session
            .last_write_offset
            .load(std::sync::atomic::Ordering::Acquire);
        let wait = (name.eq_ignore_ascii_case(b"WAIT") || name.eq_ignore_ascii_case(b"WAITAOF"))
            .then(|| connection.control.native_wait());
        let unblock = wait.as_ref().map(|wait| wait.receiver());
        let result = response_or_disconnect(
            stream,
            input,
            async {
                Ok(native
                    .handle_command(
                        &args,
                        &mut connection.native,
                        watermark,
                        false,
                        state.cluster.get().is_some(),
                        unblock,
                    )
                    .await)
            },
            &mut connection.subscription,
            (header, pending, version),
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
                stream.flush().await?;
                native
                    .serve_downstream(
                        stream,
                        std::mem::take(input),
                        &args,
                        connection.native.clone(),
                        peer.ip(),
                    )
                    .await?;
                Ok(SocketReply::Closed)
            }
        };
    }
    if let Some(server) = state.server.get() {
        let context = server_commands::CommandContext {
            database: connection.session.database(),
        };
        match response_or_disconnect(
            stream,
            input,
            async { Ok(server.execute(&args, context).await) },
            &mut connection.subscription,
            (header, pending, version),
            None,
        )
        .await?
        {
            Some(Some(reply)) => return Ok(SocketReply::Reply(reply)),
            Some(None) => {}
            None => return Ok(SocketReply::Closed),
        }
    }
    let user = connection.user();
    let session = connection.session.clone();
    let scripts = if name.eq_ignore_ascii_case(b"EVAL")
        || name.eq_ignore_ascii_case(b"EVALSHA")
        || name.eq_ignore_ascii_case(b"EVAL_RO")
        || name.eq_ignore_ascii_case(b"EVALSHA_RO")
    {
        state.scripts.get().map(|control| DebugClient {
            control: control.clone(),
            id: connection.lease.id,
            disconnected: connection.session.disconnected.clone(),
        })
    } else {
        None
    };
    let Some(reply) = dispatch(
        stream,
        input,
        tx,
        &mut connection.subscription,
        (header, pending, version),
        scripts,
        |reply| {
            let mut request = CommandRequest::client(args, reply, user, version, Some(session));
            request.write_permit = write_permit;
            request.cluster_client = cluster_client;
            Request::Command(request)
        },
    )
    .await?
    else {
        return Ok(SocketReply::Closed);
    };
    Ok(SocketReply::Reply(reply))
}

async fn write_pubsub_message(
    stream: &mut Stream,
    message: &mut pubsub::Message,
    header: &mut BytesMut,
    pending: &mut Vec<Resp>,
    version: Version,
) -> io::Result<()> {
    if !message.ready().await {
        return Ok(());
    }
    header.clear();
    let marker = if version == Version::Resp3 { '>' } else { '*' };
    write!(
        (&mut *header).writer(),
        "{marker}{}\r\n",
        message.values.len()
    )?;
    stream.write_all(header).await?;
    for value in std::mem::take(&mut message.values) {
        write_response(stream, value, header, pending, version).await?;
    }
    Ok(())
}

async fn dispatch<T: Send>(
    stream: &mut Stream,
    input: &mut BytesMut,
    tx: &mpsc::Sender<Request>,
    subscription: &mut pubsub::Subscription,
    output: (&mut BytesMut, &mut Vec<Resp>, Version),
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
            tx.send(build(reply))
                .await
                .map_err(|_| io::Error::other("engine stopped"))?;
            response
                .await
                .map_err(|_| io::Error::other("engine stopped"))
        },
        subscription,
        output,
        scripts,
    )
    .await
}

async fn response_or_disconnect<T>(
    stream: &mut Stream,
    input: &mut BytesMut,
    response: impl Future<Output = io::Result<T>>,
    subscription: &mut pubsub::Subscription,
    output: (&mut BytesMut, &mut Vec<Resp>, Version),
    scripts: Option<DebugClient>,
) -> io::Result<Option<T>> {
    let (header, pending, version) = output;
    let mut debug_connection = DebugConnection(scripts.clone());
    tokio::pin!(response);
    let mut debug_tick = tokio::time::interval(Duration::from_millis(10));
    // Read ahead only to detect EOF and retain pipelined bytes. A blocked
    // connection cannot execute its next command until this reply is delivered.
    // Stop reading at the request-buffer cap, applying TCP backpressure.
    const MAX_BUFFERED_BYTES: usize = 8 * 1024 * 1024;
    let mut chunk = [0_u8; 8192];
    loop {
        if let Some(control) = scripts.as_ref() {
            for event in control.take_debug_events() {
                write_response(stream, event, header, pending, version).await?;
            }
            if control.debugger_paused()
                && let Some(args) = protocol::parse(input).map_err(io::Error::other)?
                && let Some(reply) = control.debug_command(&args)
                && matches!(&reply, Resp::Error(_))
            {
                write_response(stream, reply, header, pending, version).await?;
            }
        }
        let room = MAX_BUFFERED_BYTES
            .saturating_sub(input.len())
            .min(chunk.len());
        tokio::select! {
            biased;
            reply = &mut response => {
                if let Some(control) = scripts.as_ref() {
                    for event in control.take_debug_events() {
                        write_response(stream, event, header, pending, version).await?;
                    }
                }
                debug_connection.0 = None;
                return reply.map(Some);
            }
            _ = debug_tick.tick(), if scripts.is_some() => {}
            message = subscription.recv(), if version == Version::Resp3 => {
                let Some(mut message) = message else { return Ok(None); };
                write_pubsub_message(stream, &mut message, header, pending, version).await?;
            }
            read = stream.read(&mut chunk[..room]), if room > 0 => {
                let n = read?;
                if n == 0 {
                    // Dropping the receiver cancels an unfulfilled blocking pop.
                    return Ok(None);
                }
                input.extend_from_slice(&chunk[..n]);
            }
        }
    }
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

// Stream large bulk/array replies instead of constructing a second copy of their payloads.
async fn write_response<W: AsyncWrite + Unpin>(
    stream: &mut W,
    response: Resp,
    header: &mut BytesMut,
    pending: &mut Vec<Resp>,
    version: Version,
) -> io::Result<u64> {
    let mut counted = CountingWriter {
        inner: stream,
        bytes: 0,
    };
    let stream = &mut counted;
    pending.push(response);
    while let Some(value) = pending.pop() {
        header.clear();
        match value {
            Resp::Bulk(Some(data)) => {
                write_bulk(stream, &data, header).await?;
            }
            Resp::BigNumber(value) => {
                if version == Version::Resp3 {
                    stream.write_all(b"(").await?;
                    stream.write_all(&value).await?;
                    stream.write_all(b"\r\n").await?;
                } else {
                    write_bulk(stream, &value, header).await?;
                }
            }
            Resp::Verbatim { format, value } => {
                if version == Version::Resp3 {
                    write!((&mut *header).writer(), "={}\r\n", value.len() + 4)?;
                    stream.write_all(header).await?;
                    stream.write_all(&format).await?;
                    stream.write_all(b":").await?;
                    stream.write_all(&value).await?;
                    stream.write_all(b"\r\n").await?;
                } else {
                    write_bulk(stream, &value, header).await?;
                }
            }
            Resp::Array(Some(values)) => {
                write!((&mut *header).writer(), "*{}\r\n", values.len())?;
                stream.write_all(header).await?;
                pending.extend(values.into_iter().rev());
            }
            Resp::Set(values) => {
                let prefix = if version == Version::Resp3 { '~' } else { '*' };
                write!((&mut *header).writer(), "{prefix}{}\r\n", values.len())?;
                stream.write_all(header).await?;
                pending.extend(values.into_iter().rev());
            }
            Resp::Map(values) => {
                if version == Version::Resp3 {
                    write!((&mut *header).writer(), "%{}\r\n", values.len())?;
                } else {
                    write!((&mut *header).writer(), "*{}\r\n", values.len() * 2)?;
                }
                stream.write_all(header).await?;
                for (key, value) in values.into_iter().rev() {
                    pending.push(value);
                    pending.push(key);
                }
            }
            Resp::Scored(values) => {
                let count = if version == Version::Resp3 {
                    values.len()
                } else {
                    values.len() * 2
                };
                write!((&mut *header).writer(), "*{count}\r\n")?;
                stream.write_all(header).await?;
                for (member, score) in values {
                    if version == Version::Resp3 {
                        stream.write_all(b"*2\r\n").await?;
                    }
                    write_bulk(stream, &member, header).await?;
                    header.clear();
                    Resp::Double(score).encode(header, version);
                    stream.write_all(header).await?;
                }
            }
            Resp::InfoField(field, value) => {
                if version == Version::Resp3 {
                    stream.write_all(b"%1\r\n").await?;
                    write_bulk(stream, &field, header).await?;
                } else {
                    stream.write_all(b"*1\r\n").await?;
                }
                pending.push(*value);
            }
            Resp::VectorResults(values) => {
                let mapped = version == Version::Resp3
                    && values
                        .first()
                        .is_some_and(|entry| entry.1.is_some() || entry.2.is_some());
                let count = if mapped {
                    values.len()
                } else {
                    values
                        .iter()
                        .map(|(_, score, attr)| {
                            1 + usize::from(score.is_some()) + usize::from(attr.is_some())
                        })
                        .sum()
                };
                let marker = if mapped { '%' } else { '*' };
                write!((&mut *header).writer(), "{marker}{count}\r\n")?;
                stream.write_all(header).await?;
                for (member, score, attr) in values {
                    write_bulk(stream, &member, header).await?;
                    if mapped && score.is_some() && attr.is_some() {
                        stream.write_all(b"*2\r\n").await?;
                    }
                    if let Some(score) = score {
                        header.clear();
                        Resp::Double(score).encode(header, version);
                        stream.write_all(header).await?;
                    }
                    if let Some(attr) = attr {
                        if let Some(attr) = attr {
                            write_bulk(stream, &attr, header).await?;
                        } else {
                            header.clear();
                            Resp::Bulk(None).encode(header, version);
                            stream.write_all(header).await?;
                        }
                    }
                }
            }
            value => {
                value.encode(header, version);
                stream.write_all(header).await?;
            }
        }
    }
    Ok(counted.bytes)
}

async fn write_bulk<W: AsyncWrite + Unpin>(
    stream: &mut W,
    data: &[u8],
    header: &mut BytesMut,
) -> io::Result<()> {
    header.clear();
    write!((&mut *header).writer(), "${}\r\n", data.len())?;
    stream.write_all(header).await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await
}

struct CountingWriter<'a, W> {
    inner: &'a mut W,
    bytes: u64,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for CountingWriter<'_, W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match std::pin::Pin::new(&mut *self.inner).poll_write(cx, data) {
            std::task::Poll::Ready(Ok(bytes)) => {
                self.bytes = self.bytes.saturating_add(bytes as u64);
                std::task::Poll::Ready(Ok(bytes))
            }
            result => result,
        }
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut *self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut *self.inner).poll_shutdown(cx)
    }
}

struct RequestTimer<'a> {
    metrics: &'a metrics::Metrics,
    started: Instant,
    error: bool,
}
impl Drop for RequestTimer<'_> {
    fn drop(&mut self) {
        self.metrics
            .command_complete(self.started.elapsed(), self.error);
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
