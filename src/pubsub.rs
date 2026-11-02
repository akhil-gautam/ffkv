//! Ephemeral, process-local Redis Pub/Sub. No publication enters the dataset/WAL.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};

use crate::{auth::Glob, protocol::Resp};

const MAX_MESSAGES: usize = 256;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_SUBSCRIPTIONS: usize = 1024;
const MAX_SUBSCRIPTION_BYTES: usize = 8 * 1024 * 1024;
const MATCH_WORK: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub struct Hub {
    clients: Mutex<BTreeMap<u64, Client>>,
}

struct Client {
    channels: BTreeSet<Bytes>,
    shard_channels: BTreeSet<Bytes>,
    patterns: BTreeMap<Bytes, Glob>,
    sender: mpsc::Sender<Message>,
    cancel: watch::Sender<bool>,
    budget: Arc<Budget>,
}

#[derive(Default)]
struct Budget {
    bytes: AtomicUsize,
    messages: AtomicUsize,
}

/// The charge survives dequeue, covering an arbitrarily slow in-flight socket write.
pub struct Message {
    pub values: Vec<Resp>,
    bytes: usize,
    budget: Arc<Budget>,
    ready: Option<watch::Receiver<bool>>,
}

impl Message {
    pub async fn ready(&mut self) -> bool {
        let Some(ready) = &mut self.ready else {
            return true;
        };
        if *ready.borrow() {
            return true;
        }
        ready.changed().await.is_ok() && *ready.borrow()
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.budget.messages.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Subscription {
    hub: Arc<Hub>,
    id: u64,
    receiver: mpsc::Receiver<Message>,
    cancel: watch::Receiver<bool>,
}

/// Commit gates enter client FIFOs immediately, preserving ordering with racing
/// unsubscribe acknowledgements. Closing an uncommitted gate suppresses delivery.
pub struct Publication {
    ready: watch::Sender<bool>,
    count: i64,
}

impl Publication {
    pub fn count(&self) -> i64 {
        self.count
    }
    pub fn deliver(self) {
        let _ = self.ready.send(true);
    }
}

fn bulk(value: Bytes) -> Resp {
    Resp::Bulk(Some(value))
}
fn error(message: &str) -> Resp {
    Resp::Error(format!("ERR {message}"))
}

fn reserve(client: &Client, values: Vec<Resp>, bytes: usize) -> Option<Message> {
    // All producers hold the registry lock; consumers only subtract charges.
    if client.budget.messages.load(Ordering::Relaxed) >= MAX_MESSAGES
        || client
            .budget
            .bytes
            .load(Ordering::Relaxed)
            .checked_add(bytes)?
            > MAX_OUTPUT_BYTES
        || client.sender.is_closed()
    {
        return None;
    }
    client.budget.bytes.fetch_add(bytes, Ordering::Relaxed);
    client.budget.messages.fetch_add(1, Ordering::Relaxed);
    Some(Message {
        values,
        bytes,
        budget: client.budget.clone(),
        ready: None,
    })
}

impl Hub {
    pub fn attach(self: &Arc<Self>, id: u64) -> Subscription {
        let (sender, receiver) = mpsc::channel(MAX_MESSAGES);
        let (cancel, cancelled) = watch::channel(false);
        self.clients.lock().expect("pubsub registry").insert(
            id,
            Client {
                channels: BTreeSet::new(),
                patterns: BTreeMap::new(),
                shard_channels: BTreeSet::new(),
                sender,
                cancel,
                budget: Arc::new(Budget::default()),
            },
        );
        Subscription {
            hub: self.clone(),
            id,
            receiver,
            cancel: cancelled,
        }
    }

    pub fn stage(
        self: &Arc<Self>,
        channel: &Bytes,
        payload: &Bytes,
    ) -> Result<Publication, &'static str> {
        self.stage_kind(channel, payload, false)
    }

    pub fn stage_shard(
        self: &Arc<Self>,
        channel: &Bytes,
        payload: &Bytes,
    ) -> Result<Publication, &'static str> {
        self.stage_kind(channel, payload, true)
    }

    fn stage_kind(
        self: &Arc<Self>,
        channel: &Bytes,
        payload: &Bytes,
        shard: bool,
    ) -> Result<Publication, &'static str> {
        let mut clients = self.clients.lock().expect("pubsub registry");
        let mut targets = BTreeMap::new();
        let mut work = MATCH_WORK;
        for (&id, client) in clients.iter() {
            let mut patterns = Vec::new();
            for (pattern, glob) in client.patterns.iter().filter(|_| !shard) {
                if glob
                    .matches(channel, &mut work)
                    .map_err(|()| "ERR channel pattern work limit exceeded")?
                {
                    patterns.push(pattern.clone());
                }
            }
            let channels = if shard {
                &client.shard_channels
            } else {
                &client.channels
            };
            if channels.contains(channel) || !patterns.is_empty() {
                targets.insert(id, patterns);
            }
        }
        let (ready, receiver) = watch::channel(false);
        let mut count = 0;
        if targets.is_empty() {
            return Ok(Publication { ready, count });
        }
        let channel = Bytes::copy_from_slice(channel);
        let payload = Bytes::copy_from_slice(payload);
        clients.retain(|id, client| {
            let Some(patterns) = targets.get(id) else {
                return true;
            };
            let start = count;
            let mut enqueue = |values, bytes| {
                let Some(mut message) = reserve(client, values, bytes) else {
                    return false;
                };
                message.ready = Some(receiver.clone());
                if client.sender.try_send(message).is_err() {
                    return false;
                }
                count += 1;
                true
            };
            let channels = if shard {
                &client.shard_channels
            } else {
                &client.channels
            };
            if channels.contains(&channel)
                && !enqueue(
                    vec![
                        bulk(Bytes::from_static(if shard {
                            b"smessage"
                        } else {
                            b"message"
                        })),
                        bulk(channel.clone()),
                        bulk(payload.clone()),
                    ],
                    channel.len() + payload.len() + 128,
                )
            {
                count = start;
                let _ = client.cancel.send(true);
                return false;
            }
            for pattern in patterns {
                if !enqueue(
                    vec![
                        bulk(Bytes::from_static(b"pmessage")),
                        bulk(pattern.clone()),
                        bulk(channel.clone()),
                        bulk(payload.clone()),
                    ],
                    pattern.len() + channel.len() + payload.len() + 160,
                ) {
                    count = start;
                    let _ = client.cancel.send(true);
                    return false;
                }
            }
            true
        });
        Ok(Publication { ready, count })
    }

    pub fn publish(
        self: &Arc<Self>,
        channel: &Bytes,
        payload: &Bytes,
    ) -> Result<i64, &'static str> {
        let publication = self.stage(channel, payload)?;
        let count = publication.count();
        publication.deliver();
        Ok(count)
    }

    pub fn publish_shard(
        self: &Arc<Self>,
        channel: &Bytes,
        payload: &Bytes,
    ) -> Result<i64, &'static str> {
        let publication = self.stage_shard(channel, payload)?;
        let count = publication.count();
        publication.deliver();
        Ok(count)
    }

    pub fn counts(&self, id: u64) -> (usize, usize, usize) {
        self.clients
            .lock()
            .expect("pubsub registry")
            .get(&id)
            .map_or((0, 0, 0), |c| {
                (c.channels.len(), c.patterns.len(), c.shard_channels.len())
            })
    }

    pub fn has_channel(&self, id: u64, channel: &[u8]) -> bool {
        self.clients
            .lock()
            .expect("pubsub registry")
            .get(&id)
            .is_some_and(|c| c.channels.contains(channel))
    }

    pub fn disconnect(&self, id: u64) {
        if let Some(client) = self.clients.lock().expect("pubsub registry").get(&id) {
            client.cancel.send_replace(true);
        }
    }

    /// Queue protocol push payloads with the same backpressure as publications.
    pub fn push(&self, id: u64, values: Vec<Resp>) -> bool {
        fn charge(value: &Resp) -> usize {
            match value {
                Resp::Bulk(Some(data)) => data.len().saturating_add(32),
                Resp::Array(Some(values)) => values
                    .iter()
                    .fold(32usize, |n, v| n.saturating_add(charge(v))),
                _ => 64,
            }
        }
        let clients = self.clients.lock().expect("pubsub registry");
        let Some(client) = clients.get(&id) else {
            return false;
        };
        let bytes = values
            .iter()
            .fold(32usize, |n, v| n.saturating_add(charge(v)));
        if reserve(client, values, bytes)
            .is_some_and(|message| client.sender.try_send(message).is_ok())
        {
            true
        } else {
            client.cancel.send_replace(true);
            false
        }
    }

    pub fn inspect(&self, args: &[Bytes]) -> Resp {
        let Some(command) = args.get(1) else {
            return error("wrong number of arguments for PUBSUB");
        };
        let clients = self.clients.lock().expect("pubsub registry");
        let shard = command.eq_ignore_ascii_case(b"SHARDNUMSUB")
            || command.eq_ignore_ascii_case(b"SHARDCHANNELS");
        if command.eq_ignore_ascii_case(b"NUMPAT") && args.len() == 2 {
            let patterns: BTreeSet<_> = clients
                .values()
                .flat_map(|client| client.patterns.keys())
                .collect();
            Resp::Integer(patterns.len() as i64)
        } else if command.eq_ignore_ascii_case(b"NUMSUB")
            || command.eq_ignore_ascii_case(b"SHARDNUMSUB")
        {
            let mut values = Vec::with_capacity((args.len() - 2) * 2);
            for channel in &args[2..] {
                values.push(bulk(channel.clone()));
                values.push(Resp::Integer(
                    clients
                        .values()
                        .filter(|client| {
                            if shard {
                                client.shard_channels.contains(channel)
                            } else {
                                client.channels.contains(channel)
                            }
                        })
                        .count() as i64,
                ));
            }
            Resp::Array(Some(values))
        } else if (command.eq_ignore_ascii_case(b"CHANNELS")
            || command.eq_ignore_ascii_case(b"SHARDCHANNELS"))
            && args.len() <= 3
        {
            let filter = match args
                .get(2)
                .map(|pattern| Glob::compile(pattern))
                .transpose()
            {
                Ok(filter) => filter,
                Err(reason) => return error(reason),
            };
            let channels: BTreeSet<_> = clients
                .values()
                .flat_map(|client| {
                    if shard {
                        client.shard_channels.iter()
                    } else {
                        client.channels.iter()
                    }
                })
                .collect();
            let mut work = MATCH_WORK;
            let mut bytes = 0_usize;
            let mut values = Vec::new();
            for channel in channels {
                let matched = match &filter {
                    Some(filter) => match filter.matches(channel, &mut work) {
                        Ok(matched) => matched,
                        Err(()) => return error("channel pattern work limit exceeded"),
                    },
                    None => true,
                };
                if matched {
                    bytes += channel.len() + 32;
                    if bytes > 8 * 1024 * 1024 {
                        return error("PUBSUB response exceeds 8 MiB");
                    }
                    values.push(bulk(channel.clone()));
                }
            }
            Resp::Array(Some(values))
        } else {
            error("unsupported PUBSUB subcommand or wrong number of arguments")
        }
    }
}

impl Subscription {
    pub fn cancellation(&self) -> watch::Receiver<bool> {
        self.cancel.clone()
    }
    pub async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }
    pub fn try_recv(&mut self) -> Option<Message> {
        self.receiver.try_recv().ok()
    }
    pub fn active(&self) -> bool {
        self.hub
            .clients
            .lock()
            .expect("pubsub registry")
            .get(&self.id)
            .is_some_and(|client| {
                !client.channels.is_empty()
                    || !client.patterns.is_empty()
                    || !client.shard_channels.is_empty()
            })
    }
    pub fn clear(&mut self) {
        if let Some(client) = self
            .hub
            .clients
            .lock()
            .expect("pubsub registry")
            .get_mut(&self.id)
        {
            client.channels.clear();
            client.patterns.clear();
            client.shard_channels.clear();
        }
        while self.receiver.try_recv().is_ok() {}
    }

    /// All acknowledgements enter the same FIFO as messages, preserving the
    /// subscription boundary even when a publisher races an unsubscribe.
    pub fn command(&mut self, args: &[Bytes]) -> Result<(), Resp> {
        let shard = args[0].eq_ignore_ascii_case(b"SSUBSCRIBE")
            || args[0].eq_ignore_ascii_case(b"SUNSUBSCRIBE");
        let subscribe = args[0].eq_ignore_ascii_case(b"SUBSCRIBE")
            || args[0].eq_ignore_ascii_case(b"PSUBSCRIBE")
            || args[0].eq_ignore_ascii_case(b"SSUBSCRIBE");
        let patterns = args[0].eq_ignore_ascii_case(b"PSUBSCRIBE")
            || args[0].eq_ignore_ascii_case(b"PUNSUBSCRIBE");
        let mut clients = self.hub.clients.lock().expect("pubsub registry");
        let Some(client) = clients.get_mut(&self.id) else {
            return Err(error("Pub/Sub client disconnected"));
        };
        let mut compiled = BTreeMap::new();
        if subscribe {
            let mut count =
                client.channels.len() + client.patterns.len() + client.shard_channels.len();
            let mut bytes: usize = client
                .channels
                .iter()
                .chain(client.patterns.keys())
                .chain(client.shard_channels.iter())
                .map(Bytes::len)
                .sum();
            let mut added = BTreeSet::new();
            for channel in &args[1..] {
                let present = if patterns {
                    client.patterns.contains_key(channel)
                } else if shard {
                    client.shard_channels.contains(channel)
                } else {
                    client.channels.contains(channel)
                };
                if !present && added.insert(channel.clone()) {
                    count += 1;
                    bytes += channel.len();
                    if count > MAX_SUBSCRIPTIONS || bytes > MAX_SUBSCRIPTION_BYTES {
                        return Err(error("subscription limit exceeded"));
                    }
                    if patterns {
                        compiled.insert(
                            Bytes::copy_from_slice(channel),
                            Glob::compile(channel).map_err(error)?,
                        );
                    }
                }
            }
        }
        let targets: Vec<Option<Bytes>> = if args.len() > 1 {
            args[1..]
                .iter()
                .map(|name| Some(Bytes::copy_from_slice(name)))
                .collect()
        } else {
            let targets: Vec<_> = if patterns {
                client.patterns.keys().cloned().map(Some).collect()
            } else if shard {
                client.shard_channels.iter().cloned().map(Some).collect()
            } else {
                client.channels.iter().cloned().map(Some).collect()
            };
            if targets.is_empty() {
                vec![None]
            } else {
                targets
            }
        };
        let kind: &'static [u8] = match (subscribe, patterns, shard) {
            (true, _, true) => b"ssubscribe",
            (false, _, true) => b"sunsubscribe",
            (true, false, false) => b"subscribe",
            (true, true, false) => b"psubscribe",
            (false, false, false) => b"unsubscribe",
            (false, true, false) => b"punsubscribe",
        };
        for target in targets {
            if let Some(channel) = &target {
                if subscribe {
                    if patterns {
                        if let Some((pattern, glob)) = compiled.remove_entry(channel) {
                            client.patterns.insert(pattern, glob);
                        }
                    } else if shard {
                        client
                            .shard_channels
                            .insert(Bytes::copy_from_slice(channel));
                    } else if !client.channels.contains(channel) {
                        client.channels.insert(Bytes::copy_from_slice(channel));
                    }
                } else if patterns {
                    client.patterns.remove(channel);
                } else if shard {
                    client.shard_channels.remove(channel);
                } else {
                    client.channels.remove(channel);
                }
            }
            let bytes = target.as_ref().map_or(0, Bytes::len) + 128;
            let values = vec![
                bulk(Bytes::from_static(kind)),
                Resp::Bulk(target),
                Resp::Integer(if shard {
                    client.shard_channels.len()
                } else {
                    client.channels.len() + client.patterns.len()
                } as i64),
            ];
            if reserve(client, values, bytes)
                .is_none_or(|message| client.sender.try_send(message).is_err())
            {
                let client = clients.remove(&self.id).expect("registered pubsub client");
                let _ = client.cancel.send(true);
                return Err(error("Pub/Sub output buffer limit exceeded"));
            }
        }
        Ok(())
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.hub
            .clients
            .lock()
            .expect("pubsub registry")
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&[u8]]) -> Vec<Bytes> {
        values
            .iter()
            .map(|value| Bytes::copy_from_slice(value))
            .collect()
    }

    #[tokio::test]
    async fn binary_patterns_counts_and_unsubscribe_boundaries() {
        let hub = Arc::new(Hub::default());
        let mut subscriber = hub.attach(1);
        subscriber
            .command(&args(&[b"SUBSCRIBE", b"a\0\xff"]))
            .unwrap();
        drop(subscriber.recv().await.unwrap());
        subscriber.command(&args(&[b"PSUBSCRIBE", b"a*"])).unwrap();
        drop(subscriber.recv().await.unwrap());
        let publication = hub
            .stage(
                &Bytes::from_static(b"a\0\xff"),
                &Bytes::from_static(b"\0\xff\r\n"),
            )
            .unwrap();
        assert_eq!(publication.count(), 2);
        subscriber.command(&args(&[b"UNSUBSCRIBE"])).unwrap();
        let mut direct = subscriber.recv().await.unwrap();
        let mut pattern = subscriber.recv().await.unwrap();
        assert_eq!(direct.values[0], bulk(Bytes::from_static(b"message")));
        assert_eq!(pattern.values[0], bulk(Bytes::from_static(b"pmessage")));
        publication.deliver();
        assert!(direct.ready().await);
        assert!(pattern.ready().await);
        assert_eq!(direct.values[2], bulk(Bytes::from_static(b"\0\xff\r\n")));
        let ack = subscriber.recv().await.unwrap();
        assert_eq!(ack.values[2], Resp::Integer(1));
        subscriber.command(&args(&[b"PUNSUBSCRIBE"])).unwrap();
        drop(subscriber.recv().await.unwrap());
        assert!(!subscriber.active());
        assert_eq!(
            hub.publish(&Bytes::from_static(b"a\0\xff"), &Bytes::new())
                .unwrap(),
            0
        );
        drop(subscriber);
        assert!(hub.clients.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn byte_bounds_include_inflight_and_uncommitted_messages() {
        let hub = Arc::new(Hub::default());
        let mut subscriber = hub.attach(1);
        subscriber
            .command(&args(&[b"SUBSCRIBE", b"channel"]))
            .unwrap();
        drop(subscriber.recv().await.unwrap());
        let payload = Bytes::from(vec![0; 6 * 1024 * 1024]);
        let first = hub
            .stage(&Bytes::from_static(b"channel"), &payload)
            .unwrap();
        let inflight = subscriber.recv().await.unwrap();
        let second = hub
            .stage(&Bytes::from_static(b"channel"), &payload)
            .unwrap();
        assert_eq!(first.count(), 1);
        assert_eq!(second.count(), 1);
        assert_eq!(
            hub.publish(&Bytes::from_static(b"channel"), &payload)
                .unwrap(),
            0
        );
        assert!(*subscriber.cancellation().borrow());
        assert!(!subscriber.active());
        assert!(hub.clients.lock().unwrap().is_empty());
        drop(inflight);
        drop(first);
        drop(second);
    }

    #[tokio::test]
    async fn aborted_publications_never_become_visible_and_release_charges() {
        let hub = Arc::new(Hub::default());
        let mut subscriber = hub.attach(1);
        subscriber
            .command(&args(&[b"SUBSCRIBE", b"channel"]))
            .unwrap();
        drop(subscriber.recv().await.unwrap());
        let publication = hub
            .stage(
                &Bytes::from_static(b"channel"),
                &Bytes::from_static(b"hidden"),
            )
            .unwrap();
        let mut message = subscriber.recv().await.unwrap();
        drop(publication);
        assert!(!message.ready().await);
        let budget = message.budget.clone();
        drop(message);
        assert_eq!(budget.bytes.load(Ordering::Relaxed), 0);
        assert_eq!(budget.messages.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn message_count_bound_and_identity_reset_cleanup() {
        let hub = Arc::new(Hub::default());
        let mut subscriber = hub.attach(1);
        subscriber.command(&args(&[b"SUBSCRIBE", b"x"])).unwrap();
        drop(subscriber.recv().await.unwrap());
        assert_eq!(
            hub.publish(&Bytes::from_static(b"x"), &Bytes::new())
                .unwrap(),
            1
        );
        subscriber.clear();
        assert!(!subscriber.active());
        assert!(subscriber.try_recv().is_none());
        subscriber.command(&args(&[b"SUBSCRIBE", b"x"])).unwrap();
        drop(subscriber.recv().await.unwrap());
        for _ in 0..MAX_MESSAGES {
            assert_eq!(
                hub.publish(&Bytes::from_static(b"x"), &Bytes::new())
                    .unwrap(),
                1
            );
        }
        assert_eq!(
            hub.publish(&Bytes::from_static(b"x"), &Bytes::new())
                .unwrap(),
            0
        );
        assert!(*subscriber.cancellation().borrow());
    }

    #[tokio::test]
    async fn excessive_publish_matching_rejects_before_delivery_without_disconnects() {
        let hub = Arc::new(Hub::default());
        let mut direct = hub.attach(1);
        let mut pattern = hub.attach(2);
        let channel = Bytes::from(vec![b'x'; 1024 * 1024]);
        direct
            .command(&[Bytes::from_static(b"SUBSCRIBE"), channel.clone()])
            .unwrap();
        pattern
            .command(&args(&[b"PSUBSCRIBE", b"private:*"]))
            .unwrap();
        drop(direct.recv().await.unwrap());
        drop(pattern.recv().await.unwrap());
        assert!(hub.publish(&channel, &Bytes::new()).is_err());
        assert!(direct.try_recv().is_none());
        assert!(pattern.try_recv().is_none());
        assert!(direct.active() && pattern.active());
        assert!(!*pattern.cancellation().borrow());
        assert_eq!(
            hub.publish(
                &Bytes::from_static(b"private:ok"),
                &Bytes::from_static(b"value")
            )
            .unwrap(),
            1
        );
        let mut message = pattern.recv().await.unwrap();
        assert!(message.ready().await);
        assert_eq!(message.values[3], bulk(Bytes::from_static(b"value")));
    }

    #[tokio::test]
    async fn queued_messages_and_acknowledgements_release_oversized_ingress_owners() {
        struct Ingress {
            data: Vec<u8>,
            released: Arc<std::sync::atomic::AtomicBool>,
        }
        impl AsRef<[u8]> for Ingress {
            fn as_ref(&self) -> &[u8] {
                &self.data
            }
        }
        impl Drop for Ingress {
            fn drop(&mut self) {
                self.released.store(true, Ordering::Relaxed);
            }
        }
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut data = vec![0; 4 * 1024 * 1024];
        data[..2].copy_from_slice(b"xy");
        let ingress = Bytes::from_owner(Ingress {
            data,
            released: released.clone(),
        });
        let hub = Arc::new(Hub::default());
        let mut subscriber = hub.attach(1);
        subscriber
            .command(&[Bytes::from_static(b"SUBSCRIBE"), ingress.slice(..1)])
            .unwrap();
        assert_eq!(
            hub.publish(&ingress.slice(..1), &ingress.slice(1..2))
                .unwrap(),
            1
        );
        drop(ingress);
        assert!(released.load(Ordering::Relaxed));
        let ack = subscriber.recv().await.unwrap();
        assert_eq!(ack.values[1], bulk(Bytes::from_static(b"x")));
        let mut message = subscriber.recv().await.unwrap();
        assert!(message.ready().await);
        assert_eq!(message.values[2], bulk(Bytes::from_static(b"y")));
    }
}
