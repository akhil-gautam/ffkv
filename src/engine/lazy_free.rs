//! Logical deletion stays on the actor; owned payload destruction runs off it.
use super::{DatabaseState, Entry};
use std::sync::{OnceLock, mpsc::Sender};

#[cfg(not(target_os = "wasi"))]
fn enqueue<T: Send + 'static>(queue: &'static OnceLock<Sender<T>>, name: &'static str, value: T) {
    let sender = queue.get_or_init(|| {
        let (sender, receiver) = std::sync::mpsc::channel::<T>();
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                for value in receiver {
                    drop(value);
                }
            })
            .expect("starting lazy release worker");
        sender
    });
    sender
        .send(value)
        .unwrap_or_else(|_| panic!("lazy release worker stopped"));
}

/// WASI has no threads, so the value is released inline.
#[cfg(target_os = "wasi")]
fn enqueue<T: Send + 'static>(_: &'static OnceLock<Sender<T>>, _: &'static str, value: T) {
    drop(value);
}

pub(super) fn entry(value: Entry) {
    static QUEUE: OnceLock<Sender<Entry>> = OnceLock::new();
    enqueue(&QUEUE, "ffkv-lazy-entry", value);
}

pub(super) fn database(value: DatabaseState) {
    static QUEUE: OnceLock<Sender<DatabaseState>> = OnceLock::new();
    enqueue(&QUEUE, "ffkv-lazy-db", value);
}
