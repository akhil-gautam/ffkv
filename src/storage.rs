//! Reconstructible, synchronous queue-payload storage.
//!
//! The WAL and snapshots, not these files, are the durability source. Recovery
//! writes a fresh generation and calls `finish_recovery` only after validating
//! those logical sources. A crash may leave incomplete derived segments; files
//! without both an owned name and a valid owned header are never removed.
//!
//! Cache limits count body bytes, not RSS, allocator/index overhead, or bytes
//! retained by replies. Live-value statistics count disk values (not inline
//! values or reference clones). Reads/writes count record IO attempts, including
//! compaction; cache hits/misses count disk-backed `Payload::read` lookups.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};

const SEGMENT_MAGIC: &[u8; 8] = b"FFKVB001";
const RECORD_MAGIC: &[u8; 8] = b"FFKVR001";
const HEADER: u64 = 64;
const PREFIX: &str = "ffkv-blob-v1-";

/// Positional file IO and file identity. WASI preview 1 offers neither on
/// stable Rust, so a store never opens there.
#[cfg(not(target_os = "wasi"))]
mod platform {
    use std::fs::{File, Metadata};
    use std::io;
    use std::os::unix::fs::{FileExt, MetadataExt};

    pub(super) fn supported() -> io::Result<()> {
        Ok(())
    }
    /// (device, inode)
    pub(super) fn identity(metadata: &Metadata) -> io::Result<(u64, u64)> {
        Ok((metadata.dev(), metadata.ino()))
    }
    pub(super) fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
        file.read_at(bytes, offset)
    }
    pub(super) fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }
}

#[cfg(target_os = "wasi")]
mod platform {
    use std::fs::{File, Metadata};
    use std::io;

    fn unsupported() -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "disk payload storage is not available on WASI",
        )
    }
    pub(super) fn supported() -> io::Result<()> {
        Err(unsupported())
    }
    pub(super) fn identity(_: &Metadata) -> io::Result<(u64, u64)> {
        Err(unsupported())
    }
    pub(super) fn read_at(_: &File, _: &mut [u8], _: u64) -> io::Result<usize> {
        Err(unsupported())
    }
    pub(super) fn write_at(_: &File, _: &[u8], _: u64) -> io::Result<usize> {
        Err(unsupported())
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub directory: PathBuf,
    pub inline_threshold: usize,
    pub cache_bytes: usize,
    /// Target segment size; a single larger record gets its own segment.
    pub segment_bytes: u64,
    /// Owned file lengths, including old generations and compaction scratch.
    /// Zero means unlimited. Unknown user files are not part of this budget.
    pub disk_budget: u64,
}

#[derive(Clone, Debug)]
pub enum Payload {
    Inline(Bytes),
    Disk(Reference),
}

#[derive(Clone)]
pub struct Reference(Arc<Value>);

impl std::fmt::Debug for Reference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reference")
            .field("id", &self.0.id)
            .field("len", &self.0.len)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub live_values: u64,
    pub live_payload_bytes: u64,
    pub physical_bytes: u64,
    pub cached_payload_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub reads: u64,
    pub writes: u64,
    pub compactions: u64,
    /// Actual lengths of owned files successfully removed in this open.
    pub reclaimed_bytes: u64,
}

pub struct Store {
    config: Config,
    generation: [u8; 16],
    // All append, relocation and deletion IO is serialized here. Readers only
    // borrow a location/file handle and never acquire this mutex.
    writer: Mutex<Writer>,
    // Never held over filesystem IO or over a value-location lock.
    live: Mutex<Live>,
}

struct Writer {
    next_value: u64,
    next_segment: u64,
    segments: Vec<Arc<Segment>>,
    retired: Vec<Arc<Segment>>,
    obsolete: Vec<Obsolete>,
}

struct Live {
    values: HashMap<u64, Weak<Value>>,
    cache: Cache,
    stats: Stats,
}

struct Value {
    store: Arc<Store>,
    id: u64,
    len: usize,
    body_crc32c: u32,
    location: RwLock<Location>,
}

#[derive(Clone)]
struct Location {
    segment: Arc<Segment>,
    offset: u64,
}

struct Segment {
    file: File,
    path: PathBuf,
    generation: [u8; 16],
    id: u64,
    len: AtomicU64,
    sealed: AtomicBool,
}

struct Obsolete {
    path: PathBuf,
    generation: [u8; 16],
    id: u64,
    len: u64,
    device: u64,
    inode: u64,
}

// Intrusive-by-ID LRU: exactly one node per cached value, no historical IDs or
// unbounded access-stamp queue. The body-byte total alone is budgeted.
struct Cache {
    entries: HashMap<u64, CacheEntry>,
    newest: Option<u64>,
    oldest: Option<u64>,
    bytes: usize,
}

struct CacheEntry {
    bytes: Bytes,
    newer: Option<u64>,
    older: Option<u64>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

fn exhausted(message: &'static str) -> io::Error {
    io::Error::other(message)
}

fn add(value: u64, increment: u64) -> io::Result<u64> {
    value
        .checked_add(increment)
        .ok_or_else(|| exhausted("payload store counter exhausted"))
}

impl Cache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            newest: None,
            oldest: None,
            bytes: 0,
        }
    }

    fn remove(&mut self, id: u64) -> Option<Bytes> {
        let entry = self.entries.remove(&id)?;
        if let Some(newer) = entry.newer {
            self.entries.get_mut(&newer).expect("LRU newer").older = entry.older;
        } else {
            self.newest = entry.older;
        }
        if let Some(older) = entry.older {
            self.entries.get_mut(&older).expect("LRU older").newer = entry.newer;
        } else {
            self.oldest = entry.newer;
        }
        self.bytes -= entry.bytes.len();
        Some(entry.bytes)
    }

    fn insert(&mut self, id: u64, bytes: Bytes, budget: usize) {
        self.remove(id);
        if bytes.len() > budget || budget == 0 {
            return;
        }
        while self.bytes > budget - bytes.len() {
            self.remove(self.oldest.expect("nonempty LRU"));
        }
        if let Some(newest) = self.newest {
            self.entries.get_mut(&newest).expect("LRU head").newer = Some(id);
        } else {
            self.oldest = Some(id);
        }
        self.bytes += bytes.len();
        self.entries.insert(
            id,
            CacheEntry {
                bytes,
                newer: None,
                older: self.newest,
            },
        );
        self.newest = Some(id);
    }

    fn get(&mut self, id: u64, budget: usize) -> Option<Bytes> {
        let bytes = self.remove(id)?;
        self.insert(id, bytes.clone(), budget);
        Some(bytes)
    }
}

impl Store {
    pub fn open(config: Config) -> io::Result<Arc<Self>> {
        platform::supported()?;
        if config.segment_bytes < HEADER * 2 + 1 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "segment_bytes must fit a segment header and a nonempty record",
            ));
        }
        if config.disk_budget != 0 && config.disk_budget < HEADER * 2 + 1 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "disk_budget must fit a segment header and a nonempty record",
            ));
        }
        fs::create_dir_all(&config.directory)?;
        let mut obsolete = Vec::new();
        let mut generations = HashSet::new();
        let mut physical_bytes = 0;
        for entry in fs::read_dir(&config.directory)? {
            let entry = entry?;
            let Some((generation, id)) = parse_name(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            // Never follow a user-created symlink, and avoid generation reuse
            // even when an interrupted segment has no valid header yet.
            generations.insert(generation);
            if !entry.file_type()?.is_file() {
                continue;
            }
            let file = File::open(entry.path())?;
            match verify_segment_header(&file, generation, id) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::InvalidData => continue,
                Err(error) => return Err(error),
            }
            let metadata = file.metadata()?;
            let (device, inode) = platform::identity(&metadata)?;
            physical_bytes = add(physical_bytes, metadata.len())?;
            obsolete.push(Obsolete {
                path: entry.path(),
                generation,
                id,
                len: metadata.len(),
                device,
                inode,
            });
        }
        let generation = loop {
            let candidate = rand::random::<[u8; 16]>();
            if !generations.contains(&candidate) {
                break candidate;
            }
        };
        Ok(Arc::new(Self {
            config,
            generation,
            writer: Mutex::new(Writer {
                next_value: 1,
                next_segment: 1,
                segments: Vec::new(),
                retired: Vec::new(),
                obsolete,
            }),
            live: Mutex::new(Live {
                values: HashMap::new(),
                cache: Cache::new(),
                stats: Stats {
                    physical_bytes,
                    ..Stats::default()
                },
            }),
        }))
    }

    pub fn put(self: &Arc<Self>, bytes: Bytes) -> io::Result<Payload> {
        if bytes.len() <= self.config.inline_threshold {
            return Ok(Payload::inline(bytes));
        }
        let len = u64::try_from(bytes.len())
            .map_err(|_| exhausted("payload length does not fit on disk"))?;
        let body_crc32c = crc32c::crc32c(&bytes);
        let mut writer = lock(&self.writer);
        self.reap(&mut writer)?;
        let id = writer.next_value;
        // Never recycle an ID, including IDs of failed writes.
        writer.next_value = add(id, 1)?;
        {
            let live = lock(&self.live);
            add(live.stats.live_values, 1)?;
            add(live.stats.live_payload_bytes, len)?;
        }
        let Writer {
            next_segment,
            segments,
            ..
        } = &mut *writer;
        let location = self.append(segments, next_segment, id, &bytes, body_crc32c)?;
        let value = Arc::new(Value {
            store: Arc::clone(self),
            id,
            len: bytes.len(),
            body_crc32c,
            location: RwLock::new(location),
        });
        {
            let mut live = lock(&self.live);
            live.stats.live_values += 1;
            live.stats.live_payload_bytes += len;
            live.values.insert(id, Arc::downgrade(&value));
            if bytes.len() <= self.config.cache_bytes {
                // A slice of a request frame must not keep the whole frame alive.
                live.cache
                    .insert(id, Bytes::copy_from_slice(&bytes), self.config.cache_bytes);
            }
        }
        Ok(Payload::Disk(Reference(value)))
    }

    pub fn stats(&self) -> Stats {
        let live = lock(&self.live);
        let mut stats = live.stats.clone();
        stats.cached_payload_bytes = live.cache.bytes as u64;
        stats
    }

    /// Reclaim relocated segments after their last in-flight reader releases them.
    pub fn reap_retired(&self) -> io::Result<()> {
        self.reap(&mut lock(&self.writer))
    }

    pub fn finish_recovery(&self) -> io::Result<()> {
        let mut writer = lock(&self.writer);
        while let Some(obsolete) = writer.obsolete.last() {
            let metadata = fs::symlink_metadata(&obsolete.path)?;
            if !metadata.file_type().is_file()
                || platform::identity(&metadata)? != (obsolete.device, obsolete.inode)
                || metadata.len() != obsolete.len
            {
                return Err(invalid("obsolete segment changed during recovery"));
            }
            let file = File::open(&obsolete.path)?;
            verify_segment_header(&file, obsolete.generation, obsolete.id)?;
            self.remove_owned(&obsolete.path, obsolete.len)?;
            writer.obsolete.pop();
        }
        self.reap(&mut writer)?;
        File::open(&self.config.directory)?.sync_all()
    }

    pub fn compact(self: &Arc<Self>) -> io::Result<Stats> {
        let mut writer = lock(&self.writer);
        self.reap(&mut writer)?;
        let values: Vec<Arc<Value>> = {
            let live = lock(&self.live);
            live.values.values().filter_map(Weak::upgrade).collect()
        };
        // Strong snapshots keep final-reference drops from invalidating the
        // relocation plan. Appends are serialized, reads are not.
        let mut staged = Vec::new();
        let mut relocations = Vec::with_capacity(values.len());
        let result = (|| {
            for value in &values {
                let old = value
                    .location
                    .read()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clone();
                let body = self.read_record(&old, value.id, value.len, value.body_crc32c)?;
                let new = self.append(
                    &mut staged,
                    &mut writer.next_segment,
                    value.id,
                    &body,
                    value.body_crc32c,
                )?;
                relocations.push(new);
            }
            // All fallible relocation IO happens before publishing any new
            // location. A failed budget/write/checksum leaves every old one.
            for segment in &staged {
                segment.file.sync_all()?;
            }
            File::open(&self.config.directory)?.sync_all()?;
            let live = lock(&self.live);
            add(live.stats.compactions, 1)?;
            Ok::<(), io::Error>(())
        })();
        if let Err(error) = result {
            // Release staged location handles before attempting reclamation.
            drop(relocations);
            writer.retired.append(&mut staged);
            let _ = self.reap(&mut writer);
            return Err(error);
        }
        for (value, location) in values.iter().zip(relocations) {
            *value
                .location
                .write()
                .unwrap_or_else(|poison| poison.into_inner()) = location;
        }
        let old_segments = std::mem::replace(&mut writer.segments, staged);
        writer.retired.extend(old_segments);
        {
            let mut live = lock(&self.live);
            live.stats.compactions += 1;
        }
        // No more IO that can affect the relocation transaction. A deletion
        // error reports failure but all locations are already fully committed.
        // Reader-pinned files stay budgeted until a later reap can close them.
        self.reap(&mut writer)?;
        drop(writer);
        drop(values);
        Ok(self.stats())
    }

    fn ensure_budget(&self, additional: u64) -> io::Result<()> {
        let physical = lock(&self.live).stats.physical_bytes;
        let total = add(physical, additional)?;
        if self.config.disk_budget != 0 && total > self.config.disk_budget {
            return Err(io::Error::new(
                ErrorKind::StorageFull,
                "payload store disk budget exceeded (including temporary segments)",
            ));
        }
        Ok(())
    }

    fn append(
        &self,
        segments: &mut Vec<Arc<Segment>>,
        next_segment: &mut u64,
        id: u64,
        bytes: &[u8],
        body_crc32c: u32,
    ) -> io::Result<Location> {
        let record_len = add(HEADER, bytes.len() as u64)?;
        let needs_segment = segments.last().is_none_or(|segment| {
            segment.sealed.load(Ordering::Relaxed)
                || segment
                    .len
                    .load(Ordering::Relaxed)
                    .checked_add(record_len)
                    .is_none_or(|end| end > self.config.segment_bytes)
        });
        let additional = if needs_segment {
            add(HEADER, record_len)?
        } else {
            record_len
        };
        self.ensure_budget(additional)?;
        {
            let mut live = lock(&self.live);
            live.stats.writes = add(live.stats.writes, 1)?;
        }
        if needs_segment {
            let segment_id = *next_segment;
            *next_segment = add(segment_id, 1)?;
            let path = self
                .config
                .directory
                .join(segment_name(self.generation, segment_id));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)?;
            let segment = Arc::new(Segment {
                file,
                path,
                generation: self.generation,
                id: segment_id,
                len: AtomicU64::new(0),
                sealed: AtomicBool::new(false),
            });
            segments.push(Arc::clone(&segment));
            let header = segment_header(self.generation, segment_id);
            if let Err(error) = self.write_at(&segment, &header, 0) {
                segment.sealed.store(true, Ordering::Relaxed);
                return Err(error);
            }
        }
        let segment = segments.last().expect("append segment");
        let offset = segment.len.load(Ordering::Relaxed);
        let header = record_header(segment, id, bytes, body_crc32c);
        let result = self
            .write_at(segment, &header, offset)
            .and_then(|()| self.write_at(segment, bytes, offset + HEADER));
        if let Err(error) = result {
            // Do not append past a partial record, even after a caller chooses
            // to inspect the failed store before shutting down.
            segment.sealed.store(true, Ordering::Relaxed);
            return Err(error);
        }
        Ok(Location {
            segment: Arc::clone(segment),
            offset,
        })
    }

    fn write_at(&self, segment: &Segment, mut bytes: &[u8], mut offset: u64) -> io::Result<()> {
        while !bytes.is_empty() {
            let written = match platform::write_at(&segment.file, bytes, offset) {
                Ok(0) => return Err(io::Error::new(ErrorKind::WriteZero, "short segment write")),
                Ok(written) => written,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            };
            offset += written as u64; // Entire append was overflow-checked first.
            let previous = segment.len.fetch_max(offset, Ordering::Relaxed);
            if offset > previous {
                // Preflight reserves sufficient address space and budget; only
                // the serialized writer can increase physical bytes.
                lock(&self.live).stats.physical_bytes += offset - previous;
            }
            bytes = &bytes[written..];
        }
        Ok(())
    }

    fn read_record(
        &self,
        location: &Location,
        id: u64,
        len: usize,
        body_crc32c: u32,
    ) -> io::Result<Bytes> {
        {
            let mut live = lock(&self.live);
            live.stats.reads = add(live.stats.reads, 1)?;
        }
        let segment = &location.segment;
        verify_segment_header(&segment.file, segment.generation, segment.id)?;
        let mut header = [0u8; HEADER as usize];
        read_exact_at(&segment.file, &mut header, location.offset)?;
        verify_checksum(&header)?;
        if &header[..8] != RECORD_MAGIC
            || header[8..24] != segment.generation
            || number(&header[24..32]) != segment.id
            || number(&header[32..40]) != id
            || number(&header[40..48]) != len as u64
            || header[48..52] != body_crc32c.to_le_bytes()
            || header[52..60] != [0; 8]
        {
            return Err(invalid("payload record identity or length mismatch"));
        }
        let mut body = Vec::new();
        body.try_reserve_exact(len).map_err(|_| {
            io::Error::new(ErrorKind::OutOfMemory, "payload read allocation failed")
        })?;
        body.resize(len, 0);
        read_exact_at(&segment.file, &mut body, location.offset + HEADER)?;
        let checksum = u32::from_le_bytes(header[48..52].try_into().expect("checksum width"));
        if crc32c::crc32c(&body) != checksum {
            return Err(invalid("payload body checksum mismatch"));
        }
        Ok(Bytes::from(body))
    }

    fn read_value(&self, value: &Value) -> io::Result<Bytes> {
        {
            let mut live = lock(&self.live);
            if live.cache.entries.contains_key(&value.id) {
                live.stats.cache_hits = add(live.stats.cache_hits, 1)?;
                return Ok(live
                    .cache
                    .get(value.id, self.config.cache_bytes)
                    .expect("cached value"));
            }
            live.stats.cache_misses = add(live.stats.cache_misses, 1)?;
        }
        let location = value
            .location
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let bytes = self.read_record(&location, value.id, value.len, value.body_crc32c)?;
        if bytes.len() <= self.config.cache_bytes {
            lock(&self.live)
                .cache
                .insert(value.id, bytes.clone(), self.config.cache_bytes);
        }
        Ok(bytes)
    }

    fn remove_owned(&self, path: &Path, len: u64) -> io::Result<()> {
        {
            let live = lock(&self.live);
            add(live.stats.reclaimed_bytes, len)?;
        }
        fs::remove_file(path)?;
        let mut live = lock(&self.live);
        live.stats.physical_bytes -= len;
        live.stats.reclaimed_bytes += len;
        Ok(())
    }

    fn reap(&self, writer: &mut Writer) -> io::Result<()> {
        let mut index = 0;
        while index < writer.retired.len() {
            let segment = &writer.retired[index];
            if Arc::strong_count(segment) == 1 {
                self.remove_owned(&segment.path, segment.len.load(Ordering::Relaxed))?;
                writer.retired.swap_remove(index);
            } else {
                index += 1;
            }
        }
        Ok(())
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        let mut live = lock(&self.store.live);
        live.values.remove(&self.id);
        live.cache.remove(self.id);
        live.stats.live_values -= 1;
        live.stats.live_payload_bytes -= self.len as u64;
    }
}

impl Payload {
    /// Owned resident storage beyond the inline Payload handle. Disk bodies
    /// and the process-wide Store cache are not resident parts of this value.
    pub(crate) fn memory_footprint(&self) -> usize {
        match self {
            Self::Inline(bytes) => bytes.len(),
            Self::Disk(_) => {
                std::mem::size_of::<Value>().saturating_add(2 * std::mem::size_of::<usize>())
            }
        }
    }

    pub(crate) fn memory_identity(&self) -> (usize, usize) {
        match self {
            Self::Inline(bytes) => (bytes.as_ptr() as usize, bytes.len()),
            Self::Disk(reference) => (Arc::as_ptr(&reference.0) as usize, 0),
        }
    }

    /// Own the body independently of potentially much larger request frames.
    pub fn inline(bytes: Bytes) -> Self {
        Self::Inline(Bytes::copy_from_slice(&bytes))
    }

    pub fn read(&self) -> io::Result<Bytes> {
        match self {
            Self::Inline(bytes) => Ok(bytes.clone()),
            Self::Disk(reference) => reference.0.store.read_value(&reference.0),
        }
    }

    /// Reject cold nonmatches from live metadata, then compare exact bytes.
    /// The caller can compute the needle CRC once for an entire list scan.
    /// Equal CRCs are only a filter, never evidence of byte equality.
    pub fn matches(&self, needle: &[u8], needle_crc32c: u32) -> io::Result<bool> {
        match self {
            Self::Inline(bytes) => Ok(bytes.as_ref() == needle),
            Self::Disk(reference) => {
                if reference.0.len != needle.len() || reference.0.body_crc32c != needle_crc32c {
                    return Ok(false);
                }
                Ok(self.read()?.as_ref() == needle)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Inline(bytes) => bytes.len(),
            Self::Disk(reference) => reference.0.len,
        }
    }

    pub fn is_disk(&self) -> bool {
        matches!(self, Self::Disk(_))
    }
}

fn number(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("u64 field width"))
}

fn segment_header(generation: [u8; 16], id: u64) -> [u8; HEADER as usize] {
    let mut header = [0u8; HEADER as usize];
    header[..8].copy_from_slice(SEGMENT_MAGIC);
    header[8..12].copy_from_slice(&1u32.to_le_bytes());
    header[12..16].copy_from_slice(&(HEADER as u32).to_le_bytes());
    header[16..32].copy_from_slice(&generation);
    header[32..40].copy_from_slice(&id.to_le_bytes());
    let crc = crc32c::crc32c(&header[..60]);
    header[60..64].copy_from_slice(&crc.to_le_bytes());
    header
}

fn record_header(
    segment: &Segment,
    id: u64,
    bytes: &[u8],
    body_crc32c: u32,
) -> [u8; HEADER as usize] {
    let mut header = [0u8; HEADER as usize];
    header[..8].copy_from_slice(RECORD_MAGIC);
    header[8..24].copy_from_slice(&segment.generation);
    header[24..32].copy_from_slice(&segment.id.to_le_bytes());
    header[32..40].copy_from_slice(&id.to_le_bytes());
    header[40..48].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
    header[48..52].copy_from_slice(&body_crc32c.to_le_bytes());
    let crc = crc32c::crc32c(&header[..60]);
    header[60..64].copy_from_slice(&crc.to_le_bytes());
    header
}

fn verify_checksum(header: &[u8; HEADER as usize]) -> io::Result<()> {
    let expected = u32::from_le_bytes(header[60..64].try_into().expect("checksum width"));
    if crc32c::crc32c(&header[..60]) != expected {
        return Err(invalid("payload header checksum mismatch"));
    }
    Ok(())
}

fn verify_segment_header(file: &File, generation: [u8; 16], id: u64) -> io::Result<()> {
    let mut header = [0u8; HEADER as usize];
    read_exact_at(file, &mut header, 0)?;
    verify_checksum(&header)?;
    if &header[..8] != SEGMENT_MAGIC
        || header[8..12] != 1u32.to_le_bytes()
        || header[12..16] != (HEADER as u32).to_le_bytes()
        || header[16..32] != generation
        || number(&header[32..40]) != id
        || header[40..60] != [0; 20]
    {
        return Err(invalid("payload segment identity mismatch"));
    }
    Ok(())
}

fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        match platform::read_at(file, bytes, offset) {
            Ok(0) => return Err(invalid("truncated payload segment or record")),
            Ok(read) => {
                offset = offset
                    .checked_add(read as u64)
                    .ok_or_else(|| invalid("payload offset overflow"))?;
                bytes = &mut bytes[read..];
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn segment_name(generation: [u8; 16], id: u64) -> String {
    use std::fmt::Write;
    let mut name = String::with_capacity(PREFIX.len() + 32 + 1 + 16 + 4);
    name.push_str(PREFIX);
    for byte in generation {
        write!(&mut name, "{byte:02x}").expect("write string");
    }
    write!(&mut name, "-{id:016x}.seg").expect("write string");
    name
}

fn parse_name(name: &str) -> Option<([u8; 16], u64)> {
    let rest = name.strip_prefix(PREFIX)?.strip_suffix(".seg")?;
    if rest.len() != 49 || rest.as_bytes()[32] != b'-' {
        return None;
    }
    if !rest
        .bytes()
        .enumerate()
        .all(|(i, byte)| i == 32 || byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut generation = [0u8; 16];
    for (index, byte) in generation.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&rest[index * 2..index * 2 + 2], 16).ok()?;
    }
    let id = u64::from_str_radix(&rest[33..], 16).ok()?;
    Some((generation, id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::FileExt;
    use std::sync::Barrier;
    use std::thread;

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    struct Directory(PathBuf);

    impl Directory {
        fn new() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "ffkv-storage-{}-{}-{:032x}",
                std::process::id(),
                sequence,
                rand::random::<u128>()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn config(&self) -> Config {
            Config {
                directory: self.0.clone(),
                inline_threshold: 4,
                cache_bytes: 0,
                segment_bytes: 256,
                disk_budget: 0,
            }
        }
    }

    impl Drop for Directory {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn location(payload: &Payload) -> Location {
        let Payload::Disk(reference) = payload else {
            panic!("expected disk payload")
        };
        reference.0.location.read().unwrap().clone()
    }

    #[test]
    fn resident_accounting_never_reads_cold_bodies() {
        let directory = Directory::new();
        let store = Store::open(directory.config()).unwrap();
        let small = store.put(Bytes::from(vec![1; 1024])).unwrap();
        let large = store.put(Bytes::from(vec![2; 64 * 1024])).unwrap();
        let before = store.stats();
        let small_resident = small.memory_footprint();
        let large_resident = large.memory_footprint();
        assert!(small_resident > 0);
        assert_eq!(small_resident, large_resident);
        assert_eq!(small.memory_identity(), small.clone().memory_identity());
        let after = store.stats();
        assert_eq!(before.reads, after.reads);
        assert_eq!(before.cache_hits, after.cache_hits);
        assert_eq!(before.cache_misses, after.cache_misses);
    }

    #[test]
    fn inline_threshold_copies_and_disk_reference_lifetime_is_shared() {
        let directory = Directory::new();
        let store = Store::open(directory.config()).unwrap();
        let frame = Bytes::from(vec![42; 4096]);
        let small = frame.slice(1..5);
        let small_pointer = small.as_ptr();
        let inline = store.put(small).unwrap();
        assert!(!inline.is_disk());
        assert_eq!(inline.len(), 4);
        assert_ne!(inline.read().unwrap().as_ptr(), small_pointer);
        let value = store.put(Bytes::from_static(b"payload")).unwrap();
        let clone = value.clone();
        assert_eq!(store.stats().live_values, 1);
        drop(value);
        assert_eq!(store.stats().live_values, 1);
        let weak_store = Arc::downgrade(&store);
        drop(store);
        assert_eq!(clone.read().unwrap(), b"payload"[..]);
        assert!(weak_store.upgrade().is_some());
        drop(clone);
        assert!(weak_store.upgrade().is_none());
    }

    #[test]
    fn final_drop_reclaims_live_index_and_cache_without_historical_ids() {
        let directory = Directory::new();
        let mut config = directory.config();
        config.cache_bytes = 64;
        let store = Store::open(config).unwrap();
        for _ in 0..500 {
            let value = store.put(Bytes::from_static(b"seven77")).unwrap();
            assert_eq!(store.stats().live_payload_bytes, 7);
            assert_eq!(store.stats().cached_payload_bytes, 7);
            drop(value);
            let live = lock(&store.live);
            assert!(live.values.is_empty());
            assert!(live.cache.entries.is_empty());
            assert_eq!(live.cache.bytes, 0);
            assert_eq!(live.stats.live_values, 0);
        }
        let before = store.stats().physical_bytes;
        let stats = store.compact().unwrap();
        assert_eq!(stats.physical_bytes, 0);
        assert_eq!(stats.reclaimed_bytes, before);
    }

    #[test]
    fn weighted_cache_has_exact_boundary_and_true_lru_eviction() {
        let directory = Directory::new();
        let mut config = directory.config();
        config.cache_bytes = 12;
        let store = Store::open(config).unwrap();
        let a = store.put(Bytes::from_static(b"aaaaaa")).unwrap();
        let b = store.put(Bytes::from_static(b"bbbbbb")).unwrap();
        assert_eq!(store.stats().cached_payload_bytes, 12);
        a.read().unwrap(); // A is now newer than B.
        let c = store.put(Bytes::from_static(b"cccccc")).unwrap();
        a.read().unwrap();
        assert_eq!(store.stats().reads, 0);
        b.read().unwrap(); // B was evicted, rather than A.
        assert_eq!(store.stats().reads, 1);
        assert_eq!(store.stats().cache_hits, 2);
        assert_eq!(store.stats().cache_misses, 1);
        assert_eq!(store.stats().cached_payload_bytes, 12);
        let large = store.put(Bytes::from_static(b"larger-than-cache")).unwrap();
        large.read().unwrap();
        large.read().unwrap();
        assert_eq!(store.stats().reads, 3);
        assert_eq!(store.stats().cached_payload_bytes, 12);
        drop((a, b, c, large));
        assert_eq!(store.stats().cached_payload_bytes, 0);
    }

    #[test]
    fn checksums_detect_body_record_header_and_segment_header_corruption() {
        for target in 0..3 {
            let directory = Directory::new();
            let store = Store::open(directory.config()).unwrap();
            let value = store.put(Bytes::from_static(b"checksummed body")).unwrap();
            let at = location(&value);
            let offset = match target {
                0 => at.offset + HEADER,
                1 => at.offset + 32,
                _ => 16,
            };
            let mut byte = [0];
            read_exact_at(&at.segment.file, &mut byte, offset).unwrap();
            byte[0] ^= 0x80;
            at.segment.file.write_all_at(&byte, offset).unwrap();
            assert_eq!(value.read().unwrap_err().kind(), ErrorKind::InvalidData);
            let old_location = location(&value);
            assert_eq!(store.compact().unwrap_err().kind(), ErrorKind::InvalidData);
            assert!(Arc::ptr_eq(
                &old_location.segment,
                &location(&value).segment
            ));
            assert_eq!(store.stats().compactions, 0);
        }
    }

    #[test]
    fn truncated_body_is_invalid_data_and_corrupt_length_cannot_allocate() {
        let directory = Directory::new();
        let store = Store::open(directory.config()).unwrap();
        let value = store.put(Bytes::from_static(b"bounded payload")).unwrap();
        let at = location(&value);
        let mut header = [0; HEADER as usize];
        read_exact_at(&at.segment.file, &mut header, at.offset).unwrap();
        header[40..48].copy_from_slice(&u64::MAX.to_le_bytes());
        let checksum = crc32c::crc32c(&header[..60]);
        header[60..64].copy_from_slice(&checksum.to_le_bytes());
        at.segment.file.write_all_at(&header, at.offset).unwrap();
        assert_eq!(value.read().unwrap_err().kind(), ErrorKind::InvalidData);
        let valid = record_header(
            &at.segment,
            match &value {
                Payload::Disk(reference) => reference.0.id,
                _ => unreachable!(),
            },
            b"bounded payload",
            crc32c::crc32c(b"bounded payload"),
        );
        at.segment.file.write_all_at(&valid, at.offset).unwrap();
        at.segment.file.set_len(at.offset + HEADER + 1).unwrap();
        assert_eq!(value.read().unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn failed_compaction_budget_keeps_every_old_location_and_cleans_scratch() {
        let directory = Directory::new();
        let mut config = directory.config();
        config.segment_bytes = 144;
        // Two 136-byte segments fit. Only one scratch segment fits afterward.
        config.disk_budget = 408;
        let store = Store::open(config).unwrap();
        let a = store.put(Bytes::from_static(b"aaaaaaaa")).unwrap();
        let b = store.put(Bytes::from_static(b"bbbbbbbb")).unwrap();
        let old_a = location(&a);
        let old_b = location(&b);
        let before = store.stats().physical_bytes;
        assert_eq!(before, 272);
        assert_eq!(store.compact().unwrap_err().kind(), ErrorKind::StorageFull);
        assert!(Arc::ptr_eq(&old_a.segment, &location(&a).segment));
        assert!(Arc::ptr_eq(&old_b.segment, &location(&b).segment));
        assert_eq!(a.read().unwrap(), b"aaaaaaaa"[..]);
        assert_eq!(b.read().unwrap(), b"bbbbbbbb"[..]);
        assert_eq!(store.stats().physical_bytes, before);
        assert_eq!(store.stats().compactions, 0);
        assert_eq!(store.stats().reclaimed_bytes, 136);
    }

    #[test]
    fn budget_failure_before_append_does_not_change_physical_or_live_bytes() {
        let directory = Directory::new();
        let mut config = directory.config();
        config.disk_budget = 136;
        let store = Store::open(config).unwrap();
        let a = store.put(Bytes::from_static(b"12345678")).unwrap();
        assert_eq!(store.stats().physical_bytes, 136);
        assert_eq!(
            store.put(Bytes::from_static(b"other")).unwrap_err().kind(),
            ErrorKind::StorageFull
        );
        assert_eq!(store.stats().physical_bytes, 136);
        assert_eq!(store.stats().live_values, 1);
        assert_eq!(a.read().unwrap(), b"12345678"[..]);
    }

    #[test]
    fn readers_pin_old_files_and_compaction_relocates_stable_references() {
        let directory = Directory::new();
        let store = Store::open(directory.config()).unwrap();
        let value = store
            .put(Bytes::from_static(b"concurrent payload"))
            .unwrap();
        let pinned = location(&value);
        let old_bytes = store.stats().physical_bytes;
        let stats = store.compact().unwrap();
        assert_eq!(stats.compactions, 1);
        assert_eq!(stats.physical_bytes, old_bytes * 2);
        assert!(pinned.segment.path.exists());
        assert!(!Arc::ptr_eq(&pinned.segment, &location(&value).segment));
        assert_eq!(
            store
                .read_record(
                    &pinned,
                    match &value {
                        Payload::Disk(reference) => reference.0.id,
                        _ => unreachable!(),
                    },
                    value.len(),
                    crc32c::crc32c(b"concurrent payload")
                )
                .unwrap(),
            b"concurrent payload"[..]
        );
        let old_path = pinned.segment.path.clone();
        drop(pinned);
        store.reap_retired().unwrap();
        assert!(!old_path.exists());
        assert_eq!(store.stats().physical_bytes, old_bytes);
        assert_eq!(value.read().unwrap(), b"concurrent payload"[..]);
    }

    #[test]
    fn concurrent_readers_and_compactions_do_real_io_without_invalidating_values() {
        let directory = Directory::new();
        let store = Store::open(directory.config()).unwrap();
        let value = store.put(Bytes::from(vec![0x5a; 8192])).unwrap();
        let barrier = Arc::new(Barrier::new(5));
        thread::scope(|scope| {
            for _ in 0..4 {
                let value = value.clone();
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..100 {
                        let body = value.read().unwrap();
                        assert_eq!(body.len(), 8192);
                        assert!(body.iter().all(|&byte| byte == 0x5a));
                    }
                });
            }
            barrier.wait();
            for _ in 0..20 {
                store.compact().unwrap();
            }
        });
        store.reap_retired().unwrap();
        assert_eq!(store.stats().compactions, 20);
        assert_eq!(store.stats().live_values, 1);
        assert_eq!(store.stats().physical_bytes, HEADER * 2 + 8192);
        assert_eq!(store.stats().cache_misses, 400);
        assert_eq!(store.stats().reads, 420);
    }

    #[test]
    fn recovery_uses_new_generation_and_deletes_only_recognized_old_segments() {
        let directory = Directory::new();
        let first = Store::open(directory.config()).unwrap();
        let value = first.put(Bytes::from_static(b"replayed bytes")).unwrap();
        let old_path = location(&value).segment.path.clone();
        let old_generation = first.generation;
        let old_len = first.stats().physical_bytes;
        drop(value);
        drop(first);
        fs::write(directory.0.join("user-data"), b"keep me").unwrap();
        let unknown = directory.0.join(segment_name([0xab; 16], 7));
        fs::write(&unknown, b"invalid owned-looking header").unwrap();
        let second = Store::open(directory.config()).unwrap();
        assert_ne!(old_generation, second.generation);
        assert!(old_path.exists());
        assert_eq!(second.stats().physical_bytes, old_len);
        let rebuilt = second.put(Bytes::from_static(b"replayed bytes")).unwrap();
        second.finish_recovery().unwrap();
        second.finish_recovery().unwrap();
        assert!(!old_path.exists());
        assert!(unknown.exists());
        assert_eq!(fs::read(directory.0.join("user-data")).unwrap(), b"keep me");
        assert_eq!(second.stats().physical_bytes, old_len);
        assert_eq!(second.stats().reclaimed_bytes, old_len);
        assert_eq!(rebuilt.read().unwrap(), b"replayed bytes"[..]);
    }

    #[test]
    fn filesystem_errors_keep_their_kind_and_do_not_become_missing_values() {
        let directory = Directory::new();
        let store = Store::open(directory.config()).unwrap();
        // Replace the configured directory with a regular file: this is real
        // filesystem failure, independent of permission bits/root privileges.
        fs::remove_dir(&directory.0).unwrap();
        fs::write(&directory.0, b"not a directory").unwrap();
        let error = store.put(Bytes::from_static(b"disk body")).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotADirectory);
        assert_eq!(store.stats().live_values, 0);
        assert_eq!(store.stats().physical_bytes, 0);
        fs::remove_file(&directory.0).unwrap();
        fs::create_dir(&directory.0).unwrap();
    }

    #[test]
    fn configuration_and_id_counter_exhaustion_are_explicit_errors() {
        let directory = Directory::new();
        let mut config = directory.config();
        config.segment_bytes = 128;
        assert_eq!(
            Store::open(config).err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
        let store = Store::open(directory.config()).unwrap();
        lock(&store.writer).next_value = u64::MAX;
        assert!(store.put(Bytes::from_static(b"no wrap")).is_err());
        assert_eq!(store.stats().physical_bytes, 0);
        assert_eq!(store.stats().live_values, 0);
    }

    #[test]
    fn matches_filters_cold_values_but_never_treats_crc_as_equality() {
        let directory = Directory::new();
        let store = Store::open(directory.config()).unwrap();
        let value = store.put(Bytes::from_static(b"original")).unwrap();
        let crc = crc32c::crc32c(b"original");
        assert!(!value.matches(b"short", crc32c::crc32c(b"short")).unwrap());
        assert!(
            !value
                .matches(b"different", crc32c::crc32c(b"different"))
                .unwrap()
        );
        assert_eq!(store.stats().reads, 0);
        assert_eq!(store.stats().cache_misses, 0);
        // Supplying the stored CRC with unequal, equal-length bytes exercises
        // the same comparison path as a genuine CRC collision.
        assert!(!value.matches(b"unequal!", crc).unwrap());
        assert_eq!(store.stats().reads, 1);
        assert!(value.matches(b"original", crc).unwrap());
        assert_eq!(store.stats().reads, 2);
        store.compact().unwrap();
        assert!(value.matches(b"original", crc).unwrap());
        let inline = Payload::inline(Bytes::from_static(b"inline"));
        assert!(inline.matches(b"inline", 0).unwrap());
        assert!(!inline.matches(b"unequal", 0).unwrap());
        let at = location(&value);
        at.segment
            .file
            .write_all_at(b"!", at.offset + HEADER)
            .unwrap();
        assert_eq!(
            value.matches(b"original", crc).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}
