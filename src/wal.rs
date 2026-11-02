//! Framed logical WAL. Recovery validates records before exposing the recovered engine.

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::journal::{Batch, Mutation};

const FILE_HEADER: &[u8; 8] = b"FFKVWAL1";
const RECORD_MAGIC: &[u8; 4] = b"FREC";
const RECORD_HEADER_LEN: usize = 24;
const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;
/// Functions are an indivisible blob: timestamp/count (12), opcode (1), length (4).
pub(crate) const MAX_FUNCTION_SNAPSHOT_BYTES: usize = MAX_PAYLOAD_LEN - 17;
const MAX_MUTATIONS: usize = 1024;
const MAX_PUSH_VALUES: usize = 1024;
const MAX_COLLECTION_ENTRIES: usize = 1024;

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Durability {
    Strict,
    Everysec,
}

pub struct Recovery {
    /// Committed data frames/chunks, excluding control frames and uncommitted tails.
    pub records: u64,
    pub truncated_bytes: u64,
}

/// Byte counts and committed data frames in the new snapshot WAL. Sequence numbers
/// restart at one and are file-local, not durable replication offsets.
#[derive(Debug)]
pub struct Checkpoint {
    pub old_bytes: u64,
    pub new_bytes: u64,
    pub data_frames: u64,
}

pub struct Wal {
    file: tokio::fs::File,
    policy: Durability,
    next_sequence: u64,
    offset: u64,
    buffer: Vec<u8>,
    dirty: bool,
    directory: PathBuf,
    failed: bool,
    // Drop the WAL handle before releasing the directory's exclusive lock.
    _lock: File,
}

impl Wal {
    /// Blocking startup: call from spawn_blocking, never on an async executor thread.
    pub fn open(
        dir: &Path,
        policy: Durability,
        apply: impl FnMut(Batch) -> io::Result<()>,
    ) -> io::Result<(Self, Recovery)> {
        ensure_directory(dir)?;
        let lock_path = dir.join("LOCK");
        let (lock, new_lock) = open_regular(&lock_path)?;
        lock.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("data directory {} is already locked", dir.display()),
            ),
            TryLockError::Error(error) => context(error, "locking data directory"),
        })?;
        if new_lock {
            lock.sync_all()
                .map_err(|error| context(error, "syncing new LOCK file"))?;
            sync_directory(dir)?;
        }

        let wal_path = dir.join("wal.ffkv");
        let (mut file, new_file) = open_regular(&wal_path)?;
        if new_file {
            file.write_all(FILE_HEADER)
                .map_err(|error| context(error, "writing WAL file header at offset 0"))?;
            file.sync_all()
                .map_err(|error| context(error, "syncing new WAL file"))?;
            sync_directory(dir)?;
        }

        let (file, recovery, offset, next_sequence) = recover_file(file, apply, true)?;
        Ok((
            Self {
                file: tokio::fs::File::from_std(file),
                policy,
                next_sequence,
                offset,
                buffer: Vec::new(),
                dirty: false,
                directory: dir.to_path_buf(),
                failed: false,
                _lock: lock,
            },
            recovery,
        ))
    }

    pub async fn append(&mut self, batch: &Batch) -> io::Result<()> {
        self.append_frames(batch).await.map(|_| ())
    }

    async fn append_frames(&mut self, batch: &Batch) -> io::Result<u64> {
        self.ensure_healthy()?;
        let mut planner = Planner::new(batch)?;
        let mut frame = FramePlan::new();
        let mut data_frames = 0_u64;
        let mut bytes = 0_u64;
        while planner.next_frame(&mut frame)? {
            data_frames = data_frames
                .checked_add(1)
                .ok_or_else(|| input_error("WAL group frame count exhausted"))?;
            bytes = bytes
                .checked_add((RECORD_HEADER_LEN + frame.length) as u64)
                .ok_or_else(|| input_error("WAL group byte count exhausted"))?;
        }
        let grouped = data_frames > 1;
        let physical_frames = if grouped {
            // BEGIN has 21 payload bytes; COMMIT has 29.
            bytes = bytes
                .checked_add((2 * RECORD_HEADER_LEN + 21 + 29) as u64)
                .ok_or_else(|| input_error("WAL group byte count exhausted"))?;
            data_frames
                .checked_add(2)
                .ok_or_else(|| input_error("WAL group frame count exhausted"))?
        } else {
            data_frames
        };
        self.offset
            .checked_add(bytes)
            .ok_or_else(|| invalid(self.offset, "WAL file offset exhausted"))?;
        self.next_sequence
            .checked_add(physical_frames)
            .ok_or_else(|| invalid(self.offset, "WAL sequence exhausted"))?;

        let group_id = self.next_sequence;
        if grouped {
            self.control(batch.timestamp_ms, b'B', group_id, None);
            self.write_frame().await?;
        }
        let mut planner = Planner::new(batch)?;
        while planner.next_frame(&mut frame)? {
            self.start_frame(frame.length);
            self.buffer
                .extend_from_slice(&batch.timestamp_ms.to_le_bytes());
            self.buffer
                .extend_from_slice(&(frame.parts.len() as u32).to_le_bytes());
            for part in &frame.parts {
                encode_part(part, &mut self.buffer);
            }
            self.write_frame().await?;
        }
        if grouped {
            self.control(batch.timestamp_ms, b'C', group_id, Some(data_frames));
            self.write_frame().await?;
        }
        self.file
            .flush()
            .await
            .map_err(|error| context(error, format!("flushing WAL at offset {}", self.offset)))?;
        if matches!(self.policy, Durability::Strict) {
            // One durability barrier covers BEGIN, all data frames, and COMMIT.
            self.sync_pending().await?;
        }
        Ok(data_frames)
    }

    /// The current logical WAL length, excluding any unflushed partial IO on failure.
    pub fn bytes(&self) -> u64 {
        self.offset
    }

    #[cfg(test)]
    /// Next physical frame sequence in this file; checkpoint resets it to one.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn policy(&self) -> Durability {
        self.policy
    }
    pub(crate) fn set_policy(&mut self, policy: Durability) {
        self.policy = policy;
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn ensure_healthy(&self) -> io::Result<()> {
        if self.failed {
            Err(io::Error::other(
                "WAL checkpoint publication failed; reopen required",
            ))
        } else {
            Ok(())
        }
    }

    /// Replaces the WAL while keeping the directory LOCK continuously held.
    /// Any error after rename is fatal: subsequent writes are refused.
    pub async fn checkpoint(
        &mut self,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<Checkpoint> {
        self.ensure_healthy()?;
        let (scratch, file) = crate::backup::Scratch::file(&self.directory)?;
        let (replacement, data_frames) = self.snapshot(file, batches).await?;
        let result = Checkpoint {
            old_bytes: self.offset,
            new_bytes: replacement.offset,
            data_frames,
        };
        fs::rename(scratch.path(), self.directory.join("wal.ffkv"))?;
        // The pathname now names replacement. Never continue writing the old inode,
        // even when syncing the directory fails.
        self.failed = true;
        sync_directory(&self.directory)?;
        *self = replacement;
        Ok(result)
    }

    /// Creates an immutable, standalone snapshot in a fresh destination directory.
    pub async fn backup(
        &mut self,
        target: &Path,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<crate::backup::Backup> {
        self.ensure_healthy()?;
        let mut scratch = crate::backup::Scratch::directory(target)?;
        let file = scratch.create_member("wal.ffkv")?;
        let (snapshot, data_frames) = self.snapshot(file, batches).await?;
        let wal_bytes = snapshot.offset;
        drop(snapshot);
        crate::backup::publish_backup(scratch, target, wal_bytes, data_frames)
    }

    async fn snapshot(
        &self,
        mut file: File,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<(Self, u64)> {
        file.write_all(FILE_HEADER)?;
        let mut snapshot = Self {
            file: tokio::fs::File::from_std(file),
            policy: Durability::Everysec,
            next_sequence: 1,
            offset: FILE_HEADER.len() as u64,
            buffer: Vec::new(),
            dirty: true,
            directory: self.directory.clone(),
            failed: false,
            _lock: self._lock.try_clone()?,
        };
        let mut data_frames = 0_u64;
        for batch in batches {
            data_frames = data_frames
                .checked_add(snapshot.append_frames(&batch?).await?)
                .ok_or_else(|| input_error("snapshot data frame count exhausted"))?;
        }
        snapshot.finish().await?;
        snapshot.policy = self.policy;
        Ok((snapshot, data_frames))
    }

    fn start_frame(&mut self, payload_len: usize) {
        self.buffer.clear();
        self.buffer.resize(RECORD_HEADER_LEN, 0);
        self.buffer.reserve_exact(payload_len);
    }

    fn control(&mut self, timestamp: i64, kind: u8, id: u64, count: Option<u64>) {
        self.start_frame(if count.is_some() { 29 } else { 21 });
        self.buffer.extend_from_slice(&timestamp.to_le_bytes());
        self.buffer.extend_from_slice(&0_u32.to_le_bytes());
        self.buffer.push(kind);
        self.buffer.extend_from_slice(&id.to_le_bytes());
        if let Some(count) = count {
            self.buffer.extend_from_slice(&count.to_le_bytes());
        }
    }

    async fn write_frame(&mut self) -> io::Result<()> {
        let payload_len = self.buffer.len() - RECORD_HEADER_LEN;
        self.buffer[..4].copy_from_slice(RECORD_MAGIC);
        self.buffer[4..8].copy_from_slice(&(payload_len as u32).to_le_bytes());
        self.buffer[8..16].copy_from_slice(&self.next_sequence.to_le_bytes());
        let header_crc = crc32c::crc32c(&self.buffer[..16]);
        let payload_crc = crc32c::crc32c(&self.buffer[RECORD_HEADER_LEN..]);
        self.buffer[16..20].copy_from_slice(&header_crc.to_le_bytes());
        self.buffer[20..24].copy_from_slice(&payload_crc.to_le_bytes());
        self.file
            .write_all(&self.buffer)
            .await
            .map_err(|error| context(error, format!("appending WAL at offset {}", self.offset)))?;
        self.dirty = true;
        // The full logical append was checked for offset/sequence overflow before writing.
        self.offset += self.buffer.len() as u64;
        self.next_sequence += 1;
        Ok(())
    }

    pub async fn periodic_sync(&mut self) -> io::Result<()> {
        self.ensure_healthy()?;
        if matches!(self.policy, Durability::Everysec) {
            self.sync_pending().await?;
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> io::Result<()> {
        self.ensure_healthy()?;
        self.file
            .flush()
            .await
            .map_err(|error| context(error, "flushing WAL on shutdown"))?;
        self.sync_pending().await
    }

    async fn sync_pending(&mut self) -> io::Result<()> {
        if self.dirty {
            self.file.sync_all().await.map_err(|error| {
                context(error, format!("syncing WAL near offset {}", self.offset))
            })?;
            self.dirty = false;
        }
        Ok(())
    }
}

// Shared by startup (repair allowed) and offline restore (strict, read-only).
fn recover_file(
    mut file: File,
    mut apply: impl FnMut(Batch) -> io::Result<()>,
    repair: bool,
) -> io::Result<(File, Recovery, u64, u64)> {
    let file_len = file.metadata()?.len();
    if file_len < FILE_HEADER.len() as u64 {
        return Err(invalid(
            0,
            "incomplete WAL file header; refusing to reset data",
        ));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut file_header = [0; FILE_HEADER.len()];
    file.read_exact(&mut file_header)
        .map_err(|error| context(error, "reading WAL file header at offset 0"))?;
    if &file_header != FILE_HEADER {
        return Err(invalid(0, "unsupported WAL file header or version"));
    }

    let mut offset = FILE_HEADER.len() as u64;
    let mut next_sequence = 1_u64;
    let mut records = 0_u64;
    let mut group: Option<RecoveryGroup> = None;
    while offset < file_len {
        let Some((record, end)) = read_record(&mut file, offset, next_sequence, file_len)? else {
            break;
        };
        match record {
            Record::Data(batch) => {
                if let Some(group) = &mut group {
                    if batch.timestamp_ms != group.timestamp {
                        return Err(invalid(offset, "group data timestamp differs from BEGIN"));
                    }
                    group.data_frames = group
                        .data_frames
                        .checked_add(1)
                        .ok_or_else(|| invalid(offset, "group data frame count exhausted"))?;
                } else {
                    apply_record(&mut apply, batch, next_sequence, offset)?;
                    records += 1;
                }
            }
            Record::Begin { timestamp, id } => {
                if group.is_some() {
                    return Err(invalid(offset, "nested WAL group BEGIN"));
                }
                if id != next_sequence {
                    return Err(invalid(offset, "BEGIN id differs from its sequence"));
                }
                group = Some(RecoveryGroup {
                    offset,
                    sequence: next_sequence,
                    timestamp,
                    data_start: end,
                    data_frames: 0,
                });
            }
            Record::Commit {
                timestamp,
                id,
                data_frames,
            } => {
                let Some(committed) = group.take() else {
                    return Err(invalid(offset, "orphan WAL group COMMIT"));
                };
                if timestamp != committed.timestamp
                    || id != committed.sequence
                    || data_frames == 0
                    || data_frames != committed.data_frames
                {
                    return Err(invalid(
                        offset,
                        "COMMIT does not match its BEGIN/data frames",
                    ));
                }
                // First pass retained only offsets/counts. Stream the now-committed data
                // through the callback, keeping at most one physical frame in memory.
                file.seek(SeekFrom::Start(committed.data_start))?;
                let mut data_offset = committed.data_start;
                for index in 0..data_frames {
                    let sequence = committed.sequence + 1 + index;
                    let Some((Record::Data(batch), data_end)) =
                        read_record(&mut file, data_offset, sequence, offset)?
                    else {
                        return Err(invalid(
                            data_offset,
                            "committed group data changed on reread",
                        ));
                    };
                    if batch.timestamp_ms != committed.timestamp {
                        return Err(invalid(data_offset, "group timestamp changed on reread"));
                    }
                    apply_record(&mut apply, batch, sequence, data_offset)?;
                    data_offset = data_end;
                }
                if data_offset != offset {
                    return Err(invalid(data_offset, "group data range differs from COMMIT"));
                }
                file.seek(SeekFrom::Start(end))?;
                records = records
                    .checked_add(data_frames)
                    .ok_or_else(|| invalid(offset, "recovered record count exhausted"))?;
            }
        }
        offset = end;
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| invalid(offset, "WAL sequence exhausted"))?;
    }
    if let Some(group) = group {
        // A valid but uncommitted group is one torn logical tail, including BEGIN.
        offset = group.offset;
        next_sequence = group.sequence;
    }

    // No modification occurs until every complete record and replay callback succeeded.
    let truncated_bytes = file_len - offset;
    if truncated_bytes != 0 {
        if !repair {
            return Err(invalid(
                offset,
                "incomplete WAL tail; offline restore never repairs",
            ));
        }
        file.set_len(offset).map_err(|error| {
            context(
                error,
                format!("truncating torn WAL tail at offset {offset}"),
            )
        })?;
        file.sync_all()
            .map_err(|error| context(error, "syncing repaired WAL tail"))?;
    }
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| context(error, format!("seeking WAL append offset {offset}")))?;
    Ok((
        file,
        Recovery {
            records,
            truncated_bytes,
        },
        offset,
        next_sequence,
    ))
}

/// Strict physical validation, with bounded one-frame memory and no source writes.
pub(crate) fn validate_complete(file: File) -> io::Result<Recovery> {
    recover_file(file, |_| Ok(()), false).map(|(_, recovery, _, _)| recovery)
}

struct RecoveryGroup {
    offset: u64,
    sequence: u64,
    timestamp: i64,
    data_start: u64,
    data_frames: u64,
}

enum Record {
    Data(Batch),
    Begin {
        timestamp: i64,
        id: u64,
    },
    Commit {
        timestamp: i64,
        id: u64,
        data_frames: u64,
    },
}

fn apply_record(
    apply: &mut impl FnMut(Batch) -> io::Result<()>,
    batch: Batch,
    sequence: u64,
    offset: u64,
) -> io::Result<()> {
    apply(batch).map_err(|error| {
        context(
            error,
            format!("applying WAL sequence {sequence} at offset {offset}"),
        )
    })
}

// Reads from the current position. A complete header is always validated, even when
// its payload is torn. Corrupt complete frames are never mistaken for incomplete groups.
fn read_record(
    file: &mut File,
    offset: u64,
    sequence: u64,
    file_len: u64,
) -> io::Result<Option<(Record, u64)>> {
    let remaining = file_len - offset;
    if remaining < RECORD_HEADER_LEN as u64 {
        return Ok(None);
    }
    let mut header = [0; RECORD_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|error| context(error, format!("reading WAL header at offset {offset}")))?;
    let payload_len = validate_header(&header, sequence, offset)?;
    if remaining - (RECORD_HEADER_LEN as u64) < payload_len as u64 {
        return Ok(None);
    }
    let mut payload = vec![0; payload_len];
    file.read_exact(&mut payload)
        .map_err(|error| context(error, format!("reading WAL payload at offset {offset}")))?;
    let checksum = u32::from_le_bytes(header[20..24].try_into().unwrap());
    if crc32c::crc32c(&payload) != checksum {
        return Err(invalid(offset, "payload CRC32C mismatch"));
    }
    let record = decode_record(Bytes::from(payload))
        .map_err(|error| context(error, format!("decoding WAL record at offset {offset}")))?;
    Ok(Some((
        record,
        offset + RECORD_HEADER_LEN as u64 + payload_len as u64,
    )))
}

fn context(error: io::Error, message: impl std::fmt::Display) -> io::Error {
    io::Error::new(error.kind(), format!("{message}: {error}"))
}

fn invalid(offset: u64, message: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("WAL offset {offset}: {message}"),
    )
}

fn input_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| context(error, format!("syncing directory {}", path.display())))
}

fn ensure_directory(path: &Path) -> io::Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "data directory {} is not a directory (symlinks are unsupported)",
                        path.display()
                    ),
                ));
            }
            return Ok(());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(context(
                error,
                format!("inspecting directory {}", path.display()),
            ));
        }
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    ensure_directory(parent)?;
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if !fs::symlink_metadata(path)?.is_dir() {
                return Err(context(
                    error,
                    format!("creating directory {}", path.display()),
                ));
            }
        }
        Err(error) => {
            return Err(context(
                error,
                format!("creating directory {}", path.display()),
            ));
        }
    }
    sync_directory(path)?;
    sync_directory(parent)
}

fn open_regular(path: &Path) -> io::Result<(File, bool)> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => Ok((file, true)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if !fs::symlink_metadata(path)?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{} is not a regular file (symlinks are unsupported)",
                        path.display()
                    ),
                ));
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|error| context(error, format!("opening {}", path.display())))?;
            if !file.metadata()?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL or LOCK is not a regular file",
                ));
            }
            Ok((file, false))
        }
        Err(error) => Err(context(error, format!("creating {}", path.display()))),
    }
}

fn validate_header(
    header: &[u8; RECORD_HEADER_LEN],
    sequence: u64,
    offset: u64,
) -> io::Result<usize> {
    if &header[..4] != RECORD_MAGIC {
        return Err(invalid(offset, "invalid record magic"));
    }
    let checksum = u32::from_le_bytes(header[16..20].try_into().unwrap());
    if crc32c::crc32c(&header[..16]) != checksum {
        return Err(invalid(offset, "record header CRC32C mismatch"));
    }
    let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    if !(12..=MAX_PAYLOAD_LEN).contains(&payload_len) {
        return Err(invalid(
            offset,
            format!("payload length {payload_len} is outside 12..={MAX_PAYLOAD_LEN}"),
        ));
    }
    let actual_sequence = u64::from_le_bytes(header[8..16].try_into().unwrap());
    if actual_sequence != sequence {
        return Err(invalid(
            offset,
            format!("record sequence {actual_sequence}, expected {sequence}"),
        ));
    }
    Ok(payload_len)
}

fn add_len(total: &mut usize, amount: usize) -> io::Result<()> {
    *total = total
        .checked_add(amount)
        .filter(|length| *length <= MAX_PAYLOAD_LEN)
        .ok_or_else(|| input_error("WAL payload exceeds 16 MiB"))?;
    Ok(())
}

fn add_blob_len(total: &mut usize, blob: &[u8]) -> io::Result<()> {
    add_len(total, 4)?;
    add_len(total, blob.len())
}

/// A bounded transport container of the existing canonical WAL payload fragments.
/// Fragments preserve one atomic timestamp and are never independently committed.
pub(crate) fn encode_native_batch(batch: &Batch) -> io::Result<Bytes> {
    const LIMIT: usize = 64 * 1024 * 1024;
    let mut planner = Planner::new(batch)?;
    let mut frame = FramePlan::new();
    let mut length = 12usize;
    let mut count = 0u32;
    while planner.next_frame(&mut frame)? {
        length = length
            .checked_add(4 + frame.length)
            .filter(|length| *length <= LIMIT)
            .ok_or_else(|| input_error("native batch exceeds 64 MiB"))?;
        count = count
            .checked_add(1)
            .ok_or_else(|| input_error("native batch fragment overflow"))?;
    }
    let mut output = Vec::with_capacity(length);
    output.extend_from_slice(b"FFKVJB01");
    output.extend_from_slice(&count.to_le_bytes());
    let mut planner = Planner::new(batch)?;
    while planner.next_frame(&mut frame)? {
        output.extend_from_slice(&(frame.length as u32).to_le_bytes());
        output.extend_from_slice(&batch.timestamp_ms.to_le_bytes());
        output.extend_from_slice(&(frame.parts.len() as u32).to_le_bytes());
        for part in &frame.parts {
            encode_part(part, &mut output);
        }
    }
    Ok(Bytes::from(output))
}

pub(crate) fn decode_native_batch(bytes: Bytes) -> io::Result<Batch> {
    if bytes.len() < 12 || bytes.len() > 64 * 1024 * 1024 || &bytes[..8] != b"FFKVJB01" {
        return Err(input_error("invalid native batch container"));
    }
    let count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if count == 0 || count > (bytes.len() - 12) / 17 {
        return Err(input_error("invalid native batch fragment count"));
    }
    let mut position = 12usize;
    let mut result: Option<Batch> = None;
    for _ in 0..count {
        let prefix = bytes
            .get(position..position + 4)
            .ok_or_else(|| input_error("truncated native batch"))?;
        let length = u32::from_le_bytes(prefix.try_into().unwrap()) as usize;
        position += 4;
        let end = position
            .checked_add(length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| input_error("truncated native batch fragment"))?;
        if !(12..=MAX_PAYLOAD_LEN).contains(&length) {
            return Err(input_error("invalid native batch fragment length"));
        }
        let batch = decode(bytes.slice(position..end))?;
        position = end;
        if let Some(result) = &mut result {
            if result.timestamp_ms != batch.timestamp_ms {
                return Err(input_error("inconsistent native batch timestamp"));
            }
            result.mutations.extend(batch.mutations);
        } else {
            result = Some(batch);
        }
    }
    if position != bytes.len() {
        return Err(input_error("trailing native batch bytes"));
    }
    result.ok_or_else(|| input_error("empty native batch"))
}

// A frame plan borrows the caller's rows. Planning is repeated before writing so even
// a late invalid row fails before BEGIN, without retaining an unbounded list of frames.
enum Part<'a> {
    Single(&'a Mutation),
    Blobs {
        tag: u8,
        key: &'a Bytes,
        values: &'a [Bytes],
        right: Option<bool>,
    },
    Hash {
        key: &'a Bytes,
        fields: &'a [(Bytes, Bytes, Option<i64>)],
    },
    Sorted {
        key: &'a Bytes,
        entries: &'a [(Bytes, f64)],
    },
}

struct SizedPart<'a> {
    part: Part<'a>,
    length: usize,
}

struct FramePlan<'a> {
    parts: Vec<Part<'a>>,
    length: usize,
}

impl FramePlan<'_> {
    fn new() -> Self {
        Self {
            parts: Vec::new(),
            length: 12,
        }
    }
}

struct Planner<'a> {
    mutations: &'a [Mutation],
    mutation: usize,
    row: usize,
    pending: Option<SizedPart<'a>>,
}

impl<'a> Planner<'a> {
    fn new(batch: &'a Batch) -> io::Result<Self> {
        if batch.mutations.is_empty() {
            return Err(input_error("WAL logical mutation count must be nonzero"));
        }
        Ok(Self {
            mutations: &batch.mutations,
            mutation: 0,
            row: 0,
            pending: None,
        })
    }

    fn next_frame(&mut self, frame: &mut FramePlan<'a>) -> io::Result<bool> {
        frame.parts.clear();
        frame.length = 12;
        while frame.parts.len() < MAX_MUTATIONS {
            let next = match self.pending.take() {
                Some(part) => Some(part),
                None => self.next_part()?,
            };
            let Some(next) = next else { break };
            if frame.length + next.length > MAX_PAYLOAD_LEN {
                self.pending = Some(next);
                break;
            }
            frame.length += next.length;
            frame.parts.push(next.part);
        }
        Ok(!frame.parts.is_empty())
    }

    fn next_part(&mut self) -> io::Result<Option<SizedPart<'a>>> {
        let Some(mutation) = self.mutations.get(self.mutation) else {
            return Ok(None);
        };
        let (part, length, end, total) = match mutation {
            Mutation::Push { key, values, right } => self.blobs(3, key, values, Some(*right))?,
            Mutation::SetAdd { key, members } => self.blobs(6, key, members, None)?,
            Mutation::SetRemove { key, members } => self.blobs(7, key, members, None)?,
            Mutation::HashDelete { key, fields } => self.blobs(9, key, fields, None)?,
            Mutation::SortedSetRemove { key, members } => self.blobs(11, key, members, None)?,
            Mutation::HashSet { key, fields } => {
                let (end, length) = chunk(key, fields, self.row, 0, |(field, value, expiry)| {
                    let mut length = 0;
                    add_blob_len(&mut length, field)?;
                    add_blob_len(&mut length, value)?;
                    add_len(&mut length, if expiry.is_some() { 9 } else { 1 })?;
                    Ok(length)
                })?;
                (
                    Part::Hash {
                        key,
                        fields: &fields[self.row..end],
                    },
                    length,
                    end,
                    fields.len(),
                )
            }
            Mutation::SortedSetAdd { key, entries } => {
                let (end, length) = chunk(key, entries, self.row, 0, |(member, score)| {
                    if score.is_nan() {
                        return Err(input_error("WAL sorted-set score must not be NaN"));
                    }
                    let mut length = 8;
                    add_blob_len(&mut length, member)?;
                    Ok(length)
                })?;
                (
                    Part::Sorted {
                        key,
                        entries: &entries[self.row..end],
                    },
                    length,
                    end,
                    entries.len(),
                )
            }
            _ => (Part::Single(mutation), single_len(mutation)?, 1, 1),
        };
        if end == total {
            self.mutation += 1;
            self.row = 0;
        } else {
            self.row = end;
        }
        Ok(Some(SizedPart { part, length }))
    }

    fn blobs(
        &self,
        tag: u8,
        key: &'a Bytes,
        values: &'a [Bytes],
        right: Option<bool>,
    ) -> io::Result<(Part<'a>, usize, usize, usize)> {
        let (end, length) = chunk(
            key,
            values,
            self.row,
            usize::from(right.is_some()),
            |value| {
                let mut length = 0;
                add_blob_len(&mut length, value)?;
                Ok(length)
            },
        )?;
        Ok((
            Part::Blobs {
                tag,
                key,
                values: &values[self.row..end],
                right,
            },
            length,
            end,
            values.len(),
        ))
    }
}

fn chunk<T>(
    key: &Bytes,
    rows: &[T],
    start: usize,
    extra: usize,
    measure: impl Fn(&T) -> io::Result<usize>,
) -> io::Result<(usize, usize)> {
    if rows.is_empty() {
        return Err(input_error("WAL mutation row count must be nonzero"));
    }
    let mut length = 1 + 4 + extra;
    add_blob_len(&mut length, key)?;
    let mut end = start;
    for row in rows[start..].iter().take(MAX_COLLECTION_ENTRIES) {
        let row_len = measure(row)?;
        if 12 + length + row_len > MAX_PAYLOAD_LEN {
            if end == start {
                return Err(input_error("WAL indivisible mutation row exceeds 16 MiB"));
            }
            break;
        }
        length += row_len;
        end += 1;
    }
    Ok((end, length))
}

fn single_len(mutation: &Mutation) -> io::Result<usize> {
    // Include the batch prefix while measuring so one indivisible event must fit.
    let mut length = 13;
    match mutation {
        Mutation::Set {
            key,
            value,
            expires_at,
        } => {
            add_blob_len(&mut length, key)?;
            add_blob_len(&mut length, value)?;
            add_len(&mut length, if expires_at.is_some() { 9 } else { 1 })?;
        }
        Mutation::Delete { key } => add_blob_len(&mut length, key)?,
        Mutation::Pop { key, count, .. } => {
            if *count == 0 {
                return Err(input_error("WAL Pop count must be nonzero"));
            }
            add_blob_len(&mut length, key)?;
            add_len(&mut length, 9)?;
        }
        Mutation::Expire { key, expires_at } => {
            add_blob_len(&mut length, key)?;
            add_len(&mut length, if expires_at.is_some() { 9 } else { 1 })?;
        }
        Mutation::Clear => {}
        Mutation::Rename { from, to } => {
            add_blob_len(&mut length, from)?;
            add_blob_len(&mut length, to)?;
        }
        Mutation::ListRemove { key, value, count } => {
            if *count == 0 {
                return Err(input_error("WAL ListRemove count must be nonzero"));
            }
            add_blob_len(&mut length, key)?;
            add_blob_len(&mut length, value)?;
            add_len(&mut length, 8)?;
        }
        Mutation::ListSet { key, value, .. } => {
            add_blob_len(&mut length, key)?;
            add_len(&mut length, 8)?;
            add_blob_len(&mut length, value)?;
        }
        Mutation::ListInsert {
            key, value, pivot, ..
        } => {
            add_blob_len(&mut length, key)?;
            add_len(&mut length, 9)?;
            add_blob_len(&mut length, value)?;
            add_blob_len(&mut length, pivot)?;
        }
        Mutation::Queue { key, effect } => {
            add_blob_len(&mut length, key)?;
            add_blob_len(&mut length, effect)?;
        }
        Mutation::Typed { key, kind, effect } => {
            if !(1..=10).contains(kind)
                || effect.is_empty()
                || effect.len() > crate::engine::typed::codec::MAX_EFFECT_BYTES
            {
                return Err(input_error("invalid typed mutation kind or effect length"));
            }
            add_blob_len(&mut length, key)?;
            add_len(&mut length, 1)?;
            add_blob_len(&mut length, effect)?;
        }
        Mutation::SelectDatabase { .. } => add_len(&mut length, 4)?,
        Mutation::ResetGlobals | Mutation::ClearNativePosition => {}
        Mutation::SwapDatabases { .. } => add_len(&mut length, 8)?,
        Mutation::Search { effect } => {
            if effect.is_empty() || effect.len() > crate::engine::typed::codec::MAX_EFFECT_BYTES {
                return Err(input_error("invalid search effect length"));
            }
            add_blob_len(&mut length, effect)?;
        }
        Mutation::Functions { snapshot } => add_blob_len(&mut length, snapshot)?,
        Mutation::NativePosition { history, .. } => {
            if history.len() != 40 || !history.iter().all(u8::is_ascii_hexdigit) {
                return Err(input_error("invalid native replication history"));
            }
            add_blob_len(&mut length, history)?;
            add_len(&mut length, 8)?;
        }
        Mutation::RdbAuxiliary { key, value } => {
            add_blob_len(&mut length, key)?;
            add_blob_len(&mut length, value)?;
        }
        _ => unreachable!("vector mutations are planned as borrowed chunks"),
    }
    Ok(length - 12)
}

fn put_blob(buffer: &mut Vec<u8>, value: &[u8]) {
    buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buffer.extend_from_slice(value);
}

fn put_expiry(buffer: &mut Vec<u8>, expires_at: Option<i64>) {
    buffer.push(u8::from(expires_at.is_some()));
    if let Some(expires_at) = expires_at {
        buffer.extend_from_slice(&expires_at.to_le_bytes());
    }
}

fn put_values(buffer: &mut Vec<u8>, values: &[Bytes]) {
    buffer.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for value in values {
        put_blob(buffer, value);
    }
}

// Payload: timestamp:i64, mutation_count:u32, then tagged mutations. All integers LE.
// A blob is length:u32 + bytes; bool/optional flags are exactly 0 or 1.
// 1 Set: key, value, optional expiry:i64. 2 Delete: key.
// 3 Push: key, right:bool, value_count:u32, values. 4 Pop: key, right:bool, count:u64.
// 5 Expire: key, optional expiry:i64 (None means PERSIST).
// 6 SetAdd / 7 SetRemove / 9 HashDelete / 11 SortedSetRemove: key, count:u32, blobs.
// 8 HashSet: key, count:u32, then (field, value, optional expiry:i64).
// 10 SortedSetAdd: key, count:u32, then (member, score:f64 LE); NaN is invalid.
// Collection counts are 1..=1024. 12 Clear: no payload. 13 Rename: from, to.
// 14 ListRemove: key, value, count:i64 (nonzero). 15 ListSet: key, index:u64, value.
// 16 Queue: key, opaque resolved effect. One indivisible bounded mutation.
// 17 Typed: key, stable kind:u8, opaque resolved bounded effect. Tags 1..16 unchanged.
fn encode_part(part: &Part<'_>, buffer: &mut Vec<u8>) {
    match part {
        Part::Blobs {
            tag,
            key,
            values,
            right,
        } => {
            buffer.push(*tag);
            put_blob(buffer, key);
            if let Some(right) = right {
                buffer.push(u8::from(*right));
            }
            put_values(buffer, values);
        }
        Part::Hash { key, fields } => {
            buffer.push(8);
            put_blob(buffer, key);
            buffer.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for (field, value, expires_at) in *fields {
                put_blob(buffer, field);
                put_blob(buffer, value);
                put_expiry(buffer, *expires_at);
            }
        }
        Part::Sorted { key, entries } => {
            buffer.push(10);
            put_blob(buffer, key);
            buffer.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for (member, score) in *entries {
                put_blob(buffer, member);
                buffer.extend_from_slice(&score.to_le_bytes());
            }
        }
        Part::Single(mutation) => match mutation {
            Mutation::Set {
                key,
                value,
                expires_at,
            } => {
                buffer.push(1);
                put_blob(buffer, key);
                put_blob(buffer, value);
                put_expiry(buffer, *expires_at);
            }
            Mutation::Delete { key } => {
                buffer.push(2);
                put_blob(buffer, key);
            }
            Mutation::Pop { key, count, right } => {
                buffer.push(4);
                put_blob(buffer, key);
                buffer.push(u8::from(*right));
                buffer.extend_from_slice(&count.to_le_bytes());
            }
            Mutation::Expire { key, expires_at } => {
                buffer.push(5);
                put_blob(buffer, key);
                put_expiry(buffer, *expires_at);
            }
            Mutation::Clear => buffer.push(12),
            Mutation::Rename { from, to } => {
                buffer.push(13);
                put_blob(buffer, from);
                put_blob(buffer, to);
            }
            Mutation::ListRemove { key, value, count } => {
                buffer.push(14);
                put_blob(buffer, key);
                put_blob(buffer, value);
                buffer.extend_from_slice(&count.to_le_bytes());
            }
            Mutation::ListSet { key, index, value } => {
                buffer.push(15);
                put_blob(buffer, key);
                buffer.extend_from_slice(&index.to_le_bytes());
                put_blob(buffer, value);
            }
            Mutation::Queue { key, effect } => {
                buffer.push(16);
                put_blob(buffer, key);
                put_blob(buffer, effect);
            }
            Mutation::Typed { key, kind, effect } => {
                buffer.push(17);
                put_blob(buffer, key);
                buffer.push(*kind);
                put_blob(buffer, effect);
            }
            Mutation::SelectDatabase { database } => {
                buffer.push(18);
                buffer.extend_from_slice(&database.to_le_bytes());
            }
            Mutation::ListInsert {
                key,
                index,
                value,
                pivot,
                before,
            } => {
                buffer.push(19);
                put_blob(buffer, key);
                buffer.extend_from_slice(&index.to_le_bytes());
                put_blob(buffer, value);
                put_blob(buffer, pivot);
                buffer.push(u8::from(*before));
            }
            Mutation::Search { effect } => {
                buffer.push(20);
                put_blob(buffer, effect);
            }
            Mutation::Functions { snapshot } => {
                buffer.push(21);
                put_blob(buffer, snapshot);
            }
            Mutation::NativePosition { history, offset } => {
                buffer.push(22);
                put_blob(buffer, history);
                buffer.extend_from_slice(&offset.to_le_bytes());
            }
            Mutation::RdbAuxiliary { key, value } => {
                buffer.push(23);
                put_blob(buffer, key);
                put_blob(buffer, value);
            }
            Mutation::ResetGlobals => buffer.push(24),
            Mutation::SwapDatabases { first, second } => {
                buffer.push(25);
                buffer.extend_from_slice(&first.to_le_bytes());
                buffer.extend_from_slice(&second.to_le_bytes());
            }
            Mutation::ClearNativePosition => buffer.push(26),
            _ => unreachable!("vector mutations are encoded as borrowed chunks"),
        },
    }
}

struct Decoder {
    payload: Bytes,
    position: usize,
}

impl Decoder {
    fn error(&self, message: impl std::fmt::Display) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("payload byte {}: {message}", self.position),
        )
    }

    fn remaining(&self) -> usize {
        self.payload.len() - self.position
    }

    fn take(&mut self, length: usize) -> io::Result<&[u8]> {
        if length > self.remaining() {
            return Err(self.error("field extends beyond payload"));
        }
        let start = self.position;
        self.position += length;
        Ok(&self.payload[start..self.position])
    }

    fn byte(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn boolean(&mut self) -> io::Result<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(self.error(format!("invalid boolean/option flag {value}"))),
        }
    }

    fn expiry(&mut self) -> io::Result<Option<i64>> {
        if self.boolean()? {
            Ok(Some(self.i64()?))
        } else {
            Ok(None)
        }
    }

    fn collection_count(&mut self, minimum_entry_len: usize) -> io::Result<usize> {
        let count = self.u32()? as usize;
        if count == 0
            || count > MAX_COLLECTION_ENTRIES
            || count > self.remaining() / minimum_entry_len
        {
            return Err(self.error(format!("invalid collection entry count {count}")));
        }
        Ok(count)
    }

    fn members(&mut self) -> io::Result<(Bytes, Vec<Bytes>)> {
        let key = self.blob()?;
        let count = self.collection_count(4)?;
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            members.push(self.blob()?);
        }
        Ok((key, members))
    }

    fn score(&mut self) -> io::Result<f64> {
        let score = f64::from_le_bytes(self.take(8)?.try_into().unwrap());
        if score.is_nan() {
            return Err(self.error("sorted-set score must not be NaN"));
        }
        Ok(score)
    }

    fn blob(&mut self) -> io::Result<Bytes> {
        let length = self.u32()? as usize;
        if length > MAX_PAYLOAD_LEN || length > self.remaining() {
            return Err(self.error(format!("invalid byte-string length {length}")));
        }
        let start = self.position;
        self.position += length;
        Ok(self.payload.slice(start..self.position))
    }
}

// Control frames reserve mutation_count=0. Their exact lengths are part of WAL1:
// BEGIN = timestamp:i64, 0:u32, 'B':u8, begin_sequence:u64 (21 bytes);
// COMMIT adds data_frame_count:u64 after the same prefix with 'C' (29 bytes).
fn decode_record(payload: Bytes) -> io::Result<Record> {
    if payload.len() < 12 || payload[8..12] != [0; 4] {
        return decode(payload).map(Record::Data);
    }
    let mut decoder = Decoder {
        payload,
        position: 0,
    };
    let timestamp = decoder.i64()?;
    let _count = decoder.u32()?;
    let kind = decoder.byte()?;
    let id = decoder.u64()?;
    let record = match kind {
        b'B' => Record::Begin { timestamp, id },
        b'C' => Record::Commit {
            timestamp,
            id,
            data_frames: decoder.u64()?,
        },
        _ => return Err(decoder.error(format!("unknown group control kind {kind}"))),
    };
    if decoder.remaining() != 0 {
        return Err(decoder.error("trailing bytes after group control"));
    }
    Ok(record)
}

fn decode(payload: Bytes) -> io::Result<Batch> {
    let mut decoder = Decoder {
        payload,
        position: 0,
    };
    let timestamp_ms = decoder.i64()?;
    let count = decoder.u32()? as usize;
    if count == 0 || count > MAX_MUTATIONS || count > decoder.remaining() {
        return Err(decoder.error(format!("invalid mutation count {count}")));
    }
    let mut mutations = Vec::with_capacity(count);
    for _ in 0..count {
        let tag = decoder.byte()?;
        let mutation = match tag {
            1 => Mutation::Set {
                key: decoder.blob()?,
                value: decoder.blob()?,
                expires_at: decoder.expiry()?,
            },
            2 => Mutation::Delete {
                key: decoder.blob()?,
            },
            3 => {
                let key = decoder.blob()?;
                let right = decoder.boolean()?;
                let count = decoder.u32()? as usize;
                if count == 0 || count > MAX_PUSH_VALUES || count > decoder.remaining() / 4 {
                    return Err(decoder.error(format!("invalid Push value count {count}")));
                }
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(decoder.blob()?);
                }
                Mutation::Push { key, values, right }
            }
            4 => {
                let key = decoder.blob()?;
                let right = decoder.boolean()?;
                let count = decoder.u64()?;
                if count == 0 {
                    return Err(decoder.error("Pop count must be nonzero"));
                }
                Mutation::Pop { key, count, right }
            }
            5 => Mutation::Expire {
                key: decoder.blob()?,
                expires_at: decoder.expiry()?,
            },
            6 => {
                let (key, members) = decoder.members()?;
                Mutation::SetAdd { key, members }
            }
            7 => {
                let (key, members) = decoder.members()?;
                Mutation::SetRemove { key, members }
            }
            8 => {
                let key = decoder.blob()?;
                let count = decoder.collection_count(9)?;
                let mut fields = Vec::with_capacity(count);
                for _ in 0..count {
                    fields.push((decoder.blob()?, decoder.blob()?, decoder.expiry()?));
                }
                Mutation::HashSet { key, fields }
            }
            9 => {
                let (key, fields) = decoder.members()?;
                Mutation::HashDelete { key, fields }
            }
            10 => {
                let key = decoder.blob()?;
                let count = decoder.collection_count(12)?;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push((decoder.blob()?, decoder.score()?));
                }
                Mutation::SortedSetAdd { key, entries }
            }
            11 => {
                let (key, members) = decoder.members()?;
                Mutation::SortedSetRemove { key, members }
            }
            12 => Mutation::Clear,
            13 => Mutation::Rename {
                from: decoder.blob()?,
                to: decoder.blob()?,
            },
            14 => {
                let key = decoder.blob()?;
                let value = decoder.blob()?;
                let count = decoder.i64()?;
                if count == 0 {
                    return Err(decoder.error("ListRemove count must be nonzero"));
                }
                Mutation::ListRemove { key, value, count }
            }
            15 => Mutation::ListSet {
                key: decoder.blob()?,
                index: decoder.u64()?,
                value: decoder.blob()?,
            },
            16 => Mutation::Queue {
                key: decoder.blob()?,
                effect: decoder.blob()?,
            },
            17 => {
                let key = decoder.blob()?;
                let kind = decoder.byte()?;
                let effect = decoder.blob()?;
                if !(1..=10).contains(&kind)
                    || effect.is_empty()
                    || effect.len() > crate::engine::typed::codec::MAX_EFFECT_BYTES
                {
                    return Err(decoder.error("invalid typed mutation kind or effect length"));
                }
                Mutation::Typed { key, kind, effect }
            }
            18 => Mutation::SelectDatabase {
                database: decoder.u32()?,
            },
            19 => Mutation::ListInsert {
                key: decoder.blob()?,
                index: decoder.u64()?,
                value: decoder.blob()?,
                pivot: decoder.blob()?,
                before: match decoder.byte()? {
                    0 => false,
                    1 => true,
                    _ => return Err(decoder.error("invalid ListInsert direction")),
                },
            },
            20 => {
                let effect = decoder.blob()?;
                if effect.is_empty() || effect.len() > crate::engine::typed::codec::MAX_EFFECT_BYTES
                {
                    return Err(decoder.error("invalid search effect length"));
                }
                Mutation::Search { effect }
            }
            21 => Mutation::Functions {
                snapshot: decoder.blob()?,
            },
            22 => {
                let history = decoder.blob()?;
                if history.len() != 40 || !history.iter().all(u8::is_ascii_hexdigit) {
                    return Err(decoder.error("invalid native replication history"));
                }
                Mutation::NativePosition {
                    history,
                    offset: decoder.u64()?,
                }
            }
            23 => Mutation::RdbAuxiliary {
                key: decoder.blob()?,
                value: decoder.blob()?,
            },
            24 => Mutation::ResetGlobals,
            25 => Mutation::SwapDatabases {
                first: decoder.u32()?,
                second: decoder.u32()?,
            },
            26 => Mutation::ClearNativePosition,
            _ => return Err(decoder.error(format!("unknown mutation tag {tag}"))),
        };
        mutations.push(mutation);
    }
    if decoder.remaining() != 0 {
        return Err(decoder.error("trailing bytes after mutations"));
    }
    Ok(Batch {
        timestamp_ms,
        mutations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{Engine, Execution},
        protocol::Resp,
    };
    use std::{
        collections::HashMap,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "ffkv-wal-{}-{timestamp}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn wal_path(&self) -> PathBuf {
            self.0.join("wal.ffkv")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn set_batch(timestamp_ms: i64, key: &'static [u8], value: &'static [u8]) -> Batch {
        Batch {
            timestamp_ms,
            mutations: vec![Mutation::Set {
                key: Bytes::from_static(key),
                value: Bytes::from_static(value),
                expires_at: None,
            }],
        }
    }

    fn run_binary(engine: &mut Engine, args: &[&[u8]]) -> Resp {
        let args: Vec<Bytes> = args.iter().map(|arg| Bytes::copy_from_slice(arg)).collect();
        match engine.execute(&args).unwrap() {
            Execution::Reply(reply) | Execution::Pushed { reply, .. } => reply,
            Execution::Block(_) | Execution::StreamBlock(_) => {
                panic!("WAL tests do not use blocking commands")
            }
        }
    }

    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        let args: Vec<&[u8]> = args.iter().map(|arg| arg.as_bytes()).collect();
        run_binary(engine, &args)
    }

    fn bulk(value: &'static [u8]) -> Resp {
        Resp::Bulk(Some(Bytes::from_static(value)))
    }

    fn array(values: &[&'static [u8]]) -> Resp {
        Resp::Array(Some(values.iter().map(|value| bulk(value)).collect()))
    }

    async fn persist(engine: &mut Engine, wal: &mut Wal, args: &[&str]) -> Resp {
        let reply = run(engine, args);
        assert!(!matches!(&reply, Resp::Error(_)), "{args:?}: {reply:?}");
        let batch = engine.take_batch().expect("command must record its change");
        wal.append(&batch).await.unwrap();
        reply
    }

    fn collection_mutation(tag: u8, count: usize) -> Mutation {
        let key = Bytes::from_static(b"k");
        let members = vec![Bytes::from_static(b"m"); count];
        match tag {
            6 => Mutation::SetAdd { key, members },
            7 => Mutation::SetRemove { key, members },
            8 => Mutation::HashSet {
                key,
                fields: members
                    .into_iter()
                    .map(|field| (field, Bytes::from_static(b"v"), None))
                    .collect(),
            },
            9 => Mutation::HashDelete {
                key,
                fields: members,
            },
            10 => Mutation::SortedSetAdd {
                key,
                entries: members.into_iter().map(|member| (member, 1.25)).collect(),
            },
            11 => Mutation::SortedSetRemove { key, members },
            _ => panic!("not a collection tag"),
        }
    }

    fn opening_error(dir: &Path) -> io::Error {
        match Wal::open(dir, Durability::Strict, |_| Ok(())) {
            Ok(_) => panic!("corrupt or locked WAL was accepted"),
            Err(error) => error,
        }
    }

    async fn two_records(dir: &TempDir) -> usize {
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        wal.append(&set_batch(100, b"retained", b"first"))
            .await
            .unwrap();
        let boundary = fs::metadata(dir.wal_path()).unwrap().len() as usize;
        wal.append(&set_batch(200, b"torn", b"second"))
            .await
            .unwrap();
        wal.finish().await.unwrap();
        drop(wal);
        boundary
    }

    fn refresh_header_crc(bytes: &mut [u8], record: usize) {
        let checksum = crc32c::crc32c(&bytes[record..record + 16]);
        bytes[record + 16..record + 20].copy_from_slice(&checksum.to_le_bytes());
    }

    fn refresh_payload_crc(bytes: &mut [u8], record: usize) {
        let length = u32::from_le_bytes(bytes[record + 4..record + 8].try_into().unwrap()) as usize;
        let checksum = crc32c::crc32c(&bytes[record + 24..record + 24 + length]);
        bytes[record + 20..record + 24].copy_from_slice(&checksum.to_le_bytes());
    }

    fn frame_boundaries(bytes: &[u8]) -> Vec<(usize, usize)> {
        let mut offset = FILE_HEADER.len();
        let mut frames = Vec::new();
        while offset < bytes.len() {
            let length =
                u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            assert!(length <= MAX_PAYLOAD_LEN);
            let end = offset + RECORD_HEADER_LEN + length;
            assert!(end <= bytes.len());
            frames.push((offset, end));
            offset = end;
        }
        frames
    }

    async fn grouped_fixture(dir: &TempDir) -> Vec<u8> {
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        wal.append(&set_batch(100, b"target", b"original"))
            .await
            .unwrap();
        let mut mutations = vec![Mutation::Delete {
            key: Bytes::from_static(b"target"),
        }];
        for index in 0..MAX_MUTATIONS + 1 {
            mutations.push(Mutation::Set {
                key: Bytes::from(format!("group:{index}")),
                value: Bytes::from_static(b"stored"),
                expires_at: None,
            });
        }
        mutations.push(Mutation::Set {
            key: Bytes::from_static(b"target"),
            value: Bytes::from_static(b"replacement"),
            expires_at: None,
        });
        wal.append(&Batch {
            timestamp_ms: 200,
            mutations,
        })
        .await
        .unwrap();
        wal.finish().await.unwrap();
        drop(wal);
        fs::read(dir.wal_path()).unwrap()
    }

    fn assert_mixed_state(engine: &mut Engine, updated: bool) {
        assert_eq!(run(engine, &["GET", "old"]), bulk(b"string"));
        assert_eq!(run(engine, &["EXISTS", "deleted"]), Resp::Integer(0));
        assert_eq!(
            run(engine, &["LRANGE", "list", "0", "-1"]),
            array(&[b"remove", b"changed", b"tail"])
        );
        assert_eq!(run(engine, &["PTTL", "list"]), Resp::Integer(-1));
        assert_eq!(run(engine, &["EXISTS", "set"]), Resp::Integer(0));
        assert_eq!(
            run(engine, &["SCARD", "renamed"]),
            Resp::Integer(if updated { 3 } else { 2 })
        );
        for member in ["a", "c"] {
            assert_eq!(
                run(engine, &["SISMEMBER", "renamed", member]),
                Resp::Integer(1)
            );
        }
        assert_eq!(
            run(engine, &["SISMEMBER", "renamed", "b"]),
            Resp::Integer(0)
        );
        assert_eq!(
            run(engine, &["HGET", "hash", "keep"]),
            bulk(if updated { b"updated" } else { b"value" })
        );
        assert_eq!(run(engine, &["HGET", "hash", "remove"]), Resp::Bulk(None));
        assert_eq!(run(engine, &["ZCARD", "zset"]), Resp::Integer(3));
        assert_eq!(
            run(engine, &["ZRANGE", "zset", "0", "-1"]),
            array(&[b"low", b"one", b"high"])
        );
    }

    #[tokio::test]
    async fn engine_collections_and_old_records_survive_append_and_multiple_reopens() {
        let dir = TempDir::new();
        let mut source = Engine::new();
        source.set_recording(true);
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        let commands: &[&[&str]] = &[
            &["SET", "old", "string"],
            &["SET", "deleted", "value"],
            &["DEL", "deleted"],
            &[
                "RPUSH", "list", "head", "remove", "middle", "remove", "tail",
            ],
            &["LSET", "list", "2", "changed"],
            &["LREM", "list", "-1", "remove"],
            &["LPOP", "list"],
            &["PEXPIREAT", "list", "4102444800000"],
            &["PERSIST", "list"],
            &["SADD", "set", "a", "b", "c"],
            &["SREM", "set", "b"],
            &["HSET", "hash", "keep", "value", "remove", "gone"],
            &["HDEL", "hash", "remove"],
            &[
                "ZADD", "zset", "-inf", "low", "1.25", "one", "inf", "high", "2", "remove",
            ],
            &["ZINCRBY", "zset", "0.5", "one"],
            &["ZREM", "zset", "remove"],
            &["RENAME", "set", "renamed"],
        ];
        for args in commands {
            persist(&mut source, &mut wal, args).await;
        }
        assert_mixed_state(&mut source, false);
        wal.finish().await.unwrap();
        drop(wal);

        let mut recovered = Engine::new();
        let (mut wal, recovery) = Wal::open(&dir.0, Durability::Everysec, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(recovery.records, commands.len() as u64);
        assert_eq!(recovery.truncated_bytes, 0);
        assert_mixed_state(&mut recovered, false);
        assert!(recovered.take_batch().is_none(), "replay must not record");
        recovered.set_recording(true);
        for args in [
            &["SADD", "renamed", "d"][..],
            &["HSET", "hash", "keep", "updated"][..],
            &["ZADD", "zset", "9", "one"][..],
        ] {
            persist(&mut recovered, &mut wal, args).await;
        }
        wal.periodic_sync().await.unwrap();
        wal.finish().await.unwrap();
        drop(wal);

        let mut recovered = Engine::new();
        let (_wal, recovery) =
            Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
        assert_eq!(recovery.records, commands.len() as u64 + 3);
        assert_eq!(recovery.truncated_bytes, 0);
        assert_mixed_state(&mut recovered, true);
    }

    #[tokio::test]
    async fn clear_is_payload_free_and_can_be_the_only_or_every_mutation() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        wal.append(&set_batch(100, b"before", b"value"))
            .await
            .unwrap();
        let before_clear = fs::metadata(dir.wal_path()).unwrap().len();
        wal.append(&Batch {
            timestamp_ms: 200,
            mutations: vec![Mutation::Clear],
        })
        .await
        .unwrap();
        assert_eq!(
            fs::metadata(dir.wal_path()).unwrap().len() - before_clear,
            (RECORD_HEADER_LEN + 12 + 1) as u64
        );
        wal.append(&Batch {
            timestamp_ms: 300,
            mutations: (0..MAX_MUTATIONS).map(|_| Mutation::Clear).collect(),
        })
        .await
        .unwrap();
        wal.append(&set_batch(400, b"after", b"retained"))
            .await
            .unwrap();
        drop(wal);
        let mut recovered = Engine::new();
        let (_wal, recovery) =
            Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
        assert_eq!(recovery.records, 4);
        assert_eq!(run(&mut recovered, &["DBSIZE"]), Resp::Integer(1));
        assert_eq!(run(&mut recovered, &["GET", "before"]), Resp::Bulk(None));
        assert_eq!(run(&mut recovered, &["GET", "after"]), bulk(b"retained"));
    }

    #[tokio::test]
    async fn hash_fields_replay_binary_blobs_and_individual_absolute_expiries() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        wal.append(&Batch {
            timestamp_ms: 100,
            mutations: vec![Mutation::HashSet {
                key: Bytes::from_static(b"h\0\xff"),
                fields: vec![
                    (Bytes::new(), Bytes::from_static(b"\0\xff\r\n"), None),
                    (
                        Bytes::from_static(b"future"),
                        Bytes::from_static(b"kept"),
                        Some(4_102_444_800_000),
                    ),
                    (
                        Bytes::from_static(b"past"),
                        Bytes::from_static(b"expired"),
                        Some(101),
                    ),
                ],
            }],
        })
        .await
        .unwrap();
        drop(wal);
        let mut recovered = Engine::new();
        let (_wal, recovery) =
            Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
        recovered.finish_recovery().unwrap();
        assert_eq!(recovery.records, 1);
        assert_eq!(
            run_binary(&mut recovered, &[b"HGET", b"h\0\xff", b""]),
            bulk(b"\0\xff\r\n")
        );
        assert_eq!(
            run_binary(&mut recovered, &[b"HLEN", b"h\0\xff"]),
            Resp::Integer(2)
        );
        assert_eq!(
            run_binary(&mut recovered, &[b"HGET", b"h\0\xff", b"past"]),
            Resp::Bulk(None)
        );
        assert_eq!(
            run_binary(
                &mut recovered,
                &[b"HPEXPIRETIME", b"h\0\xff", b"FIELDS", b"1", b"future"]
            ),
            Resp::Array(Some(vec![Resp::Integer(4_102_444_800_000)]))
        );
    }

    #[tokio::test]
    async fn torn_collection_store_never_replays_just_its_destination_delete() {
        for cut_kind in 0..3 {
            let dir = TempDir::new();
            let mut source = Engine::new();
            source.set_recording(true);
            let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
            persist(&mut source, &mut wal, &["SADD", "source", "new"]).await;
            persist(&mut source, &mut wal, &["SADD", "target", "old"]).await;
            let boundary = fs::metadata(dir.wal_path()).unwrap().len();
            assert_eq!(
                run(&mut source, &["SUNIONSTORE", "target", "source"]),
                Resp::Integer(1)
            );
            let batch = source.take_batch().unwrap();
            wal.append(&batch).await.unwrap();
            drop(wal);
            let full_len = fs::metadata(dir.wal_path()).unwrap().len();
            let cut = match cut_kind {
                0 => boundary + 13,
                // An interior payload cut must not expose any of the store.
                1 => boundary + (full_len - boundary) / 2,
                _ => full_len - 1,
            };
            let file = OpenOptions::new().write(true).open(dir.wal_path()).unwrap();
            file.set_len(cut).unwrap();
            file.sync_all().unwrap();
            drop(file);
            let mut recovered = Engine::new();
            let (mut wal, recovery) =
                Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
            assert_eq!(recovery.records, 2);
            assert_eq!(recovery.truncated_bytes, cut - boundary);
            assert_eq!(run(&mut recovered, &["SCARD", "target"]), Resp::Integer(1));
            assert_eq!(
                run(&mut recovered, &["SISMEMBER", "target", "old"]),
                Resp::Integer(1)
            );
            assert_eq!(
                run(&mut recovered, &["SISMEMBER", "target", "new"]),
                Resp::Integer(0)
            );
            recovered.set_recording(true);
            persist(&mut recovered, &mut wal, &["SADD", "target", "after"]).await;
            drop(wal);
            let mut recovered = Engine::new();
            let (_wal, recovery) =
                Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
            assert_eq!(recovery.records, 3);
            assert_eq!(recovery.truncated_bytes, 0);
            assert_eq!(run(&mut recovered, &["SCARD", "target"]), Resp::Integer(2));
        }
    }

    #[tokio::test]
    async fn every_collection_accepts_1024_entries_and_replays_into_engine_state() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        let members: Vec<Bytes> = (0..MAX_COLLECTION_ENTRIES as u32)
            .map(|index| Bytes::copy_from_slice(&index.to_le_bytes()))
            .collect();
        let mutations = [
            Mutation::SetAdd {
                key: Bytes::from_static(b"set"),
                members: members.clone(),
            },
            Mutation::SetRemove {
                key: Bytes::from_static(b"set"),
                members: members.clone(),
            },
            Mutation::HashSet {
                key: Bytes::from_static(b"hash"),
                fields: members
                    .iter()
                    .map(|member| (member.clone(), member.clone(), None))
                    .collect(),
            },
            Mutation::HashDelete {
                key: Bytes::from_static(b"hash"),
                fields: members.clone(),
            },
            Mutation::SortedSetAdd {
                key: Bytes::from_static(b"zset"),
                entries: members
                    .iter()
                    .enumerate()
                    .map(|(index, member)| (member.clone(), index as f64))
                    .collect(),
            },
            Mutation::SortedSetRemove {
                key: Bytes::from_static(b"zset"),
                members,
            },
        ];
        for (index, mutation) in mutations.into_iter().enumerate() {
            wal.append(&Batch {
                timestamp_ms: 100 + index as i64,
                mutations: vec![mutation],
            })
            .await
            .unwrap();
        }
        drop(wal);
        let mut recovered = Engine::new();
        let mut applied = 0;
        let (_wal, recovery) = Wal::open(&dir.0, Durability::Strict, |batch| {
            recovered.replay(batch)?;
            applied += 1;
            let query: &[&str] = match applied {
                1 => &["SCARD", "set"],
                2 => &["EXISTS", "set"],
                3 => &["HLEN", "hash"],
                4 => &["EXISTS", "hash"],
                5 => &["ZCARD", "zset"],
                6 => &["EXISTS", "zset"],
                _ => unreachable!(),
            };
            assert_eq!(
                run(&mut recovered, query),
                Resp::Integer(if applied % 2 == 1 { 1024 } else { 0 })
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(recovery.records, 6);
        assert_eq!(run(&mut recovered, &["DBSIZE"]), Resp::Integer(0));
    }

    #[tokio::test]
    async fn invalid_new_mutations_are_rejected_before_append_without_consuming_sequence() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        wal.append(&set_batch(100, b"before", b"unchanged"))
            .await
            .unwrap();
        let original = fs::read(dir.wal_path()).unwrap();
        let mut invalid_mutations = Vec::new();
        for tag in 6..=11 {
            invalid_mutations.push(collection_mutation(tag, 0));
        }
        for bits in [
            f64::NAN.to_bits(),
            0xfff8_0000_0000_0001,
            0x7ff0_0000_0000_0001,
        ] {
            invalid_mutations.push(Mutation::SortedSetAdd {
                key: Bytes::from_static(b"k"),
                entries: vec![(Bytes::from_static(b"member"), f64::from_bits(bits))],
            });
        }
        invalid_mutations.push(Mutation::ListRemove {
            key: Bytes::from_static(b"list"),
            value: Bytes::from_static(b"value"),
            count: 0,
        });
        let oversized = Bytes::from(vec![0; MAX_PAYLOAD_LEN]);
        invalid_mutations.extend([
            Mutation::SetAdd {
                key: Bytes::from_static(b"k"),
                members: vec![oversized.clone()],
            },
            Mutation::SetRemove {
                key: Bytes::from_static(b"k"),
                members: vec![oversized.clone()],
            },
            Mutation::HashSet {
                key: Bytes::from_static(b"k"),
                fields: vec![(Bytes::new(), oversized.clone(), Some(1_000))],
            },
            Mutation::HashDelete {
                key: Bytes::from_static(b"k"),
                fields: vec![oversized.clone()],
            },
            Mutation::SortedSetAdd {
                key: Bytes::from_static(b"k"),
                entries: vec![(oversized.clone(), 1.0)],
            },
            Mutation::SortedSetRemove {
                key: Bytes::from_static(b"k"),
                members: vec![oversized.clone()],
            },
            Mutation::Rename {
                from: oversized.clone(),
                to: Bytes::new(),
            },
            Mutation::Rename {
                from: Bytes::new(),
                to: oversized.clone(),
            },
            Mutation::ListRemove {
                key: Bytes::from_static(b"k"),
                value: oversized.clone(),
                count: -1,
            },
            Mutation::ListSet {
                key: Bytes::from_static(b"k"),
                index: 0,
                value: oversized,
            },
        ]);
        for mutation in invalid_mutations {
            let error = wal
                .append(&Batch {
                    timestamp_ms: 200,
                    // A valid leading event must not leak when a later event fails validation.
                    mutations: vec![Mutation::Clear, mutation],
                })
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(fs::read(dir.wal_path()).unwrap(), original);
        }
        let mut mutations: Vec<Mutation> =
            (0..MAX_MUTATIONS + 1).map(|_| Mutation::Clear).collect();
        mutations.push(Mutation::SortedSetAdd {
            key: Bytes::from_static(b"late"),
            entries: vec![(Bytes::new(), f64::NAN)],
        });
        assert_eq!(
            wal.append(&Batch {
                timestamp_ms: 250,
                mutations
            })
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(fs::read(dir.wal_path()).unwrap(), original);
        wal.append(&Batch {
            timestamp_ms: 300,
            mutations: vec![Mutation::SetAdd {
                key: Bytes::from_static(b"after"),
                members: vec![Bytes::from_static(b"valid")],
            }],
        })
        .await
        .unwrap();
        drop(wal);
        let mut recovered = Engine::new();
        let (_wal, recovery) =
            Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
        assert_eq!(recovery.records, 2);
        assert_eq!(run(&mut recovered, &["GET", "before"]), bulk(b"unchanged"));
        assert_eq!(
            run(&mut recovered, &["SISMEMBER", "after", "valid"]),
            Resp::Integer(1)
        );
    }

    #[tokio::test]
    async fn checksummed_malformed_new_payloads_fail_without_mutating_the_log() {
        // Offsets below are relative to the record: 24-byte header, 12-byte batch prefix,
        // one-byte tag, then a one-byte key blob. The nested count therefore starts at 42.
        let mut corruptions = Vec::new();
        for tag in 6..=11 {
            for count in [0_u32, 1025, u32::MAX, 2] {
                corruptions.push((tag, 42, count.to_le_bytes().to_vec()));
            }
            corruptions.push((tag, 46, u32::MAX.to_le_bytes().to_vec()));
        }
        corruptions.push((8, 56, vec![2])); // Invalid HashSet optional-expiry flag.
        for bits in [
            f64::NAN.to_bits(),
            0xfff8_0000_0000_0001,
            0x7ff0_0000_0000_0001,
        ] {
            corruptions.push((10, 51, bits.to_le_bytes().to_vec()));
        }
        corruptions.extend([
            (12, 0, vec![0]), // Clear has no payload: append trailing data to its frame.
            (13, 42, u32::MAX.to_le_bytes().to_vec()), // Rename destination blob.
            (14, 47, 0_i64.to_le_bytes().to_vec()), // ListRemove zero count.
            (15, 50, u32::MAX.to_le_bytes().to_vec()), // ListSet value blob.
        ]);
        for (tag, field_offset, replacement) in corruptions {
            let dir = TempDir::new();
            let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
            wal.append(&set_batch(100, b"prefix", b"retained"))
                .await
                .unwrap();
            let record = fs::metadata(dir.wal_path()).unwrap().len() as usize;
            let mutation = match tag {
                6..=11 => collection_mutation(tag, 1),
                12 => Mutation::Clear,
                13 => Mutation::Rename {
                    from: Bytes::from_static(b"k"),
                    to: Bytes::from_static(b"v"),
                },
                14 => Mutation::ListRemove {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"v"),
                    count: -1,
                },
                15 => Mutation::ListSet {
                    key: Bytes::from_static(b"k"),
                    index: 0,
                    value: Bytes::from_static(b"v"),
                },
                _ => unreachable!(),
            };
            wal.append(&Batch {
                timestamp_ms: 200,
                mutations: vec![mutation],
            })
            .await
            .unwrap();
            drop(wal);
            let mut bytes = fs::read(dir.wal_path()).unwrap();
            if tag == 12 {
                bytes.extend_from_slice(&replacement);
                let length = (bytes.len() - record - RECORD_HEADER_LEN) as u32;
                bytes[record + 4..record + 8].copy_from_slice(&length.to_le_bytes());
                refresh_header_crc(&mut bytes, record);
            } else {
                let start = record + field_offset;
                bytes[start..start + replacement.len()].copy_from_slice(&replacement);
            }
            refresh_payload_crc(&mut bytes, record);
            // Even a repairable torn tail must remain untouched after a complete bad record.
            bytes.extend_from_slice(b"FREC\x01");
            fs::write(dir.wal_path(), &bytes).unwrap();
            let error = opening_error(&dir.0);
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "tag {tag}");
            assert!(
                error.to_string().contains(&format!("offset {record}")),
                "tag {tag}: {error}"
            );
            assert_eq!(fs::read(dir.wal_path()).unwrap(), bytes, "tag {tag}");
        }
    }

    #[tokio::test]
    async fn logical_vectors_over_1024_replay_every_row_in_order() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        let count = 2 * MAX_COLLECTION_ENTRIES + 1;
        let rows: Vec<Bytes> = (0..count)
            .map(|index| Bytes::from(index.to_string()))
            .collect();
        wal.append(&Batch {
            timestamp_ms: 100,
            mutations: vec![
                Mutation::SetAdd {
                    key: Bytes::from_static(b"set"),
                    members: rows.clone(),
                },
                Mutation::HashSet {
                    key: Bytes::from_static(b"hash"),
                    fields: rows
                        .iter()
                        .map(|row| (row.clone(), row.clone(), None))
                        .collect(),
                },
                Mutation::SortedSetAdd {
                    key: Bytes::from_static(b"zset"),
                    entries: rows
                        .iter()
                        .enumerate()
                        .map(|(index, row)| (row.clone(), index as f64))
                        .collect(),
                },
                Mutation::Push {
                    key: Bytes::from_static(b"list"),
                    values: rows,
                    right: false,
                },
            ],
        })
        .await
        .unwrap();
        drop(wal);
        let mut recovered = Engine::new();
        let (_wal, recovery) =
            Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
        assert_eq!(recovery.truncated_bytes, 0);
        for args in [
            &["SCARD", "set"][..],
            &["HLEN", "hash"][..],
            &["ZCARD", "zset"][..],
            &["LLEN", "list"][..],
        ] {
            assert_eq!(run(&mut recovered, args), Resp::Integer(count as i64));
        }
        assert_eq!(
            run(&mut recovered, &["HGET", "hash", "2048"]),
            bulk(b"2048")
        );
        assert_eq!(
            run(&mut recovered, &["LRANGE", "list", "0", "0"]),
            array(&[b"2048"])
        );
        assert_eq!(
            run(&mut recovered, &["LRANGE", "list", "-1", "-1"]),
            array(&[b"0"])
        );
    }

    #[tokio::test]
    async fn batches_over_16_mib_commit_bounded_frames_and_recover_exact_values() {
        for policy in [Durability::Strict, Durability::Everysec] {
            let dir = TempDir::new();
            let (mut wal, _) = Wal::open(&dir.0, policy, |_| Ok(())).unwrap();
            wal.append(&set_batch(100, b"target", b"original"))
                .await
                .unwrap();
            let value = Bytes::from(vec![0xa5; 1024 * 1024]);
            let mut mutations = vec![Mutation::Delete {
                key: Bytes::from_static(b"target"),
            }];
            for index in 0..17 {
                mutations.push(Mutation::Set {
                    key: Bytes::from(format!("large:{index}")),
                    value: value.clone(),
                    expires_at: None,
                });
            }
            mutations.push(Mutation::Set {
                key: Bytes::from_static(b"target"),
                value: Bytes::from_static(b"replacement"),
                expires_at: None,
            });
            wal.append(&Batch {
                timestamp_ms: 200,
                mutations,
            })
            .await
            .unwrap();
            wal.finish().await.unwrap();
            assert!(wal.buffer.capacity() <= RECORD_HEADER_LEN + MAX_PAYLOAD_LEN);
            drop(wal);
            let bytes = fs::read(dir.wal_path()).unwrap();
            assert!(bytes.len() > MAX_PAYLOAD_LEN);
            let frames = frame_boundaries(&bytes);
            assert_eq!(frames.len(), 5, "prefix, BEGIN, two data frames, COMMIT");
            assert_eq!(frames[1].1 - frames[1].0, RECORD_HEADER_LEN + 21);
            assert_eq!(frames[4].1 - frames[4].0, RECORD_HEADER_LEN + 29);
            let mut recovered = Engine::new();
            let (_wal, recovery) =
                Wal::open(&dir.0, policy, |batch| recovered.replay(batch)).unwrap();
            assert_eq!(recovery.records, 3);
            assert_eq!(recovery.truncated_bytes, 0);
            assert_eq!(run(&mut recovered, &["DBSIZE"]), Resp::Integer(18));
            assert_eq!(
                run(&mut recovered, &["GET", "target"]),
                bulk(b"replacement")
            );
            for index in 0..17 {
                let key = format!("large:{index}");
                assert_eq!(
                    run(&mut recovered, &["GET", &key]),
                    Resp::Bulk(Some(value.clone()))
                );
            }
        }
    }

    #[tokio::test]
    async fn committed_large_event_count_replays_all_chunks_and_accepts_later_records() {
        let dir = TempDir::new();
        let bytes = grouped_fixture(&dir).await;
        assert_eq!(frame_boundaries(&bytes).len(), 5);
        let mut recovered = Engine::new();
        let (mut wal, recovery) =
            Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
        assert_eq!(recovery.records, 3);
        assert_eq!(recovery.truncated_bytes, 0);
        assert_eq!(
            run(&mut recovered, &["DBSIZE"]),
            Resp::Integer((MAX_MUTATIONS + 2) as i64)
        );
        assert_eq!(
            run(&mut recovered, &["GET", "target"]),
            bulk(b"replacement")
        );
        for index in 0..MAX_MUTATIONS + 1 {
            assert_eq!(
                run(&mut recovered, &["GET", &format!("group:{index}")]),
                bulk(b"stored")
            );
        }
        wal.append(&set_batch(300, b"after", b"retained"))
            .await
            .unwrap();
        drop(wal);
        let mut recovered = Engine::new();
        let (_wal, recovery) =
            Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
        assert_eq!(recovery.records, 4);
        assert_eq!(run(&mut recovered, &["GET", "after"]), bulk(b"retained"));
    }

    #[tokio::test]
    async fn uncommitted_groups_at_every_frame_boundary_never_expose_partial_state() {
        let fixture = TempDir::new();
        let bytes = grouped_fixture(&fixture).await;
        let frames = frame_boundaries(&bytes);
        let begin = frames[1].0;
        let mut cuts = vec![begin];
        for &(start, end) in &frames[1..] {
            cuts.extend([start + 13, start + RECORD_HEADER_LEN + 1, end - 1]);
            if end != bytes.len() {
                cuts.push(end);
            }
        }
        for cut in cuts {
            let dir = TempDir::new();
            fs::write(dir.wal_path(), &bytes[..cut]).unwrap();
            let mut recovered = Engine::new();
            let mut applied = 0;
            let (mut wal, recovery) = Wal::open(&dir.0, Durability::Strict, |batch| {
                applied += 1;
                recovered.replay(batch)
            })
            .unwrap();
            assert_eq!(applied, 1, "cut {cut} must not apply group chunks");
            assert_eq!(recovery.records, 1);
            assert_eq!(recovery.truncated_bytes, (cut - begin) as u64);
            assert_eq!(fs::metadata(dir.wal_path()).unwrap().len(), begin as u64);
            assert_eq!(run(&mut recovered, &["DBSIZE"]), Resp::Integer(1));
            assert_eq!(run(&mut recovered, &["GET", "target"]), bulk(b"original"));
            wal.append(&set_batch(300, b"after", b"retained"))
                .await
                .unwrap();
            drop(wal);
            let mut recovered = Engine::new();
            let (_wal, recovery) =
                Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
            assert_eq!(recovery.records, 2);
            assert_eq!(recovery.truncated_bytes, 0);
            assert_eq!(run(&mut recovered, &["GET", "target"]), bulk(b"original"));
            assert_eq!(run(&mut recovered, &["GET", "after"]), bulk(b"retained"));
        }
    }

    #[tokio::test]
    async fn complete_corruption_inside_uncommitted_group_fails_closed_without_repair() {
        let fixture = TempDir::new();
        let bytes = grouped_fixture(&fixture).await;
        let frames = frame_boundaries(&bytes);
        let record = frames[2].0;
        for corruption in 0..5 {
            let dir = TempDir::new();
            let mut corrupt = bytes[..frames[4].0].to_vec(); // Omit COMMIT entirely.
            match corruption {
                0 => corrupt[record] ^= 0x40,
                1 => corrupt[record + 16] ^= 0x40,
                2 => corrupt[record + RECORD_HEADER_LEN] ^= 0x40,
                3 => {
                    corrupt[record + 8..record + 16].copy_from_slice(&99_u64.to_le_bytes());
                    refresh_header_crc(&mut corrupt, record);
                }
                4 => {
                    corrupt[record + 32..record + 36].copy_from_slice(&1025_u32.to_le_bytes());
                    refresh_payload_crc(&mut corrupt, record);
                }
                _ => unreachable!(),
            }
            corrupt.extend_from_slice(b"FREC\x01");
            fs::write(dir.wal_path(), &corrupt).unwrap();
            let mut applied = 0;
            let error = match Wal::open(&dir.0, Durability::Strict, |_| {
                applied += 1;
                Ok(())
            }) {
                Ok(_) => panic!("corrupt uncommitted group was accepted"),
                Err(error) => error,
            };
            assert_eq!(applied, 1, "uncommitted data must never reach callback");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(fs::read(dir.wal_path()).unwrap(), corrupt);
        }
    }

    #[tokio::test]
    async fn malformed_group_controls_and_mismatches_do_not_apply_or_mutate() {
        let fixture = TempDir::new();
        let bytes = grouped_fixture(&fixture).await;
        let frames = frame_boundaries(&bytes);
        for (frame, field, replacement) in [
            (1, 24, 201_i64.to_le_bytes().to_vec()), // Timestamp mismatch with data.
            (1, 36, vec![b'?']),
            (1, 37, 99_u64.to_le_bytes().to_vec()), // BEGIN id != its sequence.
            (4, 24, 201_i64.to_le_bytes().to_vec()),
            (4, 37, 99_u64.to_le_bytes().to_vec()), // COMMIT id != BEGIN.
            (4, 45, 0_u64.to_le_bytes().to_vec()),
            (4, 45, 3_u64.to_le_bytes().to_vec()), // Incorrect data frame count.
        ] {
            let dir = TempDir::new();
            let mut corrupt = bytes.clone();
            let record = frames[frame].0;
            let start = record + field;
            corrupt[start..start + replacement.len()].copy_from_slice(&replacement);
            refresh_payload_crc(&mut corrupt, record);
            corrupt.extend_from_slice(b"FREC\x01");
            fs::write(dir.wal_path(), &corrupt).unwrap();
            let mut applied = 0;
            let error = match Wal::open(&dir.0, Durability::Strict, |_| {
                applied += 1;
                Ok(())
            }) {
                Ok(_) => panic!("malformed group control was accepted"),
                Err(error) => error,
            };
            assert_eq!(applied, 1);
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(fs::read(dir.wal_path()).unwrap(), corrupt);
        }
    }

    #[tokio::test]
    async fn a_single_hash_event_over_16_mib_splits_by_bytes_and_preserves_expiries() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        let value = Bytes::from(vec![0x5a; 1024 * 1024]);
        wal.append(&Batch {
            timestamp_ms: 100,
            mutations: vec![Mutation::HashSet {
                key: Bytes::from_static(b"large-hash"),
                fields: (0..17)
                    .map(|index| {
                        (
                            Bytes::from(index.to_string()),
                            value.clone(),
                            if index % 2 == 0 {
                                Some(4_102_444_800_000)
                            } else {
                                None
                            },
                        )
                    })
                    .collect(),
            }],
        })
        .await
        .unwrap();
        drop(wal);
        let mut recovered = Engine::new();
        let (_wal, recovery) =
            Wal::open(&dir.0, Durability::Strict, |batch| recovered.replay(batch)).unwrap();
        assert_eq!(recovery.records, 2);
        assert_eq!(recovery.truncated_bytes, 0);
        assert_eq!(
            run(&mut recovered, &["HLEN", "large-hash"]),
            Resp::Integer(17)
        );
        for index in 0..17 {
            let field = index.to_string();
            assert_eq!(
                run(&mut recovered, &["HGET", "large-hash", &field]),
                Resp::Bulk(Some(value.clone()))
            );
            assert_eq!(
                run(
                    &mut recovered,
                    &["HPEXPIRETIME", "large-hash", "FIELDS", "1", &field]
                ),
                Resp::Array(Some(vec![Resp::Integer(if index % 2 == 0 {
                    4_102_444_800_000
                } else {
                    -1
                })]))
            );
        }
    }

    #[tokio::test]
    async fn orphan_nested_empty_and_wrong_length_controls_fail_without_repair() {
        let fixture = TempDir::new();
        let bytes = grouped_fixture(&fixture).await;
        let frames = frame_boundaries(&bytes);
        let begin = frames[1].0;
        for mode in 0..5 {
            let dir = TempDir::new();
            let mut corrupt = match mode {
                0 => {
                    // A COMMIT in place of BEGIN, with an otherwise valid global sequence.
                    let mut output = bytes[..begin].to_vec();
                    output.extend_from_slice(&bytes[frames[4].0..frames[4].1]);
                    output[begin + 8..begin + 16].copy_from_slice(&2_u64.to_le_bytes());
                    refresh_header_crc(&mut output, begin);
                    output
                }
                1 => {
                    let mut output = bytes[..frames[1].1].to_vec();
                    let nested = output.len();
                    output.extend_from_slice(&bytes[frames[1].0..frames[1].1]);
                    output[nested + 8..nested + 16].copy_from_slice(&3_u64.to_le_bytes());
                    output[nested + 37..nested + 45].copy_from_slice(&3_u64.to_le_bytes());
                    refresh_header_crc(&mut output, nested);
                    refresh_payload_crc(&mut output, nested);
                    output
                }
                2 => {
                    let mut output = bytes[..frames[1].1].to_vec();
                    let commit = output.len();
                    output.extend_from_slice(&bytes[frames[4].0..frames[4].1]);
                    output[commit + 8..commit + 16].copy_from_slice(&3_u64.to_le_bytes());
                    output[commit + 45..commit + 53].copy_from_slice(&0_u64.to_le_bytes());
                    refresh_header_crc(&mut output, commit);
                    refresh_payload_crc(&mut output, commit);
                    output
                }
                3 | 4 => {
                    let mut output = bytes[..frames[1].1].to_vec();
                    let length: u32 = if mode == 3 {
                        output.push(0);
                        22
                    } else {
                        output.pop();
                        20
                    };
                    output[begin + 4..begin + 8].copy_from_slice(&length.to_le_bytes());
                    refresh_header_crc(&mut output, begin);
                    refresh_payload_crc(&mut output, begin);
                    output
                }
                _ => unreachable!(),
            };
            corrupt.extend_from_slice(b"FREC\x01");
            fs::write(dir.wal_path(), &corrupt).unwrap();
            let mut applied = 0;
            let error = match Wal::open(&dir.0, Durability::Strict, |_| {
                applied += 1;
                Ok(())
            }) {
                Ok(_) => panic!("invalid group structure {mode} was accepted"),
                Err(error) => error,
            };
            assert_eq!(applied, 1);
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(fs::read(dir.wal_path()).unwrap(), corrupt);
        }
    }

    #[tokio::test]
    async fn lock_is_exclusive_through_writes_and_finish_without_truncating_lock() {
        let dir = TempDir::new();
        let lock_path = dir.0.join("LOCK");
        fs::write(&lock_path, b"do not truncate").unwrap();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        assert_eq!(opening_error(&dir.0).kind(), io::ErrorKind::WouldBlock);
        wal.append(&set_batch(100, b"held", b"lock")).await.unwrap();
        wal.finish().await.unwrap();
        assert_eq!(opening_error(&dir.0).kind(), io::ErrorKind::WouldBlock);
        assert_eq!(fs::read(&lock_path).unwrap(), b"do not truncate");
        drop(wal);
        let (_wal, recovery) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        assert_eq!(recovery.records, 1);
    }

    #[tokio::test]
    async fn committed_prefix_survives_both_torn_tail_forms_and_accepts_new_appends() {
        for partial_header in [true, false] {
            let dir = TempDir::new();
            let boundary = two_records(&dir).await;
            let length = fs::metadata(dir.wal_path()).unwrap().len();
            let cut = if partial_header {
                boundary as u64 + 13
            } else {
                length - 1
            };
            let file = OpenOptions::new().write(true).open(dir.wal_path()).unwrap();
            file.set_len(cut).unwrap();
            file.sync_all().unwrap();
            drop(file);
            let mut recovered = HashMap::new();
            let (mut wal, recovery) = Wal::open(&dir.0, Durability::Everysec, |batch| {
                for mutation in batch.mutations {
                    if let Mutation::Set { key, value, .. } = mutation {
                        recovered.insert(key, value);
                    }
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(recovery.records, 1);
            assert_eq!(recovery.truncated_bytes, cut - boundary as u64);
            assert_eq!(fs::metadata(dir.wal_path()).unwrap().len(), boundary as u64);
            assert_eq!(
                recovered.get(b"retained".as_slice()).unwrap().as_ref(),
                b"first"
            );
            assert!(!recovered.contains_key(b"torn".as_slice()));
            wal.append(&set_batch(300, b"new", b"third")).await.unwrap();
            wal.periodic_sync().await.unwrap();
            wal.finish().await.unwrap();
            drop(wal);
            let mut timestamps = Vec::new();
            let (_wal, recovery) = Wal::open(&dir.0, Durability::Strict, |batch| {
                timestamps.push(batch.timestamp_ms);
                Ok(())
            })
            .unwrap();
            assert_eq!(recovery.records, 2);
            assert_eq!(recovery.truncated_bytes, 0);
            assert_eq!(timestamps, [100, 300]);
        }
    }

    #[tokio::test]
    async fn complete_header_or_payload_corruption_fails_without_editing_file() {
        for relative_byte in [0, 16, 24] {
            let dir = TempDir::new();
            let record = two_records(&dir).await;
            let mut bytes = fs::read(dir.wal_path()).unwrap();
            bytes[record + relative_byte] ^= 0x40;
            fs::write(dir.wal_path(), &bytes).unwrap();
            let error = opening_error(&dir.0);
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(error.to_string().contains(&format!("offset {record}")));
            assert_eq!(fs::read(dir.wal_path()).unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn validated_headers_cannot_hide_bad_lengths_sequences_or_logical_payloads() {
        for corruption in 0..6 {
            let dir = TempDir::new();
            let record = two_records(&dir).await;
            let mut bytes = fs::read(dir.wal_path()).unwrap();
            match corruption {
                0 => {
                    bytes[record + 4..record + 8]
                        .copy_from_slice(&((MAX_PAYLOAD_LEN + 1) as u32).to_le_bytes());
                    refresh_header_crc(&mut bytes, record);
                }
                1 => {
                    bytes[record + 8..record + 16].copy_from_slice(&9_u64.to_le_bytes());
                    refresh_header_crc(&mut bytes, record);
                }
                2 => {
                    // Complete, checksummed payload with a byte-string length beyond the frame.
                    bytes[record + 37..record + 41].copy_from_slice(&u32::MAX.to_le_bytes());
                    refresh_payload_crc(&mut bytes, record);
                }
                3 => {
                    bytes[record + 32..record + 36].copy_from_slice(&1025_u32.to_le_bytes());
                    refresh_payload_crc(&mut bytes, record);
                }
                4 => {
                    bytes[record + 36] = 255;
                    refresh_payload_crc(&mut bytes, record);
                }
                5 => {
                    // The final byte is Set's None-expiry flag, not arbitrary padding.
                    *bytes.last_mut().unwrap() = 2;
                    refresh_payload_crc(&mut bytes, record);
                }
                _ => unreachable!(),
            }
            fs::write(dir.wal_path(), &bytes).unwrap();
            let error = opening_error(&dir.0);
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(error.to_string().contains(&format!("offset {record}")));
            assert_eq!(fs::read(dir.wal_path()).unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn nested_push_counts_and_flags_fail_without_allocating_unbounded_values() {
        for (field_offset, replacement) in [
            (43, 0_u32.to_le_bytes().to_vec()),
            (43, 1025_u32.to_le_bytes().to_vec()),
            (43, u32::MAX.to_le_bytes().to_vec()),
            (42, vec![2]),
        ] {
            let dir = TempDir::new();
            let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
            wal.append(&Batch {
                timestamp_ms: 100,
                mutations: vec![Mutation::Push {
                    key: Bytes::from_static(b"k"),
                    values: vec![Bytes::from_static(b"value")],
                    right: true,
                }],
            })
            .await
            .unwrap();
            drop(wal);
            let mut bytes = fs::read(dir.wal_path()).unwrap();
            let start = FILE_HEADER.len() + field_offset;
            bytes[start..start + replacement.len()].copy_from_slice(&replacement);
            refresh_payload_crc(&mut bytes, FILE_HEADER.len());
            fs::write(dir.wal_path(), &bytes).unwrap();
            assert_eq!(opening_error(&dir.0).kind(), io::ErrorKind::InvalidData);
            assert_eq!(fs::read(dir.wal_path()).unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn failed_apply_does_not_repair_an_otherwise_torn_tail() {
        let dir = TempDir::new();
        let second_record = two_records(&dir).await;
        let mut file = OpenOptions::new()
            .append(true)
            .open(dir.wal_path())
            .unwrap();
        file.write_all(b"FREC\x01").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let original = fs::read(dir.wal_path()).unwrap();
        let mut applied = 0;
        let error = match Wal::open(&dir.0, Durability::Strict, |_| {
            applied += 1;
            if applied == 2 {
                Err(io::Error::other("logical invariant failed"))
            } else {
                Ok(())
            }
        }) {
            Ok(_) => panic!("failed replay callback was ignored"),
            Err(error) => error,
        };
        assert_eq!(applied, 2);
        assert!(error.to_string().contains(&format!(
            "applying WAL sequence 2 at offset {second_record}"
        )));
        assert_eq!(fs::read(dir.wal_path()).unwrap(), original);
        let (_wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
    }

    #[test]
    fn existing_empty_unknown_and_partial_file_headers_are_not_reset() {
        for bytes in [
            b"".as_slice(),
            b"FFKVWAL".as_slice(),
            b"FFKVWAL2".as_slice(),
        ] {
            let dir = TempDir::new();
            fs::write(dir.wal_path(), bytes).unwrap();
            assert_eq!(opening_error(&dir.0).kind(), io::ErrorKind::InvalidData);
            assert_eq!(fs::read(dir.wal_path()).unwrap(), bytes);
        }
    }

    #[test]
    fn nested_new_directories_are_created_and_nonregular_paths_are_rejected() {
        let dir = TempDir::new();
        let nested = dir.0.join("one").join("two");
        let (wal, _) = Wal::open(&nested, Durability::Strict, |_| Ok(())).unwrap();
        assert_eq!(fs::read(nested.join("wal.ffkv")).unwrap(), FILE_HEADER);
        drop(wal);
        let invalid_dir = dir.0.join("not-a-directory");
        fs::write(&invalid_dir, b"untouched").unwrap();
        assert_eq!(
            opening_error(&invalid_dir).kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(fs::read(&invalid_dir).unwrap(), b"untouched");
        let another = TempDir::new();
        fs::create_dir(another.wal_path()).unwrap();
        assert_eq!(
            opening_error(&another.0).kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[tokio::test]
    async fn checkpoint_reclaims_resets_sequences_keeps_lock_and_continues_append() {
        for policy in [Durability::Strict, Durability::Everysec] {
            let dir = TempDir::new();
            let (mut wal, _) = Wal::open(&dir.0, policy, |_| Ok(())).unwrap();
            for _ in 0..10 {
                wal.append(&set_batch(1, b"old", b"obsolete"))
                    .await
                    .unwrap();
            }
            let old_bytes = wal.bytes();
            let lock_before = fs::read(dir.0.join("LOCK")).unwrap();
            let checkpoint = wal
                .checkpoint(std::iter::once(Ok(set_batch(2, b"live", b"value"))))
                .await
                .unwrap();
            assert_eq!(checkpoint.old_bytes, old_bytes);
            assert_eq!(
                checkpoint.new_bytes,
                fs::metadata(dir.wal_path()).unwrap().len()
            );
            assert!(checkpoint.new_bytes < checkpoint.old_bytes);
            assert_eq!(checkpoint.data_frames, 1);
            assert_eq!(wal.next_sequence(), 2);
            assert_eq!(opening_error(&dir.0).kind(), io::ErrorKind::WouldBlock);
            assert_eq!(fs::read(dir.0.join("LOCK")).unwrap(), lock_before);
            wal.append(&set_batch(3, b"after", b"append"))
                .await
                .unwrap();
            wal.finish().await.unwrap();
            drop(wal);
            let mut keys = Vec::new();
            let (wal, recovery) = Wal::open(&dir.0, policy, |batch| {
                for mutation in batch.mutations {
                    if let Mutation::Set { key, .. } = mutation {
                        keys.push(key);
                    }
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(
                keys,
                [Bytes::from_static(b"live"), Bytes::from_static(b"after")]
            );
            assert_eq!(recovery.records, 2);
            assert_eq!(recovery.truncated_bytes, 0);
            assert_eq!(wal.next_sequence(), 3);
        }
    }

    #[tokio::test]
    async fn checkpoint_empty_and_large_atomic_groups_have_actual_stats() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        let batch = Batch {
            timestamp_ms: 9,
            mutations: (0..2049)
                .map(|_| Mutation::Delete {
                    key: Bytes::from_static(b"key"),
                })
                .collect(),
        };
        let stats = wal.checkpoint(std::iter::once(Ok(batch))).await.unwrap();
        assert_eq!(stats.data_frames, 3);
        assert_eq!(wal.next_sequence(), 6); // BEGIN + three data + COMMIT.
        let recovery = validate_complete(File::open(dir.wal_path()).unwrap()).unwrap();
        assert_eq!(recovery.records, 3);
        assert_eq!(recovery.truncated_bytes, 0);
        let empty = wal.checkpoint(std::iter::empty()).await.unwrap();
        assert_eq!(empty.old_bytes, stats.new_bytes);
        assert_eq!(empty.new_bytes, 8);
        assert_eq!(empty.data_frames, 0);
        assert_eq!(wal.next_sequence(), 1);
        assert_eq!(fs::read(dir.wal_path()).unwrap(), FILE_HEADER);
        wal.append(&set_batch(10, b"new", b"value")).await.unwrap();
        drop(wal);
        assert_eq!(
            Wal::open(&dir.0, Durability::Strict, |_| Ok(()))
                .unwrap()
                .1
                .records,
            1
        );
    }

    #[tokio::test]
    async fn checkpoint_iterator_and_rename_failures_preserve_old_writer() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        wal.append(&set_batch(1, b"before", b"value"))
            .await
            .unwrap();
        let original = fs::read(dir.wal_path()).unwrap();
        let sequence = wal.next_sequence();
        let batches = [
            Ok(set_batch(2, b"scratch", b"discard")),
            Err(io::Error::other("snapshot export failed")),
        ];
        assert!(wal.checkpoint(batches.into_iter()).await.is_err());
        assert_eq!(fs::read(dir.wal_path()).unwrap(), original);
        assert_eq!(wal.next_sequence(), sequence);
        assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 2);
        // A destination-directory collision forces rename failure after the
        // replacement file has been fully written and synced.
        let saved = dir.0.join("saved.ffkv");
        fs::rename(dir.wal_path(), &saved).unwrap();
        fs::create_dir(dir.wal_path()).unwrap();
        assert!(wal.checkpoint(std::iter::empty()).await.is_err());
        assert_eq!(fs::read(&saved).unwrap(), original);
        assert_eq!(wal.next_sequence(), sequence);
        fs::remove_dir(dir.wal_path()).unwrap();
        fs::rename(saved, dir.wal_path()).unwrap();
        wal.append(&set_batch(3, b"after", b"value")).await.unwrap();
        drop(wal);
        assert_eq!(
            Wal::open(&dir.0, Durability::Strict, |_| Ok(()))
                .unwrap()
                .1
                .records,
            2
        );
    }

    #[tokio::test]
    async fn queue_effect_is_opaque_bounded_and_strictly_framed() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        let batch = Batch {
            timestamp_ms: 7,
            mutations: vec![Mutation::Queue {
                key: Bytes::from_static(b"q"),
                effect: Bytes::from_static(b"\0opaque\xff"),
            }],
        };
        wal.append(&batch).await.unwrap();
        let original = fs::read(dir.wal_path()).unwrap();
        let too_large = Batch {
            timestamp_ms: 8,
            mutations: vec![Mutation::Queue {
                key: Bytes::from_static(b"q"),
                effect: Bytes::from(vec![0; MAX_PAYLOAD_LEN]),
            }],
        };
        assert_eq!(
            wal.append(&too_large).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(
            wal.checkpoint(std::iter::once(Ok(too_large)))
                .await
                .is_err()
        );
        assert_eq!(fs::read(dir.wal_path()).unwrap(), original);
        drop(wal);
        let mut seen = 0;
        let (wal, _) = Wal::open(&dir.0, Durability::Strict, |batch| {
            assert!(
                matches!(&batch.mutations[0], Mutation::Queue { key, effect }
                if key.as_ref() == b"q" && effect.as_ref() == b"\0opaque\xff")
            );
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1);
        drop(wal);
        // Corrupt the nested effect length but repair CRC: the opaque bytes still
        // require exact binary framing and cannot consume bytes past the frame.
        let mut malformed = original;
        let effect_length = FILE_HEADER.len() + RECORD_HEADER_LEN + 12 + 1 + 4 + 1;
        malformed[effect_length..effect_length + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        refresh_payload_crc(&mut malformed, FILE_HEADER.len());
        fs::write(dir.wal_path(), &malformed).unwrap();
        assert_eq!(opening_error(&dir.0).kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(dir.wal_path()).unwrap(), malformed);
    }
    #[tokio::test]
    async fn typed_effect_tags_are_bounded_and_recover_exact_binary_payloads() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0, Durability::Strict, |_| Ok(())).unwrap();
        for kind in 1..=10 {
            wal.append(&Batch {
                timestamp_ms: kind as i64,
                mutations: vec![Mutation::Typed {
                    key: Bytes::from_static(b"typed\0\xff"),
                    kind,
                    effect: Bytes::from_static(b"\x01\0\xffopaque"),
                }],
            })
            .await
            .unwrap();
        }
        let before = fs::read(dir.wal_path()).unwrap();
        for (kind, effect) in [
            (0, Bytes::from_static(b"x")),
            (11, Bytes::from_static(b"x")),
            (1, Bytes::new()),
            (
                1,
                Bytes::from(vec![0; crate::engine::typed::codec::MAX_EFFECT_BYTES + 1]),
            ),
        ] {
            assert_eq!(
                wal.append(&Batch {
                    timestamp_ms: 11,
                    mutations: vec![Mutation::Typed {
                        key: Bytes::from_static(b"typed"),
                        kind,
                        effect,
                    }]
                })
                .await
                .unwrap_err()
                .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        assert_eq!(fs::read(dir.wal_path()).unwrap(), before);
        drop(wal);
        let mut next = 1;
        let (wal, _) = Wal::open(&dir.0, Durability::Strict, |batch| {
            assert!(matches!(&batch.mutations[0], Mutation::Typed { key, kind, effect }
                if key.as_ref() == b"typed\0\xff" && *kind == next && effect.as_ref() == b"\x01\0\xffopaque"));
            next += 1;
            Ok(())
        }).unwrap();
        assert_eq!(next, 11);
        drop(wal);
        let mut corrupt = before;
        let kind_position =
            FILE_HEADER.len() + RECORD_HEADER_LEN + 12 + 1 + 4 + b"typed\0\xff".len();
        corrupt[kind_position] = 11;
        refresh_payload_crc(&mut corrupt, FILE_HEADER.len());
        fs::write(dir.wal_path(), &corrupt).unwrap();
        assert_eq!(opening_error(&dir.0).kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(dir.wal_path()).unwrap(), corrupt);
    }
}
