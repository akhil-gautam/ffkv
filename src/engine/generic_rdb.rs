//! Streaming Redis snapshot framing: at most one bounded encoded key is materialized.
use super::{Engine, entry_object, object_mutations};
use crate::{journal::Batch, rdb};
use bytes::Bytes;
use std::io::{self, Read, Write};

#[derive(Default)]
pub struct RdbMetadata {
    pub function_sources: Vec<Bytes>,
    pub search_effects: Vec<Bytes>,
    pub auxiliary: Vec<(Bytes, Bytes)>,
}

#[cfg(test)]
struct CheckedWriter<W> {
    inner: W,
    checksum: u64,
}
#[cfg(test)]
impl<W: Write> Write for CheckedWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(data)?;
        self.checksum = rdb::crc64(self.checksum, &data[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
struct CheckedReader<R> {
    inner: R,
    checksum: u64,
}
impl<R: Read> Read for CheckedReader<R> {
    fn read(&mut self, data: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(data)?;
        self.checksum = rdb::crc64(self.checksum, &data[..n]);
        Ok(n)
    }
}
fn byte(input: &mut impl Read) -> io::Result<u8> {
    let mut b = [0];
    input.read_exact(&mut b)?;
    Ok(b[0])
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl Engine {
    #[cfg(test)]
    /// Append dataset records only. The caller owns the single file header/footer and
    /// global function/search metadata when composing multiple paused shards.
    pub fn write_rdb_records(&self, out: &mut impl Write, now: i64) -> io::Result<()> {
        self.write_rdb_records_mode(out, now, false)
    }
    /// Native stream snapshots preserve every payload at the advertised offset,
    /// including elapsed/held deadlines. Access metadata still uses the real clock.
    pub(crate) fn write_replication_rdb_records(
        &self,
        out: &mut impl Write,
        now: i64,
    ) -> io::Result<()> {
        self.write_rdb_records_mode(out, now, true)
    }
    fn write_rdb_records_mode(
        &self,
        out: &mut impl Write,
        now: i64,
        preserve_expired: bool,
    ) -> io::Result<()> {
        let payload_now = if preserve_expired { -1 } else { now };
        let databases = std::iter::once((self.active_database, &self.entries, &self.held_entries))
            .chain(
                self.databases
                    .iter()
                    .map(|(number, state)| (*number, &state.entries, &state.held_entries)),
            );
        for (database, entries, held) in databases {
            out.write_all(&[254])?;
            rdb::write_length(out, u64::from(database))?;
            for (key, entry) in entries
                .iter()
                .chain(held.iter().filter(|_| preserve_expired || now == 0))
            {
                if entry
                    .expires_at
                    .is_some_and(|deadline| deadline <= payload_now)
                {
                    continue;
                }
                use super::super::Value;
                let (kind, count, minimum) = match &entry.value {
                    Value::String(_) => (0, 0, None),
                    Value::List(values) => (1, values.len(), None),
                    Value::Set(values) => (2, values.len(), None),
                    Value::Hash(values) => {
                        let (count, minimum) = values.snapshot(payload_now).fold(
                            (0usize, None),
                            |(count, minimum), (_, _, deadline)| {
                                (
                                    count + 1,
                                    match (minimum, deadline) {
                                        (Some(a), Some(b)) => Some(i64::min(a, b)),
                                        (a, b) => a.or(b),
                                    },
                                )
                            },
                        );
                        if count == 0 {
                            continue;
                        }
                        (if minimum.is_some() { 24 } else { 4 }, count, minimum)
                    }
                    Value::SortedSet(values) => (5, values.snapshot().count(), None),
                    Value::Queue(_) | Value::Typed(_) => (7, 0, None),
                };
                let idle = if entry.access.last_access_ms > 0 {
                    now.saturating_sub(entry.access.last_access_ms).max(0) / 1000
                } else {
                    0
                };
                out.write_all(&[248])?;
                rdb::write_length(out, idle as u64)?;
                out.write_all(&[249, entry.access.frequency])?;
                if let Some(deadline) = entry.expires_at {
                    out.write_all(&[252])?;
                    out.write_all(&deadline.to_le_bytes())?;
                }
                out.write_all(&[kind])?;
                rdb::write_string(out, key)?;
                match &entry.value {
                    Value::String(value) => rdb::write_string(out, value)?,
                    Value::List(values) => {
                        rdb::write_length(out, count as u64)?;
                        for value in values {
                            rdb::write_string(out, &value.read()?)?;
                        }
                    }
                    Value::Set(values) => {
                        rdb::write_length(out, count as u64)?;
                        for value in values.snapshot() {
                            rdb::write_string(out, value)?;
                        }
                    }
                    Value::Hash(values) => {
                        if let Some(minimum) = minimum {
                            out.write_all(&(minimum as u64).to_le_bytes())?;
                        }
                        rdb::write_length(out, count as u64)?;
                        for (field, value, deadline) in values.snapshot(payload_now) {
                            if let Some(minimum) = minimum {
                                rdb::write_length(
                                    out,
                                    deadline.map_or(0, |deadline| (deadline - minimum + 1) as u64),
                                )?;
                            }
                            rdb::write_string(out, field)?;
                            rdb::write_string(out, value)?;
                        }
                    }
                    Value::SortedSet(values) => {
                        rdb::write_length(out, count as u64)?;
                        for (member, score) in values.snapshot() {
                            rdb::write_string(out, member)?;
                            out.write_all(&score.to_le_bytes())?;
                        }
                    }
                    Value::Queue(_) | Value::Typed(_) => {
                        let object = entry_object(entry, payload_now, self.reply_limit)?;
                        rdb::write_object_body(out, &object)?;
                    }
                }
            }
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn write_rdb(&self, out: &mut impl Write, now: i64) -> io::Result<()> {
        self.write_rdb_with_metadata(out, now, &RdbMetadata::default())
    }
    #[cfg(test)]
    pub fn write_rdb_with_metadata(
        &self,
        out: &mut impl Write,
        now: i64,
        metadata: &RdbMetadata,
    ) -> io::Result<()> {
        let mut checked = CheckedWriter {
            inner: out,
            checksum: 0,
        };
        write!(checked, "REDIS{:04}", rdb::VERSION)?;
        for (source_key, value) in &metadata.auxiliary {
            checked.write_all(&[250])?;
            rdb::write_string(&mut checked, source_key)?;
            rdb::write_string(&mut checked, value)?;
        }
        for effect in &metadata.search_effects {
            checked.write_all(&[250])?;
            rdb::write_string(&mut checked, b"ffkv.search.v1")?;
            rdb::write_string(&mut checked, effect)?;
        }
        for source in &metadata.function_sources {
            checked.write_all(&[245])?;
            rdb::write_string(&mut checked, source)?;
        }
        self.write_rdb_records(&mut checked, now)?;
        checked.write_all(&[255])?;
        let checksum = checked.checksum;
        checked.inner.write_all(&checksum.to_le_bytes())?;
        Ok(())
    }
    #[cfg(test)]
    /// Stage all decoded values, checking the file checksum before dataset publication.
    /// The caller installs returned global metadata under the same paused barrier.
    pub fn install_rdb(&mut self, input: &mut impl Read, now: i64) -> io::Result<RdbMetadata> {
        self.install_rdb_mode(input, now, false)
    }

    /// Replicas retain snapshot deadlines even when they elapsed in transit:
    /// only the source's subsequent resolved expiration may delete that state.
    pub(crate) fn install_replica_rdb(
        &mut self,
        input: &mut impl Read,
        now: i64,
    ) -> io::Result<RdbMetadata> {
        self.install_rdb_mode(input, now, true)
    }

    fn install_rdb_mode(
        &mut self,
        input: &mut impl Read,
        now: i64,
        preserve_expired: bool,
    ) -> io::Result<RdbMetadata> {
        // Redis expiration timestamps are unsigned. A pre-epoch replay clock
        // retains even deadline zero without changing the real access clock.
        let replay_now = if preserve_expired { -1 } else { now };
        let mut checked = CheckedReader {
            inner: input,
            checksum: 0,
        };
        let mut header = [0; 9];
        checked.read_exact(&mut header)?;
        if &header[..5] != b"REDIS" {
            return Err(invalid("invalid RDB header"));
        }
        let version = std::str::from_utf8(&header[5..])
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .filter(|v| *v > 0 && *v <= rdb::VERSION)
            .ok_or_else(|| invalid("unsupported RDB version"))?;
        let mut staged = Engine::new();
        staged.storage = self.storage.clone();
        let mut metadata = RdbMetadata::default();
        let (mut expires_at, mut idle, mut frequency) = (None, None, None);
        let mut metadata_bytes = 0usize;
        let mut context = rdb::SnapshotContext::new(self.reply_limit);
        loop {
            match byte(&mut checked)? {
                255 => break,
                254 => {
                    let database = rdb::read_stream_length(&mut checked)?;
                    if database >= 65_536 {
                        return Err(invalid("RDB database index out of range"));
                    }
                    staged.switch_database(database as u32);
                }
                253 => {
                    let mut b = [0; 4];
                    checked.read_exact(&mut b)?;
                    expires_at = Some(i64::from(u32::from_le_bytes(b)) * 1000);
                }
                252 => {
                    let mut b = [0; 8];
                    checked.read_exact(&mut b)?;
                    let deadline = u64::from_le_bytes(b);
                    expires_at = Some(
                        i64::try_from(deadline).map_err(|_| invalid("RDB expiration overflow"))?,
                    );
                }
                251 => {
                    rdb::read_stream_length(&mut checked)?;
                    rdb::read_stream_length(&mut checked)?;
                }
                248 => idle = Some(rdb::read_stream_length(&mut checked)?),
                249 => frequency = Some(byte(&mut checked)?),
                250 => {
                    let key = rdb::read_stream_string(&mut checked, self.reply_limit)?;
                    let value = rdb::read_stream_string(&mut checked, self.reply_limit)?;
                    metadata_bytes = metadata_bytes
                        .checked_add(key.len() + value.len())
                        .filter(|n| *n <= self.reply_limit)
                        .ok_or_else(|| invalid("RDB metadata budget exceeded"))?;
                    if key.as_ref() == b"ffkv.search.v1" {
                        metadata.search_effects.push(value);
                    } else {
                        metadata.auxiliary.push((key, value));
                    }
                }
                245 => {
                    let source = rdb::read_stream_string(&mut checked, self.reply_limit)?;
                    metadata_bytes = metadata_bytes
                        .checked_add(source.len())
                        .filter(|n| *n <= self.reply_limit)
                        .ok_or_else(|| invalid("RDB metadata budget exceeded"))?;
                    metadata.function_sources.push(source);
                }
                rdb::OPCODE_HASH_TEMPLATE => {
                    rdb::read_stream_hash_template(&mut checked, &mut context)?
                }
                kind => {
                    let key = rdb::read_stream_string(&mut checked, self.reply_limit)?;
                    let object = rdb::read_stream_object_with_context(
                        &mut checked,
                        kind,
                        self.reply_limit,
                        &context,
                    )?;
                    if preserve_expired || !expires_at.is_some_and(|deadline| deadline <= now) {
                        if staged.entries.contains_key(&key) {
                            return Err(invalid("duplicate RDB key"));
                        }
                        let database = staged.database();
                        let mut mutations =
                            object_mutations(object, &key, replay_now, self.reply_limit)?;
                        mutations.insert(0, crate::journal::Mutation::SelectDatabase { database });
                        staged.replay(Batch {
                            timestamp_ms: replay_now,
                            mutations,
                        })?;
                        if staged.entries.contains_key(&key)
                            && let Some(deadline) = expires_at
                        {
                            staged.replay(Batch {
                                timestamp_ms: replay_now,
                                mutations: vec![
                                    crate::journal::Mutation::SelectDatabase { database },
                                    crate::journal::Mutation::Expire {
                                        key: key.clone(),
                                        expires_at: Some(deadline),
                                    },
                                ],
                            })?;
                        }
                        if let Some(entry) = staged.entries.get_mut(&key) {
                            entry.access = super::super::AccessMetadata::new(now);
                            if let Some(seconds) = idle {
                                entry.access.last_access_ms = now.saturating_sub(
                                    i64::try_from(seconds)
                                        .unwrap_or(i64::MAX)
                                        .saturating_mul(1000),
                                );
                            }
                            if let Some(value) = frequency {
                                entry.access.frequency = value;
                            }
                        }
                    }
                    expires_at = None;
                    idle = None;
                    frequency = None;
                }
            }
        }
        if expires_at.is_some() || idle.is_some() || frequency.is_some() {
            return Err(invalid("dangling RDB key metadata"));
        }
        if version >= 5 {
            let checksum = checked.checksum;
            let mut expected = [0; 8];
            checked.inner.read_exact(&mut expected)?;
            let expected = u64::from_le_bytes(expected);
            if expected != 0 && expected != checksum {
                return Err(invalid("RDB checksum mismatch"));
            }
        }
        let mut extra = [0];
        if checked.inner.read(&mut extra)? != 0 {
            return Err(invalid("trailing RDB bytes"));
        }
        staged.switch_database(self.active_database);
        std::mem::swap(&mut self.entries, &mut staged.entries);
        std::mem::swap(&mut self.expirations, &mut staged.expirations);
        std::mem::swap(&mut self.key_index, &mut staged.key_index);
        std::mem::swap(&mut self.hash_expirations, &mut staged.hash_expirations);
        std::mem::swap(&mut self.queue_expirations, &mut staged.queue_expirations);
        std::mem::swap(&mut self.databases, &mut staged.databases);
        std::mem::swap(&mut self.held_entries, &mut staged.held_entries);
        std::mem::swap(&mut self.retained_hashes, &mut staged.retained_hashes);
        Ok(metadata)
    }
}
