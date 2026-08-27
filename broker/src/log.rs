use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use bincode_next::config;
use bincode_next::{Decode, Encode};

use crate::state::Record;

/// Record framing: `u32` payload length, `u32` CRC32 of the payload.
const HEADER_LEN: usize = 8;

/// What a committed log entry instructs the cluster to do.
///
/// `Noop` exists because a freshly elected leader appends one to discover its
/// own commit point without waiting for client traffic.
#[derive(Encode, Decode, Debug, Clone, PartialEq)]
pub enum EntryPayload {
    Noop,
    Queue(Record),
}

/// One slot in the replicated log. `term` and `index` belong to Raft; the
/// payload belongs to the application.
#[derive(Encode, Decode, Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub payload: EntryPayload,
}

/// The durable replicated log.
///
/// Entries are held in memory for the protocol to compare and stream, and
/// mirrored to an append-only file. Index is 1-based; index 0 means "empty",
/// which is what `prev_log_index` carries for the very first entry.
pub struct RaftLog {
    file: File,
    entries: Vec<LogEntry>,
    /// Byte offset where each entry's frame begins, so a truncation can shrink
    /// the file without rewriting it.
    offsets: Vec<u64>,
    bytes: u64,
}

impl RaftLog {
    /// Opens the log at `path`, creating it if absent, and loads what is there.
    /// A torn trailing record — the ordinary result of crashing mid-append — is
    /// discarded and truncated away.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();

        let mut data = Vec::new();
        match File::open(&path) {
            Ok(mut file) => {
                file.read_to_end(&mut data)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        let (entries, offsets, valid_len) = parse(&data);
        if valid_len < data.len() {
            eprintln!(
                "raft log: discarding {} trailing byte(s) from an incomplete append",
                data.len() - valid_len
            );
            OpenOptions::new().write(true).open(&path)?.set_len(valid_len as u64)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(RaftLog { file, entries, offsets, bytes: valid_len as u64 })
    }

    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Term of the entry at `index`, or 0 for index 0 (the empty-log sentinel).
    /// `None` means the index is past the end and there is nothing to compare.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.entries.get((index - 1) as usize).map(|e| e.term)
    }

    pub fn entry(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    /// Entries from `index` onward, capped at `limit`, for streaming to a
    /// follower that is behind.
    pub fn entries_from(&self, index: u64, limit: usize) -> Vec<LogEntry> {
        if index == 0 || index > self.last_index() {
            return Vec::new();
        }
        let start = (index - 1) as usize;
        let end = (start + limit).min(self.entries.len());
        self.entries[start..end].to_vec()
    }

    pub fn append(&mut self, new: &[LogEntry]) -> io::Result<()> {
        if new.is_empty() {
            return Ok(());
        }

        let mut buf = Vec::new();
        let mut offsets = Vec::with_capacity(new.len());
        let mut at = self.bytes;
        for entry in new {
            offsets.push(at);
            let before = buf.len();
            frame(entry, &mut buf).map_err(io::Error::other)?;
            at += (buf.len() - before) as u64;
        }

        // One write and one fsync for the whole slice, so an AppendEntries
        // carrying many entries costs a single flush.
        self.file.write_all(&buf)?;
        self.file.sync_data()?;

        self.bytes = at;
        self.entries.extend_from_slice(new);
        self.offsets.extend_from_slice(&offsets);
        Ok(())
    }

    /// Discards everything after `index`. Used when a follower's log diverges
    /// from the leader's and the conflicting suffix has to go.
    pub fn truncate_after(&mut self, index: u64) -> io::Result<()> {
        if index >= self.last_index() {
            return Ok(());
        }
        let keep = index as usize;
        let new_bytes = self.offsets[keep];

        self.file.set_len(new_bytes)?;
        self.file.sync_data()?;

        self.entries.truncate(keep);
        self.offsets.truncate(keep);
        self.bytes = new_bytes;
        Ok(())
    }
}

fn frame(entry: &LogEntry, out: &mut Vec<u8>) -> Result<(), bincode_next::error::EncodeError> {
    let payload = bincode_next::encode_to_vec(entry, config::standard())?;
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(())
}

pub fn encode_entry(entry: &LogEntry) -> Result<Vec<u8>, bincode_next::error::EncodeError> {
    bincode_next::encode_to_vec(entry, config::standard())
}

pub fn decode_entry(bytes: &[u8]) -> Result<LogEntry, bincode_next::error::DecodeError> {
    bincode_next::decode_from_slice::<LogEntry, _>(bytes, config::standard()).map(|(entry, _)| entry)
}

/// Decodes entries until one fails to parse, returning them with their byte
/// offsets and the offset of the first bad byte.
fn parse(data: &[u8]) -> (Vec<LogEntry>, Vec<u64>, usize) {
    let mut entries = Vec::new();
    let mut offsets = Vec::new();
    let mut offset = 0;

    while offset + HEADER_LEN <= data.len() {
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[offset + 4..offset + HEADER_LEN].try_into().unwrap());

        let start = offset + HEADER_LEN;
        let Some(end) = start.checked_add(len).filter(|end| *end <= data.len()) else {
            break;
        };

        let payload = &data[start..end];
        if crc32fast::hash(payload) != crc {
            break;
        }
        let Ok(entry) = decode_entry(payload) else { break };

        entries.push(entry);
        offsets.push(offset as u64);
        offset = end;
    }

    (entries, offsets, offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("ferroqueue-log-{name}.log"));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            payload: EntryPayload::Queue(Record::Enqueued {
                job_id: index as u128,
                payload: format!("job-{index}"),
                created_at: index as i64,
            }),
        }
    }

    #[test]
    fn opens_when_the_file_does_not_exist() {
        let path = scratch("missing");
        assert!(!path.exists());

        let log = RaftLog::open(&path).expect("open should create the log");
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.term_at(0), Some(0), "index 0 is the empty-log sentinel");
        assert!(path.exists());
    }

    #[test]
    fn reloads_what_it_appended() {
        let path = scratch("roundtrip");

        let mut log = RaftLog::open(&path).unwrap();
        log.append(&[entry(1, 1), entry(1, 2), entry(2, 3)]).unwrap();
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);

        let log = RaftLog::open(&path).unwrap();
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(4), None, "past the end has no term to compare");
    }

    #[test]
    fn truncate_shrinks_memory_and_file_and_survives_reload() {
        let path = scratch("truncate");

        let mut log = RaftLog::open(&path).unwrap();
        log.append(&[entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)]).unwrap();
        let full_len = std::fs::metadata(&path).unwrap().len();

        log.truncate_after(2).unwrap();
        assert_eq!(log.last_index(), 2);
        assert!(std::fs::metadata(&path).unwrap().len() < full_len);

        // Appending after a truncation must land at index 3 again.
        log.append(&[entry(5, 3)]).unwrap();
        assert_eq!(log.term_at(3), Some(5));

        let log = RaftLog::open(&path).unwrap();
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(3), Some(5), "the overwritten entry is what reloads");
    }

    #[test]
    fn discards_a_torn_trailing_record() {
        let path = scratch("torn");

        let mut log = RaftLog::open(&path).unwrap();
        log.append(&[entry(1, 1)]).unwrap();
        let intact_len = std::fs::metadata(&path).unwrap().len();

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        let mut partial = Vec::new();
        frame(&entry(1, 2), &mut partial).unwrap();
        partial.truncate(partial.len() - 3);
        file.write_all(&partial).unwrap();
        file.sync_data().unwrap();

        let log = RaftLog::open(&path).unwrap();
        assert_eq!(log.last_index(), 1, "the torn entry must not load");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            intact_len,
            "the torn bytes must be truncated away"
        );
    }
}
