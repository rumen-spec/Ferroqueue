use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::thread;

use bincode_next::config;
use bincode_next::{Decode, Encode};
use chrono::DateTime;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::broker::QueueService;
use crate::job::{Job, JobState, New};
const HEADER_LEN: usize = 8;
const MAX_BATCH: usize = 1024;

#[derive(Encode, Decode, Debug, Clone)]
pub enum Record {
    Enqueued { job_id: u128, payload: String, created_at: i64 },
    Acked { job_id: u128 },
    Retried { job_id: u128, retries: i32 },
    DeadLettered { job_id: u128 },
}

enum Cmd {
    Write(Record, oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

pub struct Wal {
    tx: mpsc::Sender<Cmd>,
}

impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Wal")
    }
}

impl Wal {
    pub fn open(path: impl Into<PathBuf>) -> io::Result<(Self, Vec<Record>)> {
        let path = path.into();

        let mut data = Vec::new();
        match File::open(&path) {
            Ok(mut file) => {
                file.read_to_end(&mut data)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        let (records, valid_len) = parse(&data);
        if valid_len < data.len() {
            eprintln!(
                "wal: discarding {} trailing byte(s) from an incomplete append",
                data.len() - valid_len
            );
            OpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(valid_len as u64)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let (tx, rx) = mpsc::channel(MAX_BATCH);
        thread::spawn(move || writer_loop(rx, file));

        Ok((Wal { tx }, records))
    }

    pub async fn append(&self, record: Record) -> io::Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Write(record, ack_tx))
            .await
            .map_err(|_| writer_stopped())?;
        ack_rx.await.map_err(|_| writer_stopped())
    }

    pub async fn shutdown(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown(done_tx)).await.is_ok() {
            let _ = done_rx.await;
        }
    }
}

fn writer_stopped() -> io::Error {
    io::Error::other("wal writer stopped")
}

fn writer_loop(mut rx: mpsc::Receiver<Cmd>, mut file: File) {
    let mut buf: Vec<u8> = Vec::new();
    let mut waiters: Vec<oneshot::Sender<()>> = Vec::new();

    while let Some(mut cmd) = rx.blocking_recv() {
        buf.clear();
        waiters.clear();
        let mut stopping = false;

        loop {
            match cmd {
                Cmd::Write(record, ack) => match frame(&record, &mut buf) {
                    Ok(()) => waiters.push(ack),
                    Err(e) => eprintln!("wal: dropping unencodable record: {e}"),
                },
                Cmd::Shutdown(done) => {
                    waiters.push(done);
                    stopping = true;
                }
            }

            if stopping || waiters.len() >= MAX_BATCH {
                break;
            }
            match rx.try_recv() {
                Ok(next) => cmd = next,
                Err(_) => break,
            }
        }

        if !buf.is_empty() {
            if let Err(e) = file.write_all(&buf).and_then(|_| file.sync_data()) {
                // Waiters are dropped unsignalled, so `append` reports the
                // failure and the caller declines to apply the transition.
                eprintln!("wal: append failed, {} record(s) rejected: {e}", waiters.len());
                waiters.clear();
                if stopping {
                    break;
                }
                continue;
            }
        }

        for waiter in waiters.drain(..) {
            let _ = waiter.send(());
        }
        if stopping {
            break;
        }
    }
}

fn frame(record: &Record, out: &mut Vec<u8>) -> Result<(), bincode_next::error::EncodeError> {
    let payload = bincode_next::encode_to_vec(record, config::standard())?;
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(())
}

fn parse(data: &[u8]) -> (Vec<Record>, usize) {
    let mut records = Vec::new();
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
        let Ok((record, _)) =
            bincode_next::decode_from_slice::<Record, _>(payload, config::standard())
        else {
            break;
        };

        records.push(record);
        offset = end;
    }

    (records, offset)
}

pub async fn replay(service: &QueueService, records: Vec<Record>) {
    for record in records {
        match record {
            Record::Enqueued { job_id, payload, created_at } => {
                let id = Uuid::from_u128(job_id);
                let mut job = Job::new(payload);
                job.set_id(id);
                if let Some(created_at) = DateTime::from_timestamp_micros(created_at) {
                    job.set_created_at(created_at);
                }
                service.jobs.lock().await.insert(id, job);
                service.ready_queue.lock().await.push_back(id);
            }
            Record::Retried { job_id, retries } => {
                let id = Uuid::from_u128(job_id);
                if let Some(job) = service.jobs.lock().await.get_mut(&id) {
                    job.set_retries(retries);
                }
            }
            Record::Acked { job_id } => {
                let id = Uuid::from_u128(job_id);
                service.jobs.lock().await.remove(&id);
                service.ready_queue.lock().await.retain(|queued| *queued != id);
            }
            Record::DeadLettered { job_id } => {
                let id = Uuid::from_u128(job_id);
                service.ready_queue.lock().await.retain(|queued| *queued != id);
                if let Some(job) = service.jobs.lock().await.get_mut(&id) {
                    job.set_state(JobState::DLQ);
                }
                service.dl_queue.lock().await.push_back(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("ferroqueue-test-{name}.wal"));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn enqueued(n: u128) -> Record {
        Record::Enqueued { job_id: n, payload: format!("job-{n}"), created_at: n as i64 }
    }

    #[tokio::test]
    async fn opens_when_the_file_does_not_exist() {
        let path = scratch("missing");
        assert!(!Path::new(&path).exists());

        let (wal, records) = Wal::open(&path).expect("open should create the log");
        assert!(records.is_empty());
        wal.shutdown().await;

        assert!(Path::new(&path).exists());
    }

    #[tokio::test]
    async fn replays_what_it_appended() {
        let path = scratch("roundtrip");

        let (wal, records) = Wal::open(&path).unwrap();
        assert!(records.is_empty());
        wal.append(enqueued(1)).await.unwrap();
        wal.append(enqueued(2)).await.unwrap();
        wal.append(Record::Acked { job_id: 1 }).await.unwrap();
        wal.shutdown().await;

        let (wal, records) = Wal::open(&path).unwrap();
        wal.shutdown().await;

        assert_eq!(records.len(), 3);
        assert!(matches!(records[0], Record::Enqueued { job_id: 1, .. }));
        assert!(matches!(records[1], Record::Enqueued { job_id: 2, .. }));
        assert!(matches!(records[2], Record::Acked { job_id: 1 }));
    }

    #[tokio::test]
    async fn discards_a_torn_trailing_record() {
        let path = scratch("torn");

        let (wal, _) = Wal::open(&path).unwrap();
        wal.append(enqueued(1)).await.unwrap();
        wal.shutdown().await;

        let intact_len = std::fs::metadata(&path).unwrap().len();

        // Simulate crashing partway through the next append.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        let mut partial = Vec::new();
        frame(&enqueued(2), &mut partial).unwrap();
        partial.truncate(partial.len() - 3);
        file.write_all(&partial).unwrap();
        file.sync_data().unwrap();

        let (wal, records) = Wal::open(&path).unwrap();
        wal.shutdown().await;

        assert_eq!(records.len(), 1, "the torn record must not be replayed");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            intact_len,
            "the torn bytes must be truncated away"
        );
    }
}
