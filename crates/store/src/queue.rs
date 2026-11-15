use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use fjall::Keyspace;
use serde::Deserialize;
use types::Job;

use crate::{BackfillProgress, Error, Store};

const JETSTREAM_POSITION: &str = "position:jetstream";
const BACKFILL_POSITION: &str = "position:backfill";
const BACKFILL_STATE_BEFORE_QUEUE: &str = "backfill";
const KEY_LEN: usize = 9;

type Key = [u8; KEY_LEN];

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Lane {
    Live,
    Background,
}

impl Lane {
    const ALL: [Lane; 2] = [Lane::Live, Lane::Background];

    fn index(self) -> usize {
        match self {
            Lane::Live => 0,
            Lane::Background => 1,
        }
    }

    fn of(key: &Key) -> Result<Lane, Error> {
        match key[0] {
            0 => Ok(Lane::Live),
            1 => Ok(Lane::Background),
            _ => Err(Error::BadQueueKey),
        }
    }
}

pub enum Position {
    Jetstream(u64),
    Backfill(BackfillProgress),
}

#[derive(Debug, PartialEq)]
pub struct Queued {
    key: Key,
    pub job: Job,
}

// TODO JOHANNA: older versions saved backfill progress in this format.
// Production finished its backfill under that version. We still read it so
// backfill does not run again after upgrading. The old value has two
// counters after the progress, which we ignore.
#[derive(Deserialize)]
struct BackfillStateBeforeQueue {
    progress: BackfillProgress,
}

#[derive(Default)]
pub(crate) struct QueueState {
    next_sequence: AtomicU64,
    heads: [AtomicU64; 2],
    depths: [AtomicUsize; 2],
}

impl QueueState {
    pub(crate) fn load(queue: &Keyspace) -> Result<QueueState, Error> {
        let state = QueueState::default();
        for entry in queue.iter() {
            let key = parse_key(&entry.key()?)?;
            state.depths[Lane::of(&key)?.index()].fetch_add(1, Ordering::Relaxed);
            state
                .next_sequence
                .fetch_max(sequence(&key) + 1, Ordering::Relaxed);
        }
        Ok(state)
    }
}

impl Store {
    pub fn enqueue(
        &self,
        lane: Lane,
        jobs: Vec<Job>,
        position: Option<Position>,
    ) -> Result<(), Error> {
        let mut batch = self.database.batch();
        for job in &jobs {
            let value = postcard::to_stdvec(job)?;
            batch.insert(&self.queue, self.next_key(lane), value);
        }
        match position {
            Some(Position::Jetstream(cursor)) => {
                let value = postcard::to_stdvec(&cursor)?;
                batch.insert(&self.meta, JETSTREAM_POSITION, value);
            }
            Some(Position::Backfill(progress)) => {
                let value = postcard::to_stdvec(&progress)?;
                batch.insert(&self.meta, BACKFILL_POSITION, value);
            }
            None => {}
        }
        batch.commit()?;
        self.queue_state.depths[lane.index()].fetch_add(jobs.len(), Ordering::Relaxed);
        Ok(())
    }

    // Reading past tombstones is a classic issue with persisted job queues.
    // To avoid having to scan through the tombstones continuously, we instead find
    // the head of each lane at startup and then keep track of it. The downside to this
    // is that if a job is picked up and never finished, we wouldn't find out until the
    // next restart.
    pub fn next_jobs(&self, limit: usize) -> Result<Vec<Queued>, Error> {
        let mut jobs = Vec::with_capacity(limit);
        for lane in Lane::ALL {
            let head = &self.queue_state.heads[lane.index()];
            let from = key(lane, head.load(Ordering::Relaxed));
            let remaining = limit - jobs.len();
            for entry in self.queue.range(from..=key(lane, u64::MAX)).take(remaining) {
                let (key, value) = entry.into_inner()?;
                let key = parse_key(&key)?;
                let job = postcard::from_bytes(&value)?;
                head.store(sequence(&key) + 1, Ordering::Relaxed);
                jobs.push(Queued { key, job });
            }
        }
        Ok(jobs)
    }

    pub fn finish(&self, jobs: &[Queued]) -> Result<(), Error> {
        let mut batch = self.database.batch();
        for job in jobs {
            batch.remove(&self.queue, job.key);
        }
        batch.commit()?;
        for job in jobs {
            let lane = Lane::of(&job.key)?;
            self.queue_state.depths[lane.index()].fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn queue_depth(&self, lane: Lane) -> usize {
        self.queue_state.depths[lane.index()].load(Ordering::Relaxed)
    }

    pub fn jetstream_position(&self) -> Result<Option<u64>, Error> {
        let Some(bytes) = self.meta.get(JETSTREAM_POSITION)? else {
            return Ok(None);
        };
        Ok(Some(postcard::from_bytes(&bytes)?))
    }

    pub fn backfill_position(&self) -> Result<BackfillProgress, Error> {
        if let Some(bytes) = self.meta.get(BACKFILL_POSITION)? {
            return Ok(postcard::from_bytes(&bytes)?);
        }
        let Some(bytes) = self.meta.get(BACKFILL_STATE_BEFORE_QUEUE)? else {
            return Ok(BackfillProgress::default());
        };
        let state: BackfillStateBeforeQueue = postcard::from_bytes(&bytes)?;
        Ok(state.progress)
    }

    fn next_key(&self, lane: Lane) -> Key {
        let sequence = self
            .queue_state
            .next_sequence
            .fetch_add(1, Ordering::Relaxed);
        key(lane, sequence)
    }
}

fn key(lane: Lane, sequence: u64) -> Key {
    let mut key = [0; KEY_LEN];
    key[0] = lane.index() as u8;
    key[1..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn parse_key(bytes: &[u8]) -> Result<Key, Error> {
    bytes.try_into().map_err(|_| Error::BadQueueKey)
}

fn sequence(key: &Key) -> u64 {
    let mut sequence = [0; 8];
    sequence.copy_from_slice(&key[1..]);
    u64::from_be_bytes(sequence)
}

#[cfg(test)]
mod tests {
    use types::{Did, Facts};

    use super::*;

    fn open() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn job(name: &str) -> Job {
        Job {
            did: Did::try_from(format!("did:plc:{name}")).unwrap(),
            seen: Facts::default(),
        }
    }

    fn names(jobs: &[Queued]) -> Vec<&str> {
        jobs.iter().map(|queued| queued.job.did.as_str()).collect()
    }

    #[test]
    fn jobs_come_back_in_the_order_they_were_added() {
        let (_dir, store) = open();

        store
            .enqueue(Lane::Live, vec![job("a"), job("b")], None)
            .unwrap();
        store.enqueue(Lane::Live, vec![job("c")], None).unwrap();

        let jobs = store.next_jobs(2).unwrap();
        assert_eq!(names(&jobs), ["did:plc:a", "did:plc:b"]);
        assert_eq!(names(&store.next_jobs(2).unwrap()), ["did:plc:c"]);
    }

    #[test]
    fn live_jobs_come_before_background_jobs() {
        let (_dir, store) = open();

        store
            .enqueue(Lane::Background, vec![job("old")], None)
            .unwrap();
        store.enqueue(Lane::Live, vec![job("new")], None).unwrap();

        let jobs = store.next_jobs(10).unwrap();
        assert_eq!(names(&jobs), ["did:plc:new", "did:plc:old"]);
    }

    #[test]
    fn a_finished_job_is_gone_after_reopening() {
        let (dir, store) = open();
        store
            .enqueue(Lane::Live, vec![job("a"), job("b")], None)
            .unwrap();
        let mut jobs = store.next_jobs(10).unwrap();
        let unfinished = jobs.split_off(1);

        store.finish(&jobs).unwrap();
        store.persist().unwrap();
        drop(store);
        let store = Store::open(dir.path()).unwrap();

        assert_eq!(store.next_jobs(10).unwrap(), unfinished);
    }

    #[test]
    fn jobs_are_saved_together_with_the_trigger_position() {
        let (dir, store) = open();
        let progress = BackfillProgress {
            cursor: Some("next".to_owned()),
            finished: false,
        };

        store
            .enqueue(Lane::Live, vec![job("a")], Some(Position::Jetstream(7)))
            .unwrap();
        store
            .enqueue(
                Lane::Background,
                vec![job("b")],
                Some(Position::Backfill(progress.clone())),
            )
            .unwrap();
        store.persist().unwrap();
        drop(store);
        let store = Store::open(dir.path()).unwrap();

        assert_eq!(store.jetstream_position().unwrap(), Some(7));
        assert_eq!(store.backfill_position().unwrap(), progress);
        let jobs = store.next_jobs(10).unwrap();
        assert_eq!(names(&jobs), ["did:plc:a", "did:plc:b"]);
    }

    #[test]
    fn a_backfill_finished_before_the_queue_existed_stays_finished() {
        let (_dir, store) = open();
        let progress = BackfillProgress {
            cursor: None,
            finished: true,
        };
        let counts = (10_u64, 4_u64);
        let bytes = postcard::to_stdvec(&(progress, counts)).unwrap();
        store
            .meta
            .insert(BACKFILL_STATE_BEFORE_QUEUE, bytes)
            .unwrap();

        assert!(store.backfill_position().unwrap().finished);
    }

    #[test]
    fn the_depth_counts_unfinished_jobs_in_each_lane() {
        let (dir, store) = open();
        store
            .enqueue(Lane::Live, vec![job("a"), job("b")], None)
            .unwrap();
        store
            .enqueue(Lane::Background, vec![job("c")], None)
            .unwrap();
        let jobs = store.next_jobs(1).unwrap();
        store.finish(&jobs).unwrap();

        assert_eq!(store.queue_depth(Lane::Live), 1);
        assert_eq!(store.queue_depth(Lane::Background), 1);

        store.persist().unwrap();
        drop(store);
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.queue_depth(Lane::Live), 1);
        assert_eq!(store.queue_depth(Lane::Background), 1);
    }
}
