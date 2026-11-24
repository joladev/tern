use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use fjall::Keyspace;
use serde::Serialize;
use serde::de::DeserializeOwned;
use types::{Did, Job, LabelJob};

use crate::{BackfillProgress, Error, Store};

const JETSTREAM_POSITION: &str = "position:jetstream";
const BACKFILL_POSITION: &str = "position:backfill";
const STALE_POSITION: &str = "position:stale";
const KEY_LEN: usize = 9;
const FEED_LANE: usize = 2;
const LANES: usize = 3;

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
}

pub enum Position {
    Jetstream(u64),
    Backfill(BackfillProgress),
    Stale(Option<Did>),
}

#[derive(Debug, PartialEq)]
pub struct Queued<T = Job> {
    key: Key,
    pub job: T,
}

#[derive(Default)]
pub(crate) struct QueueState {
    next_sequence: AtomicU64,
    heads: [AtomicU64; LANES],
    depths: [AtomicUsize; LANES],
}

impl QueueState {
    pub(crate) fn load(queue: &Keyspace) -> Result<QueueState, Error> {
        let state = QueueState::default();
        for entry in queue.iter() {
            let key = parse_key(&entry.key()?)?;
            state.depths[lane_of(&key)].fetch_add(1, Ordering::Relaxed);
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
        self.add(lane.index(), &jobs, position)
    }

    pub fn enqueue_feed(
        &self,
        jobs: Vec<LabelJob>,
        position: Option<Position>,
    ) -> Result<(), Error> {
        self.add(FEED_LANE, &jobs, position)
    }

    fn add<T: Serialize>(
        &self,
        lane: usize,
        jobs: &[T],
        position: Option<Position>,
    ) -> Result<(), Error> {
        let mut batch = self.database.batch();
        for job in jobs {
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
            Some(Position::Stale(after)) => {
                let value = postcard::to_stdvec(&after)?;
                batch.insert(&self.meta, STALE_POSITION, value);
            }
            None => {}
        }
        batch.commit()?;
        self.queue_state.depths[lane].fetch_add(jobs.len(), Ordering::Relaxed);
        Ok(())
    }

    pub fn next_jobs(&self, limit: usize) -> Result<Vec<Queued>, Error> {
        let mut jobs = Vec::with_capacity(limit);
        for lane in Lane::ALL {
            let remaining = limit - jobs.len();
            jobs.extend(self.take(lane.index(), remaining)?);
        }
        Ok(jobs)
    }

    pub fn next_feed_jobs(&self, limit: usize) -> Result<Vec<Queued<LabelJob>>, Error> {
        self.take(FEED_LANE, limit)
    }

    // Reading past tombstones is a classic issue with persisted job queues.
    // To avoid having to scan through the tombstones continuously, we instead find
    // the head of each lane at startup and then keep track of it. The downside to this
    // is that if a job is picked up and never finished, we wouldn't find out until the
    // next restart.
    fn take<T: DeserializeOwned>(
        &self,
        lane: usize,
        limit: usize,
    ) -> Result<Vec<Queued<T>>, Error> {
        let head = &self.queue_state.heads[lane];
        let from = key(lane, head.load(Ordering::Relaxed));
        let mut jobs = Vec::new();
        for entry in self.queue.range(from..=key(lane, u64::MAX)).take(limit) {
            let (key, value) = entry.into_inner()?;
            let key = parse_key(&key)?;
            let job = postcard::from_bytes(&value)?;
            head.store(sequence(&key) + 1, Ordering::Relaxed);
            jobs.push(Queued { key, job });
        }
        Ok(jobs)
    }

    pub fn finish<T>(&self, jobs: &[Queued<T>]) -> Result<(), Error> {
        let mut batch = self.database.batch();
        for job in jobs {
            batch.remove(&self.queue, job.key);
        }
        batch.commit()?;
        for job in jobs {
            self.queue_state.depths[lane_of(&job.key)].fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn queue_depth(&self, lane: Lane) -> usize {
        self.queue_state.depths[lane.index()].load(Ordering::Relaxed)
    }

    pub fn feed_depth(&self) -> usize {
        self.queue_state.depths[FEED_LANE].load(Ordering::Relaxed)
    }

    pub fn jetstream_position(&self) -> Result<Option<u64>, Error> {
        let Some(bytes) = self.meta.get(JETSTREAM_POSITION)? else {
            return Ok(None);
        };
        Ok(Some(postcard::from_bytes(&bytes)?))
    }

    pub fn backfill_position(&self) -> Result<BackfillProgress, Error> {
        let Some(bytes) = self.meta.get(BACKFILL_POSITION)? else {
            return Ok(BackfillProgress::default());
        };
        Ok(postcard::from_bytes(&bytes)?)
    }

    pub fn stale_position(&self) -> Result<Option<Did>, Error> {
        let Some(bytes) = self.meta.get(STALE_POSITION)? else {
            return Ok(None);
        };
        Ok(postcard::from_bytes(&bytes)?)
    }

    fn next_key(&self, lane: usize) -> Key {
        let sequence = self
            .queue_state
            .next_sequence
            .fetch_add(1, Ordering::Relaxed);
        key(lane, sequence)
    }
}

fn key(lane: usize, sequence: u64) -> Key {
    let mut key = [0; KEY_LEN];
    key[0] = lane as u8;
    key[1..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn parse_key(bytes: &[u8]) -> Result<Key, Error> {
    let key: Key = bytes.try_into().map_err(|_| Error::BadQueueKey)?;
    if lane_of(&key) >= LANES {
        return Err(Error::BadQueueKey);
    }
    Ok(key)
}

fn lane_of(key: &Key) -> usize {
    usize::from(key[0])
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
    fn the_feed_lane_holds_label_jobs_apart_from_jobs_to_refresh() {
        let (_dir, store) = open();
        let labels = LabelJob {
            did: job("labelled").did,
            records: Vec::new(),
        };

        store.enqueue_feed(vec![labels.clone()], None).unwrap();
        store.enqueue(Lane::Live, vec![job("new")], None).unwrap();

        assert_eq!(names(&store.next_jobs(10).unwrap()), ["did:plc:new"]);
        let fed = store.next_feed_jobs(10).unwrap();
        assert_eq!(fed[0].job, labels);
        assert_eq!(store.feed_depth(), 1);
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
        let scanned = job("c").did;

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
        let position = Position::Stale(Some(scanned.clone()));
        store
            .enqueue(Lane::Background, Vec::new(), Some(position))
            .unwrap();
        store.persist().unwrap();
        drop(store);
        let store = Store::open(dir.path()).unwrap();

        assert_eq!(store.jetstream_position().unwrap(), Some(7));
        assert_eq!(store.stale_position().unwrap(), Some(scanned));
        let jobs = store.next_jobs(10).unwrap();
        assert_eq!(names(&jobs), ["did:plc:a", "did:plc:b"]);
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
