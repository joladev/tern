use fjall::PersistMode;
use serde::Deserialize;

use crate::queue::BACKFILL_POSITION;
use crate::{BackfillProgress, Error, Store};

const FORMAT: u32 = 3;
const FORMAT_KEY: &str = "format";
const BACKFILL_STATE_BEFORE_QUEUE: &str = "backfill";

#[derive(Deserialize)]
struct BackfillStateBeforeQueue {
    progress: BackfillProgress,
}

impl Store {
    pub(crate) fn migrate(&self) -> Result<(), Error> {
        match self.format()? {
            FORMAT => Ok(()),
            0 if self.accounts.is_empty()? => self.set_format(),
            2 => self.upgrade_from_v2(),
            format => Err(Error::UnsupportedFormat(format)),
        }
    }

    fn upgrade_from_v2(&self) -> Result<(), Error> {
        let mut batch = self.database.batch();
        if let Some(bytes) = self.meta.get(BACKFILL_STATE_BEFORE_QUEUE)? {
            if self.meta.get(BACKFILL_POSITION)?.is_none() {
                let state: BackfillStateBeforeQueue = postcard::from_bytes(&bytes)?;
                let progress = postcard::to_stdvec(&state.progress)?;
                batch.insert(&self.meta, BACKFILL_POSITION, progress);
            }
            batch.remove(&self.meta, BACKFILL_STATE_BEFORE_QUEUE);
        }
        batch.insert(&self.meta, FORMAT_KEY, postcard::to_stdvec(&FORMAT)?);
        batch.commit()?;
        self.database.persist(PersistMode::SyncData)?;
        Ok(())
    }

    fn format(&self) -> Result<u32, Error> {
        let Some(bytes) = self.meta.get(FORMAT_KEY)? else {
            return Ok(0);
        };
        Ok(postcard::from_bytes(&bytes)?)
    }

    fn set_format(&self) -> Result<(), Error> {
        Ok(self
            .meta
            .insert(FORMAT_KEY, postcard::to_stdvec(&FORMAT)?)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backfill_finished_before_the_queue_existed_stays_finished() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let progress = BackfillProgress {
            cursor: None,
            finished: true,
        };
        let counts = (10_u64, 4_u64);
        let old_state = postcard::to_stdvec(&(progress, counts)).unwrap();
        let old_format = postcard::to_stdvec(&2_u32).unwrap();
        store
            .meta
            .insert(BACKFILL_STATE_BEFORE_QUEUE, old_state)
            .unwrap();
        store.meta.insert(FORMAT_KEY, old_format).unwrap();
        store.persist().unwrap();
        drop(store);

        let store = Store::open(dir.path()).unwrap();

        assert!(store.backfill_position().unwrap().finished);
    }
}
