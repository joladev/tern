use std::ops::Bound;

use fjall::{Keyspace, PersistMode};
use types::{Account, Did, Job, LabelJob, LabelRecord};

use crate::codec::{self, Stored};
use crate::queue::{self, BACKFILL_POSITION, STALE_POSITION};
use crate::{BackfillProgress, ENRICHMENT, EnrichmentRow, Error, LABELS, Store};

const FORMAT: u32 = 7;
const FORMAT_KEY: &str = "format";
const UPGRADED: &str = "upgrade:";
const POSITIONS: &str = "position:";
const FIRST_VERSION: u8 = 1;
const ROWS_PER_BATCH: usize = 10_000;

impl Store {
    pub(crate) fn migrate(&self) -> Result<(), Error> {
        match self.format()? {
            FORMAT => Ok(()),
            0 if self.accounts.is_empty()? => self.set_format(),
            6 => self.add_versions(),
            format => Err(Error::UnsupportedFormat(format)),
        }
    }

    fn add_versions(&self) -> Result<(), Error> {
        self.add_versions_to(&self.accounts, "accounts", |_, row| {
            versioned::<Account>(row)
        })?;
        self.add_versions_to(&self.enrichment, ENRICHMENT, |_, row| {
            versioned::<EnrichmentRow>(row)
        })?;
        self.add_versions_to(&self.labels, LABELS, |_, row| {
            versioned::<Vec<LabelRecord>>(row)
        })?;
        self.add_versions_to(&self.queue, "queue", versioned_job)?;

        let mut batch = self.database.batch();
        for entry in self.meta.prefix(POSITIONS) {
            let (key, row) = entry.into_inner()?;
            let position = versioned_position(&key, &row)?;
            batch.insert(&self.meta, key, position);
        }
        for entry in self.meta.prefix(UPGRADED) {
            batch.remove(&self.meta, entry.key()?);
        }
        batch.insert(&self.meta, FORMAT_KEY, postcard::to_stdvec(&FORMAT)?);
        batch.commit()?;
        Ok(self.database.persist(PersistMode::SyncData)?)
    }

    fn add_versions_to(
        &self,
        keyspace: &Keyspace,
        name: &str,
        versioned: impl Fn(&[u8], &[u8]) -> Result<Vec<u8>, Error>,
    ) -> Result<(), Error> {
        let marker = format!("{UPGRADED}{name}");
        loop {
            let upgraded = self.meta.get(&marker)?;
            let start = match &upgraded {
                Some(last) => Bound::Excluded(&last[..]),
                None => Bound::Unbounded,
            };
            let mut batch = self.database.batch();
            let mut last = None;
            let rows = keyspace.range::<&[u8], _>((start, Bound::Unbounded));
            for entry in rows.take(ROWS_PER_BATCH) {
                let (key, row) = entry.into_inner()?;
                batch.insert(keyspace, key.clone(), versioned(&key, &row)?);
                last = Some(key);
            }
            let Some(last) = last else {
                return Ok(());
            };
            batch.insert(&self.meta, &marker, last);
            batch.commit()?;
        }
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

fn versioned<T: Stored>(row: &[u8]) -> Result<Vec<u8>, Error> {
    let versioned = [&[FIRST_VERSION], row].concat();
    codec::decode::<T>(&versioned)?;
    Ok(versioned)
}

fn versioned_job(key: &[u8], row: &[u8]) -> Result<Vec<u8>, Error> {
    if queue::in_feed_lane(key)? {
        versioned::<LabelJob>(row)
    } else {
        versioned::<Job>(row)
    }
}

fn versioned_position(key: &[u8], row: &[u8]) -> Result<Vec<u8>, Error> {
    if key == BACKFILL_POSITION.as_bytes() {
        versioned::<BackfillProgress>(row)
    } else if key == STALE_POSITION.as_bytes() {
        versioned::<Option<Did>>(row)
    } else {
        versioned::<u64>(row)
    }
}

#[cfg(test)]
mod tests {
    use types::{Enrichment, Facts, Handle};

    use super::*;

    fn did(name: &str) -> Did {
        Did::try_from(format!("did:plc:{name}")).unwrap()
    }

    fn account(handle: &str) -> Account {
        Account {
            handle: Handle::try_from(handle.to_owned()).ok(),
            ..Account::default()
        }
    }

    fn unversioned<T: serde::Serialize>(value: &T) -> Vec<u8> {
        postcard::to_stdvec(value).unwrap()
    }

    fn format_6(dir: &tempfile::TempDir) -> Store {
        let store = Store::open(dir.path()).unwrap();
        store.meta.insert(FORMAT_KEY, unversioned(&6_u32)).unwrap();
        store
    }

    #[test]
    fn a_format_6_store_gets_a_version_on_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let store = format_6(&dir);
        let alice = did("alice");
        let enrichment = EnrichmentRow {
            enrichment: Enrichment {
                followers: 5,
                created_at: None,
            },
            fetched_at: 10,
        };
        let job = Job {
            did: alice.clone(),
            seen: Facts::default(),
        };
        let label_job = LabelJob {
            did: alice.clone(),
            records: Vec::new(),
        };
        let rows = [
            (
                &store.accounts,
                "did:plc:alice",
                unversioned(&account("alice.test")),
            ),
            (&store.enrichment, "did:plc:alice", unversioned(&enrichment)),
            (
                &store.labels,
                "did:plc:alice",
                unversioned(&Vec::<LabelRecord>::new()),
            ),
            (&store.meta, "position:jetstream", unversioned(&7_u64)),
            (&store.meta, STALE_POSITION, unversioned(&Some(&alice))),
        ];
        for (keyspace, key, row) in rows {
            keyspace.insert(key, row).unwrap();
        }
        store.queue.insert([0; 9], unversioned(&job)).unwrap();
        let feed_key = [2, 0, 0, 0, 0, 0, 0, 0, 1];
        store
            .queue
            .insert(feed_key, unversioned(&label_job))
            .unwrap();
        store.persist().unwrap();
        drop(store);

        let store = Store::open(dir.path()).unwrap();

        assert_eq!(store.format().unwrap(), FORMAT);
        assert_eq!(store.account(&alice).unwrap(), Some(account("alice.test")));
        assert_eq!(store.enrichment(&alice).unwrap().followers, 5);
        assert!(store.labels(&alice).unwrap().is_empty());
        assert_eq!(store.next_jobs(1).unwrap()[0].job, job);
        assert_eq!(store.next_feed_jobs(1).unwrap()[0].job, label_job);
        assert_eq!(store.jetstream_position().unwrap(), Some(7));
        assert_eq!(store.stale_position().unwrap(), Some(alice));
    }

    #[test]
    fn an_upgrade_that_was_cut_short_continues_after_the_last_upgraded_row() {
        let dir = tempfile::tempdir().unwrap();
        let store = format_6(&dir);
        store
            .put_account(&did("alice"), &account("alice.test"))
            .unwrap();
        store
            .meta
            .insert("upgrade:accounts", "did:plc:alice")
            .unwrap();
        let old_bob = unversioned(&account("bob.test"));
        store.accounts.insert("did:plc:bob", old_bob).unwrap();
        store.persist().unwrap();
        drop(store);

        let store = Store::open(dir.path()).unwrap();

        let alice = store.account(&did("alice")).unwrap();
        assert_eq!(alice, Some(account("alice.test")));
        let bob = store.account(&did("bob")).unwrap();
        assert_eq!(bob, Some(account("bob.test")));
        assert!(store.meta.get("upgrade:accounts").unwrap().is_none());
    }
}
