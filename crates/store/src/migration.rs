use std::ops::Bound;

use fjall::{PersistMode, Slice};
use serde::{Deserialize, Serialize};
use types::{Account, Enrichment, Handle, Profile};

use crate::{EnrichmentRow, Error, Store, value_for};

const FORMAT: u32 = 1;
const FORMAT_KEY: &str = "format";
const PROGRESS_KEY: &str = "migration";
const CHUNK: usize = 1000;

#[derive(Deserialize, Serialize)]
struct AccountV0 {
    handle: Option<Handle>,
    display_name: Option<String>,
    avatar_cid: Option<String>,
    created_at: Option<i64>,
    active: bool,
}

#[derive(Deserialize, Serialize)]
struct EnrichmentRowV0 {
    followers: u64,
    fetched_at: i64,
}

impl Store {
    pub(crate) fn migrate(&self) -> Result<(), Error> {
        match self.format()? {
            FORMAT => Ok(()),
            0 => self.move_created_at(),
            format => Err(Error::UnsupportedFormat(format)),
        }
    }

    fn format(&self) -> Result<u32, Error> {
        let Some(bytes) = self.meta.get(FORMAT_KEY)? else {
            return Ok(0);
        };
        Ok(postcard::from_bytes(&bytes)?)
    }

    fn move_created_at(&self) -> Result<(), Error> {
        tracing::info!("moving account creation times into enrichment");
        let mut after = self.meta.get(PROGRESS_KEY)?;
        while let Some(last) = self.move_chunk(after.as_deref())? {
            after = Some(last);
        }
        let mut batch = self.database.batch();
        batch.insert(&self.meta, FORMAT_KEY, postcard::to_stdvec(&FORMAT)?);
        batch.remove(&self.meta, PROGRESS_KEY);
        batch.commit()?;
        self.database.persist(PersistMode::SyncData)?;
        tracing::info!("store migration finished");
        Ok(())
    }

    fn move_chunk(&self, after: Option<&[u8]>) -> Result<Option<Slice>, Error> {
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let range = (start, Bound::Unbounded);
        let mut rows = self
            .enrichment
            .range::<&[u8], _>(range)
            .map(|entry| entry.into_inner())
            .peekable();
        let mut batch = self.database.batch();
        let mut last = None;
        for entry in self.accounts.range::<&[u8], _>(range).take(CHUNK) {
            let (did, bytes) = entry.into_inner()?;
            let old: AccountV0 = postcard::from_bytes(&bytes)?;
            if let Some(bytes) = value_for(&mut rows, &did)? {
                let row: EnrichmentRowV0 = postcard::from_bytes(&bytes)?;
                let row = EnrichmentRow {
                    enrichment: Enrichment {
                        followers: row.followers,
                        created_at: old.created_at,
                    },
                    fetched_at: row.fetched_at,
                };
                batch.insert(&self.enrichment, did.clone(), postcard::to_stdvec(&row)?);
            }
            let account = Account {
                handle: old.handle,
                profile: Profile {
                    display_name: old.display_name,
                    avatar_cid: old.avatar_cid,
                },
                active: old.active,
            };
            batch.insert(&self.accounts, did.clone(), postcard::to_stdvec(&account)?);
            last = Some(did);
        }
        if let Some(last) = &last {
            batch.insert(&self.meta, PROGRESS_KEY, last.clone());
            batch.commit()?;
        }
        Ok(last)
    }
}

#[cfg(test)]
mod tests {
    use types::Did;

    use super::*;

    fn did(name: &str) -> Did {
        Did::try_from(format!("did:plc:{name}")).unwrap()
    }

    fn put_legacy(store: &Store, name: &str, row: Option<EnrichmentRowV0>) {
        let account = AccountV0 {
            handle: Some(Handle::try_from(format!("{name}.test")).unwrap()),
            display_name: Some(name.to_owned()),
            avatar_cid: None,
            created_at: Some(42),
            active: true,
        };
        let key = did(name);
        let bytes = postcard::to_stdvec(&account).unwrap();
        store.accounts.insert(key.as_str(), bytes).unwrap();
        if let Some(row) = row {
            let bytes = postcard::to_stdvec(&row).unwrap();
            store.enrichment.insert(key.as_str(), bytes).unwrap();
        }
    }

    #[test]
    fn a_legacy_store_moves_created_at_into_the_enrichment() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let row = EnrichmentRowV0 {
            followers: 7,
            fetched_at: 10,
        };
        put_legacy(&store, "alice", Some(row));
        put_legacy(&store, "bob", None);
        store.meta.remove(FORMAT_KEY).unwrap();
        drop(store);

        let store = Store::open(dir.path()).unwrap();

        let alice = store.enrichment_row(&did("alice")).unwrap().unwrap();
        let enrichment = Enrichment {
            followers: 7,
            created_at: Some(42),
        };
        assert_eq!((alice.enrichment, alice.fetched_at), (enrichment, 10));
        let bob = store.account(&did("bob")).unwrap().unwrap();
        assert_eq!(bob.profile.display_name.as_deref(), Some("bob"));
        assert!(store.enrichment_row(&did("bob")).unwrap().is_none());
        assert_eq!(store.format().unwrap(), FORMAT);
    }
}
