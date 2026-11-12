use std::ops::Bound;

use fjall::{Keyspace, PersistMode, Slice};
use jiff::Timestamp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use types::{Account, Enrichment, Handle, Label, Profile};

use crate::{EnrichmentRow, Error, Store};

const FORMAT: u32 = 2;
const FORMAT_KEY: &str = "format";
const ACCOUNTS_PROGRESS_KEY: &str = "migration:accounts";
const ENRICHMENT_PROGRESS_KEY: &str = "migration:enrichment";
const LABELS_PROGRESS_KEY: &str = "migration:labels";
const CHUNK: usize = 1000;

#[derive(Deserialize, Serialize)]
struct AccountV1 {
    handle: Option<Handle>,
    profile: Profile,
    active: bool,
}

impl AccountV1 {
    fn convert(self) -> Account {
        Account {
            handle: self.handle,
            profile: self.profile,
            active: self.active,
            verification: None,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct EnrichmentRowV1 {
    followers: u64,
    created_at: Option<i64>,
    fetched_at: i64,
}

impl EnrichmentRowV1 {
    fn convert(self) -> EnrichmentRow {
        EnrichmentRow {
            enrichment: Enrichment {
                followers: self.followers,
                created_at: self
                    .created_at
                    .and_then(|seconds| Timestamp::from_second(seconds).ok())
                    .map(|created_at| format!("{created_at:.3}")),
            },
            fetched_at: self.fetched_at,
        }
    }
}

/// Labels stored before their times were checked keep only the valid ones.
fn keep_valid_labels(labels: Vec<Label>) -> Vec<Label> {
    labels.into_iter().filter(Label::has_valid_times).collect()
}

impl Store {
    pub(crate) fn migrate(&self) -> Result<(), Error> {
        match self.format()? {
            FORMAT => Ok(()),
            0 if self.accounts.is_empty()? => self.set_format(),
            1 => self.upgrade_from_v1(),
            format => Err(Error::UnsupportedFormat(format)),
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

    fn upgrade_from_v1(&self) -> Result<(), Error> {
        tracing::info!(
            "adding verifications, storing creation times as text and checking label times"
        );
        self.convert(&self.accounts, ACCOUNTS_PROGRESS_KEY, AccountV1::convert)?;
        self.convert(
            &self.enrichment,
            ENRICHMENT_PROGRESS_KEY,
            EnrichmentRowV1::convert,
        )?;
        self.convert(&self.labels, LABELS_PROGRESS_KEY, keep_valid_labels)?;
        let mut batch = self.database.batch();
        batch.insert(&self.meta, FORMAT_KEY, postcard::to_stdvec(&FORMAT)?);
        batch.remove(&self.meta, ACCOUNTS_PROGRESS_KEY);
        batch.remove(&self.meta, ENRICHMENT_PROGRESS_KEY);
        batch.remove(&self.meta, LABELS_PROGRESS_KEY);
        batch.commit()?;
        self.database.persist(PersistMode::SyncData)?;
        tracing::info!("store migration finished");
        Ok(())
    }

    fn convert<Old: DeserializeOwned, New: Serialize>(
        &self,
        keyspace: &Keyspace,
        progress_key: &str,
        convert: impl Fn(Old) -> New,
    ) -> Result<(), Error> {
        let mut after = self.meta.get(progress_key)?;
        while let Some(last) =
            self.convert_chunk(keyspace, progress_key, after.as_deref(), &convert)?
        {
            after = Some(last);
        }
        Ok(())
    }

    fn convert_chunk<Old: DeserializeOwned, New: Serialize>(
        &self,
        keyspace: &Keyspace,
        progress_key: &str,
        after: Option<&[u8]>,
        convert: &impl Fn(Old) -> New,
    ) -> Result<Option<Slice>, Error> {
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut batch = self.database.batch();
        let mut last = None;
        for entry in keyspace
            .range::<&[u8], _>((start, Bound::Unbounded))
            .take(CHUNK)
        {
            let (key, bytes) = entry.into_inner()?;
            let old: Old = postcard::from_bytes(&bytes)?;
            batch.insert(keyspace, key.clone(), postcard::to_stdvec(&convert(old))?);
            last = Some(key);
        }
        if let Some(last) = &last {
            batch.insert(&self.meta, progress_key, last.clone());
            batch.commit()?;
        }
        Ok(last)
    }
}

#[cfg(test)]
mod tests {
    use types::Did;

    use super::*;
    use crate::{LabelOrigin, labels_key};

    #[test]
    fn a_v1_store_is_upgraded() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let alice = Did::try_from("did:plc:alice".to_owned()).unwrap();
        let handle = Handle::try_from("alice.test".to_owned()).unwrap();
        let account = AccountV1 {
            handle: Some(handle.clone()),
            profile: Profile::default(),
            active: true,
        };
        let bytes = postcard::to_stdvec(&account).unwrap();
        store.accounts.insert(alice.as_str(), bytes).unwrap();
        let row = EnrichmentRowV1 {
            followers: 7,
            created_at: Some(1_729_857_638),
            fetched_at: 10,
        };
        let bytes = postcard::to_stdvec(&row).unwrap();
        store.enrichment.insert(alice.as_str(), bytes).unwrap();
        let label = |exp: &str| Label {
            src: alice.clone(),
            uri: alice.as_str().to_owned(),
            cid: None,
            val: "spam".to_owned(),
            cts: "2026-01-01T00:00:00Z".to_owned(),
            exp: Some(exp.to_owned()),
        };
        let labels = vec![label("2026-02-01T00:00:00Z"), label("never")];
        let key = labels_key(&alice, &LabelOrigin::Appview);
        let bytes = postcard::to_stdvec(&labels).unwrap();
        store.labels.insert(key, bytes).unwrap();
        store
            .meta
            .insert(FORMAT_KEY, postcard::to_stdvec(&1u32).unwrap())
            .unwrap();
        drop(store);

        let store = Store::open(dir.path()).unwrap();

        let row = store.enrichment_row(&alice).unwrap().unwrap();
        let enrichment = Enrichment {
            followers: 7,
            created_at: Some("2024-10-25T12:00:38.000Z".to_owned()),
        };
        assert_eq!((row.enrichment, row.fetched_at), (enrichment, 10));
        let account = Account {
            handle: Some(handle),
            ..Account::default()
        };
        assert_eq!(store.account(&alice).unwrap(), Some(account));
        assert_eq!(
            store.labels(&alice).unwrap(),
            [label("2026-02-01T00:00:00Z")]
        );
        assert_eq!(store.format().unwrap(), FORMAT);
    }
}
