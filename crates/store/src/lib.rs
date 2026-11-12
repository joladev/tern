mod migration;

use std::iter::Peekable;
use std::ops::Bound;
use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvPair, PersistMode, Slice};
use serde::{Deserialize, Serialize};
use types::{Account, Did, Enrichment, Label};

const BACKFILL_STATE: &str = "backfill";
const ENRICHMENT: &str = "counts";
const LABELS: &str = "labels";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database operation failed")]
    Database(#[from] fjall::Error),
    #[error("failed to encode or decode a stored value")]
    Codec(#[from] postcard::Error),
    #[error("found a malformed DID in the store")]
    BadDid(#[source] types::ParseError),
    #[error("the store has format {0}, which this build does not support")]
    UnsupportedFormat(u32),
}

pub struct StoredAccount {
    pub did: Did,
    pub account: Account,
    pub enrichment: Enrichment,
    pub labels: Vec<Label>,
}

pub struct Scan {
    pub stale: Vec<Did>,
    pub last: Option<Did>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct BackfillProgress {
    pub cursor: Option<String>,
    pub finished: bool,
}

#[derive(Deserialize, Serialize)]
pub struct EnrichmentRow {
    pub enrichment: Enrichment,
    pub fetched_at: i64,
}

pub enum LabelOrigin {
    Appview,
}

impl LabelOrigin {
    fn key(&self) -> &str {
        match self {
            LabelOrigin::Appview => "appview",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct BackfillCounts {
    pub listed: u64,
    pub filled: u64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct BackfillState {
    pub progress: BackfillProgress,
    pub counts: BackfillCounts,
}

pub struct Store {
    database: Database,
    accounts: Keyspace,
    enrichment: Keyspace,
    labels: Keyspace,
    meta: Keyspace,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store, Error> {
        let database = Database::builder(path).open()?;
        let accounts = database.keyspace("accounts", KeyspaceCreateOptions::default)?;
        let enrichment = database.keyspace(ENRICHMENT, KeyspaceCreateOptions::default)?;
        let labels = database.keyspace(LABELS, KeyspaceCreateOptions::default)?;
        let meta = database.keyspace("meta", KeyspaceCreateOptions::default)?;
        let store = Store {
            database,
            accounts,
            enrichment,
            labels,
            meta,
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn account(&self, did: &Did) -> Result<Option<Account>, Error> {
        let Some(bytes) = self.accounts.get(did.as_str())? else {
            return Ok(None);
        };
        Ok(Some(postcard::from_bytes(&bytes)?))
    }

    pub fn enrichment(&self, did: &Did) -> Result<Enrichment, Error> {
        let row = self.enrichment_row(did)?;
        Ok(row.map(|row| row.enrichment).unwrap_or_default())
    }

    pub fn put_account(&self, did: &Did, account: &Account) -> Result<(), Error> {
        let value = postcard::to_stdvec(account)?;
        Ok(self.accounts.insert(did.as_str(), value)?)
    }

    pub fn delete_account(&self, did: &Did) -> Result<(), Error> {
        let mut batch = self.database.batch();
        batch.remove(&self.accounts, did.as_str());
        batch.remove(&self.enrichment, did.as_str());
        for entry in self.labels.prefix(labels_prefix(did)) {
            batch.remove(&self.labels, entry.key()?);
        }
        Ok(batch.commit()?)
    }

    pub fn put_labels(
        &self,
        did: &Did,
        origin: LabelOrigin,
        labels: &[Label],
    ) -> Result<bool, Error> {
        let key = labels_key(did, &origin);
        let stored = self.labels.get(&key)?;
        if labels.is_empty() {
            if stored.is_some() {
                self.labels.remove(key)?;
            }
            return Ok(stored.is_some());
        }
        let value = postcard::to_stdvec(labels)?;
        if stored.as_deref() == Some(value.as_slice()) {
            return Ok(false);
        }
        self.labels.insert(key, value)?;
        Ok(true)
    }

    pub fn labels(&self, did: &Did) -> Result<Vec<Label>, Error> {
        let entries = self.labels.prefix(labels_prefix(did));
        merge_labels(entries.map(|entry| entry.into_inner()))
    }

    pub fn put_enrichment(
        &self,
        did: &Did,
        enrichment: &Enrichment,
        fetched_at: i64,
    ) -> Result<(), Error> {
        let row = EnrichmentRow {
            enrichment: enrichment.clone(),
            fetched_at,
        };
        let value = postcard::to_stdvec(&row)?;
        Ok(self.enrichment.insert(did.as_str(), value)?)
    }

    pub fn accounts_after(
        &self,
        after: Option<&Did>,
        limit: usize,
    ) -> Result<Vec<StoredAccount>, Error> {
        let rows = self.rows_after(after, limit)?;
        let mut labels = self
            .labels
            .range::<&str, _>(following(after))
            .map(|entry| entry.into_inner())
            .peekable();
        rows.into_iter()
            .map(|(did, account, row)| {
                let labels = labels_for(&mut labels, &did)?;
                Ok(StoredAccount {
                    did,
                    account,
                    enrichment: row.map(|row| row.enrichment).unwrap_or_default(),
                    labels,
                })
            })
            .collect()
    }

    pub fn scan_stale(
        &self,
        after: Option<&Did>,
        cutoff: i64,
        limit: usize,
    ) -> Result<Scan, Error> {
        let rows = self.rows_after(after, limit)?;
        let last = rows.last().map(|(did, _, _)| did.clone());
        let stale = rows
            .into_iter()
            .filter(|(_, _, row)| row.as_ref().is_none_or(|row| row.fetched_at < cutoff))
            .map(|(did, _, _)| did)
            .collect();
        Ok(Scan { stale, last })
    }

    pub fn has_account(&self, did: &Did) -> Result<bool, Error> {
        Ok(self.accounts.contains_key(did.as_str())?)
    }

    pub fn backfill_state(&self) -> Result<BackfillState, Error> {
        let Some(bytes) = self.meta.get(BACKFILL_STATE)? else {
            return Ok(BackfillState::default());
        };
        Ok(postcard::from_bytes(&bytes)?)
    }

    pub fn put_backfill_state(&self, state: &BackfillState) -> Result<(), Error> {
        let value = postcard::to_stdvec(state)?;
        Ok(self.meta.insert(BACKFILL_STATE, value)?)
    }

    pub fn persist(&self) -> Result<(), Error> {
        Ok(self.database.persist(PersistMode::SyncData)?)
    }

    fn rows_after(
        &self,
        after: Option<&Did>,
        limit: usize,
    ) -> Result<Vec<(Did, Account, Option<EnrichmentRow>)>, Error> {
        let mut enrichment = self
            .enrichment
            .range::<&str, _>(following(after))
            .map(|entry| entry.into_inner())
            .peekable();
        let accounts = self.accounts.range::<&str, _>(following(after));
        let mut rows = Vec::with_capacity(limit);
        for entry in accounts.take(limit) {
            let (did, account) = entry.into_inner()?;
            let row = value_for(&mut enrichment, &did)?
                .map(|bytes| postcard::from_bytes(&bytes))
                .transpose()?;
            let account = postcard::from_bytes(&account)?;
            rows.push((did_from_bytes(&did)?, account, row));
        }
        Ok(rows)
    }

    pub fn enrichment_row(&self, did: &Did) -> Result<Option<EnrichmentRow>, Error> {
        let Some(bytes) = self.enrichment.get(did.as_str())? else {
            return Ok(None);
        };
        Ok(Some(postcard::from_bytes(&bytes)?))
    }
}

fn value_for(
    rows: &mut Peekable<impl Iterator<Item = fjall::Result<KvPair>>>,
    key: &[u8],
) -> Result<Option<Slice>, Error> {
    let before =
        |row: &fjall::Result<KvPair>| row.as_ref().map_or(true, |(row_key, _)| &**row_key < key);
    while let Some(row) = rows.next_if(before) {
        row?;
    }
    let found = rows.next_if(|row| row.as_ref().is_ok_and(|(row_key, _)| &**row_key == key));
    Ok(found.transpose()?.map(|(_, value)| value))
}

fn following(did: Option<&Did>) -> (Bound<&str>, Bound<&str>) {
    let start = did.map_or(Bound::Unbounded, |did| Bound::Excluded(did.as_str()));
    (start, Bound::Unbounded)
}

fn labels_for(
    entries: &mut Peekable<impl Iterator<Item = fjall::Result<KvPair>>>,
    did: &Did,
) -> Result<Vec<Label>, Error> {
    let prefix = labels_prefix(did);
    let before = |entry: &fjall::Result<KvPair>| {
        entry
            .as_ref()
            .map_or(true, |(key, _)| &**key < prefix.as_slice())
    };
    while let Some(entry) = entries.next_if(before) {
        entry?;
    }
    let owned = std::iter::from_fn(|| {
        entries.next_if(|entry| {
            entry
                .as_ref()
                .is_ok_and(|(key, _)| key.starts_with(&prefix))
        })
    });
    merge_labels(owned)
}

fn merge_labels(entries: impl Iterator<Item = fjall::Result<KvPair>>) -> Result<Vec<Label>, Error> {
    let mut labels = Vec::new();
    for entry in entries {
        let (_, bytes) = entry?;
        let from_origin: Vec<Label> = postcard::from_bytes(&bytes)?;
        for label in from_origin {
            if !labels.contains(&label) {
                labels.push(label);
            }
        }
    }
    Ok(labels)
}

fn labels_prefix(did: &Did) -> Vec<u8> {
    let mut prefix = did.as_str().as_bytes().to_vec();
    prefix.push(0);
    prefix
}

fn labels_key(did: &Did, origin: &LabelOrigin) -> Vec<u8> {
    let mut key = labels_prefix(did);
    key.extend_from_slice(origin.key().as_bytes());
    key
}

fn did_from_bytes(bytes: &[u8]) -> Result<Did, Error> {
    let did = String::from_utf8_lossy(bytes).into_owned();
    Did::try_from(did).map_err(Error::BadDid)
}

#[cfg(test)]
mod tests {
    use types::Handle;

    use super::*;

    fn did(name: &str) -> Did {
        Did::try_from(format!("did:plc:{name}")).unwrap()
    }

    fn account(handle: &str) -> Account {
        let handle = Handle::try_from(handle.to_owned()).unwrap();
        Account {
            handle: Some(handle),
            ..Account::default()
        }
    }

    fn open() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn stores_and_overwrites_accounts() {
        let (_dir, store) = open();
        let alice = did("alice");

        store.put_account(&alice, &account("alice.test")).unwrap();
        store.put_account(&alice, &account("alice2.test")).unwrap();

        let stored = store.account(&alice).unwrap().unwrap();
        assert_eq!(stored, account("alice2.test"));
    }

    #[test]
    fn deleting_an_account_removes_all_of_its_data() {
        let (_dir, store) = open();
        let alice = did("alice");
        let enrichment = Enrichment {
            followers: 5,
            created_at: None,
        };
        store.put_account(&alice, &account("alice.test")).unwrap();
        store.put_enrichment(&alice, &enrichment, 10).unwrap();
        store
            .put_labels(&alice, LabelOrigin::Appview, &[label("spam")])
            .unwrap();

        store.delete_account(&alice).unwrap();

        assert_eq!(store.account(&alice).unwrap(), None);
        assert_eq!(store.enrichment(&alice).unwrap(), Enrichment::default());
        assert!(store.labels(&alice).unwrap().is_empty());
    }

    fn label(val: &str) -> Label {
        Label {
            src: did("labeler"),
            uri: "did:plc:alice".to_owned(),
            cid: None,
            val: val.to_owned(),
            cts: "2026-01-01T00:00:00Z".to_owned(),
            exp: None,
        }
    }

    #[test]
    fn putting_labels_replaces_them_and_an_empty_list_clears_them() {
        let (_dir, store) = open();
        let alice = did("alice");

        store
            .put_labels(&alice, LabelOrigin::Appview, &[label("spam")])
            .unwrap();
        store
            .put_labels(&alice, LabelOrigin::Appview, &[label("porn")])
            .unwrap();
        assert_eq!(store.labels(&alice).unwrap(), [label("porn")]);

        store.put_labels(&alice, LabelOrigin::Appview, &[]).unwrap();
        assert!(store.labels(&alice).unwrap().is_empty());
    }

    #[test]
    fn putting_labels_reports_whether_they_changed() {
        let (_dir, store) = open();
        let alice = did("alice");
        let put = |labels: &[Label]| store.put_labels(&alice, LabelOrigin::Appview, labels);

        assert!(put(&[label("spam")]).unwrap());
        assert!(!put(&[label("spam")]).unwrap());
        assert!(put(&[]).unwrap());
        assert!(!put(&[]).unwrap());
    }

    #[test]
    fn listed_accounts_carry_their_labels() {
        let (_dir, store) = open();
        for name in ["a", "b", "c"] {
            store.put_account(&did(name), &account("x.test")).unwrap();
        }
        store
            .put_labels(&did("b"), LabelOrigin::Appview, &[label("spam")])
            .unwrap();

        let rows = store.accounts_after(Some(&did("a")), 10).unwrap();

        let found: Vec<_> = rows.into_iter().map(|row| (row.did, row.labels)).collect();
        assert_eq!(found, [(did("b"), vec![label("spam")]), (did("c"), vec![])]);
    }

    #[test]
    fn labels_belong_to_one_account_only() {
        let (_dir, store) = open();
        store
            .put_labels(&did("alice"), LabelOrigin::Appview, &[label("spam")])
            .unwrap();

        assert!(store.labels(&did("ali")).unwrap().is_empty());
        assert!(store.labels(&did("alice2")).unwrap().is_empty());
    }

    #[test]
    fn lists_accounts_with_their_enrichment_in_order_after_a_position() {
        let (_dir, store) = open();
        let enrichment = Enrichment {
            followers: 5,
            created_at: None,
        };
        for name in ["c", "a", "b"] {
            store.put_account(&did(name), &account("x.test")).unwrap();
        }
        store.put_enrichment(&did("a"), &enrichment, 10).unwrap();

        let dids =
            |rows: Vec<StoredAccount>| rows.into_iter().map(|row| row.did).collect::<Vec<_>>();
        let first = store.accounts_after(None, 2).unwrap();
        assert_eq!(first[0].enrichment, enrichment);
        assert_eq!(dids(first), [did("a"), did("b")]);
        let rest = store.accounts_after(Some(&did("b")), 10).unwrap();
        assert_eq!(dids(rest), [did("c")]);
    }

    #[test]
    fn rows_without_an_account_are_skipped() {
        let (_dir, store) = open();
        for name in ["a", "c"] {
            store.put_account(&did(name), &account("x.test")).unwrap();
        }
        let enrichment = |followers| Enrichment {
            followers,
            created_at: None,
        };
        store.put_enrichment(&did("a"), &enrichment(1), 10).unwrap();
        store.put_enrichment(&did("b"), &enrichment(2), 10).unwrap();
        store.put_enrichment(&did("c"), &enrichment(3), 10).unwrap();

        let rows = store.accounts_after(None, 10).unwrap();

        let found: Vec<_> = rows
            .into_iter()
            .map(|row| (row.did, row.enrichment.followers))
            .collect();
        assert_eq!(found, [(did("a"), 1), (did("c"), 3)]);
    }

    #[test]
    fn a_scan_finds_accounts_fetched_before_the_cutoff_or_never() {
        let (_dir, store) = open();
        for name in ["a", "b", "c", "d"] {
            store.put_account(&did(name), &account("x.test")).unwrap();
        }
        store
            .put_enrichment(&did("a"), &Enrichment::default(), 10)
            .unwrap();
        store
            .put_enrichment(&did("b"), &Enrichment::default(), 50)
            .unwrap();

        let first = store.scan_stale(None, 20, 3).unwrap();
        assert_eq!(first.stale, [did("a"), did("c")]);
        assert_eq!(first.last, Some(did("c")));

        let rest = store.scan_stale(first.last.as_ref(), 20, 3).unwrap();
        assert_eq!(rest.stale, [did("d")]);
        let end = store.scan_stale(rest.last.as_ref(), 20, 3).unwrap();
        assert_eq!(end.last, None);
    }

    #[test]
    fn saves_backfill_state() {
        let (_dir, store) = open();
        assert_eq!(store.backfill_state().unwrap(), BackfillState::default());

        let state = BackfillState {
            progress: BackfillProgress {
                cursor: Some("page2".to_owned()),
                finished: false,
            },
            counts: BackfillCounts {
                listed: 10,
                filled: 4,
            },
        };
        store.put_backfill_state(&state).unwrap();

        assert_eq!(store.backfill_state().unwrap(), state);
    }
}
