mod codec;
mod migration;
mod queue;

use std::iter::Peekable;
use std::ops::Bound;
use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvPair, PersistMode, Slice};
use serde::{Deserialize, Serialize};
use types::{Account, Did, Enrichment, Label, LabelRecord};

use queue::QueueState;
pub use queue::{Lane, Position, Queued};

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
    #[error("found a malformed key in the queue")]
    BadQueueKey,
    #[error("the store has format {0}, which this build does not support")]
    UnsupportedFormat(u32),
    #[error("found a stored `{kind}` with version {version}, which this build does not support")]
    UnsupportedVersion { kind: &'static str, version: u8 },
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

pub struct Store {
    database: Database,
    accounts: Keyspace,
    enrichment: Keyspace,
    labels: Keyspace,
    meta: Keyspace,
    queue: Keyspace,
    queue_state: QueueState,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store, Error> {
        let database = Database::builder(path).open()?;
        let accounts = database.keyspace("accounts", KeyspaceCreateOptions::default)?;
        let enrichment = database.keyspace(ENRICHMENT, KeyspaceCreateOptions::default)?;
        let labels = database.keyspace(LABELS, KeyspaceCreateOptions::default)?;
        let meta = database.keyspace("meta", KeyspaceCreateOptions::default)?;
        let queue = database.keyspace("queue", KeyspaceCreateOptions::default)?;
        let queue_state = QueueState::load(&queue)?;
        let store = Store {
            database,
            accounts,
            enrichment,
            labels,
            meta,
            queue,
            queue_state,
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn account(&self, did: &Did) -> Result<Option<Account>, Error> {
        let Some(bytes) = self.accounts.get(did.as_str())? else {
            return Ok(None);
        };
        Ok(Some(codec::decode(&bytes)?))
    }

    pub fn enrichment(&self, did: &Did) -> Result<Enrichment, Error> {
        let row = self.enrichment_row(did)?;
        Ok(row.map(|row| row.enrichment).unwrap_or_default())
    }

    pub fn put_account(&self, did: &Did, account: &Account) -> Result<(), Error> {
        let value = codec::encode(account)?;
        Ok(self.accounts.insert(did.as_str(), value)?)
    }

    pub fn blank_account(&self, did: &Did) -> Result<(), Error> {
        let blank = Account {
            active: false,
            ..Account::default()
        };
        let mut batch = self.database.batch();
        batch.insert(&self.accounts, did.as_str(), codec::encode(&blank)?);
        batch.remove(&self.enrichment, did.as_str());
        Ok(batch.commit()?)
    }

    pub fn store_label_records(&self, did: &Did, records: &[LabelRecord]) -> Result<(), Error> {
        let stored = self.label_records(did)?;
        let mut kept = stored.clone();
        for record in records {
            let same = kept
                .iter_mut()
                .find(|kept| kept.label.is_same_label(&record.label));
            match same {
                Some(kept) if record.label.cts.timestamp() > kept.label.cts.timestamp() => {
                    *kept = record.clone();
                }
                Some(_) => {}
                None => kept.push(record.clone()),
            }
        }
        if kept != stored {
            let value = codec::encode(&kept)?;
            self.labels.insert(did.as_str(), value)?;
        }
        Ok(())
    }

    pub fn labels(&self, did: &Did) -> Result<Vec<Label>, Error> {
        Ok(live(self.label_records(did)?))
    }

    fn label_records(&self, did: &Did) -> Result<Vec<LabelRecord>, Error> {
        let Some(bytes) = self.labels.get(did.as_str())? else {
            return Ok(Vec::new());
        };
        codec::decode(&bytes)
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
        let value = codec::encode(&row)?;
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
                let labels = value_for(&mut labels, did.as_str().as_bytes())?
                    .map(|bytes| codec::decode(&bytes))
                    .transpose()?
                    .map(live)
                    .unwrap_or_default();
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
            .filter(|(_, account, _)| account.active)
            .filter(|(_, _, row)| row.as_ref().is_none_or(|row| row.fetched_at < cutoff))
            .map(|(did, _, _)| did)
            .collect();
        Ok(Scan { stale, last })
    }

    pub fn has_account(&self, did: &Did) -> Result<bool, Error> {
        Ok(self.accounts.contains_key(did.as_str())?)
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
                .map(|bytes| codec::decode(&bytes))
                .transpose()?;
            let account = codec::decode(&account)?;
            rows.push((did_from_bytes(&did)?, account, row));
        }
        Ok(rows)
    }

    pub fn enrichment_row(&self, did: &Did) -> Result<Option<EnrichmentRow>, Error> {
        let Some(bytes) = self.enrichment.get(did.as_str())? else {
            return Ok(None);
        };
        Ok(Some(codec::decode(&bytes)?))
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

fn live(records: Vec<LabelRecord>) -> Vec<Label> {
    records
        .into_iter()
        .filter(|record| !record.withdrawn)
        .map(|record| record.label)
        .collect()
}

fn did_from_bytes(bytes: &[u8]) -> Result<Did, Error> {
    let did = String::from_utf8_lossy(bytes).into_owned();
    Did::try_from(did).map_err(Error::BadDid)
}

#[cfg(test)]
mod tests {
    use types::{Datetime, Handle};

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
    fn blanking_an_account_keeps_an_inactive_row_and_labels_from_labelers() {
        let (_dir, store) = open();
        let alice = did("alice");
        let enrichment = Enrichment {
            followers: 5,
            created_at: None,
        };
        store.put_account(&alice, &account("alice.test")).unwrap();
        store.put_enrichment(&alice, &enrichment, 10).unwrap();
        store
            .store_label_records(&alice, &[issued(label("spam"))])
            .unwrap();

        store.blank_account(&alice).unwrap();

        let blank = Account {
            active: false,
            ..Account::default()
        };
        assert_eq!(store.account(&alice).unwrap(), Some(blank));
        assert_eq!(store.enrichment(&alice).unwrap(), Enrichment::default());
        assert_eq!(store.labels(&alice).unwrap(), [label("spam")]);
    }

    fn label(val: &str) -> Label {
        Label {
            src: did("labeler"),
            uri: "did:plc:alice".to_owned(),
            cid: None,
            val: val.to_owned(),
            cts: Datetime::try_from("2026-01-01T00:00:00Z".to_owned()).unwrap(),
            exp: None,
        }
    }

    fn issued(label: Label) -> LabelRecord {
        LabelRecord {
            label,
            withdrawn: false,
        }
    }

    fn withdrawn(label: Label) -> LabelRecord {
        let cts = Datetime::try_from("2026-02-01T00:00:00Z".to_owned()).unwrap();
        LabelRecord {
            label: Label { cts, ..label },
            withdrawn: true,
        }
    }

    #[test]
    fn the_newest_record_for_a_label_is_the_one_that_counts() {
        let (_dir, store) = open();
        let alice = did("alice");
        let spam = issued(label("spam"));

        store
            .store_label_records(&alice, std::slice::from_ref(&spam))
            .unwrap();
        assert_eq!(store.labels(&alice).unwrap(), [label("spam")]);

        store
            .store_label_records(&alice, &[withdrawn(label("spam")), spam])
            .unwrap();
        assert!(store.labels(&alice).unwrap().is_empty());
    }

    #[test]
    fn a_withdrawal_for_another_version_leaves_the_label() {
        let (_dir, store) = open();
        let alice = did("alice");
        let version = |cid: &str| Label {
            cid: Some(cid.to_owned()),
            ..label("porn")
        };

        store
            .store_label_records(&alice, &[issued(version("a")), withdrawn(version("b"))])
            .unwrap();

        assert_eq!(store.labels(&alice).unwrap(), [version("a")]);
    }

    #[test]
    fn listed_accounts_carry_their_labels() {
        let (_dir, store) = open();
        for name in ["a", "b", "c"] {
            store.put_account(&did(name), &account("x.test")).unwrap();
        }
        store
            .store_label_records(&did("b"), &[issued(label("spam"))])
            .unwrap();

        let rows = store.accounts_after(Some(&did("a")), 10).unwrap();

        let found: Vec<_> = rows.into_iter().map(|row| (row.did, row.labels)).collect();
        assert_eq!(found, [(did("b"), vec![label("spam")]), (did("c"), vec![])]);
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
    fn a_scan_skips_inactive_accounts() {
        let (_dir, store) = open();
        let inactive = Account {
            active: false,
            ..account("x.test")
        };
        store.put_account(&did("a"), &inactive).unwrap();

        let scan = store.scan_stale(None, 20, 3).unwrap();

        assert!(scan.stale.is_empty());
        assert_eq!(scan.last, Some(did("a")));
    }
}
