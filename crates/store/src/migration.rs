use std::ops::Bound;

use fjall::{OwnedWriteBatch, PersistMode, Slice};
use serde::Deserialize;
use types::{
    Account, AppviewVerification, Did, Enrichment, Facts, Handle, HandleFact, Job, Label, Profile,
    Status,
};

use crate::{Error, Store};

const FORMAT: u32 = 6;
const FORMAT_KEY: &str = "format";
const UPGRADED_ACCOUNTS: &str = "upgrade:accounts";
const ROWS_PER_BATCH: usize = 10_000;

#[derive(Deserialize)]
struct AccountBeforeSelfLabels {
    handle: Option<Handle>,
    profile: ProfileBeforeSelfLabels,
    active: bool,
    verification: Option<AppviewVerification>,
}

#[derive(Deserialize)]
struct ProfileBeforeSelfLabels {
    display_name: Option<String>,
    avatar_cid: Option<String>,
}

#[derive(Deserialize)]
struct JobBeforeSelfLabels {
    did: Did,
    seen: FactsBeforeSelfLabels,
}

#[derive(Deserialize)]
struct FactsBeforeSelfLabels {
    handle: Option<HandleFact>,
    profile: Option<ProfileBeforeSelfLabels>,
    status: Option<Status>,
    enrichment: Option<Enrichment>,
    #[expect(dead_code, reason = "labels from the AppView are no longer kept")]
    labels: Option<Vec<Label>>,
    verification: Option<Option<AppviewVerification>>,
}

impl From<ProfileBeforeSelfLabels> for Profile {
    fn from(profile: ProfileBeforeSelfLabels) -> Profile {
        Profile {
            display_name: profile.display_name,
            avatar_cid: profile.avatar_cid,
            self_labels: None,
        }
    }
}

impl From<AccountBeforeSelfLabels> for Account {
    fn from(account: AccountBeforeSelfLabels) -> Account {
        Account {
            handle: account.handle,
            profile: account.profile.into(),
            active: account.active,
            verification: account.verification,
        }
    }
}

impl From<JobBeforeSelfLabels> for Job {
    fn from(job: JobBeforeSelfLabels) -> Job {
        Job {
            did: job.did,
            seen: Facts {
                handle: job.seen.handle,
                profile: job.seen.profile.map(Profile::from),
                status: job.seen.status,
                enrichment: job.seen.enrichment,
                verification: job.seen.verification,
            },
        }
    }
}

impl Store {
    pub(crate) fn migrate(&self) -> Result<(), Error> {
        match self.format()? {
            FORMAT => Ok(()),
            0 if self.accounts.is_empty()? => self.set_format(),
            3 => self.upgrade_from_v3(),
            4 | 5 => self.clear_labels(),
            format => Err(Error::UnsupportedFormat(format)),
        }
    }

    fn upgrade_from_v3(&self) -> Result<(), Error> {
        self.upgrade_accounts()?;
        let mut batch = self.database.batch();
        for entry in self.queue.iter() {
            let (key, value) = entry.into_inner()?;
            let job: JobBeforeSelfLabels = postcard::from_bytes(&value)?;
            batch.insert(&self.queue, key, postcard::to_stdvec(&Job::from(job))?);
        }
        batch.remove(&self.meta, UPGRADED_ACCOUNTS);
        batch.insert(&self.meta, FORMAT_KEY, postcard::to_stdvec(&4_u32)?);
        batch.commit()?;
        self.database.persist(PersistMode::SyncData)?;
        self.clear_labels()
    }

    fn clear_labels(&self) -> Result<(), Error> {
        self.labels.clear()?;
        self.set_format()?;
        self.database.persist(PersistMode::SyncData)?;
        Ok(())
    }

    fn upgrade_accounts(&self) -> Result<(), Error> {
        let upgraded = self.meta.get(UPGRADED_ACCOUNTS)?;
        let start = match &upgraded {
            Some(last) => Bound::Excluded(&last[..]),
            None => Bound::Unbounded,
        };
        let mut batch = self.database.batch();
        let mut last = None;
        let rows = self.accounts.range::<&[u8], _>((start, Bound::Unbounded));
        for (count, entry) in rows.enumerate() {
            let (key, value) = entry.into_inner()?;
            let account: AccountBeforeSelfLabels = postcard::from_bytes(&value)?;
            let account = postcard::to_stdvec(&Account::from(account))?;
            batch.insert(&self.accounts, key.clone(), account);
            last = Some(key);
            if count % ROWS_PER_BATCH == ROWS_PER_BATCH - 1 {
                let full = std::mem::replace(&mut batch, self.database.batch());
                self.commit_upgraded(full, last.take())?;
            }
        }
        self.commit_upgraded(batch, last)
    }

    fn commit_upgraded(
        &self,
        mut batch: OwnedWriteBatch,
        last: Option<Slice>,
    ) -> Result<(), Error> {
        if let Some(last) = last {
            batch.insert(&self.meta, UPGRADED_ACCOUNTS, last);
        }
        Ok(batch.commit()?)
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
    use types::Datetime;

    use super::*;

    type OldProfile = (Option<String>, Option<String>);
    type OldAccount = (
        Option<Handle>,
        OldProfile,
        bool,
        Option<AppviewVerification>,
    );

    fn did(name: &str) -> Did {
        Did::try_from(format!("did:plc:{name}")).unwrap()
    }

    fn old_account(handle: &str) -> Vec<u8> {
        let handle = Handle::try_from(handle.to_owned()).ok();
        let profile = (Some("Name".to_owned()), None);
        let account: OldAccount = (handle, profile, true, None);
        postcard::to_stdvec(&account).unwrap()
    }

    fn format_3(dir: &tempfile::TempDir) -> Store {
        let store = Store::open(dir.path()).unwrap();
        let old_format = postcard::to_stdvec(&3_u32).unwrap();
        store.meta.insert(FORMAT_KEY, old_format).unwrap();
        store
    }

    fn handle_of(store: &Store, name: &str) -> String {
        let account = store.account(&did(name)).unwrap().unwrap();
        assert_eq!(account.profile.display_name.as_deref(), Some("Name"));
        account.handle.unwrap().to_string()
    }

    #[test]
    fn a_format_3_store_is_upgraded() {
        let dir = tempfile::tempdir().unwrap();
        let store = format_3(&dir);
        let label = Label {
            src: did("labeler"),
            uri: "did:plc:alice".to_owned(),
            cid: None,
            val: "spam".to_owned(),
            cts: Datetime::try_from("2026-01-01T00:00:00Z".to_owned()).unwrap(),
            exp: None,
        };
        let old_facts = (
            None::<HandleFact>,
            None::<OldProfile>,
            Some(Status::Inactive),
            None::<Enrichment>,
            Some(vec![&label]),
            None::<Option<AppviewVerification>>,
        );
        let old_job = postcard::to_stdvec(&(did("alice"), old_facts)).unwrap();
        let old_labels = postcard::to_stdvec(&vec![&label]).unwrap();
        let accounts = &store.accounts;
        accounts
            .insert("did:plc:alice", old_account("alice.test"))
            .unwrap();
        store
            .labels
            .insert("did:plc:alice\0appview", old_labels)
            .unwrap();
        store.queue.insert([0; 9], old_job).unwrap();
        store.persist().unwrap();
        drop(store);

        let store = Store::open(dir.path()).unwrap();

        assert_eq!(handle_of(&store, "alice"), "alice.test");
        assert!(store.labels(&did("alice")).unwrap().is_empty());
        let jobs = store.next_jobs(1).unwrap();
        assert_eq!(jobs[0].job.seen.status, Some(Status::Inactive));
    }

    #[test]
    fn a_format_5_store_has_its_label_table_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let old_format = postcard::to_stdvec(&5_u32).unwrap();
        store.labels.insert("did:plc:alice", "leftover").unwrap();
        store.meta.insert(FORMAT_KEY, old_format).unwrap();
        store.persist().unwrap();
        drop(store);

        let store = Store::open(dir.path()).unwrap();

        assert!(store.labels.is_empty().unwrap());
        assert_eq!(store.format().unwrap(), FORMAT);
    }

    #[test]
    fn an_upgrade_that_was_cut_short_continues_after_the_last_upgraded_account() {
        let dir = tempfile::tempdir().unwrap();
        let store = format_3(&dir);
        let upgraded = Account {
            handle: Handle::try_from("alice.test".to_owned()).ok(),
            profile: Profile {
                display_name: Some("Name".to_owned()),
                ..Profile::default()
            },
            ..Account::default()
        };
        store.put_account(&did("alice"), &upgraded).unwrap();
        store
            .meta
            .insert(UPGRADED_ACCOUNTS, "did:plc:alice")
            .unwrap();
        let accounts = &store.accounts;
        accounts
            .insert("did:plc:bob", old_account("bob.test"))
            .unwrap();
        store.persist().unwrap();
        drop(store);

        let store = Store::open(dir.path()).unwrap();

        assert_eq!(handle_of(&store, "alice"), "alice.test");
        assert_eq!(handle_of(&store, "bob"), "bob.test");
    }
}
