use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use appview::ProfileView;
use search::Actor;
use store::{BackfillState, Store};
use tokio::sync::mpsc::{self, error::TryRecvError};
use types::{Account, Did, Enrichment, Handle, Profile};

use crate::lookup::Source;
use crate::score::Scorer;

const REBUILD_CHUNK: usize = 1000;

const MESSAGES_PER_STEP: usize = 100;

pub enum Message {
    Change {
        seq: u64,
        did: Did,
        change: Change,
    },
    LookedUp {
        accounts: Vec<(Did, Option<ProfileView>)>,
        source: Source,
    },
    Commit,
}

pub enum Change {
    Handle(Option<Handle>),
    Profile(Profile),
    Status { active: bool },
    Deleted,
}

pub struct Writer {
    store: Arc<Store>,
    index: search::Writer,
    scorer: Box<dyn Scorer>,
    has_changes: bool,
    rebuild: Option<Rebuild>,
}

struct Rebuild {
    after: Option<Did>,
}

impl Writer {
    pub fn new(store: Arc<Store>, index: search::Writer, scorer: Box<dyn Scorer>) -> Writer {
        Writer {
            store,
            index,
            scorer,
            has_changes: false,
            rebuild: None,
        }
    }

    pub fn run(mut self, mut inbox: mpsc::Receiver<Message>) -> anyhow::Result<()> {
        if self.index.needs_rebuild() {
            self.start_rebuild()?;
        }
        while self.rebuild.is_some() {
            let inbox_open = self.apply_queued(&mut inbox)?;
            if !inbox_open {
                return self.commit();
            }
            self.rebuild_step()?;
        }
        while let Some(message) = inbox.blocking_recv() {
            self.apply(message)?;
        }
        self.commit()
    }

    pub fn rebuild(mut self) -> anyhow::Result<()> {
        self.start_rebuild()?;
        while self.rebuild.is_some() {
            self.rebuild_step()?;
        }
        Ok(())
    }

    fn apply_queued(&mut self, inbox: &mut mpsc::Receiver<Message>) -> anyhow::Result<bool> {
        for _ in 0..MESSAGES_PER_STEP {
            match inbox.try_recv() {
                Ok(message) => self.apply(message)?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(false),
            }
        }
        Ok(true)
    }

    fn start_rebuild(&mut self) -> anyhow::Result<()> {
        self.index
            .start_rebuild()
            .context("failed to start the rebuild")?;
        self.rebuild = Some(Rebuild { after: None });
        metrics::gauge!("tern_index_rebuilding").set(1.0);
        tracing::info!("index rebuild started");
        Ok(())
    }

    fn rebuild_step(&mut self) -> anyhow::Result<()> {
        let rebuild = self.rebuild.as_mut().expect("a rebuild is running");
        let rows = self
            .store
            .accounts_after(rebuild.after.as_ref(), REBUILD_CHUNK)?;
        rebuild.after = rows.last().map(|row| row.did.clone());

        let now = now();
        for row in &rows {
            let searchable = self.searchable(&row.did, &row.account, &row.enrichment, now);
            if let Some((actor, score)) = searchable {
                self.index.upsert_rebuilt(&actor, score)?;
            }
        }
        metrics::counter!("tern_index_rebuilt_accounts_total").increment(rows.len() as u64);
        self.has_changes = true;

        if rows.len() < REBUILD_CHUNK {
            self.finish_rebuild()?;
        }
        Ok(())
    }

    fn finish_rebuild(&mut self) -> anyhow::Result<()> {
        self.commit()?;
        self.index
            .finish_rebuild()
            .context("failed to switch to the rebuilt index")?;
        self.rebuild = None;
        metrics::gauge!("tern_index_rebuilding").set(0.0);
        metrics::gauge!("tern_indexed_accounts").set(self.index.num_docs() as f64);
        tracing::info!("index rebuild finished");
        Ok(())
    }

    fn apply(&mut self, message: Message) -> anyhow::Result<()> {
        match message {
            Message::Change { seq, did, change } => {
                self.apply_change(&did, change)?;
                self.index.set_cursor(seq);
                self.has_changes = true;
            }
            Message::LookedUp { accounts, source } => {
                let mut created = 0;
                for (did, view) in accounts {
                    if self.apply_lookup(&did, view, source.may_create())? {
                        created += 1;
                    }
                }
                self.save_backfill(source, created)?;
                self.has_changes = true;
            }
            Message::Commit => self.commit()?,
        }
        Ok(())
    }

    fn apply_change(&self, did: &Did, change: Change) -> anyhow::Result<()> {
        let mut account = self.store.account(did)?.unwrap_or_default();
        match change {
            Change::Handle(handle) => account.handle = handle,
            Change::Profile(profile) => account.profile = profile,
            Change::Status { active } => account.active = active,
            Change::Deleted => {
                self.store.delete_account(did)?;
                self.index.remove(did);
                return Ok(());
            }
        }
        self.store.put_account(did, &account)?;
        let enrichment = self.store.enrichment(did)?;
        self.reindex(did, &account, &enrichment, now())
    }

    fn apply_lookup(
        &mut self,
        did: &Did,
        view: Option<ProfileView>,
        may_create: bool,
    ) -> anyhow::Result<bool> {
        let stored = self.store.account(did)?;
        let Some(view) = view else {
            if let Some(account) = stored {
                self.apply_enrichment(did, &account, Enrichment::default(), false)?;
            }
            return Ok(false);
        };
        if stored.is_none() && !may_create {
            return Ok(false);
        }

        let is_new = stored.is_none();
        let mut account = stored.unwrap_or_default();
        let updates_handle = view.handle.is_some() && account.handle != view.handle;
        let updates_profile = account.profile != view.profile;
        if updates_handle {
            account.handle = view.handle;
        }
        if updates_profile {
            account.profile = view.profile;
        }
        let account_changed = is_new || updates_handle || updates_profile;
        if account_changed {
            self.store.put_account(did, &account)?;
        }
        self.apply_enrichment(did, &account, view.enrichment, account_changed)?;
        Ok(is_new)
    }

    fn apply_enrichment(
        &mut self,
        did: &Did,
        account: &Account,
        enrichment: Enrichment,
        account_changed: bool,
    ) -> anyhow::Result<()> {
        let now = now();
        let previous = self.store.enrichment_row(did)?;
        self.store.put_enrichment(did, enrichment, now)?;
        let previous_score = previous.map(|row| self.scorer.score(&row.enrichment, row.fetched_at));
        let score_changed = previous_score != Some(self.scorer.score(&enrichment, now));
        if account_changed || score_changed {
            self.reindex(did, account, &enrichment, now)?;
        }
        Ok(())
    }

    fn save_backfill(&self, source: Source, created: u64) -> anyhow::Result<()> {
        let Source::Backfill { progress, listed } = source else {
            return Ok(());
        };
        if progress.finished {
            tracing::info!("backfill finished");
        }
        let mut state = self.store.backfill_state()?;
        state.progress = progress;
        state.counts.listed += listed;
        state.counts.filled += created;
        self.store.put_backfill_state(&state)?;
        record_backfill(&state);
        Ok(())
    }

    fn reindex(
        &self,
        did: &Did,
        account: &Account,
        enrichment: &Enrichment,
        now: i64,
    ) -> anyhow::Result<()> {
        match self.searchable(did, account, enrichment, now) {
            Some((actor, score)) => self.index.upsert(&actor, score)?,
            None => self.index.remove(did),
        }
        Ok(())
    }

    fn searchable(
        &self,
        did: &Did,
        account: &Account,
        enrichment: &Enrichment,
        now: i64,
    ) -> Option<(Actor, u64)> {
        let handle = account.searchable_handle()?;
        let actor = Actor {
            did: did.clone(),
            handle: handle.clone(),
            display_name: account.profile.display_name.clone(),
            avatar_cid: account.profile.avatar_cid.clone(),
        };
        Some((actor, self.scorer.score(enrichment, now)))
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        if !self.has_changes {
            return Ok(());
        }
        let start = Instant::now();
        self.store.persist().context("failed to flush the store")?;
        self.index.commit().context("failed to commit the index")?;
        self.has_changes = false;
        metrics::histogram!("tern_commit_seconds").record(start.elapsed());
        metrics::gauge!("tern_indexed_accounts").set(self.index.num_docs() as f64);
        tracing::debug!(cursor = ?self.index.cursor(), "committed");
        Ok(())
    }
}

fn now() -> i64 {
    jiff::Timestamp::now().as_second()
}

pub fn record_backfill(state: &BackfillState) {
    let finished = f64::from(u8::from(state.progress.finished));
    metrics::gauge!("tern_backfill_finished").set(finished);
    metrics::gauge!("tern_backfill_listed").set(state.counts.listed as f64);
    metrics::gauge!("tern_backfill_filled").set(state.counts.filled as f64);
}

#[cfg(test)]
mod tests {
    use store::{BackfillCounts, BackfillProgress};
    use types::Profile;

    use super::*;
    use crate::score::Reach;

    struct Fixture {
        _dir: tempfile::TempDir,
        store: Arc<Store>,
        reader: search::Reader,
        writer: Writer,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(&dir.path().join("store")).unwrap());
        let (reader, index) = search::open_in_memory().unwrap();
        let writer = Writer::new(Arc::clone(&store), index, Box::new(Reach));
        Fixture {
            _dir: dir,
            store,
            reader,
            writer,
        }
    }

    fn did(name: &str) -> Did {
        Did::try_from(format!("did:plc:{name}")).unwrap()
    }

    fn handle(name: &str) -> Option<Handle> {
        Some(Handle::try_from(format!("{name}.test")).unwrap())
    }

    fn change(seq: u64, name: &str, change: Change) -> Message {
        Message::Change {
            seq,
            did: did(name),
            change,
        }
    }

    impl Fixture {
        fn apply(&mut self, messages: Vec<Message>) {
            for message in messages {
                self.writer.apply(message).unwrap();
            }
            self.writer.apply(Message::Commit).unwrap();
        }

        fn found(&self, query: &str) -> Vec<String> {
            let actors = self.reader.search(query, 10).unwrap();
            actors
                .into_iter()
                .map(|actor| actor.handle.to_string())
                .collect()
        }
    }

    #[test]
    fn an_account_with_a_handle_becomes_searchable() {
        let mut fixture = fixture();

        fixture.apply(vec![change(1, "a", Change::Handle(handle("alice")))]);

        assert_eq!(fixture.found("ali"), ["alice.test"]);
    }

    #[test]
    fn an_account_without_a_handle_is_stored_but_not_searchable() {
        let mut fixture = fixture();
        let profile = Profile {
            display_name: Some("Alice".to_owned()),
            ..Profile::default()
        };

        fixture.apply(vec![change(1, "a", Change::Profile(profile))]);

        assert!(fixture.store.account(&did("a")).unwrap().is_some());
        assert!(fixture.found("ali").is_empty());
    }

    #[test]
    fn deactivating_hides_an_account_and_reactivating_restores_it() {
        let mut fixture = fixture();
        fixture.apply(vec![
            change(1, "a", Change::Handle(handle("alice"))),
            change(2, "a", Change::Status { active: false }),
        ]);
        assert!(fixture.found("ali").is_empty());

        fixture.apply(vec![change(3, "a", Change::Status { active: true })]);

        assert_eq!(fixture.found("ali"), ["alice.test"]);
    }

    #[test]
    fn deleting_removes_the_account_everywhere() {
        let mut fixture = fixture();
        fixture.apply(vec![
            change(1, "a", Change::Handle(handle("alice"))),
            change(2, "a", Change::Deleted),
        ]);

        assert_eq!(fixture.store.account(&did("a")).unwrap(), None);
        assert!(fixture.found("ali").is_empty());
    }

    fn view(name: &str, followers: u64) -> ProfileView {
        ProfileView {
            did: did(name),
            handle: handle(name),
            profile: Profile::default(),
            enrichment: Enrichment {
                followers,
                created_at: None,
            },
        }
    }

    fn lookup(views: Vec<(&str, Option<ProfileView>)>, source: Source) -> Message {
        let accounts = views
            .into_iter()
            .map(|(name, view)| (did(name), view))
            .collect();
        Message::LookedUp { accounts, source }
    }

    fn looked_up(views: Vec<(&str, Option<ProfileView>)>) -> Message {
        lookup(views, Source::Refresh)
    }

    fn backfilled(views: Vec<(&str, Option<ProfileView>)>) -> Message {
        let progress = BackfillProgress::default();
        lookup(
            views,
            Source::Backfill {
                progress,
                listed: 0,
            },
        )
    }

    #[test]
    fn a_lookup_reorders_results() {
        let mut fixture = fixture();
        fixture.apply(vec![
            change(1, "a", Change::Handle(handle("alice"))),
            change(2, "b", Change::Handle(handle("alicia"))),
        ]);

        fixture.apply(vec![looked_up(vec![("b", Some(view("alicia", 500)))])]);

        assert_eq!(fixture.found("ali"), ["alicia.test", "alice.test"]);
    }

    #[test]
    fn replaying_the_same_events_gives_the_same_result() {
        let mut fixture = fixture();
        for _ in 0..2 {
            fixture.apply(vec![
                change(1, "a", Change::Handle(handle("alice"))),
                change(2, "a", Change::Status { active: true }),
            ]);
        }

        assert_eq!(fixture.found("ali"), ["alice.test"]);
    }

    #[test]
    fn rebuild_restores_the_index_from_the_store() {
        let mut fixture = fixture();
        fixture.apply(vec![change(1, "a", Change::Handle(handle("alice")))]);

        let reader = fixture.reader.clone();
        fixture.writer.rebuild().unwrap();

        assert_eq!(reader.search("ali", 10).unwrap().len(), 1);
    }

    #[test]
    fn changes_made_during_a_rebuild_are_kept() {
        let mut fixture = fixture();
        fixture.apply(vec![change(1, "a", Change::Handle(handle("alice")))]);

        fixture.writer.start_rebuild().unwrap();
        fixture.apply(vec![
            change(2, "a", Change::Deleted),
            change(3, "b", Change::Handle(handle("bob"))),
        ]);
        fixture.writer.rebuild_step().unwrap();

        assert!(fixture.writer.rebuild.is_none());
        assert!(fixture.found("ali").is_empty());
        assert_eq!(fixture.found("bob"), ["bob.test"]);
    }

    #[test]
    fn a_full_queue_does_not_stall_a_rebuild() {
        let mut fixture = fixture();
        let (sender, mut inbox) = mpsc::channel(MESSAGES_PER_STEP + 1);
        for _ in 0..=MESSAGES_PER_STEP {
            sender.try_send(Message::Commit).unwrap();
        }

        assert!(fixture.writer.apply_queued(&mut inbox).unwrap());

        assert_eq!(inbox.len(), 1);
    }

    fn named(name: &str) -> Profile {
        Profile {
            display_name: Some(name.to_owned()),
            ..Profile::default()
        }
    }

    #[test]
    fn a_backfill_lookup_creates_an_account_we_do_not_have() {
        let mut fixture = fixture();
        let ada = ProfileView {
            profile: named("Ada"),
            ..view("ada", 0)
        };

        fixture.apply(vec![backfilled(vec![("ada", Some(ada)), ("x", None)])]);

        assert_eq!(fixture.found("ada"), ["ada.test"]);
        assert_eq!(fixture.store.account(&did("x")).unwrap(), None);
    }

    #[test]
    fn a_refresh_lookup_does_not_bring_back_a_deleted_account() {
        let mut fixture = fixture();
        fixture.apply(vec![
            change(1, "a", Change::Handle(handle("alice"))),
            change(2, "a", Change::Deleted),
        ]);

        fixture.apply(vec![looked_up(vec![("a", Some(view("alice", 5)))])]);

        assert_eq!(fixture.store.account(&did("a")).unwrap(), None);
        assert!(fixture.found("ali").is_empty());
    }

    #[test]
    fn a_lookup_replaces_the_handle_and_profile_but_never_clears_the_handle() {
        let mut fixture = fixture();
        fixture.apply(vec![
            change(1, "a", Change::Handle(handle("old"))),
            change(2, "a", Change::Profile(named("Old Name"))),
            change(3, "b", Change::Handle(handle("bob"))),
        ]);
        let alice = ProfileView {
            handle: handle("new"),
            profile: named("New Name"),
            ..view("a", 0)
        };
        let bob = ProfileView {
            handle: None,
            ..view("b", 0)
        };

        fixture.apply(vec![looked_up(vec![("a", Some(alice)), ("b", Some(bob))])]);

        assert_eq!(fixture.found("new"), ["new.test"]);
        assert!(fixture.found("old").is_empty());
        assert_eq!(fixture.found("bob"), ["bob.test"]);
    }

    #[test]
    fn a_lookup_rescores_an_account_that_only_got_older() {
        let mut fixture = fixture();
        let created = now() - 400 * 86_400;
        let enrichment = Enrichment {
            followers: 0,
            created_at: Some(created),
        };
        fixture.apply(vec![change(1, "ann", Change::Handle(handle("ann")))]);
        let indexed = |name: &str| Actor {
            did: did(name),
            handle: handle(name).unwrap(),
            display_name: None,
            avatar_cid: None,
        };
        fixture
            .store
            .put_enrichment(&did("ann"), enrichment, created)
            .unwrap();
        fixture.writer.index.upsert(&indexed("ann"), 0).unwrap();
        fixture.writer.index.upsert(&indexed("anna"), 50).unwrap();
        fixture.writer.index.commit().unwrap();
        assert_eq!(fixture.found("an"), ["anna.test", "ann.test"]);

        let ann = ProfileView {
            enrichment,
            ..view("ann", 0)
        };
        fixture.apply(vec![looked_up(vec![("ann", Some(ann))])]);

        assert_eq!(fixture.found("an"), ["ann.test", "anna.test"]);
    }

    #[test]
    fn a_backfill_lookup_saves_where_backfill_resumes_and_counts_accounts() {
        let mut fixture = fixture();
        let progress = BackfillProgress {
            cursor: Some("next".to_owned()),
            finished: false,
        };
        let accounts = vec![(did("a"), Some(view("a", 0))), (did("x"), None)];

        fixture.apply(vec![Message::LookedUp {
            accounts,
            source: Source::Backfill {
                progress: progress.clone(),
                listed: 2,
            },
        }]);

        let state = BackfillState {
            progress,
            counts: BackfillCounts {
                listed: 2,
                filled: 1,
            },
        };
        assert_eq!(fixture.store.backfill_state().unwrap(), state);
    }
}
