use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use search::Actor;
use store::{Queued, Store};
use tokio::sync::mpsc;
use tokio::task::block_in_place;
use types::{
    Account, Did, Enrichment, Facts, Fetched, HandleFact, Label, LabelJob, LabelRecord, Status,
};

use crate::score;

const REBUILD_CHUNK: usize = 1000;
const COMMIT_INTERVAL: Duration = Duration::from_secs(1);

pub struct Round {
    pub jobs: Vec<Queued>,
    pub fetched: Vec<Fetched>,
}

pub struct Writer {
    store: Arc<Store>,
    index: search::Writer,
    has_changes: bool,
    rebuild: Option<Rebuild>,
    finished: Vec<Queued>,
    finished_feed: Vec<Queued<LabelJob>>,
}

struct Rebuild {
    after: Option<Did>,
}

impl Writer {
    pub fn new(store: Arc<Store>, index: search::Writer) -> Writer {
        Writer {
            store,
            index,
            has_changes: false,
            rebuild: None,
            finished: Vec::new(),
            finished_feed: Vec::new(),
        }
    }

    pub async fn run(
        mut self,
        mut inbox: mpsc::Receiver<Round>,
        mut feed: mpsc::Receiver<Vec<Queued<LabelJob>>>,
    ) -> anyhow::Result<()> {
        if self.index.needs_rebuild() {
            self.start_rebuild()?;
        }
        let mut commit = tokio::time::interval(COMMIT_INTERVAL);
        loop {
            tokio::select! {
                round = inbox.recv() => match round {
                    Some(round) => block_in_place(|| self.apply_round(round))?,
                    None => break,
                },
                // To avoid slowing down refresh work, the feed channel is
                // only read when the writer has no other work to do. In practice
                // this will be between refreshes.
                Some(jobs) = feed.recv(), if inbox.is_empty() => {
                    block_in_place(|| self.apply_feed(jobs))?;
                }
                _ = commit.tick() => block_in_place(|| self.commit())?,
                () = std::future::ready(()), if self.rebuild.is_some() => {
                    block_in_place(|| self.rebuild_step())?;
                }
            }
        }
        block_in_place(|| self.commit())
    }

    pub fn rebuild(mut self) -> anyhow::Result<()> {
        self.start_rebuild()?;
        while self.rebuild.is_some() {
            self.rebuild_step()?;
        }
        Ok(())
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

        let count = rows.len();
        let now = now();
        for row in rows {
            let searchable =
                self.searchable(&row.did, &row.account, &row.enrichment, row.labels, now);
            if let Some((actor, score)) = searchable {
                self.index.upsert_rebuilt(&actor, score)?;
            }
        }
        metrics::counter!("tern_index_rebuilt_accounts_total").increment(count as u64);
        self.has_changes = true;

        if count < REBUILD_CHUNK {
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
        self.record_index();
        tracing::info!("index rebuild finished");
        Ok(())
    }

    fn apply_round(&mut self, round: Round) -> anyhow::Result<()> {
        for mut queued in round.jobs {
            let did = &queued.job.did;
            let seen = std::mem::take(&mut queued.job.seen);
            let fetched = round
                .fetched
                .iter()
                .filter_map(|fetched| fetched.facts.get(did).cloned());
            let facts = fetched.fold(seen, Facts::or);
            self.apply_facts(did, facts)?;
            self.finished.push(queued);
        }
        self.has_changes = true;
        Ok(())
    }

    fn apply_feed(&mut self, jobs: Vec<Queued<LabelJob>>) -> anyhow::Result<()> {
        for queued in jobs {
            self.apply_records(&queued.job.did, &queued.job.records)?;
            self.finished_feed.push(queued);
        }
        self.has_changes = true;
        Ok(())
    }

    fn apply_facts(&self, did: &Did, facts: Facts) -> anyhow::Result<()> {
        let before = self.document(did)?;
        self.save(did, facts)?;
        self.reindex(did, before)
    }

    fn apply_records(&self, did: &Did, records: &[LabelRecord]) -> anyhow::Result<()> {
        let before = self.document(did)?;
        self.store.store_label_records(did, records)?;
        self.reindex(did, before)
    }

    fn reindex(&self, did: &Did, before: Option<(Actor, u64)>) -> anyhow::Result<()> {
        let after = self.document(did)?;
        if before == after {
            return Ok(());
        }
        match after {
            Some((actor, score)) => self.index.upsert(&actor, score)?,
            None => self.index.remove(did),
        }
        Ok(())
    }

    fn save(&self, did: &Did, facts: Facts) -> anyhow::Result<()> {
        if facts.status == Some(Status::Deleted) {
            return Ok(self.store.blank_account(did)?);
        }
        let stored = self.store.account(did)?;
        let mut account = stored.clone().unwrap_or_default();
        if let Some(status) = facts.status {
            account.active = status == Status::Active;
        }
        // Live jobs run before background jobs, so if an account is deleted
        // just after it's scheduled for a weekly refresh, we would refresh
        // a deleted/inactive account. The Bluesky AppView is also cached
        // and can be behind, so it might return conflicting information.
        // This check protects us from accidentally reviving inactive accounts.
        if account.active {
            match facts.handle {
                Some(HandleFact::Resolved(handle)) => account.handle = Some(handle),
                Some(HandleFact::Invalid) => account.handle = None,
                None => {}
            }
            if let Some(profile) = facts.profile {
                account.profile = profile;
            }
            if let Some(verification) = facts.verification {
                account.verification = verification;
            }
            let enrichment = match facts.enrichment {
                Some(enrichment) => enrichment,
                None => self.store.enrichment(did)?,
            };
            self.store.put_enrichment(did, &enrichment, now())?;
        }
        if stored.as_ref() != Some(&account) {
            self.store.put_account(did, &account)?;
        }
        Ok(())
    }

    fn document(&self, did: &Did) -> anyhow::Result<Option<(Actor, u64)>> {
        let Some(account) = self.store.account(did)? else {
            return Ok(None);
        };
        let (enrichment, fetched_at) = match self.store.enrichment_row(did)? {
            Some(row) => (row.enrichment, row.fetched_at),
            None => (Enrichment::default(), now()),
        };
        let labels = self.store.labels(did)?;
        Ok(self.searchable(did, &account, &enrichment, labels, fetched_at))
    }

    fn searchable(
        &self,
        did: &Did,
        account: &Account,
        enrichment: &Enrichment,
        mut labels: Vec<Label>,
        now: i64,
    ) -> Option<(Actor, u64)> {
        let handle = account.searchable_handle()?;
        if let Some(self_labels) = &account.profile.self_labels {
            labels.extend(self_labels.labels(did));
        }
        let actor = Actor {
            did: did.clone(),
            handle: handle.clone(),
            display_name: account.profile.display_name.clone(),
            avatar_cid: account.profile.avatar_cid.clone(),
            labels,
            created_at: enrichment.created_at.clone(),
            verification: account.current_verification().cloned(),
        };
        Some((actor, score::reach(enrichment, now)))
    }

    fn record_index(&self) {
        metrics::gauge!("tern_indexed_accounts").set(self.index.num_docs() as f64);
        metrics::gauge!("tern_index_segments").set(self.index.segments() as f64);
        metrics::gauge!("tern_index_deleted_docs").set(self.index.deleted_docs() as f64);
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        if !self.has_changes {
            return Ok(());
        }
        let start = Instant::now();
        self.store.persist().context("failed to flush the store")?;
        self.index.commit().context("failed to commit the index")?;
        self.store.finish(&self.finished)?;
        self.store.finish(&self.finished_feed)?;
        self.finished.clear();
        self.finished_feed.clear();
        self.has_changes = false;
        metrics::histogram!("tern_commit_seconds").record(start.elapsed());
        self.record_index();
        Ok(())
    }
}

fn now() -> i64 {
    jiff::Timestamp::now().as_second()
}

#[cfg(test)]
mod tests {
    use store::Lane;
    use types::{
        AppviewVerification, Datetime, Handle, Job, Profile, SelfLabels, VerificationState,
    };

    use super::*;

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
        let writer = Writer::new(Arc::clone(&store), index);
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

    fn handle(name: &str) -> Facts {
        let handle = Handle::try_from(format!("{name}.test")).unwrap();
        Facts {
            handle: Some(HandleFact::Resolved(handle)),
            ..Facts::default()
        }
    }

    fn status(status: Status) -> Facts {
        Facts {
            status: Some(status),
            ..Facts::default()
        }
    }

    fn followers(followers: u64) -> Facts {
        Facts {
            enrichment: Some(Enrichment {
                followers,
                created_at: None,
            }),
            ..Facts::default()
        }
    }

    fn named(name: &str) -> Facts {
        Facts {
            profile: Some(Profile {
                display_name: Some(name.to_owned()),
                ..Profile::default()
            }),
            ..Facts::default()
        }
    }

    fn label(val: &str) -> Label {
        Label {
            src: did("labeler"),
            uri: "did:plc:a".to_owned(),
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

    fn answer(name: &str, facts: Facts) -> Fetched {
        Fetched {
            facts: [(did(name), facts)].into(),
            failed: Vec::new(),
        }
    }

    fn failure(name: &str) -> Fetched {
        Fetched {
            facts: [].into(),
            failed: vec![did(name)],
        }
    }

    impl Fixture {
        fn refresh(&mut self, name: &str, seen: Facts, fetched: Vec<Fetched>) {
            let job = Job {
                did: did(name),
                seen,
            };
            self.store.enqueue(Lane::Live, vec![job], None).unwrap();
            self.round(fetched);
        }

        fn round(&mut self, fetched: Vec<Fetched>) {
            let round = Round {
                jobs: self.store.next_jobs(10).unwrap(),
                fetched,
            };
            self.writer.apply_round(round).unwrap();
            self.writer.commit().unwrap();
        }

        fn feed(&mut self, name: &str, record: LabelRecord) {
            let job = LabelJob {
                did: did(name),
                records: vec![record],
            };
            self.store.enqueue_feed(vec![job], None).unwrap();
            let jobs = self.store.next_feed_jobs(10).unwrap();
            self.writer.apply_feed(jobs).unwrap();
            self.writer.commit().unwrap();
        }

        fn refreshed_at(&self, name: &str) -> i64 {
            let row = self.store.enrichment_row(&did(name)).unwrap();
            row.map_or(0, |row| row.fetched_at)
        }

        fn fetched(&mut self, name: &str, facts: Facts) {
            self.refresh(name, Facts::default(), vec![answer(name, facts)]);
        }

        fn seen(&mut self, name: &str, facts: Facts) {
            self.refresh(name, facts, Vec::new());
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

        fixture.fetched("a", handle("alice"));

        assert_eq!(fixture.found("ali"), ["alice.test"]);
    }

    #[test]
    fn an_account_without_a_handle_is_stored_but_not_searchable() {
        let mut fixture = fixture();

        fixture.fetched("a", named("Alice"));

        assert!(fixture.store.account(&did("a")).unwrap().is_some());
        assert!(fixture.found("ali").is_empty());
    }

    #[test]
    fn an_invalid_handle_clears_the_handle() {
        let mut fixture = fixture();
        fixture.fetched("a", handle("alice"));

        let invalid = Facts {
            handle: Some(HandleFact::Invalid),
            ..Facts::default()
        };
        fixture.fetched("a", invalid);

        assert!(fixture.found("ali").is_empty());
    }

    #[test]
    fn what_the_trigger_saw_wins_over_what_was_fetched() {
        let mut fixture = fixture();
        let fetched = handle("alice").or(named("Old Name"));

        fixture.refresh("a", named("New Name"), vec![answer("a", fetched)]);

        let found = fixture.reader.search("ali", 1).unwrap();
        assert_eq!(found[0].display_name.as_deref(), Some("New Name"));
    }

    #[test]
    fn deactivating_hides_an_account_and_reactivating_restores_it() {
        let mut fixture = fixture();
        fixture.fetched("a", handle("alice"));

        fixture.seen("a", status(Status::Inactive));
        assert!(fixture.found("ali").is_empty());

        fixture.seen("a", status(Status::Active));
        assert_eq!(fixture.found("ali"), ["alice.test"]);
    }

    #[test]
    fn a_deleted_account_is_blanked_and_a_later_job_does_not_bring_it_back() {
        let mut fixture = fixture();
        fixture.fetched("a", handle("alice"));

        fixture.seen("a", status(Status::Deleted));
        fixture.fetched("a", handle("alice"));

        let account = fixture.store.account(&did("a")).unwrap().unwrap();
        assert_eq!(account.handle, None);
        assert!(fixture.found("ali").is_empty());
    }

    #[test]
    fn more_followers_rank_an_account_higher() {
        let mut fixture = fixture();
        fixture.fetched("a", handle("alice"));
        fixture.fetched("b", handle("alicia"));

        fixture.fetched("b", followers(500));

        assert_eq!(fixture.found("ali"), ["alicia.test", "alice.test"]);
    }

    #[test]
    fn replaying_the_same_job_gives_the_same_result() {
        let mut fixture = fixture();
        for _ in 0..2 {
            fixture.fetched("a", handle("alice").or(followers(5)));
        }

        assert_eq!(fixture.found("ali"), ["alice.test"]);
    }

    #[test]
    fn a_job_leaves_the_queue_once_every_source_answered() {
        let mut fixture = fixture();

        fixture.fetched("a", handle("alice"));

        assert_eq!(fixture.store.queue_depth(Lane::Live), 0);
        assert!(fixture.refreshed_at("a") > 0);
    }

    #[test]
    fn a_job_with_a_failed_source_keeps_what_was_learned_and_still_ends() {
        let mut fixture = fixture();
        let fetched = vec![answer("a", handle("alice")), failure("a")];

        fixture.refresh("a", named("Alice"), fetched);

        assert_eq!(fixture.found("ali"), ["alice.test"]);
        assert_eq!(fixture.store.queue_depth(Lane::Live), 0);
        assert!(fixture.refreshed_at("a") > 0);
    }

    #[test]
    fn rebuild_restores_the_index_from_the_store() {
        let mut fixture = fixture();
        fixture.fetched("a", handle("alice"));

        let reader = fixture.reader.clone();
        fixture.writer.rebuild().unwrap();

        assert_eq!(reader.search("ali", 10).unwrap().len(), 1);
    }

    #[test]
    fn changes_made_during_a_rebuild_are_kept() {
        let mut fixture = fixture();
        fixture.fetched("a", handle("alice"));

        fixture.writer.start_rebuild().unwrap();
        fixture.seen("a", status(Status::Deleted));
        fixture.fetched("b", handle("bob"));
        fixture.writer.rebuild_step().unwrap();

        assert!(fixture.writer.rebuild.is_none());
        assert!(fixture.found("ali").is_empty());
        assert_eq!(fixture.found("bob"), ["bob.test"]);
    }

    #[test]
    fn a_refresh_rescores_an_account_that_only_got_older() {
        let mut fixture = fixture();
        let created = now() - 400 * 86_400;
        let created_at = jiff::Timestamp::from_second(created).unwrap();
        let enrichment = Enrichment {
            followers: 0,
            created_at: Some(Datetime::try_from(created_at.to_string()).unwrap()),
        };
        let aged = Facts {
            enrichment: Some(enrichment.clone()),
            ..Facts::default()
        };
        fixture.fetched("ann", handle("ann"));
        fixture
            .store
            .put_enrichment(&did("ann"), &enrichment, created)
            .unwrap();
        let anna = Actor {
            did: did("anna"),
            handle: Handle::try_from("anna.test".to_owned()).unwrap(),
            display_name: None,
            avatar_cid: None,
            labels: Vec::new(),
            created_at: None,
            verification: None,
        };
        fixture.writer.index.upsert(&anna, 50).unwrap();
        fixture.writer.index.commit().unwrap();
        assert_eq!(fixture.found("an"), ["anna.test", "ann.test"]);

        fixture.fetched("ann", aged);

        assert_eq!(fixture.found("an"), ["ann.test", "anna.test"]);
    }

    #[test]
    fn a_label_from_the_feed_shows_on_the_account_until_it_is_withdrawn() {
        let mut fixture = fixture();
        fixture.fetched("a", handle("a"));

        fixture.feed("a", issued(label("spam")));
        let found = fixture.reader.search("a.test", 1).unwrap();
        assert_eq!(found[0].labels, [label("spam")]);

        fixture.feed("a", withdrawn(label("spam")));
        let found = fixture.reader.search("a.test", 1).unwrap();
        assert!(found[0].labels.is_empty());
    }

    #[test]
    fn a_label_for_an_unknown_account_is_kept_without_creating_the_account() {
        let mut fixture = fixture();

        fixture.feed("a", issued(label("spam")));
        assert!(fixture.store.account(&did("a")).unwrap().is_none());

        fixture.fetched("a", handle("a"));
        let found = fixture.reader.search("a.test", 1).unwrap();
        assert_eq!(found[0].labels, [label("spam")]);
    }

    #[test]
    fn self_labels_on_the_profile_are_shown_after_the_labels_from_labelers() {
        let mut fixture = fixture();
        let self_labels = SelfLabels {
            values: vec!["porn".to_owned()],
            cid: None,
            created_at: None,
        };
        let profile = Facts {
            profile: Some(Profile {
                self_labels: Some(self_labels.clone()),
                ..Profile::default()
            }),
            ..handle("a")
        };
        fixture.feed("a", issued(label("spam")));

        fixture.fetched("a", profile);

        let found = fixture.reader.search("a.test", 1).unwrap();
        let shown = [vec![label("spam")], self_labels.labels(&did("a"))].concat();
        assert_eq!(found[0].labels, shown);
    }

    #[test]
    fn a_verified_account_with_a_filtered_handle_is_searchable() {
        let mut fixture = fixture();
        fixture.fetched("coons", handle("coons"));
        assert!(fixture.found("coons").is_empty());
        let state = VerificationState {
            verifications: Vec::new(),
            verified_status: "valid".to_owned(),
            trusted_verifier_status: "none".to_owned(),
        };
        let verification = AppviewVerification {
            handle: Handle::try_from("coons.test".to_owned()).ok(),
            display_name: None,
            state: state.clone(),
        };
        let verified = Facts {
            verification: Some(Some(verification)),
            ..Facts::default()
        };

        fixture.fetched("coons", verified);

        let found = fixture.reader.search("coons", 1).unwrap();
        assert_eq!(found[0].handle.as_str(), "coons.test");
        assert_eq!(found[0].verification, Some(state));
    }
}
