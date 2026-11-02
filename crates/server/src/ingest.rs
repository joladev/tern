use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;
use jetstream::{Event, EventKind, Subscription};
use jiff::Timestamp;
use slingshot::Resolution;
use store::Store;
use tokio::sync::mpsc::{self, error::TrySendError};
use types::Did;

use crate::lookup::{Lookup, Source};
use crate::writer::{Change, Message};

const MAX_EVENTS_IN_FLIGHT: usize = 16;
const LOOKUP_ATTEMPTS: u32 = 3;
const FIRST_RETRY_DELAY: Duration = Duration::from_secs(1);

pub trait HandleResolver {
    fn resolve(
        &self,
        did: &Did,
    ) -> impl Future<Output = Result<Resolution, slingshot::Error>> + Send;
}

impl HandleResolver for slingshot::Client {
    async fn resolve(&self, did: &Did) -> Result<Resolution, slingshot::Error> {
        self.resolve_handle(did).await
    }
}

pub struct Ingest<R> {
    pub subscription: Subscription,
    pub resolver: R,
    pub store: Arc<Store>,
    pub writer: mpsc::Sender<Message>,
    pub lookups: mpsc::Sender<Lookup>,
}

struct Changes {
    messages: Vec<Message>,
    new_account: Option<Did>,
}

impl<R: HandleResolver + Sync> Ingest<R> {
    pub async fn run(self) -> anyhow::Result<()> {
        let Ingest {
            mut subscription,
            resolver,
            store,
            writer,
            lookups,
        } = self;
        let mut in_flight = FuturesOrdered::new();
        loop {
            tokio::select! {
                event = subscription.next(),
                    if in_flight.len() < MAX_EVENTS_IN_FLIGHT =>
                {
                    let event = event?;
                    record_event(&event);
                    metrics::gauge!("tern_jetstream_queue_depth")
                        .set(subscription.queue_depth() as f64);
                    in_flight.push_back(changes_for(&resolver, &store, event));
                }
                Some(changes) = in_flight.next() => {
                    let changes = changes?;
                    for message in changes.messages {
                        writer.send(message).await.context("the writer stopped")?;
                    }
                    if let Some(did) = changes.new_account {
                        queue_lookup(&lookups, did)?;
                    }
                }
            }
        }
    }
}

fn queue_lookup(lookups: &mpsc::Sender<Lookup>, did: Did) -> anyhow::Result<()> {
    let lookup = Lookup {
        dids: vec![did],
        source: Source::NewAccount,
    };
    match lookups.try_send(lookup) {
        Ok(()) | Err(TrySendError::Full(_)) => Ok(()),
        Err(TrySendError::Closed(_)) => bail!("the lookup queue closed"),
    }
}

async fn changes_for<R: HandleResolver>(
    resolver: &R,
    store: &Store,
    event: Event,
) -> anyhow::Result<Changes> {
    let Event { seq, did, kind, .. } = event;
    let mut changes = Vec::with_capacity(2);

    let is_new = !matches!(kind, EventKind::Deleted) && !store.has_account(&did)?;

    if needs_handle(store, &did, &kind)? {
        match resolve_handle(resolver, &did).await {
            Resolution::Handle(handle) => {
                changes.push(Change::Handle(Some(handle)));
            }
            Resolution::Invalid => changes.push(Change::Handle(None)),
            Resolution::Unknown => {}
        }
    }

    match kind {
        EventKind::Identity => {}
        EventKind::Profile(profile) => changes.push(Change::Profile(profile)),
        EventKind::Status { active } => changes.push(Change::Status { active }),
        EventKind::Deleted => changes.push(Change::Deleted),
    }

    let messages = changes.into_iter().map(|change| Message::Change {
        seq,
        did: did.clone(),
        change,
    });
    Ok(Changes {
        messages: messages.collect(),
        new_account: is_new.then_some(did),
    })
}

fn needs_handle(store: &Store, did: &Did, kind: &EventKind) -> anyhow::Result<bool> {
    Ok(match kind {
        EventKind::Identity => true,
        EventKind::Deleted => false,
        EventKind::Profile(_) | EventKind::Status { .. } => {
            let account = store.account(did)?;
            account.and_then(|account| account.handle).is_none()
        }
    })
}

pub(crate) async fn resolve_handle<R: HandleResolver>(resolver: &R, did: &Did) -> Resolution {
    let mut delay = FIRST_RETRY_DELAY;
    for attempt in 1..=LOOKUP_ATTEMPTS {
        let start = Instant::now();
        let result = resolver.resolve(did).await;
        metrics::histogram!("tern_handle_lookup_seconds").record(start.elapsed());
        metrics::counter!(
            "tern_handle_lookups_total",
            "outcome" => lookup_outcome(&result),
        )
        .increment(1);

        match result {
            Ok(resolution) => return resolution,
            Err(err) => {
                let error = anyhow::Error::new(err);
                tracing::warn!(%did, attempt, "handle lookup failed: {error:#}");
            }
        }
        if attempt < LOOKUP_ATTEMPTS {
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
    }

    metrics::counter!("tern_handle_lookups_abandoned_total").increment(1);
    tracing::warn!(%did, "giving up on the handle lookup");
    Resolution::Unknown
}

fn record_event(event: &Event) {
    let kind = match event.kind {
        EventKind::Identity => "identity",
        EventKind::Profile(_) => "profile",
        EventKind::Status { .. } => "status",
        EventKind::Deleted => "deleted",
    };
    metrics::counter!("tern_events_total", "kind" => kind).increment(1);
    metrics::gauge!("tern_jetstream_cursor").set(event.seq as f64);
    if let Some(time) = event.time {
        let lag = Timestamp::now().duration_since(time).as_secs_f64();
        metrics::gauge!("tern_jetstream_lag_seconds").set(lag);
    }
}

fn lookup_outcome(result: &Result<Resolution, slingshot::Error>) -> &'static str {
    match result {
        Ok(Resolution::Handle(_)) => "handle",
        Ok(Resolution::Invalid) => "invalid",
        Ok(Resolution::Unknown) => "unknown",
        Err(_) => "error",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use reqwest::StatusCode;
    use types::{Account, Handle, Profile};

    use super::*;

    struct FakeResolver {
        answer: Option<Resolution>,
        attempts: AtomicU32,
    }

    impl FakeResolver {
        fn answering(answer: Resolution) -> FakeResolver {
            FakeResolver {
                answer: Some(answer),
                attempts: AtomicU32::new(0),
            }
        }

        fn failing() -> FakeResolver {
            FakeResolver {
                answer: None,
                attempts: AtomicU32::new(0),
            }
        }
    }

    impl HandleResolver for FakeResolver {
        async fn resolve(&self, _did: &Did) -> Result<Resolution, slingshot::Error> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            let error = slingshot::Error::Status(StatusCode::GATEWAY_TIMEOUT);
            self.answer.clone().ok_or(error)
        }
    }

    fn open_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn did() -> Did {
        Did::try_from("did:plc:alice".to_owned()).unwrap()
    }

    fn handle() -> Handle {
        Handle::try_from("alice.test".to_owned()).unwrap()
    }

    async fn changes(resolver: &FakeResolver, store: &Store, kind: EventKind) -> Vec<&'static str> {
        let event = Event {
            seq: 1,
            did: did(),
            kind,
            time: None,
        };
        let changes = changes_for(resolver, store, event).await.unwrap();
        changes
            .messages
            .into_iter()
            .map(|message| match message {
                Message::Change {
                    change: Change::Handle(_),
                    ..
                } => "handle",
                Message::Change {
                    change: Change::Profile(_),
                    ..
                } => "profile",
                Message::Change {
                    change: Change::Status { .. },
                    ..
                } => "status",
                Message::Change {
                    change: Change::Deleted,
                    ..
                } => "deleted",
                _ => "other",
            })
            .collect()
    }

    #[tokio::test]
    async fn identity_events_always_look_up_the_handle() {
        let (_dir, store) = open_store();
        let resolver = FakeResolver::answering(Resolution::Handle(handle()));
        let found = changes(&resolver, &store, EventKind::Identity).await;
        assert_eq!(found, ["handle"]);
    }

    #[tokio::test]
    async fn a_profile_for_an_unknown_account_looks_up_the_handle_first() {
        let (_dir, store) = open_store();
        let resolver = FakeResolver::answering(Resolution::Handle(handle()));
        let kind = EventKind::Profile(Profile::default());
        assert_eq!(
            changes(&resolver, &store, kind).await,
            ["handle", "profile"]
        );
    }

    #[tokio::test]
    async fn a_profile_for_a_known_account_needs_no_lookup() {
        let (_dir, store) = open_store();
        let account = Account {
            handle: Some(handle()),
            ..Account::default()
        };
        store.put_account(&did(), &account).unwrap();
        let resolver = FakeResolver::answering(Resolution::Handle(handle()));

        let kind = EventKind::Profile(Profile::default());
        assert_eq!(changes(&resolver, &store, kind).await, ["profile"]);
    }

    async fn new_account(store: &Store, kind: EventKind) -> Option<Did> {
        let resolver = FakeResolver::answering(Resolution::Handle(handle()));
        let event = Event {
            seq: 1,
            did: did(),
            kind,
            time: None,
        };
        changes_for(&resolver, store, event)
            .await
            .unwrap()
            .new_account
    }

    #[tokio::test]
    async fn the_first_event_for_an_account_queues_a_lookup() {
        let (_dir, store) = open_store();
        assert_eq!(new_account(&store, EventKind::Identity).await, Some(did()));
    }

    #[tokio::test]
    async fn events_for_known_or_deleted_accounts_queue_none() {
        let (_dir, store) = open_store();
        assert_eq!(new_account(&store, EventKind::Deleted).await, None);

        store.put_account(&did(), &Account::default()).unwrap();
        assert_eq!(new_account(&store, EventKind::Identity).await, None);
    }

    #[tokio::test]
    async fn an_unknown_lookup_result_leaves_the_account_alone() {
        let (_dir, store) = open_store();
        let resolver = FakeResolver::answering(Resolution::Unknown);
        let found = changes(&resolver, &store, EventKind::Identity).await;
        assert!(found.is_empty());
    }

    #[tokio::test]
    async fn an_invalid_handle_clears_the_handle() {
        let (_dir, store) = open_store();
        let resolver = FakeResolver::answering(Resolution::Invalid);
        let found = changes(&resolver, &store, EventKind::Identity).await;
        assert_eq!(found, ["handle"]);
    }

    #[tokio::test]
    async fn deleted_accounts_are_not_looked_up() {
        let (_dir, store) = open_store();
        let resolver = FakeResolver::failing();
        let found = changes(&resolver, &store, EventKind::Deleted).await;
        assert_eq!(found, ["deleted"]);
        assert_eq!(resolver.attempts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_lookup_gives_up_and_the_event_still_applies() {
        let (_dir, store) = open_store();
        let resolver = FakeResolver::failing();
        let kind = EventKind::Status { active: true };

        let found = changes(&resolver, &store, kind).await;

        assert_eq!(found, ["status"]);
        assert_eq!(resolver.attempts.load(Ordering::Relaxed), LOOKUP_ATTEMPTS);
    }
}
