use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use futures_util::future::join_all;
use store::{Lane, Queued, Store};
use tokio::sync::mpsc;
use tokio::time::sleep;
use types::{Did, Fetched};

use crate::sources::Source;
use crate::writer::{Message, Round};

const ROUND_SIZE: usize = 200;
const IDLE_PAUSE: Duration = Duration::from_millis(100);

pub struct Refresh {
    pub store: Arc<Store>,
    pub sources: Vec<Box<dyn Source>>,
    pub writer: mpsc::Sender<Message>,
}

impl Refresh {
    pub async fn run(self) -> anyhow::Result<()> {
        loop {
            let jobs = self.store.next_jobs(ROUND_SIZE)?;
            self.record_depth();
            if jobs.is_empty() {
                sleep(IDLE_PAUSE).await;
                continue;
            }

            let round = self.round(jobs).await;
            let sent = self.writer.send(Message::Round(round)).await;
            sent.context("the writer stopped")?;
        }
    }

    async fn round(&self, jobs: Vec<Queued>) -> Round {
        let dids = distinct_dids(&jobs);
        let asked = self
            .sources
            .iter()
            .map(|source| ask(source.as_ref(), &dids));
        let fetched = join_all(asked).await;
        Round { jobs, fetched }
    }

    fn record_depth(&self) {
        for (lane, name) in [(Lane::Live, "live"), (Lane::Background, "background")] {
            let depth = self.store.queue_depth(lane);
            metrics::gauge!("tern_queue_depth", "lane" => name).set(depth as f64);
        }
    }
}

async fn ask(source: &dyn Source, dids: &[Did]) -> Fetched {
    let start = Instant::now();
    let fetched = source.fetch(dids).await;
    metrics::histogram!("tern_source_fetch_seconds", "source" => source.name())
        .record(start.elapsed());
    metrics::counter!("tern_source_failures_total", "source" => source.name())
        .increment(fetched.failed.len() as u64);
    fetched
}

fn distinct_dids(jobs: &[Queued]) -> Vec<Did> {
    let mut dids: Vec<Did> = jobs.iter().map(|queued| queued.job.did.clone()).collect();
    dids.sort();
    dids.dedup();
    dids
}

#[cfg(test)]
mod tests {
    use futures_util::future::BoxFuture;
    use types::{Facts, Job, Profile};

    use super::*;

    struct Naming(&'static str);

    impl Source for Naming {
        fn name(&self) -> &'static str {
            "naming"
        }

        fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched> {
            let facts = Facts {
                profile: Some(Profile {
                    display_name: Some(self.0.to_owned()),
                    avatar_cid: None,
                }),
                ..Facts::default()
            };
            let facts = dids.iter().map(|did| (did.clone(), facts.clone()));
            let fetched = Fetched {
                facts: facts.collect(),
                failed: Vec::new(),
            };
            Box::pin(async move { fetched })
        }
    }

    fn did(name: &str) -> Did {
        Did::try_from(format!("did:plc:{name}")).unwrap()
    }

    #[tokio::test]
    async fn a_round_asks_every_source_once_about_each_account() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let (writer, _inbox) = mpsc::channel(1);
        let refresh = Refresh {
            store: Arc::clone(&store),
            sources: vec![Box::new(Naming("first")), Box::new(Naming("second"))],
            writer,
        };
        let job = |name| Job {
            did: did(name),
            seen: Facts::default(),
        };
        let jobs = vec![job("a"), job("a"), job("b")];
        store.enqueue(Lane::Live, jobs, None).unwrap();

        let round = refresh.round(store.next_jobs(10).unwrap()).await;

        assert_eq!(round.jobs.len(), 3);
        assert_eq!(round.fetched.len(), 2);
        assert_eq!(round.fetched[0].facts.len(), 2);
    }
}
