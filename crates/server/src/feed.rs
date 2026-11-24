use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use store::{Queued, Store};
use tokio::sync::mpsc;
use tokio::time::sleep;
use types::LabelJob;

const BATCH_SIZE: usize = 200;
const IDLE_PAUSE: Duration = Duration::from_millis(100);

pub struct Feed {
    pub store: Arc<Store>,
    pub writer: mpsc::Sender<Vec<Queued<LabelJob>>>,
}

impl Feed {
    pub async fn run(self) -> anyhow::Result<()> {
        loop {
            let jobs = self.store.next_feed_jobs(BATCH_SIZE)?;
            let depth = self.store.feed_depth();
            metrics::gauge!("tern_queue_depth", "lane" => "feed").set(depth as f64);
            if jobs.is_empty() {
                sleep(IDLE_PAUSE).await;
                continue;
            }

            let sent = self.writer.send(jobs).await;
            sent.context("the writer stopped")?;
        }
    }
}

#[cfg(test)]
mod tests {
    use types::Did;

    use super::*;

    #[tokio::test]
    async fn jobs_in_the_feed_lane_are_handed_to_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let (writer, mut inbox) = mpsc::channel(1);
        let job = LabelJob {
            did: Did::try_from("did:plc:a".to_owned()).unwrap(),
            records: Vec::new(),
        };
        store.enqueue_feed(vec![job.clone()], None).unwrap();

        tokio::spawn(Feed { store, writer }.run());

        let batch = inbox.recv().await.unwrap();
        assert_eq!(batch[0].job, job);
    }
}
