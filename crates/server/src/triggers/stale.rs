use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use jiff::{SignedDuration, Timestamp};
use store::{Lane, Position, Store};
use tokio::time::sleep;
use types::{Facts, Job};

use super::room_in_the_background_lane;

const SCAN_CHUNK: usize = 1000;
const SCAN_PAUSE: Duration = Duration::from_millis(100);

pub struct Stale {
    pub store: Arc<Store>,
    pub stale_after: SignedDuration,
}

impl Stale {
    pub async fn run(self) -> anyhow::Result<()> {
        let mut after = self.store.stale_position()?;
        loop {
            room_in_the_background_lane(&self.store).await;
            let cutoff = (Timestamp::now() - self.stale_after).as_second();
            let store = Arc::clone(&self.store);
            let scan = tokio::task::spawn_blocking(move || {
                store.scan_stale(after.as_ref(), cutoff, SCAN_CHUNK)
            })
            .await
            .context("the stale scan panicked")??;
            after = scan.last;

            let jobs = scan.stale.into_iter().map(|did| Job {
                did,
                seen: Facts::default(),
            });
            let position = Position::Stale(after.clone());
            self.store
                .enqueue(Lane::Background, jobs.collect(), Some(position))?;
            sleep(SCAN_PAUSE).await;
        }
    }
}
