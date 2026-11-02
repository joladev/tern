use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use appview::MAX_ACCOUNTS_PER_CALL;
use jiff::{SignedDuration, Timestamp};
use store::Store;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::lookup::{Lookup, Source};

const SCAN_CHUNK: usize = 1000;
const SCAN_PAUSE: Duration = Duration::from_millis(100);

pub struct Refresh {
    pub store: Arc<Store>,
    pub queue: mpsc::Sender<Lookup>,
    pub stale_after: SignedDuration,
}

impl Refresh {
    pub async fn run(self) -> anyhow::Result<()> {
        let mut after = None;
        loop {
            let cutoff = (Timestamp::now() - self.stale_after).as_second();
            let store = Arc::clone(&self.store);
            let scan = tokio::task::spawn_blocking(move || {
                store.scan_stale(after.as_ref(), cutoff, SCAN_CHUNK)
            })
            .await
            .context("the stale scan panicked")??;
            after = scan.last;
            for dids in scan.stale.chunks(MAX_ACCOUNTS_PER_CALL) {
                let lookup = Lookup {
                    dids: dids.to_vec(),
                    source: Source::Refresh,
                };
                let sent = self.queue.send(lookup).await;
                sent.context("the lookup queue closed")?;
            }
            sleep(SCAN_PAUSE).await;
        }
    }
}
