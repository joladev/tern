use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use appview::MAX_ACCOUNTS_PER_CALL;
use store::{BackfillProgress, Store};
use tokio::sync::mpsc;
use tokio::time::sleep;
use types::Did;

use crate::lookup::{Lookup, Source, backoff};

pub struct Backfill {
    pub hubble: hubble::Client,
    pub store: Arc<Store>,
    pub queue: mpsc::Sender<Lookup>,
}

impl Backfill {
    pub async fn run(self) -> anyhow::Result<()> {
        let progress = self.store.backfill_state()?.progress;
        if progress.finished {
            return Ok(());
        }
        match self.hubble.active_total().await {
            Ok(total) => {
                metrics::gauge!("tern_backfill_source_total").set(total as f64);
            }
            Err(err) => {
                let error = anyhow::Error::new(err);
                tracing::warn!("failed to read Hubble's total: {error:#}");
            }
        }

        tracing::info!(cursor = ?progress.cursor, "backfill started");
        let mut cursor = progress.cursor;
        loop {
            let page = self.list(cursor.as_deref()).await;
            let active = active_dids(page.repos);
            let listed = active.len() as u64;
            let dids = self.unknown(active)?;
            let here = BackfillProgress {
                cursor,
                finished: false,
            };
            let next = BackfillProgress {
                cursor: page.cursor.clone(),
                finished: page.cursor.is_none(),
            };
            for lookup in lookups(dids, listed, here, next) {
                let sent = self.queue.send(lookup).await;
                sent.context("the lookup queue closed")?;
            }
            let Some(next_cursor) = page.cursor else {
                return Ok(());
            };
            cursor = Some(next_cursor);
        }
    }

    async fn list(&self, cursor: Option<&str>) -> hubble::Page {
        let mut delay = Duration::ZERO;
        loop {
            match self.hubble.list_repos(cursor).await {
                Ok(page) => return page,
                Err(err) => {
                    delay = backoff(delay);
                    let error = anyhow::Error::new(err);
                    tracing::warn!(?delay, "Hubble call failed: {error:#}");
                    sleep(delay).await;
                }
            }
        }
    }

    fn unknown(&self, dids: Vec<Did>) -> anyhow::Result<Vec<Did>> {
        let mut unknown = Vec::new();
        for did in dids {
            if !self.store.has_account(&did)? {
                unknown.push(did);
            }
        }
        Ok(unknown)
    }
}

fn active_dids(repos: Vec<hubble::Repo>) -> Vec<Did> {
    repos
        .into_iter()
        .filter(|repo| repo.active)
        .filter_map(|repo| Did::try_from(repo.did).ok())
        .collect()
}

fn lookups(
    dids: Vec<Did>,
    listed: u64,
    here: BackfillProgress,
    next: BackfillProgress,
) -> Vec<Lookup> {
    let mut chunks: Vec<Vec<Did>> = dids
        .chunks(MAX_ACCOUNTS_PER_CALL)
        .map(<[Did]>::to_vec)
        .collect();
    if chunks.is_empty() {
        chunks.push(Vec::new());
    }
    let last = chunks.len() - 1;
    chunks
        .into_iter()
        .enumerate()
        .map(|(i, dids)| {
            let source = if i == last {
                Source::Backfill {
                    progress: next.clone(),
                    listed,
                }
            } else {
                Source::Backfill {
                    progress: here.clone(),
                    listed: 0,
                }
            };
            Lookup { dids, source }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backfill(cursor: &str, listed: u64) -> Source {
        let progress = BackfillProgress {
            cursor: Some(cursor.to_owned()),
            finished: false,
        };
        Source::Backfill { progress, listed }
    }

    fn progress(cursor: &str) -> BackfillProgress {
        BackfillProgress {
            cursor: Some(cursor.to_owned()),
            finished: false,
        }
    }

    #[test]
    fn a_page_resumes_at_its_start_until_its_last_lookup() {
        let dids = (0..60)
            .map(|i| Did::try_from(format!("did:plc:{i:02}")).unwrap())
            .collect();

        let lookups = lookups(dids, 70, progress("here"), progress("next"));

        let sizes: Vec<_> = lookups.iter().map(|l| l.dids.len()).collect();
        assert_eq!(sizes, [25, 25, 10]);
        let sources: Vec<_> = lookups.into_iter().map(|l| l.source).collect();
        let here = backfill("here", 0);
        assert_eq!(sources, [here.clone(), here, backfill("next", 70)]);
    }

    #[test]
    fn a_page_with_nothing_to_look_up_still_saves_its_position() {
        let lookups = lookups(Vec::new(), 3, progress("here"), progress("next"));

        assert_eq!(lookups.len(), 1);
        assert!(lookups[0].dids.is_empty());
        assert_eq!(lookups[0].source, backfill("next", 3));
    }
}
