use std::sync::Arc;
use std::time::Duration;

use store::{BackfillProgress, Lane, Position, Store};
use tokio::time::sleep;
use types::{Did, Facts, Job};

use super::room_in_the_background_lane;

const MIN_FAILURE_DELAY: Duration = Duration::from_secs(1);
const MAX_FAILURE_DELAY: Duration = Duration::from_secs(300);

pub struct Backfill {
    pub hubble: hubble::Client,
    pub store: Arc<Store>,
}

impl Backfill {
    pub async fn run(self) -> anyhow::Result<()> {
        let progress = self.store.backfill_position()?;
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
            room_in_the_background_lane(&self.store).await;
            let page = self.list(cursor.as_deref()).await;
            let active = active_dids(page.repos);
            metrics::counter!("tern_backfill_listed").increment(active.len() as u64);

            let progress = BackfillProgress {
                cursor: page.cursor.clone(),
                finished: page.cursor.is_none(),
            };
            let position = Position::Backfill(progress);
            let jobs = self.jobs_for_unknown(active)?;
            self.store.enqueue(Lane::Background, jobs, Some(position))?;

            let Some(next) = page.cursor else {
                metrics::gauge!("tern_backfill_finished").set(1.0);
                tracing::info!("backfill finished");
                return Ok(());
            };
            cursor = Some(next);
        }
    }

    async fn list(&self, cursor: Option<&str>) -> hubble::Page {
        let mut delay = Duration::ZERO;
        loop {
            match self.hubble.list_repos(cursor).await {
                Ok(page) => return page,
                Err(err) => {
                    delay = (delay * 2).clamp(MIN_FAILURE_DELAY, MAX_FAILURE_DELAY);
                    let error = anyhow::Error::new(err);
                    tracing::warn!(?delay, "Hubble call failed: {error:#}");
                    sleep(delay).await;
                }
            }
        }
    }

    fn jobs_for_unknown(&self, dids: Vec<Did>) -> anyhow::Result<Vec<Job>> {
        let mut jobs = Vec::new();
        for did in dids {
            if !self.store.has_account(&did)? {
                let seen = Facts::default();
                jobs.push(Job { did, seen });
            }
        }
        Ok(jobs)
    }
}

fn active_dids(repos: Vec<hubble::Repo>) -> Vec<Did> {
    repos
        .into_iter()
        .filter(|repo| repo.active)
        .filter_map(|repo| Did::try_from(repo.did).ok())
        .collect()
}
