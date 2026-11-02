use std::time::Duration;

use anyhow::Context as _;
use appview::ProfileView;
use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;
use store::BackfillProgress;
use tokio::sync::mpsc;
use tokio::time::sleep;
use types::Did;

use crate::writer::Message;

const MIN_FAILURE_DELAY: Duration = Duration::from_secs(1);
const MAX_FAILURE_DELAY: Duration = Duration::from_secs(300);

pub struct Lookup {
    pub dids: Vec<Did>,
    pub source: Source,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    Backfill {
        progress: BackfillProgress,
        listed: u64,
    },
    Refresh,
    NewAccount,
}

impl Source {
    pub fn may_create(&self) -> bool {
        matches!(self, Source::Backfill { .. })
    }

    fn label(&self) -> &'static str {
        match self {
            Source::Backfill { .. } => "backfill",
            Source::Refresh => "refresh",
            Source::NewAccount => "new_account",
        }
    }
}

pub struct Lookups {
    pub appview: appview::Client,
    pub queue: mpsc::Receiver<Lookup>,
    pub writer: mpsc::Sender<Message>,
    pub concurrency: usize,
}

impl Lookups {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut calls = FuturesOrdered::new();
        loop {
            tokio::select! {
                lookup = self.queue.recv(), if calls.len() < self.concurrency => {
                    let Some(lookup) = lookup else { break };
                    calls.push_back(look_up(self.appview.clone(), lookup));
                }
                Some(message) = calls.next() => {
                    let sent = self.writer.send(message).await;
                    sent.context("the writer stopped")?;
                }
            }
        }
        while let Some(message) = calls.next().await {
            self.writer
                .send(message)
                .await
                .context("the writer stopped")?;
        }
        Ok(())
    }
}

async fn look_up(appview: appview::Client, lookup: Lookup) -> Message {
    let views = if lookup.dids.is_empty() {
        Vec::new()
    } else {
        profiles(&appview, &lookup.dids, lookup.source.label()).await
    };
    let accounts = lookup
        .dids
        .into_iter()
        .map(|did| {
            let view = views.iter().find(|view| view.did == did).cloned();
            (did, view)
        })
        .collect();
    Message::LookedUp {
        accounts,
        source: lookup.source,
    }
}

async fn profiles(
    appview: &appview::Client,
    dids: &[Did],
    source: &'static str,
) -> Vec<ProfileView> {
    let mut delay = Duration::ZERO;
    loop {
        match appview.profiles(dids).await {
            Ok(views) => {
                record_call(source, "success");
                return views;
            }
            Err(err) => {
                record_call(source, "failure");
                delay = backoff(delay);
                let error = anyhow::Error::new(err);
                tracing::warn!(?delay, "AppView lookup failed: {error:#}");
                sleep(delay).await;
            }
        }
    }
}

fn record_call(source: &'static str, outcome: &'static str) {
    metrics::counter!(
        "tern_enrichment_calls_total",
        "source" => source,
        "outcome" => outcome,
    )
    .increment(1);
}

pub(crate) fn backoff(delay: Duration) -> Duration {
    (delay * 2).clamp(MIN_FAILURE_DELAY, MAX_FAILURE_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_failure_waits_at_least_the_minimum() {
        assert_eq!(backoff(Duration::from_millis(10)), MIN_FAILURE_DELAY);
    }

    #[test]
    fn repeated_failures_double_the_delay() {
        assert_eq!(backoff(Duration::from_secs(4)), Duration::from_secs(8));
    }

    #[test]
    fn the_delay_stops_growing_at_the_maximum() {
        assert_eq!(backoff(MAX_FAILURE_DELAY), MAX_FAILURE_DELAY);
    }
}
