use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jiff::Timestamp;
use labeler::{Labels, Subscription};
use store::{Position, Store};
use types::{Did, LabelJob, LabelRecord};

use super::room_in_the_feed_lane;

const POSITION_INTERVAL: Duration = Duration::from_secs(1);

pub struct Labeler {
    pub labeler: Did,
    pub subscription: Subscription,
    pub store: Arc<Store>,
}

impl Labeler {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut position_saved = Instant::now();
        loop {
            let labels = self.subscription.next().await?;
            self.record(&labels);

            let seq = labels.seq;
            let jobs = jobs(&self.labeler, labels.records);
            // Periodically update the cursor even if there are no
            // new labels we care about, we skip most labels.
            if jobs.is_empty() && position_saved.elapsed() < POSITION_INTERVAL {
                continue;
            }
            room_in_the_feed_lane(&self.store).await;
            let position = Position::Labeler {
                labeler: self.labeler.clone(),
                seq,
            };
            self.store.enqueue_feed(jobs, Some(position))?;
            position_saved = Instant::now();
        }
    }

    fn record(&self, labels: &Labels) {
        let labeler = self.labeler.to_string();
        metrics::gauge!("tern_labeler_cursor", "labeler" => labeler.clone()).set(labels.seq as f64);
        if let Some(record) = labels.records.last() {
            let issued = record.label.cts.timestamp();
            let lag = Timestamp::now().duration_since(issued).as_secs_f64();
            metrics::gauge!("tern_labeler_lag_seconds", "labeler" => labeler).set(lag);
        }
    }
}

fn jobs(labeler: &Did, records: Vec<LabelRecord>) -> Vec<LabelJob> {
    let mut by_account: BTreeMap<Did, Vec<LabelRecord>> = BTreeMap::new();
    for record in records {
        if record.label.src != *labeler {
            continue;
        }
        if let Some(did) = record.label.account() {
            by_account.entry(did).or_default().push(record);
        }
    }
    by_account
        .into_iter()
        .map(|(did, records)| LabelJob { did, records })
        .collect()
}

#[cfg(test)]
mod tests {
    use types::{Datetime, Label};

    use super::*;

    fn did(name: &str) -> Did {
        Did::try_from(format!("did:plc:{name}")).unwrap()
    }

    fn record(src: &str, uri: &str) -> LabelRecord {
        LabelRecord {
            label: Label {
                src: did(src),
                uri: uri.to_owned(),
                cid: None,
                val: "spam".to_owned(),
                cts: Datetime::try_from("2026-01-01T00:00:00Z".to_owned()).unwrap(),
                exp: None,
            },
            withdrawn: false,
        }
    }

    #[test]
    fn labels_on_accounts_and_profiles_become_one_job_per_account() {
        let on_account = record("labeler", "did:plc:alice");
        let on_profile = record("labeler", "at://did:plc:alice/app.bsky.actor.profile/self");
        let on_post = record("labeler", "at://did:plc:alice/app.bsky.feed.post/3k");
        let from_another_issuer = record("other", "did:plc:bob");
        let records = vec![
            on_account.clone(),
            on_post,
            on_profile.clone(),
            from_another_issuer,
        ];

        let jobs = jobs(&did("labeler"), records);

        let expected = LabelJob {
            did: did("alice"),
            records: vec![on_account, on_profile],
        };
        assert_eq!(jobs, [expected]);
    }
}
