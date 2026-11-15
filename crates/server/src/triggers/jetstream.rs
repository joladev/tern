use std::sync::Arc;

use jetstream::{Event, EventKind, Subscription};
use jiff::Timestamp;
use store::{Lane, Position, Store};
use types::{Facts, Job, Status};

pub struct Jetstream {
    pub subscription: Subscription,
    pub store: Arc<Store>,
}

impl Jetstream {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let event = self.subscription.next().await?;
            record_event(&event);
            metrics::gauge!("tern_jetstream_queue_depth")
                .set(self.subscription.queue_depth() as f64);

            let position = Position::Jetstream(event.seq);
            let job = Job {
                did: event.did,
                seen: seen(event.kind),
            };
            self.store.enqueue(Lane::Live, vec![job], Some(position))?;
        }
    }
}

fn seen(kind: EventKind) -> Facts {
    let mut facts = Facts::default();
    match kind {
        EventKind::Identity => {}
        EventKind::Profile(profile) => facts.profile = Some(profile),
        EventKind::Status { active: true } => facts.status = Some(Status::Active),
        EventKind::Status { active: false } => facts.status = Some(Status::Inactive),
        EventKind::Deleted => facts.status = Some(Status::Deleted),
    }
    facts
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

#[cfg(test)]
mod tests {
    use types::Profile;

    use super::*;

    #[test]
    fn a_job_carries_what_its_event_says() {
        let profile = Profile {
            display_name: Some("Alice".to_owned()),
            avatar_cid: None,
        };

        assert_eq!(seen(EventKind::Identity), Facts::default());
        assert_eq!(
            seen(EventKind::Profile(profile.clone())).profile,
            Some(profile)
        );
        assert_eq!(
            seen(EventKind::Status { active: false }).status,
            Some(Status::Inactive)
        );
        assert_eq!(seen(EventKind::Deleted).status, Some(Status::Deleted));
    }
}
