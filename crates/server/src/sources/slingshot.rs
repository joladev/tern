use std::time::Instant;

use futures_util::future::BoxFuture;
use futures_util::stream::{self, StreamExt};
use slingshot::{Client, Error, Resolution};
use types::{Did, Facts, Fetched, HandleFact, Profile};

use super::Source;

pub struct SlingshotIdentity {
    pub client: Client,
    pub concurrency: usize,
}

impl Source for SlingshotIdentity {
    fn name(&self) -> &'static str {
        "slingshot_identity"
    }

    fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched> {
        let calls = dids.iter().map(|did| async move {
            let start = Instant::now();
            let handle = self.client.resolve_handle(did).await;
            metrics::histogram!("tern_handle_lookup_seconds").record(start.elapsed());
            metrics::counter!("tern_handle_lookups_total", "outcome" => outcome(&handle))
                .increment(1);
            (did, handle.map(handle_facts))
        });
        Box::pin(gather(calls.collect(), self.concurrency))
    }
}

pub struct SlingshotProfile {
    pub client: Client,
    pub concurrency: usize,
}

impl Source for SlingshotProfile {
    fn name(&self) -> &'static str {
        "slingshot_profile"
    }

    fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched> {
        let calls = dids.iter().map(|did| async move {
            let profile = self.client.profile(did).await;
            (did, profile.map(profile_facts))
        });
        Box::pin(gather(calls.collect(), self.concurrency))
    }
}

async fn gather<'a, Call>(calls: Vec<Call>, concurrency: usize) -> Fetched
where
    Call: Future<Output = (&'a Did, Result<Facts, Error>)>,
{
    let mut calls = stream::iter(calls).buffer_unordered(concurrency);
    let mut fetched = Fetched::default();
    while let Some((did, result)) = calls.next().await {
        match result {
            Ok(facts) => {
                fetched.facts.insert(did.clone(), facts);
            }
            Err(err) => {
                let error = &err as &dyn std::error::Error;
                tracing::warn!(%did, error, "Slingshot call failed");
                fetched.failed.push(did.clone());
            }
        }
    }
    fetched
}

fn outcome(result: &Result<Resolution, Error>) -> &'static str {
    match result {
        Ok(Resolution::Handle(_)) => "handle",
        Ok(Resolution::Invalid) => "invalid",
        Ok(Resolution::Unknown) => "unknown",
        Err(_) => "error",
    }
}

fn handle_facts(resolution: Resolution) -> Facts {
    let handle = match resolution {
        Resolution::Handle(handle) => Some(HandleFact::Resolved(handle)),
        Resolution::Invalid => Some(HandleFact::Invalid),
        Resolution::Unknown => None,
    };
    Facts {
        handle,
        ..Facts::default()
    }
}

fn profile_facts(profile: Profile) -> Facts {
    Facts {
        profile: Some(profile),
        ..Facts::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalid_handle_is_reported_as_invalid() {
        let facts = handle_facts(Resolution::Invalid);
        assert_eq!(facts.handle, Some(HandleFact::Invalid));
    }
}
