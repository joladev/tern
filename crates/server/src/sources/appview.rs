use std::collections::HashMap;

use appview::{Client, MAX_ACCOUNTS_PER_CALL, ProfileView};
use futures_util::future::BoxFuture;
use futures_util::stream::{self, StreamExt};
use types::{Did, Enrichment, Facts, Fetched};

use super::Source;

pub struct Appview {
    pub client: Client,
    pub concurrency: usize,
}

impl Source for Appview {
    fn name(&self) -> &'static str {
        "appview"
    }

    fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched> {
        Box::pin(self.facts(dids))
    }
}

impl Appview {
    async fn facts(&self, dids: &[Did]) -> Fetched {
        let calls: Vec<_> = dids
            .chunks(MAX_ACCOUNTS_PER_CALL)
            .map(|dids| async move { (dids, self.client.profiles(dids).await) })
            .collect();
        let mut calls = stream::iter(calls).buffer_unordered(self.concurrency);
        let mut fetched = Fetched::default();
        while let Some((dids, result)) = calls.next().await {
            let outcome = if result.is_ok() { "success" } else { "failure" };
            metrics::counter!("tern_enrichment_calls_total", "outcome" => outcome).increment(1);
            match result {
                Ok(views) => fetched.facts.extend(facts_for(dids, views)),
                Err(err) => {
                    let error = &err as &dyn std::error::Error;
                    tracing::warn!(error, "AppView call failed");
                    fetched.failed.extend_from_slice(dids);
                }
            }
        }
        fetched
    }
}

fn facts_for(dids: &[Did], views: Vec<ProfileView>) -> HashMap<Did, Facts> {
    let mut views: HashMap<_, _> = views
        .into_iter()
        .map(|view| (view.did.clone(), view))
        .collect();
    dids.iter()
        .map(|did| {
            let facts = match views.remove(did) {
                Some(view) => Facts {
                    enrichment: Some(view.enrichment),
                    verification: Some(view.verification),
                    ..Facts::default()
                },
                None => Facts {
                    enrichment: Some(Enrichment::default()),
                    ..Facts::default()
                },
            };
            (did.clone(), facts)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use types::Profile;

    use super::*;

    #[test]
    fn an_account_the_appview_leaves_out_gets_empty_enrichment() {
        let did = |name: &str| Did::try_from(format!("did:plc:{name}")).unwrap();
        let known = ProfileView {
            did: did("known"),
            handle: None,
            profile: Profile::default(),
            enrichment: Enrichment {
                followers: 5,
                created_at: None,
            },
            verification: None,
        };

        let facts = facts_for(&[did("known"), did("unknown")], vec![known.clone()]);

        let known_facts = Facts {
            enrichment: Some(known.enrichment),
            verification: Some(None),
            ..Facts::default()
        };
        let unknown_facts = Facts {
            enrichment: Some(Enrichment::default()),
            ..Facts::default()
        };
        assert_eq!(facts[&did("known")], known_facts);
        assert_eq!(facts[&did("unknown")], unknown_facts);
    }
}
