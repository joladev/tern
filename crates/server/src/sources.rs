use futures_util::future::BoxFuture;
use types::{Did, Fetched};

pub trait Source: Send + Sync {
    fn name(&self) -> &'static str;
    fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched>;
}

pub struct SlingshotIdentity {
    pub client: slingshot::Client,
    pub concurrency: usize,
}

impl Source for SlingshotIdentity {
    fn name(&self) -> &'static str {
        "slingshot_identity"
    }

    fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched> {
        Box::pin(self.client.identities(dids, self.concurrency))
    }
}

pub struct SlingshotProfile {
    pub client: slingshot::Client,
    pub concurrency: usize,
}

impl Source for SlingshotProfile {
    fn name(&self) -> &'static str {
        "slingshot_profile"
    }

    fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched> {
        Box::pin(self.client.profiles(dids, self.concurrency))
    }
}

pub struct Appview {
    pub client: appview::Client,
    pub concurrency: usize,
}

impl Source for Appview {
    fn name(&self) -> &'static str {
        "appview"
    }

    fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched> {
        Box::pin(self.client.facts(dids, self.concurrency))
    }
}
