pub mod appview;
pub mod slingshot;

use futures_util::future::BoxFuture;
use types::{Did, Fetched};

pub trait Source: Send + Sync {
    fn name(&self) -> &'static str;
    fn fetch<'a>(&'a self, dids: &'a [Did]) -> BoxFuture<'a, Fetched>;
}
