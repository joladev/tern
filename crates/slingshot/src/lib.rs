use reqwest::{Client as HttpClient, StatusCode};
use serde::Deserialize;
use types::{Did, Handle};

const RESOLVE_PATH: &str = "/xrpc/com.bad-example.identity.resolveMiniDoc";

#[derive(Clone)]
pub struct Client {
    http: HttpClient,
    base_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to reach Slingshot")]
    Request(#[source] reqwest::Error),
    #[error("Slingshot returned status {0}")]
    Status(StatusCode),
    #[error("failed to read Slingshot's response")]
    Body(#[source] reqwest::Error),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Resolution {
    Handle(Handle),
    Invalid,
    Unknown,
}

#[derive(Deserialize)]
struct MiniDoc {
    handle: String,
}

impl Client {
    pub fn new(http: HttpClient, base_url: String) -> Client {
        Client { http, base_url }
    }

    pub async fn resolve_handle(&self, did: &Did) -> Result<Resolution, Error> {
        let response = self
            .http
            .get(format!("{}{RESOLVE_PATH}", self.base_url))
            .query(&[("identifier", did.as_str())])
            .send()
            .await
            .map_err(Error::Request)?;

        match response.status() {
            StatusCode::OK => {}
            StatusCode::BAD_REQUEST => return Ok(Resolution::Unknown),
            status => return Err(Error::Status(status)),
        }

        let doc: MiniDoc = response.json().await.map_err(Error::Body)?;
        Ok(Handle::try_from(doc.handle).map_or(Resolution::Invalid, Resolution::Handle))
    }
}
