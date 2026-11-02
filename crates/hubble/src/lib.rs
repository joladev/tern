use reqwest::{Client as HttpClient, StatusCode};
use serde::Deserialize;

const LIST_REPOS_PATH: &str = "/xrpc/com.atproto.sync.listRepos";
const GET_STATS_PATH: &str = "/xrpc/blue.microcosm.hubble.getStats";
const PAGE_SIZE: &str = "1000";

#[derive(Clone)]
pub struct Client {
    http: HttpClient,
    base_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to reach Hubble")]
    Request(#[source] reqwest::Error),
    #[error("Hubble returned status {0}")]
    Status(StatusCode),
    #[error("failed to read Hubble's response")]
    Body(#[source] reqwest::Error),
}

#[derive(Debug, Deserialize)]
pub struct Repo {
    pub did: String,
    pub active: bool,
}

#[derive(Debug, Deserialize)]
pub struct Page {
    pub repos: Vec<Repo>,
    pub cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Stats {
    repos_by_account_status: AccountStatuses,
}

#[derive(Deserialize)]
struct AccountStatuses {
    active: u64,
}

impl Client {
    pub fn new(http: HttpClient, base_url: String) -> Client {
        Client { http, base_url }
    }

    pub async fn list_repos(&self, cursor: Option<&str>) -> Result<Page, Error> {
        let mut query = vec![("limit", PAGE_SIZE)];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }
        self.get(LIST_REPOS_PATH, &query).await
    }

    pub async fn active_total(&self) -> Result<u64, Error> {
        let stats: Stats = self.get(GET_STATS_PATH, &[]).await?;
        Ok(stats.repos_by_account_status.active)
    }

    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, Error> {
        let response = self
            .http
            .get(format!("{}{path}", self.base_url))
            .query(query)
            .send()
            .await
            .map_err(Error::Request)?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Status(status));
        }
        response.json().await.map_err(Error::Body)
    }
}
