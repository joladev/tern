use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::{Value, json};

const SERVICE_URL: &str = "http://127.0.0.1:13000";
const MOCK_URL: &str = "http://127.0.0.1:13001";
const METRICS_URL: &str = "http://127.0.0.1:13002";
const SEARCH_PATH: &str = "/xrpc/town.cove.actor.searchActorsTypeahead";
const BLUESKY_SEARCH_PATH: &str = "/xrpc/app.bsky.actor.searchActorsTypeahead";
const POLL_INTERVAL: Duration = Duration::from_millis(50);

pub const CHANGE_TIMEOUT: Duration = Duration::from_secs(5);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    pub did: String,
    pub handle: String,
    pub display_name: Option<String>,
    pub avatar: Option<String>,
}

#[derive(Deserialize)]
struct SearchResponse {
    actors: Vec<Actor>,
}

pub struct Harness {
    client: Client,
}

impl Harness {
    pub fn connect() -> Harness {
        let harness = Harness {
            client: Client::new(),
        };
        harness.wait_until_healthy();
        harness
    }

    pub fn wait_until_healthy(&self) {
        let url = format!("{SERVICE_URL}/xrpc/_health");
        let healthy = || self.client.get(&url).send().is_ok();
        assert!(
            wait_for(STARTUP_TIMEOUT, healthy),
            "the service never started"
        );
    }

    pub fn search(&self, query: &str) -> Vec<Actor> {
        let response = self.request(query, "10");
        assert_eq!(response.status(), StatusCode::OK);
        let body: SearchResponse = response.json().unwrap();
        body.actors
    }

    pub fn search_as_bluesky(&self, query: &str) -> Vec<Actor> {
        let response = self
            .client
            .get(format!("{SERVICE_URL}{BLUESKY_SEARCH_PATH}"))
            .query(&[("q", query)])
            .send()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: SearchResponse = response.json().unwrap();
        body.actors
    }

    pub fn stats(&self) -> Value {
        let url = format!("{SERVICE_URL}/xrpc/town.cove.actor.getStats");
        let response = self.client.get(url).send().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.json().unwrap()
    }

    pub fn search_with_limit(&self, limit: &str) -> (StatusCode, Value) {
        let response = self.request("a", limit);
        (response.status(), response.json().unwrap())
    }

    pub fn allowed_origin(&self) -> Option<String> {
        let response = self.request_with_origin("https://example.test");
        let origin = response.headers().get("access-control-allow-origin")?;
        Some(origin.to_str().unwrap().to_owned())
    }

    pub fn preflight_allowed_headers(&self, header: &str) -> Option<String> {
        let response = self
            .client
            .request(
                reqwest::Method::OPTIONS,
                format!("{SERVICE_URL}{SEARCH_PATH}"),
            )
            .header("origin", "https://example.test")
            .header("access-control-request-method", "GET")
            .header("access-control-request-headers", header)
            .send()
            .unwrap();
        let allowed = response.headers().get("access-control-allow-headers")?;
        Some(allowed.to_str().unwrap().to_owned())
    }

    fn request(&self, query: &str, limit: &str) -> reqwest::blocking::Response {
        self.client
            .get(format!("{SERVICE_URL}{SEARCH_PATH}"))
            .query(&[("q", query), ("limit", limit)])
            .send()
            .unwrap()
    }

    fn request_with_origin(&self, origin: &str) -> reqwest::blocking::Response {
        self.client
            .get(format!("{SERVICE_URL}{SEARCH_PATH}"))
            .query(&[("q", "a")])
            .header("Origin", origin)
            .send()
            .unwrap()
    }

    pub fn metrics(&self) -> String {
        let url = format!("{METRICS_URL}/metrics");
        self.client.get(url).send().unwrap().text().unwrap()
    }

    pub fn wait_for_metric(&self, name: &str, value: f64) {
        let reached = wait_for(CHANGE_TIMEOUT, || {
            self.metrics().lines().any(|line| {
                line.strip_prefix(name)
                    .and_then(|rest| rest.trim().parse::<f64>().ok())
                    == Some(value)
            })
        });
        assert!(reached, "{name} never reached {value}");
    }

    pub fn public_metrics_status(&self) -> StatusCode {
        let url = format!("{SERVICE_URL}/metrics");
        self.client.get(url).send().unwrap().status()
    }

    pub fn wait_for_top_result(&self, query: &str, handle: &str) -> Duration {
        let start = Instant::now();
        let found = wait_for(CHANGE_TIMEOUT, || {
            self.search(query)
                .first()
                .is_some_and(|a| a.handle == handle)
        });
        assert!(found, "`{query}` never found {handle} first");
        start.elapsed()
    }

    pub fn wait_until_gone(&self, query: &str) {
        let gone = wait_for(CHANGE_TIMEOUT, || self.search(query).is_empty());
        assert!(gone, "`{query}` still has results");
    }

    pub fn create_account(&self, did: &str, handle: &str, name: &str) {
        self.control(
            "accounts",
            json!({
                "did": did,
                "handle": handle,
                "display_name": name,
                "avatar_cid": "bafkreimock",
                "created_at": "2024-01-01T00:00:00Z",
            }),
        );
    }

    pub fn change_handle(&self, did: &str, handle: &str) {
        self.control("handle", json!({ "did": did, "handle": handle }));
    }

    pub fn fail_lookup(&self, did: &str) {
        self.control("failing-lookup", json!({ "did": did }));
    }

    pub fn deactivate(&self, did: &str) {
        self.control("status", json!({ "did": did, "active": false }));
    }

    pub fn reactivate(&self, did: &str) {
        self.control("status", json!({ "did": did, "active": true }));
    }

    pub fn delete(&self, did: &str) {
        self.control(
            "status",
            json!({ "did": did, "active": false, "status": "deleted" }),
        );
    }

    pub fn set_followers(&self, did: &str, followers: u64) {
        self.control("followers", json!({ "did": did, "followers": followers }));
    }

    pub fn jetstream_cursors(&self) -> Vec<Option<u64>> {
        self.client
            .get(format!("{MOCK_URL}/control/connections"))
            .send()
            .unwrap()
            .json()
            .unwrap()
    }

    pub fn restart_service(&self) {
        let status = Command::new("docker")
            .args(["compose", "restart", "tern"])
            .status()
            .unwrap();
        assert!(status.success());
        self.wait_until_healthy();
    }

    fn control(&self, route: &str, body: Value) {
        let response = self
            .client
            .post(format!("{MOCK_URL}/control/{route}"))
            .json(&body)
            .send()
            .unwrap();
        assert!(response.status().is_success(), "{route} failed");
    }
}

fn wait_for(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        sleep(POLL_INTERVAL);
    }
    false
}
