use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Context as _;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, RawQuery, State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::fixture::{self, Account};

const EVENT_TYPE_PREFIX: &str = "network.bsky.jetstream.subscribeEvents";
const SUBPROTOCOL: &str = "xrpc.v1.json";
const LIVE_EVENT_BUFFER: usize = 1024;
const AVATAR_BASE_URL: &str = "https://cdn.bsky.app/img/avatar/plain";

type Frame = (u64, String);

struct World {
    accounts: HashMap<String, Account>,
    failing_lookups: HashSet<String>,
    frames: Vec<String>,
    live: broadcast::Sender<Frame>,
    connections: Vec<Option<u64>>,
}

impl World {
    fn add_account(&mut self, account: Account) {
        self.accounts.insert(account.did.clone(), account.clone());
        self.emit_identity(&account.did);
        self.emit_status(&account.did, true, None);
        self.emit_profile(&account);
    }

    fn emit_identity(&mut self, did: &str) {
        self.emit(
            "identity",
            did,
            json!({ "identity": { "did": did, "seq": 1, "time": now() } }),
        );
    }

    fn emit_status(&mut self, did: &str, active: bool, status: Option<&str>) {
        let mut account = json!({ "active": active, "did": did, "seq": 1 });
        if let Some(status) = status {
            account["status"] = json!(status);
        }
        self.emit("account", did, json!({ "account": account }));
    }

    fn emit_profile(&mut self, account: &Account) {
        let mut record = json!({ "$type": "app.bsky.actor.profile" });
        if let Some(name) = &account.display_name {
            record["displayName"] = json!(name);
        }
        if let Some(created_at) = &account.created_at {
            record["createdAt"] = json!(created_at);
        }
        if let Some(cid) = &account.avatar_cid {
            record["avatar"] = json!({
                "$type": "blob",
                "mimeType": "image/jpeg",
                "ref": { "$link": cid },
                "size": 1,
            });
        }
        self.emit(
            "commit",
            &account.did,
            json!({
                "collection": "app.bsky.actor.profile",
                "operation": "update",
                "record": record,
                "rev": "3mock",
                "rkey": "self",
            }),
        );
    }

    fn emit(&mut self, kind: &str, did: &str, mut payload: Value) {
        let seq = self.frames.len() as u64 + 1;
        payload["$type"] = json!(format!("{EVENT_TYPE_PREFIX}#{kind}"));
        payload["did"] = json!(did);
        payload["seq"] = json!(seq);
        payload["time"] = json!(now());

        let frame = json!({ "$type": "message", "payload": payload });
        let frame = frame.to_string();
        self.frames.push(frame.clone());
        let _ = self.live.send((seq, frame));
    }
}

fn now() -> String {
    jiff::Timestamp::now().to_string()
}

type SharedWorld = Arc<Mutex<World>>;

pub async fn run(
    fixture_path: &Path,
    backfill_path: Option<&Path>,
    bind: SocketAddr,
) -> anyhow::Result<()> {
    let (live, _) = broadcast::channel(LIVE_EVENT_BUFFER);
    let mut world = World {
        accounts: HashMap::new(),
        failing_lookups: HashSet::new(),
        frames: Vec::new(),
        live,
        connections: Vec::new(),
    };
    for account in fixture::read(fixture_path)? {
        world.add_account(account);
    }
    for account in backfill_path
        .map(fixture::read)
        .transpose()?
        .unwrap_or_default()
    {
        world.accounts.insert(account.did.clone(), account);
    }
    tracing::info!(accounts = world.accounts.len(), "loaded fixture");

    let app = Router::new()
        .route(
            "/xrpc/network.bsky.jetstream.subscribeEvents",
            get(subscribe_events),
        )
        .route(
            "/xrpc/com.bad-example.identity.resolveMiniDoc",
            get(resolve_mini_doc),
        )
        .route("/xrpc/app.bsky.actor.getProfiles", get(get_profiles))
        .route("/xrpc/com.atproto.sync.listRepos", get(list_repos))
        .route("/xrpc/blue.microcosm.hubble.getStats", get(get_stats))
        .route("/control/accounts", post(create_account))
        .route("/control/handle", post(change_handle))
        .route("/control/failing-lookup", post(fail_lookup))
        .route("/control/status", post(change_status))
        .route("/control/followers", post(change_followers))
        .route("/control/connections", get(connections))
        .with_state(Arc::new(Mutex::new(world)));

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind {bind}"))?;
    axum::serve(listener, app)
        .await
        .context("the mock server failed")
}

#[derive(Deserialize)]
struct SubscribeParams {
    cursor: Option<u64>,
}

async fn subscribe_events(
    State(world): State<SharedWorld>,
    Query(params): Query<SubscribeParams>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade
        .protocols([SUBPROTOCOL])
        .on_upgrade(move |socket| stream_frames(world, params.cursor, socket))
}

async fn stream_frames(world: SharedWorld, cursor: Option<u64>, mut socket: WebSocket) {
    let (backlog, mut live) = {
        let mut world = world.lock().expect("the world lock is not poisoned");
        world.connections.push(cursor);
        let start = cursor.unwrap_or(1).saturating_sub(1) as usize;
        let backlog = world.frames.get(start..).unwrap_or_default().to_vec();
        (backlog, world.live.subscribe())
    };

    for frame in backlog {
        if socket.send(Message::text(frame)).await.is_err() {
            return;
        }
    }
    while let Ok((_, frame)) = live.recv().await {
        if socket.send(Message::text(frame)).await.is_err() {
            return;
        }
    }
}

#[derive(Deserialize)]
struct ResolveParams {
    identifier: String,
}

async fn resolve_mini_doc(
    State(world): State<SharedWorld>,
    Query(params): Query<ResolveParams>,
) -> Response {
    let world = world.lock().expect("the world lock is not poisoned");
    let account = Some(&params.identifier)
        .filter(|did| !world.failing_lookups.contains(*did))
        .and_then(|did| world.accounts.get(did));
    match account {
        Some(account) => Json(json!({
            "did": account.did,
            "handle": account.handle,
            "pds": "http://mock",
            "signing_key": "mock",
        }))
        .into_response(),
        None => {
            let body = json!({
                "error": "InvalidRequest",
                "message": "Failed to get DID doc",
            });
            (StatusCode::BAD_REQUEST, Json(body)).into_response()
        }
    }
}

async fn get_profiles(State(world): State<SharedWorld>, RawQuery(query): RawQuery) -> Json<Value> {
    let query = query.unwrap_or_default();
    let world = world.lock().expect("the world lock is not poisoned");
    let profiles: Vec<_> = form_urlencoded::parse(query.as_bytes())
        .filter(|(name, _)| name == "actors")
        .filter_map(|(_, did)| world.accounts.get(did.as_ref()))
        .map(|account| {
            let avatar = account
                .avatar_cid
                .as_ref()
                .map(|cid| format!("{AVATAR_BASE_URL}/{}/{cid}@jpeg", account.did));
            json!({
                "did": account.did,
                "handle": account.handle,
                "displayName": account.display_name,
                "avatar": avatar,
                "createdAt": account.created_at,
                "followersCount": account.followers,
            })
        })
        .collect();
    Json(json!({ "profiles": profiles }))
}

#[derive(Deserialize)]
struct ListReposParams {
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn list_repos(
    State(world): State<SharedWorld>,
    Query(params): Query<ListReposParams>,
) -> Json<Value> {
    let world = world.lock().expect("the world lock is not poisoned");
    let mut dids: Vec<&String> = world.accounts.keys().collect();
    dids.sort();
    let after = params.cursor.unwrap_or_default();
    let page: Vec<&String> = dids
        .into_iter()
        .filter(|did| **did > after)
        .take(params.limit.unwrap_or(500))
        .collect();
    let cursor = (page.len() == params.limit.unwrap_or(500))
        .then(|| page.last().map(|did| did.to_string()))
        .flatten();
    let repos: Vec<_> = page
        .iter()
        .map(|did| json!({ "did": did, "active": true, "rev": "3mock", "head": "" }))
        .collect();
    Json(json!({ "repos": repos, "cursor": cursor }))
}

async fn get_stats(State(world): State<SharedWorld>) -> Json<Value> {
    let world = world.lock().expect("the world lock is not poisoned");
    let active = world.accounts.len();
    Json(json!({ "repos": active, "reposByAccountStatus": { "active": active } }))
}

async fn create_account(State(world): State<SharedWorld>, Json(account): Json<Account>) {
    let mut world = world.lock().expect("the world lock is not poisoned");
    world.add_account(account);
}

#[derive(Deserialize)]
struct HandleChange {
    did: String,
    handle: String,
}

async fn change_handle(
    State(world): State<SharedWorld>,
    Json(change): Json<HandleChange>,
) -> StatusCode {
    let mut world = world.lock().expect("the world lock is not poisoned");
    let Some(account) = world.accounts.get_mut(&change.did) else {
        return StatusCode::NOT_FOUND;
    };
    account.handle = change.handle;
    world.emit_identity(&change.did);
    StatusCode::OK
}

#[derive(Deserialize)]
struct FailingLookup {
    did: String,
}

async fn fail_lookup(State(world): State<SharedWorld>, Json(request): Json<FailingLookup>) {
    let mut world = world.lock().expect("the world lock is not poisoned");
    world.failing_lookups.insert(request.did.clone());
    world.emit_identity(&request.did);
}

#[derive(Deserialize)]
struct StatusChange {
    did: String,
    active: bool,
    status: Option<String>,
}

async fn change_status(State(world): State<SharedWorld>, Json(change): Json<StatusChange>) {
    let mut world = world.lock().expect("the world lock is not poisoned");
    world.emit_status(&change.did, change.active, change.status.as_deref());
}

#[derive(Deserialize)]
struct FollowerChange {
    did: String,
    followers: u64,
}

async fn change_followers(
    State(world): State<SharedWorld>,
    Json(change): Json<FollowerChange>,
) -> StatusCode {
    let mut world = world.lock().expect("the world lock is not poisoned");
    let Some(account) = world.accounts.get_mut(&change.did) else {
        return StatusCode::NOT_FOUND;
    };
    account.followers = change.followers;
    StatusCode::OK
}

async fn connections(State(world): State<SharedWorld>) -> Json<Vec<Option<u64>>> {
    let world = world.lock().expect("the world lock is not poisoned");
    Json(world.connections.clone())
}
