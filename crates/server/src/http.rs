use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use jiff::Timestamp;
use search::{Actor, Reader};
use serde::{Deserialize, Serialize};
use tower_http::cors::{AllowHeaders, Any, CorsLayer};
use types::{Did, Handle, Label};

use crate::telemetry::Exporter;

const AVATAR_BASE_URL: &str = "https://cdn.bsky.app/img/avatar/plain";
const DEFAULT_LIMIT: usize = 10;
const PREFLIGHT_MAX_AGE: Duration = Duration::from_secs(86_400);
const MAX_LIMIT: usize = 100;

pub fn router(reader: Reader) -> Router {
    Router::new()
        .route(
            "/xrpc/town.cove.actor.searchActorsTypeahead",
            get(typeahead),
        )
        .route("/xrpc/app.bsky.actor.searchActorsTypeahead", get(typeahead))
        .route("/xrpc/town.cove.actor.getStats", get(stats))
        .route("/xrpc/_health", get(health))
        .layer(cors())
        .with_state(reader)
}

pub fn metrics_router(exporter: Arc<Exporter>) -> Router {
    Router::new()
        .route("/metrics", get(render_metrics))
        .with_state(exporter)
}

fn cors() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET])
        .allow_headers(AllowHeaders::mirror_request())
        .max_age(PREFLIGHT_MAX_AGE)
}

#[derive(Deserialize)]
struct TypeaheadParams {
    q: Option<String>,
    term: Option<String>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct TypeaheadResponse {
    actors: Vec<ActorView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorView {
    did: Did,
    handle: Handle,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avatar: Option<String>,
    labels: Vec<LabelView>,
}

impl ActorView {
    fn new(actor: Actor, now: Timestamp) -> ActorView {
        let avatar = actor
            .avatar_cid
            .map(|cid| format!("{AVATAR_BASE_URL}/{}/{cid}", actor.did));
        let labels = actor
            .labels
            .into_iter()
            .filter(|label| !has_expired(label, now))
            .map(LabelView::from)
            .collect();
        ActorView {
            did: actor.did,
            handle: actor.handle,
            display_name: actor.display_name,
            avatar,
            labels,
        }
    }
}

#[derive(Serialize)]
struct LabelView {
    src: Did,
    uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cid: Option<String>,
    val: String,
    cts: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    exp: Option<String>,
}

impl From<Label> for LabelView {
    fn from(label: Label) -> LabelView {
        LabelView {
            src: label.src,
            uri: label.uri,
            cid: label.cid,
            val: label.val,
            cts: label.cts,
            exp: label.exp,
        }
    }
}

fn has_expired(label: &Label, now: Timestamp) -> bool {
    label
        .exp
        .as_deref()
        .and_then(|exp| exp.parse::<Timestamp>().ok())
        .is_some_and(|exp| exp <= now)
}

async fn typeahead(
    State(reader): State<Reader>,
    params: Result<Query<TypeaheadParams>, QueryRejection>,
) -> Result<Json<TypeaheadResponse>, XrpcError> {
    let Query(params) =
        params.map_err(|rejection| XrpcError::InvalidRequest(rejection.body_text()))?;
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        let message = format!("limit must be between 1 and {MAX_LIMIT}");
        return Err(XrpcError::InvalidRequest(message));
    }
    let query = params.q.or(params.term).unwrap_or_default();

    let start = Instant::now();
    let search = move || reader.search(&query, limit);
    let actors = tokio::task::spawn_blocking(search)
        .await
        .map_err(|err| XrpcError::Internal(err.into()))?
        .map_err(|err| XrpcError::Internal(err.into()))?;

    metrics::histogram!("tern_search_seconds").record(start.elapsed());

    let now = Timestamp::now();
    let actors = actors
        .into_iter()
        .map(|actor| ActorView::new(actor, now))
        .collect();
    Ok(Json(TypeaheadResponse { actors }))
}

#[derive(Serialize)]
struct StatsResponse {
    index: IndexStats,
}

#[derive(Serialize)]
struct IndexStats {
    accounts: u64,
}

async fn stats(State(reader): State<Reader>) -> Json<StatsResponse> {
    let index = IndexStats {
        accounts: reader.num_docs(),
    };
    Json(StatsResponse { index })
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn render_metrics(State(exporter): State<Arc<Exporter>>) -> String {
    exporter.render()
}

enum XrpcError {
    InvalidRequest(String),
    Internal(anyhow::Error),
}

#[derive(Serialize)]
struct XrpcErrorBody {
    error: &'static str,
    message: String,
}

impl IntoResponse for XrpcError {
    fn into_response(self) -> Response {
        let (status, error, message) = match self {
            XrpcError::InvalidRequest(message) => {
                (StatusCode::BAD_REQUEST, "InvalidRequest", message)
            }
            XrpcError::Internal(err) => {
                tracing::error!("search failed: {err:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalServerError",
                    "search failed".to_owned(),
                )
            }
        };
        (status, Json(XrpcErrorBody { error, message })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(exp: Option<&str>) -> Label {
        Label {
            src: Did::try_from("did:plc:labeler".to_owned()).unwrap(),
            uri: "did:plc:alice".to_owned(),
            cid: None,
            val: "spam".to_owned(),
            cts: "2026-01-01T00:00:00Z".to_owned(),
            exp: exp.map(str::to_owned),
        }
    }

    #[test]
    fn expired_labels_are_left_out() {
        let actor = Actor {
            did: Did::try_from("did:plc:alice".to_owned()).unwrap(),
            handle: Handle::try_from("alice.test".to_owned()).unwrap(),
            display_name: None,
            avatar_cid: None,
            labels: vec![label(Some("2026-02-01T00:00:00Z")), label(None)],
        };
        let now: Timestamp = "2026-03-01T00:00:00Z".parse().unwrap();

        let view = ActorView::new(actor, now);

        let exps: Vec<_> = view.labels.iter().map(|label| label.exp.clone()).collect();
        assert_eq!(exps, [None]);
    }
}
