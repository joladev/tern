mod frame;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use reqwest::Client as HttpClient;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use types::Did;

pub use frame::{Frame, Labels, ParseError};

const SUBSCRIBE_PATH: &str = "/xrpc/com.atproto.label.subscribeLabels";
const LABELER_SERVICE: &str = "#atproto_labeler";
const WEB_DID_PREFIX: &str = "did:web:";
const FUTURE_CURSOR: &str = "FutureCursor";
const PING_INTERVAL: Duration = Duration::from_secs(30);
const UNANSWERED_PINGS: u32 = 3;
const MIN_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(300);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("the labeler reader stopped unexpectedly")]
    Stopped,
}

#[derive(Debug, thiserror::Error)]
enum ResolveError {
    #[error("failed to fetch the labeler's DID document")]
    Request(#[source] reqwest::Error),
    #[error("the labeler's DID document has no labeler service")]
    NoLabelerService,
}

pub struct Subscription {
    queue: mpsc::Receiver<Labels>,
}

impl Subscription {
    pub async fn next(&mut self) -> Result<Labels, Error> {
        self.queue.recv().await.ok_or(Error::Stopped)
    }
}

pub fn subscribe(
    http: HttpClient,
    plc_url: String,
    labeler: Did,
    cursor: u64,
    queue_size: usize,
) -> Subscription {
    spawn(Connection::new(http, plc_url, labeler, cursor), queue_size)
}

fn spawn(mut connection: Connection, queue_size: usize) -> Subscription {
    let (sender, queue) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let labels = tokio::select! {
                () = sender.closed() => return,
                labels = connection.next() => labels,
            };
            if sender.send(labels).await.is_err() {
                return;
            }
        }
    });
    Subscription { queue }
}

#[derive(Deserialize)]
struct DidDocument {
    #[serde(default)]
    service: Vec<Service>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Service {
    id: String,
    service_endpoint: serde_json::Value,
}

impl DidDocument {
    fn labeler_endpoint(&self) -> Option<&str> {
        let is_labeler = |service: &&Service| service.id.ends_with(LABELER_SERVICE);
        self.service
            .iter()
            .find(is_labeler)?
            .service_endpoint
            .as_str()
    }
}

struct Connection {
    http: HttpClient,
    plc_url: String,
    labeler: Did,
    cursor: u64,
    socket: Option<Socket>,
    retry_delay: Duration,
    ping_interval: Duration,
}

impl Connection {
    fn new(http: HttpClient, plc_url: String, labeler: Did, cursor: u64) -> Connection {
        Connection {
            http,
            plc_url,
            labeler,
            cursor,
            socket: None,
            retry_delay: Duration::ZERO,
            ping_interval: PING_INTERVAL,
        }
    }

    async fn next(&mut self) -> Labels {
        loop {
            if let Some(labels) = self.read_labels().await {
                self.cursor = labels.seq;
                self.retry_delay = Duration::ZERO;
                return labels;
            }
            self.socket = None;
            self.retry_delay = (self.retry_delay * 2).clamp(MIN_RETRY_DELAY, MAX_RETRY_DELAY);
            sleep(self.retry_delay).await;
        }
    }

    async fn read_labels(&mut self) -> Option<Labels> {
        let socket = match &mut self.socket {
            Some(socket) => socket,
            None => {
                let socket = self.connect().await?;
                self.socket.insert(socket)
            }
        };

        let mut unanswered_pings = 0;
        loop {
            let message = match timeout(self.ping_interval, socket.next()).await {
                Ok(Some(Ok(message))) => message,
                Ok(Some(Err(err))) => {
                    tracing::warn!(labeler = %self.labeler, error = %err, "labeler read failed");
                    return None;
                }
                Ok(None) => return None,
                Err(_) if unanswered_pings == UNANSWERED_PINGS => {
                    tracing::warn!(labeler = %self.labeler, "the labeler went quiet, reconnecting");
                    return None;
                }
                Err(_) => {
                    unanswered_pings += 1;
                    socket.send(Message::Ping(Default::default())).await.ok()?;
                    continue;
                }
            };
            unanswered_pings = 0;
            let Message::Binary(bytes) = message else {
                continue;
            };
            match Frame::parse(&bytes) {
                Ok(Frame::Labels(labels)) => return Some(labels),
                Ok(Frame::Ignored) => {}
                Ok(Frame::Failed(reason)) => {
                    tracing::warn!(labeler = %self.labeler, %reason, "the labeler closed the stream");
                    if reason == FUTURE_CURSOR {
                        self.cursor = 0;
                    }
                    return None;
                }
                Err(err) => {
                    let error = &err as &dyn std::error::Error;
                    tracing::warn!(labeler = %self.labeler, error, "dropping the labeler connection");
                    return None;
                }
            }
        }
    }

    async fn connect(&self) -> Option<Socket> {
        let endpoint = match self.endpoint().await {
            Ok(endpoint) => endpoint,
            Err(err) => {
                let error = &err as &dyn std::error::Error;
                tracing::warn!(labeler = %self.labeler, error, "failed to find the labeler");
                return None;
            }
        };
        let url = format!("{endpoint}{SUBSCRIBE_PATH}?cursor={}", self.cursor);
        match tokio_tungstenite::connect_async(url).await {
            Ok((socket, _)) => {
                tracing::info!(labeler = %self.labeler, cursor = self.cursor, "connected to the labeler");
                Some(socket)
            }
            Err(err) => {
                tracing::warn!(labeler = %self.labeler, error = %err, "labeler connection failed");
                None
            }
        }
    }

    async fn endpoint(&self) -> Result<String, ResolveError> {
        let url = document_url(&self.plc_url, &self.labeler);
        let response = self.http.get(url).send().await;
        let response = response
            .and_then(reqwest::Response::error_for_status)
            .map_err(ResolveError::Request)?;
        let document: DidDocument = response.json().await.map_err(ResolveError::Request)?;
        let endpoint = document.labeler_endpoint();
        let endpoint = endpoint.ok_or(ResolveError::NoLabelerService)?;
        Ok(endpoint.replacen("http", "ws", 1))
    }
}

fn document_url(plc_url: &str, labeler: &Did) -> String {
    match labeler.as_str().strip_prefix(WEB_DID_PREFIX) {
        Some(host) => format!("https://{}/.well-known/did.json", host.replace("%3A", ":")),
        None => format!("{plc_url}/{labeler}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use ciborium::cbor;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    use super::*;

    fn did(text: &str) -> Did {
        Did::try_from(text.to_owned()).unwrap()
    }

    fn frame(header: ciborium::Value, body: ciborium::Value) -> Message {
        let mut bytes = Vec::new();
        ciborium::into_writer(&header, &mut bytes).unwrap();
        ciborium::into_writer(&body, &mut bytes).unwrap();
        Message::binary(bytes)
    }

    fn labels_frame(seq: u64) -> Message {
        let header = cbor!({ "op" => 1, "t" => "#labels" }).unwrap();
        frame(header, cbor!({ "seq" => seq, "labels" => [] }).unwrap())
    }

    fn error_frame(error: &str) -> Message {
        let header = cbor!({ "op" => -1 }).unwrap();
        frame(header, cbor!({ "error" => error }).unwrap())
    }

    async fn serve_did_document(labeler_endpoint: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let body = format!(
            r##"{{"service":[{{"id":"#atproto_labeler","serviceEndpoint":"{labeler_endpoint}"}}]}}"##
        );
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                let _ = stream.read(&mut request).await.unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        url
    }

    #[expect(
        clippy::result_large_err,
        reason = "tungstenite sets the callback's error type"
    )]
    async fn accept(
        listener: &TcpListener,
        urls: &Mutex<Vec<String>>,
    ) -> WebSocketStream<TcpStream> {
        let (stream, _) = listener.accept().await.unwrap();
        accept_hdr_async(stream, |request: &Request, response: Response| {
            urls.lock().unwrap().push(request.uri().to_string());
            Ok(response)
        })
        .await
        .unwrap()
    }

    async fn subscribe_to(listener: &TcpListener, cursor: u64) -> Subscription {
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let plc_url = serve_did_document(endpoint).await;
        subscribe(
            HttpClient::new(),
            plc_url,
            did("did:plc:labeler"),
            cursor,
            1,
        )
    }

    #[tokio::test]
    async fn a_dropped_connection_resumes_after_the_last_sequence_number() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut subscription = subscribe_to(&listener, 0).await;
        let urls = Arc::new(Mutex::new(Vec::new()));
        let server_urls = Arc::clone(&urls);
        let server = tokio::spawn(async move {
            let mut first = accept(&listener, &server_urls).await;
            first.send(labels_frame(5)).await.unwrap();
            drop(first);
            let mut second = accept(&listener, &server_urls).await;
            second.send(labels_frame(6)).await.unwrap();
            second
        });

        assert_eq!(subscription.next().await.unwrap().seq, 5);
        assert_eq!(subscription.next().await.unwrap().seq, 6);
        let _socket = server.await.unwrap();
        let urls = urls.lock().unwrap();
        assert!(urls[0].ends_with("cursor=0"), "{}", urls[0]);
        assert!(urls[1].ends_with("cursor=5"), "{}", urls[1]);
    }

    #[tokio::test]
    async fn a_cursor_from_the_future_restarts_from_the_first_label() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut subscription = subscribe_to(&listener, 9).await;
        let urls = Arc::new(Mutex::new(Vec::new()));
        let server_urls = Arc::clone(&urls);
        let server = tokio::spawn(async move {
            let mut first = accept(&listener, &server_urls).await;
            first.send(error_frame(FUTURE_CURSOR)).await.unwrap();
            let mut second = accept(&listener, &server_urls).await;
            second.send(labels_frame(1)).await.unwrap();
            (first, second)
        });

        assert_eq!(subscription.next().await.unwrap().seq, 1);
        let _sockets = server.await.unwrap();
        let urls = urls.lock().unwrap();
        assert!(urls[0].ends_with("cursor=9"), "{}", urls[0]);
        assert!(urls[1].ends_with("cursor=0"), "{}", urls[1]);
    }

    #[test]
    fn the_labeler_service_is_found_in_a_did_document() {
        let document: DidDocument = serde_json::from_str(
            r##"{"id":"did:plc:labeler","service":[
                {"id":"#atproto_pds","type":"AtprotoPersonalDataServer",
                    "serviceEndpoint":"https://pds.test"},
                {"id":"#atproto_labeler","type":"AtprotoLabeler",
                    "serviceEndpoint":"https://mod.test"}]}"##,
        )
        .unwrap();

        assert_eq!(document.labeler_endpoint(), Some("https://mod.test"));
    }

    #[test]
    fn a_web_did_has_its_document_on_its_own_host() {
        let plc = document_url("https://plc.test", &did("did:plc:labeler"));
        assert_eq!(plc, "https://plc.test/did:plc:labeler");
        let web = document_url("https://plc.test", &did("did:web:labeler.test"));
        assert_eq!(web, "https://labeler.test/.well-known/did.json");
    }
}
