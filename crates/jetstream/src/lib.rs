mod event;

use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub use event::{Event, EventKind, Frame, ParseError};

const SUBSCRIBE_PATH: &str = "/xrpc/network.bsky.jetstream.subscribeEvents";
const SUBPROTOCOL: &str = "xrpc.v1.json";
const FILTER: &str = "kinds=commit&kinds=identity&kinds=account\
                      &collections=app.bsky.actor.profile";
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const READ_TIMEOUT: Duration = Duration::from_secs(90);
const CURSOR_TOO_OLD: &str = "CursorTooOld";
const OLDEST_RETAINED: u64 = 1_000_000_000_000_000;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid Jetstream URL")]
    Url(#[source] tungstenite::Error),
    #[error("Jetstream rejected the connection: {0}")]
    Rejected(String),
    #[error("the Jetstream reader stopped unexpectedly")]
    Stopped,
}

pub struct Subscription {
    queue: mpsc::Receiver<Result<Event, Error>>,
}

impl Subscription {
    pub async fn next(&mut self) -> Result<Event, Error> {
        self.queue.recv().await.unwrap_or(Err(Error::Stopped))
    }

    pub fn queue_depth(&self) -> usize {
        self.queue.len()
    }
}

pub fn subscribe(base_url: String, cursor: Option<u64>, queue_size: usize) -> Subscription {
    spawn(Connection::new(base_url, cursor), queue_size)
}

fn spawn(mut connection: Connection, queue_size: usize) -> Subscription {
    let (sender, queue) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let result = tokio::select! {
                () = sender.closed() => return,
                result = connection.next() => result,
            };
            let failed = result.is_err();
            if sender.send(result).await.is_err() || failed {
                return;
            }
        }
    });
    Subscription { queue }
}

#[derive(Deserialize)]
struct Rejection {
    error: String,
}

struct Connection {
    base_url: String,
    cursor: Option<u64>,
    socket: Option<Socket>,
    read_timeout: Duration,
}

impl Connection {
    fn new(base_url: String, cursor: Option<u64>) -> Connection {
        Connection {
            base_url,
            cursor,
            socket: None,
            read_timeout: READ_TIMEOUT,
        }
    }

    async fn next(&mut self) -> Result<Event, Error> {
        loop {
            if let Some(event) = self.read_event().await? {
                self.cursor = Some(event.seq + 1);
                return Ok(event);
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    async fn read_event(&mut self) -> Result<Option<Event>, Error> {
        let socket = match &mut self.socket {
            Some(socket) => socket,
            None => match self.connect().await? {
                Some(socket) => self.socket.insert(socket),
                None => return Ok(None),
            },
        };

        loop {
            let message = match timeout(self.read_timeout, socket.next()).await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(_) => {
                    tracing::warn!("Jetstream went quiet, reconnecting");
                    break;
                }
            };
            let text = match message {
                Ok(Message::Text(text)) => text,
                Ok(_) => continue,
                Err(err) => {
                    tracing::warn!(error = %err, "Jetstream read failed");
                    break;
                }
            };
            match Frame::parse(&text) {
                Ok(Frame::Event(event)) => return Ok(Some(event)),
                Ok(Frame::Ignored) => {}
                Ok(Frame::Failed(reason)) => {
                    tracing::warn!(%reason, "Jetstream closed the stream");
                    break;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "skipping Jetstream frame");
                }
            }
        }

        self.socket = None;
        Ok(None)
    }

    async fn connect(&mut self) -> Result<Option<Socket>, Error> {
        let mut url = format!("{}{SUBSCRIBE_PATH}?{FILTER}", self.base_url);
        if let Some(cursor) = self.cursor {
            url.push_str(&format!("&cursor={cursor}"));
        }

        let mut request = url.into_client_request().map_err(Error::Url)?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static(SUBPROTOCOL),
        );

        match tokio_tungstenite::connect_async(request).await {
            Ok((socket, _)) => {
                tracing::info!(cursor = ?self.cursor, "connected to Jetstream");
                Ok(Some(socket))
            }
            Err(tungstenite::Error::Http(response))
                if response.status() == StatusCode::BAD_REQUEST =>
            {
                let reason = rejection_reason(response.body().as_deref());
                if reason != CURSOR_TOO_OLD {
                    return Err(Error::Rejected(reason));
                }
                tracing::warn!(
                    cursor = ?self.cursor,
                    "the cursor expired, continuing from the oldest data"
                );
                self.cursor = Some(OLDEST_RETAINED);
                Ok(None)
            }
            Err(err) => {
                tracing::warn!(error = %err, "Jetstream connection failed");
                Ok(None)
            }
        }
    }
}

fn rejection_reason(body: Option<&[u8]>) -> String {
    let body = body.unwrap_or_default();
    match serde_json::from_slice::<Rejection>(body) {
        Ok(rejection) => rejection.error,
        Err(_) => String::from_utf8_lossy(body).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures_util::SinkExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};

    use super::*;

    const SHORT_TIMEOUT: Duration = Duration::from_millis(50);

    fn identity_frame(seq: u64) -> String {
        format!(
            r#"{{"$type":"message","payload":{{
            "$type":"network.bsky.jetstream.subscribeEvents#identity",
            "did":"did:plc:alice","seq":{seq},"time":"t"}}}}"#
        )
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
        accept_hdr_async(stream, |request: &Request, mut response: Response| {
            urls.lock().unwrap().push(request.uri().to_string());
            response.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_static(SUBPROTOCOL),
            );
            Ok(response)
        })
        .await
        .unwrap()
    }

    #[expect(
        clippy::result_large_err,
        reason = "tungstenite sets the callback's error type"
    )]
    async fn reject(listener: &TcpListener, error: &str) {
        let (stream, _) = listener.accept().await.unwrap();
        let body = format!(r#"{{"error":"{error}","message":"refused"}}"#);
        let rejected = accept_hdr_async(stream, |_: &Request, _: Response| {
            let response: ErrorResponse = Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Some(body))
                .unwrap();
            Err(response)
        })
        .await;
        assert!(rejected.is_err());
    }

    async fn listen() -> (TcpListener, String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        (listener, url, Arc::new(Mutex::new(Vec::new())))
    }

    fn subscribe_quickly(url: String, cursor: Option<u64>) -> Subscription {
        let mut connection = Connection::new(url, cursor);
        connection.read_timeout = SHORT_TIMEOUT;
        spawn(connection, 1)
    }

    #[tokio::test]
    async fn a_quiet_socket_reconnects_after_the_last_event() {
        let (listener, url, urls) = listen().await;
        let server_urls = Arc::clone(&urls);
        let server = tokio::spawn(async move {
            let mut first = accept(&listener, &server_urls).await;
            first.send(Message::text(identity_frame(5))).await.unwrap();
            let mut second = accept(&listener, &server_urls).await;
            second.send(Message::text(identity_frame(6))).await.unwrap();
            (first, second)
        });

        let mut subscription = subscribe_quickly(url, None);

        assert_eq!(subscription.next().await.unwrap().seq, 5);
        assert_eq!(subscription.next().await.unwrap().seq, 6);
        let _sockets = server.await.unwrap();
        let urls = urls.lock().unwrap();
        assert!(!urls[0].contains("cursor"), "{}", urls[0]);
        assert!(urls[1].contains("cursor=6"), "{}", urls[1]);
    }

    #[tokio::test]
    async fn a_full_queue_waits_then_resumes_after_the_last_queued_event() {
        let (listener, url, urls) = listen().await;
        let server_urls = Arc::clone(&urls);
        let server = tokio::spawn(async move {
            let mut first = accept(&listener, &server_urls).await;
            for seq in 1..=3 {
                first
                    .send(Message::text(identity_frame(seq)))
                    .await
                    .unwrap();
            }
            let mut second = accept(&listener, &server_urls).await;
            second.send(Message::text(identity_frame(4))).await.unwrap();
            (first, second)
        });

        let mut subscription = subscribe_quickly(url, None);
        tokio::time::sleep(SHORT_TIMEOUT * 4).await;
        assert_eq!(subscription.queue_depth(), 1);

        for seq in 1..=4 {
            assert_eq!(subscription.next().await.unwrap().seq, seq);
        }
        let _sockets = server.await.unwrap();
        let urls = urls.lock().unwrap();
        assert!(urls[1].contains("cursor=4"), "{}", urls[1]);
    }

    #[tokio::test]
    async fn an_expired_cursor_continues_from_the_oldest_data() {
        let (listener, url, urls) = listen().await;
        let server_urls = Arc::clone(&urls);
        let server = tokio::spawn(async move {
            reject(&listener, CURSOR_TOO_OLD).await;
            let mut socket = accept(&listener, &server_urls).await;
            socket.send(Message::text(identity_frame(6))).await.unwrap();
            socket
        });

        let mut subscription = subscribe_quickly(url, Some(5));

        assert_eq!(subscription.next().await.unwrap().seq, 6);
        let _socket = server.await.unwrap();
        let urls = urls.lock().unwrap();
        let expected = format!("cursor={OLDEST_RETAINED}");
        assert!(urls[0].contains(&expected), "{}", urls[0]);
    }

    #[tokio::test]
    async fn a_rejected_connection_is_reported() {
        let (listener, url, _urls) = listen().await;
        tokio::spawn(async move { reject(&listener, "InvalidRequest").await });

        let mut subscription = subscribe_quickly(url, None);

        let error = subscription.next().await.unwrap_err();
        assert!(matches!(error, Error::Rejected(reason) if reason == "InvalidRequest"));
    }

    #[test]
    fn rejections_keep_their_reason() {
        let body = br#"{"error":"InvalidRequest","message":"bad"}"#;
        assert_eq!(rejection_reason(Some(body)), "InvalidRequest");
        assert_eq!(rejection_reason(Some(b"nope")), "nope");
    }
}
