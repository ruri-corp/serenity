//! Real loopback proxy/WebSocket traffic; no Discord requests or credentials.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use super::*;
use crate::gateway::ConnectionStage;
use crate::model::id::ShardId;

const REFUSE: usize = 0;
const HANG: usize = 1;
const GATEWAY: usize = 2;
const REIDENTIFY: usize = 3;
const STALL_CLOSE: usize = 4;
const LOOP_INVALIDATION: usize = 5;
const READY_INVALIDATION: usize = 6;
const RESUME_OUTAGE: usize = 7;
const INVALID_SEQUENCE: usize = 8;
const SESSION_TIMEOUT: usize = 9;
const RESUMABLE_INVALIDATION: usize = 10;
const DELAY_HELLO: usize = 11;
const CLOSE_WITHOUT_SESSION: usize = 12;
const FATAL_AUTH: usize = 13;
const RESUME_IO_OUTAGE: usize = 14;
const CUT_HANDSHAKE: usize = 15;
const FATAL_SHARD: usize = 16;
const FATAL_SHARDING: usize = 17;
const BUDGET: Duration = Duration::from_secs(3);

struct Proxy {
    url: String,
    mode: Arc<AtomicUsize>,
    connects: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    identifies: Arc<AtomicUsize>,
    resumes: Arc<AtomicUsize>,
    events: mpsc::UnboundedReceiver<&'static str>,
    server: JoinHandle<()>,
}

struct ConnectionGuard(Arc<AtomicUsize>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Proxy {
    async fn new(mode: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let mode = Arc::new(AtomicUsize::new(mode));
        let connects = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let identifies = Arc::new(AtomicUsize::new(0));
        let resumes = Arc::new(AtomicUsize::new(0));
        let (events_tx, events) = mpsc::unbounded_channel();
        let (m, c, a) = (Arc::clone(&mode), Arc::clone(&connects), Arc::clone(&active));
        let (ic, rc) = (Arc::clone(&identifies), Arc::clone(&resumes));
        let server = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        a.fetch_add(1, Ordering::SeqCst);
                        let guard = ConnectionGuard(Arc::clone(&a));
                        let (m, c, tx) = (Arc::clone(&m), Arc::clone(&c), events_tx.clone());
                        let (ic, rc) = (Arc::clone(&ic), Arc::clone(&rc));
                        handlers.spawn(async move {
                            let _guard = guard;
                            serve(socket, m, c, ic, rc, tx).await;
                        });
                    },
                    Some(result) = handlers.join_next(), if !handlers.is_empty() => {
                        result.unwrap();
                    },
                }
            }
        });
        Self {
            url,
            mode,
            connects,
            active,
            identifies,
            resumes,
            events,
            server,
        }
    }

    async fn event(&mut self, expected: &'static str) {
        timeout(Duration::from_secs(15), async {
            while let Some(event) = self.events.recv().await {
                if event == expected {
                    return;
                }
            }
            panic!("fake proxy stopped before {expected}");
        })
        .await
        .unwrap_or_else(|_| panic!("did not observe {expected}"));
    }

    async fn stays_stopped(&self) {
        let before = self.connects.load(Ordering::SeqCst);
        self.mode.store(GATEWAY, Ordering::SeqCst);
        // Longer than the production queuer retry interval. A leaked queuer now
        // reaches a functioning Gateway and increments the CONNECT count.
        sleep(Duration::from_secs(6)).await;
        assert_eq!(self.connects.load(Ordering::SeqCst), before);
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
    }

    async fn finish(self) {
        self.server.abort();
        drop(self.server.await);
    }
}

async fn serve(
    mut socket: TcpStream,
    mode: Arc<AtomicUsize>,
    connects: Arc<AtomicUsize>,
    identifies: Arc<AtomicUsize>,
    resumes: Arc<AtomicUsize>,
    events: mpsc::UnboundedSender<&'static str>,
) {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let Ok(byte) = socket.read_u8().await else { return };
        header.push(byte);
        assert!(header.len() < 8192);
    }
    assert!(header.starts_with(b"CONNECT "));
    connects.fetch_add(1, Ordering::SeqCst);
    events.send("CONNECT").expect("test event receiver remains alive");
    let behavior = mode.load(Ordering::SeqCst);
    if behavior == REFUSE {
        drop(socket.write_all(b"HTTP/1.1 503 Unavailable\r\n\r\n").await);
        return;
    }
    if behavior == HANG {
        let mut byte = [0];
        // Deliberately never answer CONNECT. Cancellation must close this socket.
        while matches!(socket.read(&mut byte).await, Ok(1)) {}
        return;
    }
    if socket.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n").await.is_err() {
        return;
    }
    if behavior == CUT_HANDSHAKE {
        // The CONNECT succeeds but the peer disappears during the WS handshake.
        return;
    }
    let Ok(mut ws) = tokio_tungstenite::accept_async(socket).await else { return };
    if behavior == DELAY_HELLO {
        sleep(Duration::from_millis(150)).await;
    }
    if behavior == CLOSE_WITHOUT_SESSION {
        mode.store(GATEWAY, Ordering::SeqCst);
        drop(ws.close(None).await);
        return;
    }
    if ws
        .send(Message::Text(json!({"op":10,"d":{"heartbeat_interval":60000}}).to_string()))
        .await
        .is_err()
    {
        return;
    }
    while let Some(Ok(message)) = ws.next().await {
        match message {
            Message::Text(text) => {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                let response = match value["op"].as_u64() {
                    Some(2) => {
                        identifies.fetch_add(1, Ordering::SeqCst);
                        events.send("IDENTIFY").expect("test event receiver remains alive");
                        if behavior == REIDENTIFY || behavior == LOOP_INVALIDATION {
                            if behavior == REIDENTIFY {
                                mode.store(GATEWAY, Ordering::SeqCst);
                            }
                            json!({"op":9,"d":false})
                        } else {
                            json!({"op":0,"t":"READY","s":1,"d":{
                                "v":10,"user":{"id":"1","username":"fixture","discriminator":"0000","avatar":null,"bot":true},
                                "guilds":[],"session_id":"synthetic-session",
                                "resume_gateway_url":"ws://gateway.invalid",
                                "application":{"id":"1","flags":0}
                            }})
                        }
                    },
                    Some(6) => {
                        assert_eq!(value["d"]["session_id"], "synthetic-session");
                        assert_eq!(value["d"]["seq"], 1);
                        resumes.fetch_add(1, Ordering::SeqCst);
                        events.send("RESUME").expect("test receiver remains alive");
                        json!({"op":0,"t":"RESUMED","s":2,"d":{}})
                    },
                    Some(1) => json!({"op":11,"d":null}),
                    _ => continue,
                };
                if ws.send(Message::Text(response.to_string())).await.is_err() {
                    return;
                }
                if value["op"] == 2 {
                    match behavior {
                        READY_INVALIDATION | RESUMABLE_INVALIDATION => {
                            let resumable = behavior == RESUMABLE_INVALIDATION;
                            if resumable {
                                mode.store(GATEWAY, Ordering::SeqCst);
                            }
                            drop(
                                ws.send(Message::Text(json!({"op":9,"d":resumable}).to_string()))
                                    .await,
                            );
                        },
                        RESUME_IO_OUTAGE => {
                            sleep(Duration::from_millis(100)).await;
                            mode.store(REFUSE, Ordering::SeqCst);
                            #[allow(deprecated)]
                            ws.get_mut().set_linger(Some(Duration::ZERO)).unwrap();
                            return;
                        },
                        RESUME_OUTAGE | INVALID_SEQUENCE | SESSION_TIMEOUT | FATAL_AUTH
                        | FATAL_SHARD | FATAL_SHARDING => {
                            let code = match behavior {
                                INVALID_SEQUENCE => 4007,
                                SESSION_TIMEOUT => 4009,
                                FATAL_AUTH => 4004,
                                FATAL_SHARD => 4010,
                                FATAL_SHARDING => 4011,
                                _ => 4000,
                            };
                            mode.store(
                                if behavior == RESUME_OUTAGE { REFUSE } else { GATEWAY },
                                Ordering::SeqCst,
                            );
                            drop(
                                ws.close(Some(
                                    tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                        code: code.into(),
                                        reason: "fixture".into(),
                                    },
                                ))
                                .await,
                            );
                            return;
                        },
                        _ => {},
                    }
                }
            },
            Message::Close(_) => {
                events.send("CLOSE").expect("test event receiver remains alive");
                if behavior == STALL_CLOSE {
                    // Do not flush tungstenite's automatic close reply. Observe
                    // the underlying TCP EOF when terminal cancellation runs.
                    let mut byte = [0];
                    while matches!(ws.get_mut().read(&mut byte).await, Ok(1)) {}
                } else {
                    drop(ws.flush().await);
                }
                return;
            },
            _ => {},
        }
    }
}

/// Assemble the real Client without ClientBuilder's external GET /gateway.
fn client(proxy: &Proxy) -> Client {
    client_with_gate(proxy, None)
}

fn client_with_gate(proxy: &Proxy, gate: Option<Arc<dyn crate::gateway::IdentifyGate>>) -> Client {
    let data = Arc::new(RwLock::new(TypeMap::new()));
    let http = Arc::new(Http::new("synthetic.gateway.token"));
    let ws_url = Arc::new(Mutex::new("ws://gateway.invalid".to_owned()));
    #[cfg(feature = "cache")]
    let cache = Arc::new(Cache::default());
    let (shard_manager, shard_manager_return_value) = ShardManager::new(ShardManagerOptions {
        data: Arc::clone(&data),
        event_handlers: vec![],
        raw_event_handlers: vec![],
        #[cfg(feature = "framework")]
        framework: Arc::new(OnceLock::new()),
        shard_index: 0,
        shard_init: 0,
        shard_total: 0,
        #[cfg(feature = "voice")]
        voice_manager: None,
        ws_url: Arc::clone(&ws_url),
        ws_proxy: Arc::new(Mutex::new(Some(proxy.url.clone()))),
        #[cfg(feature = "cache")]
        cache: Arc::clone(&cache),
        http: Arc::clone(&http),
        intents: GatewayIntents::empty(),
        presence: None,
        identify_gate: gate,
    });
    Client {
        data,
        shard_manager,
        shard_manager_return_value,
        #[cfg(feature = "voice")]
        voice_manager: None,
        ws_url,
        #[cfg(feature = "cache")]
        cache,
        http,
    }
}

fn start(proxy: &Proxy) -> (Arc<ShardManager>, JoinHandle<Result<()>>) {
    let mut client = client(proxy);
    let manager = Arc::clone(&client.shard_manager);
    (manager, tokio::spawn(async move { client.start().await }))
}

async fn stop(manager: &ShardManager, client: JoinHandle<Result<()>>) {
    // No fallback: exceeding Ruri's budget fails the test.
    timeout(BUDGET, manager.shutdown_terminal()).await.unwrap();
    timeout(BUDGET, client).await.unwrap().unwrap().unwrap();
    assert!(manager.runners.lock().await.is_empty());
    timeout(BUDGET, manager.shutdown_terminal()).await.unwrap();
}

#[tokio::test]
async fn terminal_shutdown_stops_empty_runner_retry_after_proxy_refusal() {
    let mut proxy = Proxy::new(REFUSE).await;
    let (manager, client) = start(&proxy);
    proxy.event("CONNECT").await;
    assert!(manager.runners.lock().await.is_empty());
    stop(&manager, client).await;
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn terminal_shutdown_interrupts_hung_proxy_connect() {
    let mut proxy = Proxy::new(HANG).await;
    let (manager, client) = start(&proxy);
    proxy.event("CONNECT").await;
    assert!(manager.runners.lock().await.is_empty());
    stop(&manager, client).await;
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn initial_start_and_internal_reidentify_survive_until_terminal_shutdown() {
    let mut proxy = Proxy::new(REIDENTIFY).await;
    let (manager, client) = start(&proxy);
    proxy.event("IDENTIFY").await;
    proxy.event("IDENTIFY").await;
    timeout(BUDGET, async {
        loop {
            if manager.runners.lock().await.values().any(|r| r.stage == ConnectionStage::Connected)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(proxy.connects.load(Ordering::SeqCst), 2);
    stop(&manager, client).await;
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn terminal_shutdown_interrupts_stalled_runner_close_handshake() {
    let mut proxy = Proxy::new(STALL_CLOSE).await;
    let (manager, client) = start(&proxy);
    proxy.event("IDENTIFY").await;
    let closing = Arc::clone(&manager);
    let closing = tokio::spawn(async move { closing.shutdown(ShardId(0), 1000).await });
    proxy.event("CLOSE").await;
    stop(&manager, client).await;
    // The ordinary shutdown waiter is external to the connection task group.
    closing.abort();
    drop(closing.await);
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn aborted_client_parent_does_not_prevent_force_join_of_connections() {
    let mut proxy = Proxy::new(HANG).await;
    let (manager, client) = start(&proxy);
    proxy.event("CONNECT").await;
    client.abort();
    drop(client.await);
    timeout(BUDGET, manager.shutdown_terminal_force()).await.unwrap();
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn proxy_recovery_reuses_one_client_without_service_replacement() {
    let mut proxy = Proxy::new(REFUSE).await;
    let (manager, client) = start(&proxy);
    proxy.event("CONNECT").await;
    proxy.mode.store(GATEWAY, Ordering::SeqCst);
    proxy.event("IDENTIFY").await;
    assert_eq!(proxy.connects.load(Ordering::SeqCst), 2);
    stop(&manager, client).await;
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn canceled_terminal_waiter_retains_stop_and_handles_for_another_waiter() {
    let mut proxy = Proxy::new(HANG).await;
    let (manager, client) = start(&proxy);
    proxy.event("CONNECT").await;
    let mut stopping = Box::pin(manager.shutdown_terminal());
    // Poll on this thread so the stop is requested but the connection tasks
    // cannot yet acknowledge. Dropping the waiter must not detach those tasks.
    assert!(std::future::poll_fn(|cx| { std::task::Poll::Ready(stopping.as_mut().poll(cx)) })
        .await
        .is_pending());
    drop(stopping);
    timeout(BUDGET, manager.shutdown_terminal_force()).await.unwrap();
    timeout(BUDGET, client).await.unwrap().unwrap().unwrap();
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn terminal_shutdown_fences_concurrent_restart_and_retry_delay() {
    let mut proxy = Proxy::new(REFUSE).await;
    let (manager, client) = start(&proxy);
    proxy.event("CONNECT").await;
    // Queue another start while the first failed boot is in its five-second
    // delay. Also exercise a restart which can enqueue boot after shutdown.
    manager.initialize().unwrap();
    let restarting = Arc::clone(&manager);
    let restarting = tokio::spawn(async move { restarting.restart_shard(ShardId(0)).await });
    tokio::task::yield_now().await;
    stop(&manager, client).await;
    timeout(Duration::from_secs(6), restarting).await.unwrap().unwrap();
    manager.initialize().unwrap();
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn websocket_connect_timeout_bounds_unanswered_proxy_connect() {
    let mut proxy = Proxy::new(HANG).await;
    let proxy_url = Url::parse(&proxy.url).unwrap();
    let connecting = tokio::spawn(async move {
        crate::gateway::WsClient::connect(
            Url::parse("ws://gateway.invalid").unwrap(),
            Some(proxy_url),
        )
        .await
    });
    proxy.event("CONNECT").await;
    let result = timeout(Duration::from_secs(35), connecting).await.unwrap().unwrap();
    assert!(
        matches!(result, Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::TimedOut)
    );
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[path = "identify_tests.rs"]
mod identify_tests;
