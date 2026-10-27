//! Protocol regressions use only the parent module's synthetic loopback proxy.
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex as SyncMutex;

use tokio::sync::Notify;
use tokio::time::Instant;

use super::*;
use crate::gateway::{
    ConnectionAttemptId,
    GatewayError,
    IdentifyAdmission,
    IdentifyDecision,
    IdentifyGate,
};
use crate::model::gateway::ShardInfo;

struct Gate {
    remaining: AtomicUsize,
    charged: AtomicUsize,
    before_calls: AtomicUsize,
    ready: AtomicUsize,
    requeues: AtomicUsize,
    streaks: SyncMutex<Vec<u32>>,
    decisions: SyncMutex<VecDeque<IdentifyDecision>>,
    refuse_at_send: AtomicBool,
    hold: AtomicBool,
    release: Notify,
    expire_first: bool,
    valid_until: SyncMutex<Option<Instant>>,
    attempts: SyncMutex<Vec<ConnectionAttemptId>>,
}

impl Gate {
    fn new(remaining: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(remaining),
            charged: AtomicUsize::new(0),
            before_calls: AtomicUsize::new(0),
            ready: AtomicUsize::new(0),
            requeues: AtomicUsize::new(0),
            streaks: SyncMutex::new(vec![]),
            decisions: SyncMutex::new(VecDeque::new()),
            refuse_at_send: AtomicBool::new(false),
            hold: AtomicBool::new(false),
            release: Notify::new(),
            expire_first: false,
            valid_until: SyncMutex::new(None),
            attempts: SyncMutex::new(vec![]),
        }
    }
}

#[crate::async_trait]
impl IdentifyGate for Gate {
    async fn before_fresh_session(&self, _: ShardInfo, streak: u32) -> IdentifyDecision {
        let call = self.before_calls.fetch_add(1, Ordering::SeqCst);
        self.streaks.lock().unwrap().push(streak);
        if self.hold.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        if let Some(decision) = self.decisions.lock().unwrap().pop_front() {
            return decision;
        }
        if self.expire_first {
            *self.valid_until.lock().unwrap() = Some(
                Instant::now()
                    + if call == 0 { Duration::from_millis(50) } else { Duration::from_secs(60) },
            );
        }
        if self.remaining.load(Ordering::SeqCst) == 0 {
            IdentifyDecision::Refuse("fixture quota reserve".into())
        } else {
            IdentifyDecision::Proceed
        }
    }

    fn try_identify(&self, _: ShardInfo, attempt: ConnectionAttemptId) -> IdentifyAdmission {
        if self.valid_until.lock().unwrap().is_some_and(|until| until <= Instant::now()) {
            self.requeues.fetch_add(1, Ordering::SeqCst);
            self.hold.store(true, Ordering::SeqCst);
            return IdentifyAdmission::RequeueForAdmission;
        }
        if self.refuse_at_send.load(Ordering::SeqCst) {
            return IdentifyAdmission::Refuse("fixture send-boundary refusal".into());
        }
        if self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_err()
        {
            return IdentifyAdmission::Refuse("fixture quota reserve".into());
        }
        self.attempts.lock().unwrap().push(attempt);
        self.charged.fetch_add(1, Ordering::SeqCst);
        IdentifyAdmission::Send
    }

    fn session_ready(&self, _: ShardInfo, attempt: ConnectionAttemptId) {
        if attempt.is_active() {
            self.ready.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn gated_start(proxy: &Proxy, gate: Arc<Gate>) -> (Arc<ShardManager>, JoinHandle<Result<()>>) {
    let mut client = client_with_gate(proxy, Some(gate));
    let manager = Arc::clone(&client.shard_manager);
    (manager, tokio::spawn(async move { client.start().await }))
}

async fn until(mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(20), async {
        while !condition() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition before test deadline");
}

async fn refused(manager: &ShardManager, task: JoinHandle<Result<()>>, reason: &str) {
    let error = timeout(Duration::from_secs(25), task).await.unwrap().unwrap().unwrap_err();
    assert!(
        matches!(error, Error::Gateway(GatewayError::IdentifyRefused(ref text)) if text == reason),
        "{error:?}"
    );
    // Client return is not proof of shutdown: the owner joins from outside.
    timeout(BUDGET, manager.shutdown_terminal()).await.unwrap();
    assert!(manager.runners.lock().await.is_empty());
}

#[tokio::test]
async fn identify_gate_refuses_before_connect_without_self_join() {
    let proxy = Proxy::new(GATEWAY).await;
    let (manager, task) = gated_start(&proxy, Arc::new(Gate::new(0)));
    refused(&manager, task, "fixture quota reserve").await;
    assert_eq!(proxy.connects.load(Ordering::SeqCst), 0);
    assert_eq!(proxy.identifies.load(Ordering::SeqCst), 0);
    proxy.finish().await;
}

#[tokio::test]
async fn identify_gate_refuses_at_send_without_reconnect_fallback() {
    let proxy = Proxy::new(GATEWAY).await;
    let gate = Arc::new(Gate::new(1));
    gate.refuse_at_send.store(true, Ordering::SeqCst);
    let (manager, task) = gated_start(&proxy, gate);
    refused(&manager, task, "fixture send-boundary refusal").await;
    assert_eq!(proxy.connects.load(Ordering::SeqCst), 1);
    assert_eq!(proxy.identifies.load(Ordering::SeqCst), 0);
    assert_eq!(proxy.resumes.load(Ordering::SeqCst), 0);
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn identify_gate_bounds_reidentify_without_ready() {
    let proxy = Proxy::new(LOOP_INVALIDATION).await;
    let gate = Arc::new(Gate::new(2));
    let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
    refused(&manager, task, "fixture quota reserve").await;
    assert_eq!(proxy.identifies.load(Ordering::SeqCst), 2);
    assert_eq!(gate.charged.load(Ordering::SeqCst), 2);
    assert_eq!(*gate.streaks.lock().unwrap(), vec![0, 1, 2]);
    proxy.finish().await;
}

#[tokio::test]
async fn identify_gate_ready_disconnect_loop_does_not_reset_streak_or_allowance() {
    let proxy = Proxy::new(READY_INVALIDATION).await;
    let gate = Arc::new(Gate::new(2));
    let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
    refused(&manager, task, "fixture quota reserve").await;
    assert_eq!(proxy.identifies.load(Ordering::SeqCst), 2);
    assert_eq!(gate.ready.load(Ordering::SeqCst), 2);
    assert_eq!(*gate.streaks.lock().unwrap(), vec![0, 1, 2]);
    assert!(gate.attempts.lock().unwrap().iter().all(|id| !id.is_active()));
    proxy.finish().await;
}

#[tokio::test]
async fn identify_gate_delayed_hello_requeues_without_sending_until_readmitted() {
    let proxy = Proxy::new(DELAY_HELLO).await;
    let gate = Arc::new(Gate {
        expire_first: true,
        ..Gate::new(1)
    });
    let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
    until(|| gate.before_calls.load(Ordering::SeqCst) == 2).await;
    assert_eq!(gate.requeues.load(Ordering::SeqCst), 1);
    assert_eq!(proxy.identifies.load(Ordering::SeqCst), 0);
    assert_eq!(gate.charged.load(Ordering::SeqCst), 0);
    assert_eq!(*gate.streaks.lock().unwrap(), vec![0, 0]);
    gate.hold.store(false, Ordering::SeqCst);
    gate.release.notify_one();
    until(|| proxy.identifies.load(Ordering::SeqCst) == 1).await;
    stop(&manager, task).await;
    proxy.finish().await;
}

#[tokio::test]
async fn identify_gate_defer_rechecks_and_can_refuse_after_wait() {
    let proxy = Proxy::new(GATEWAY).await;
    let gate = Arc::new(Gate::new(1));
    gate.decisions.lock().unwrap().extend([
        IdentifyDecision::DeferUntil(Instant::now() + Duration::from_millis(100)),
        IdentifyDecision::Refuse("allowance changed during wait".into()),
    ]);
    let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
    refused(&manager, task, "allowance changed during wait").await;
    assert_eq!(gate.before_calls.load(Ordering::SeqCst), 2);
    assert_eq!(proxy.connects.load(Ordering::SeqCst), 0);
    proxy.finish().await;
}

#[tokio::test]
async fn identify_gate_terminal_stop_cancels_rest_admission_and_cooldown() {
    for pending_rest in [true, false] {
        let proxy = Proxy::new(GATEWAY).await;
        let gate = Arc::new(Gate::new(1));
        if pending_rest {
            gate.hold.store(true, Ordering::SeqCst);
        } else {
            gate.decisions
                .lock()
                .unwrap()
                .push_back(IdentifyDecision::DeferUntil(Instant::now() + Duration::from_secs(900)));
        }
        let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
        until(|| gate.before_calls.load(Ordering::SeqCst) == 1).await;
        stop(&manager, task).await;
        gate.release.notify_one();
        assert_eq!(proxy.connects.load(Ordering::SeqCst), 0);
        proxy.stays_stopped().await;
        proxy.finish().await;
    }
}

#[tokio::test]
async fn identify_gate_invalid_sequence_and_session_timeout_use_fresh_admission() {
    for mode in [INVALID_SEQUENCE, SESSION_TIMEOUT] {
        let proxy = Proxy::new(mode).await;
        let gate = Arc::new(Gate::new(2));
        let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
        until(|| proxy.identifies.load(Ordering::SeqCst) == 2).await;
        stop(&manager, task).await;
        assert_eq!(gate.charged.load(Ordering::SeqCst), 2);
        assert_eq!(proxy.resumes.load(Ordering::SeqCst), 0);
        proxy.finish().await;
    }
}

#[tokio::test]
async fn identify_gate_close_without_session_reidentifies_and_resumable_invalidation_resumes() {
    for mode in [CLOSE_WITHOUT_SESSION, RESUMABLE_INVALIDATION] {
        let proxy = Proxy::new(mode).await;
        let gate = Arc::new(Gate::new(1));
        let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
        until(|| {
            if mode == RESUMABLE_INVALIDATION {
                proxy.resumes.load(Ordering::SeqCst) == 1
            } else {
                proxy.identifies.load(Ordering::SeqCst) == 1
            }
        })
        .await;
        stop(&manager, task).await;
        assert_eq!(proxy.identifies.load(Ordering::SeqCst), 1);
        assert_eq!(gate.charged.load(Ordering::SeqCst), 1);
        proxy.finish().await;
    }
}

#[tokio::test]
async fn identify_gate_resume_outage_recovers_after_initial_retries_without_identify() {
    let proxy = Proxy::new(RESUME_OUTAGE).await;
    let gate = Arc::new(Gate::new(1));
    let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
    // Original CONNECT plus immediate resume and retries at 2, 5 and 10 seconds.
    until(|| proxy.connects.load(Ordering::SeqCst) >= 5).await;
    assert!(!task.is_finished());
    proxy.mode.store(GATEWAY, Ordering::SeqCst);
    timeout(Duration::from_secs(35), async {
        while proxy.resumes.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop(&manager, task).await;
    assert_eq!(proxy.identifies.load(Ordering::SeqCst), 1);
    assert_eq!(gate.before_calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.charged.load(Ordering::SeqCst), 1);
    proxy.finish().await;
}

#[tokio::test]
async fn identify_gate_stop_interrupts_resume_thirty_second_wait() {
    let proxy = Proxy::new(RESUME_OUTAGE).await;
    let (manager, task) = gated_start(&proxy, Arc::new(Gate::new(1)));
    until(|| proxy.connects.load(Ordering::SeqCst) >= 5).await;
    stop(&manager, task).await;
    assert_eq!(proxy.identifies.load(Ordering::SeqCst), 1);
    proxy.stays_stopped().await;
    proxy.finish().await;
}

#[tokio::test]
async fn identify_gate_fatal_authentication_still_ends_client() {
    for mode in [FATAL_AUTH, FATAL_SHARD, FATAL_SHARDING] {
        let proxy = Proxy::new(mode).await;
        let (manager, task) = gated_start(&proxy, Arc::new(Gate::new(2)));
        let error = timeout(BUDGET, task).await.unwrap().unwrap().unwrap_err();
        assert!(matches!(
            error,
            Error::Gateway(
                GatewayError::InvalidAuthentication
                    | GatewayError::InvalidShardData
                    | GatewayError::OverloadedShard
            )
        ));
        timeout(BUDGET, manager.shutdown_terminal()).await.unwrap();
        assert_eq!(proxy.identifies.load(Ordering::SeqCst), 1);
        assert_eq!(proxy.resumes.load(Ordering::SeqCst), 0);
        proxy.finish().await;
    }
}

#[tokio::test]
async fn identify_gate_io_loss_and_incomplete_handshake_keep_resumable_state() {
    let proxy = Proxy::new(RESUME_IO_OUTAGE).await;
    let gate = Arc::new(Gate::new(1));
    let (manager, task) = gated_start(&proxy, Arc::clone(&gate));
    until(|| proxy.connects.load(Ordering::SeqCst) >= 2).await;
    assert_eq!(gate.ready.load(Ordering::SeqCst), 1);
    proxy.mode.store(CUT_HANDSHAKE, Ordering::SeqCst);
    until(|| proxy.connects.load(Ordering::SeqCst) >= 3).await;
    // Let the failed handshake be consumed, not just its CONNECT accepted.
    sleep(Duration::from_millis(100)).await;
    assert!(!task.is_finished());
    proxy.mode.store(GATEWAY, Ordering::SeqCst);
    until(|| proxy.resumes.load(Ordering::SeqCst) == 1).await;
    stop(&manager, task).await;
    assert_eq!(proxy.identifies.load(Ordering::SeqCst), 1);
    assert_eq!(gate.charged.load(Ordering::SeqCst), 1);
    proxy.finish().await;
}
