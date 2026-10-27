//! Admission shared by the queuer and runners. Network policy belongs to the caller.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::time::{Duration, Instant};

use crate::async_trait;
use crate::model::gateway::ShardInfo;
use crate::model::id::ShardId;

/// The result of admission before opening a fresh Gateway connection.
#[derive(Debug)]
pub enum IdentifyDecision {
    /// Open the connection, subject to a final check immediately before IDENTIFY.
    Proceed,
    /// Wait outside the runner, then consult admission again.
    DeferUntil(Instant),
    /// End this client with the supplied diagnostic.
    Refuse(String),
}

/// The synchronous decision immediately before an IDENTIFY write.
#[derive(Debug)]
pub enum IdentifyAdmission {
    /// Charge the attempt and send. Failed or ambiguous writes remain charged.
    Send,
    /// Retire this connection and ask for admission again without sending.
    RequeueForAdmission,
    /// End this client with the supplied diagnostic.
    Refuse(String),
}

/// Identity and lifetime of one continuous connection attempt.
///
/// Clones do not extend the connection's lifetime. `is_active()` becomes false on
/// disconnect, replacement or cancellation, even if the same session is resumed.
/// Policies can use this to fence delayed stability timers and late observations.
#[derive(Clone, Debug)]
pub struct ConnectionAttemptId {
    id: u64,
    active: Arc<AtomicBool>,
    shard: ShardId,
    state: Weak<Mutex<State>>,
}

impl ConnectionAttemptId {
    /// A process-unique identifier, including across manager replacements.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.id
    }

    /// Reset the retry streak only if this is still the active connection.
    ///
    /// The policy chooses its stability interval and calls this after that
    /// interval. A retired connection's timer cannot reset replacement state.
    /// Do not call from `try_identify`, which already holds the admission lock.
    #[must_use]
    pub fn mark_stable(&self) -> bool {
        let Some(state) = self.state.upgrade() else { return false };
        let mut state = state.lock().expect("identify state poisoned");
        if state.closed || !self.is_active() {
            return false;
        }
        let Some(shard) = state.shards.get_mut(&self.shard) else { return false };
        if !shard.ready || !shard.attempt.as_ref().is_some_and(|id| id.id == self.id) {
            return false;
        }
        shard.streak = 0;
        true
    }

    /// Whether this connection attempt has not yet been retired.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
}

/// Optional client policy for fresh sessions. RESUME never calls admission.
///
/// Implementations own quota reconciliation and retry policy. The asynchronous
/// check may read REST and returns an absolute cooldown deadline. The synchronous
/// check must atomically validate and charge, without network I/O or waiting.
/// READY is an observation, never permission to replenish quota.
#[async_trait]
pub trait IdentifyGate: Send + Sync {
    /// Consult policy before connecting; repeated calls must not restart a cooldown.
    async fn before_fresh_session(&self, shard: ShardInfo, streak: u32) -> IdentifyDecision;
    /// Validate admission and charge an attempted write in one atomic operation.
    fn try_identify(&self, shard: ShardInfo, attempt: ConnectionAttemptId) -> IdentifyAdmission;
    /// Observe READY for the active connection, without blocking the runner.
    fn session_ready(&self, shard: ShardInfo, attempt: ConnectionAttemptId);
}

#[derive(Debug, Default)]
struct ShardState {
    streak: u32,
    attempt: Option<ConnectionAttemptId>,
    ready: bool,
}

impl ShardState {
    fn retire(&mut self) {
        if let Some(attempt) = self.attempt.take() {
            attempt.active.store(false, Ordering::Release);
        }
        self.ready = false;
    }
}

#[derive(Debug, Default)]
struct State {
    closed: bool,
    last_identify: Option<Instant>,
    shards: HashMap<ShardId, ShardState>,
}

pub(super) struct IdentifyCoordinator {
    pub(super) gate: Option<Arc<dyn IdentifyGate>>,
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for IdentifyCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentifyCoordinator").field("state", &self.state).finish_non_exhaustive()
    }
}

impl IdentifyCoordinator {
    pub(super) fn new(gate: Option<Arc<dyn IdentifyGate>>) -> Self {
        Self {
            gate,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    pub(super) fn is_closed(&self) -> bool {
        self.state.lock().expect("identify state poisoned").closed
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock().expect("identify state poisoned");
        state.closed = true;
        for shard in state.shards.values_mut() {
            shard.retire();
        }
    }

    pub(super) fn streak(&self, id: ShardId) -> u32 {
        self.state.lock().expect("identify state poisoned").shards.entry(id).or_default().streak
    }

    pub(super) fn next_identify(&self) -> Option<Instant> {
        self.state
            .lock()
            .expect("identify state poisoned")
            .last_identify
            .map(|at| at + Duration::from_secs(5))
    }

    pub(super) fn begin(&self, shard: ShardInfo) -> AttemptGuard {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let mut state = self.state.lock().expect("identify state poisoned");
        let active = !state.closed;
        let entry = state.shards.entry(shard.id).or_default();
        entry.retire();
        let id = ConnectionAttemptId {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            active: Arc::new(AtomicBool::new(active)),
            shard: shard.id,
            state: Arc::downgrade(&self.state),
        };
        entry.attempt = Some(id.clone());
        AttemptGuard {
            id,
            shard,
            state: Arc::clone(&self.state),
        }
    }

    pub(super) fn admit(
        &self,
        shard: ShardInfo,
        attempt: &ConnectionAttemptId,
    ) -> IdentifyAdmission {
        let mut state = self.state.lock().expect("identify state poisoned");
        if state.closed || !attempt.is_active() {
            return IdentifyAdmission::RequeueForAdmission;
        }
        // Keep the original boot pacing for clients without a policy. Gated
        // clients additionally pace actual writes, including delayed Hello.
        if self.gate.is_some()
            && state.last_identify.is_some_and(|at| at.elapsed() < Duration::from_secs(5))
        {
            return IdentifyAdmission::RequeueForAdmission;
        }
        let admission = self
            .gate
            .as_ref()
            .map_or(IdentifyAdmission::Send, |gate| gate.try_identify(shard, attempt.clone()));
        match &admission {
            IdentifyAdmission::Send => {
                state.last_identify = Some(Instant::now());
                let entry = state.shards.entry(shard.id).or_default();
                entry.streak = entry.streak.saturating_add(1);
            },
            IdentifyAdmission::Refuse(_) => state.closed = true,
            IdentifyAdmission::RequeueForAdmission => {},
        }
        admission
    }

    pub(super) fn ready(&self, shard: ShardInfo, attempt: &ConnectionAttemptId) {
        let mut state = self.state.lock().expect("identify state poisoned");
        if state.closed || !attempt.is_active() {
            return;
        }
        let entry = state.shards.entry(shard.id).or_default();
        if !entry.ready {
            entry.ready = true;
            drop(state);
            if let Some(gate) = &self.gate {
                gate.session_ready(shard, attempt.clone());
            }
        }
    }
}

/// Owned by the runner future: cancellation also invalidates policy observations.
pub(super) struct AttemptGuard {
    pub(super) id: ConnectionAttemptId,
    shard: ShardInfo,
    state: Arc<Mutex<State>>,
}

impl AttemptGuard {
    pub(super) fn retire(&self) {
        let mut state = self.state.lock().expect("identify state poisoned");
        if let Some(entry) = state.shards.get_mut(&self.shard.id) {
            if entry.attempt.as_ref().is_some_and(|id| id.id == self.id.id) {
                entry.retire();
            }
        }
        self.id.active.store(false, Ordering::Release);
    }
}

impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.retire();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Allow;
    #[async_trait]
    impl IdentifyGate for Allow {
        async fn before_fresh_session(&self, _: ShardInfo, _: u32) -> IdentifyDecision {
            IdentifyDecision::Proceed
        }
        fn try_identify(&self, _: ShardInfo, _: ConnectionAttemptId) -> IdentifyAdmission {
            IdentifyAdmission::Send
        }
        fn session_ready(&self, _: ShardInfo, _: ConnectionAttemptId) {}
    }

    #[tokio::test(start_paused = true)]
    async fn minimum_spacing_tracks_send_not_boot_and_survives_runner_replacement() {
        let coordinator = IdentifyCoordinator::new(Some(Arc::new(Allow)));
        let shard = ShardInfo::new(ShardId(0), 1);
        let first = coordinator.begin(shard);
        tokio::time::advance(Duration::from_secs(20)).await; // Delayed Hello.
        assert!(matches!(coordinator.admit(shard, &first.id), IdentifyAdmission::Send));
        drop(first);
        let second = coordinator.begin(shard);
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(matches!(
            coordinator.admit(shard, &second.id),
            IdentifyAdmission::RequeueForAdmission
        ));
        assert_eq!(coordinator.streak(shard.id), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(coordinator.admit(shard, &second.id), IdentifyAdmission::Send));
        assert_eq!(coordinator.streak(shard.id), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn stability_policy_can_reset_only_the_current_ready_connection() {
        let coordinator = IdentifyCoordinator::new(Some(Arc::new(Allow)));
        let shard = ShardInfo::new(ShardId(0), 1);
        let first = coordinator.begin(shard);
        assert!(matches!(coordinator.admit(shard, &first.id), IdentifyAdmission::Send));
        coordinator.ready(shard, &first.id);
        tokio::time::advance(Duration::from_secs(59)).await;
        first.retire();
        let second = coordinator.begin(shard);
        assert!(matches!(coordinator.admit(shard, &second.id), IdentifyAdmission::Send));
        coordinator.ready(shard, &first.id); // Late callback from retired runner.
        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(!first.id.mark_stable());
        assert_eq!(coordinator.streak(shard.id), 2);
        coordinator.ready(shard, &second.id);
        tokio::time::advance(Duration::from_secs(59)).await;
        assert_eq!(coordinator.streak(shard.id), 2);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(second.id.mark_stable()); // Policy's sixty-second timer.
        second.retire();
        assert_eq!(coordinator.streak(shard.id), 0);
        assert!(!first.id.is_active());
        assert!(!second.id.is_active());
    }

    #[tokio::test(start_paused = true)]
    async fn canceled_runner_and_terminal_close_invalidate_observations_and_admission() {
        let coordinator = IdentifyCoordinator::new(Some(Arc::new(Allow)));
        let shard = ShardInfo::new(ShardId(0), 1);
        let first = coordinator.begin(shard);
        let observation = first.id.clone();
        drop(first);
        assert!(!observation.is_active());
        let second = coordinator.begin(shard);
        assert_ne!(observation.get(), second.id.get());
        coordinator.close();
        assert!(!second.id.is_active());
        assert!(matches!(
            coordinator.admit(shard, &second.id),
            IdentifyAdmission::RequeueForAdmission
        ));
        assert!(!coordinator.begin(shard).id.is_active());
    }
}
