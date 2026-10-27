//! Ownership of the Gateway connection tasks, independent of shutdown waiters.

use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::{AbortHandle, JoinHandle};

use crate::internal::tokio::spawn_named;

#[derive(Debug)]
struct Task {
    abort: AbortHandle,
    join: AsyncMutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Default)]
struct State {
    closed: bool,
    tasks: Vec<Arc<Task>>,
}

#[derive(Debug)]
pub(super) struct GatewayTasks {
    state: Mutex<State>,
    stop: watch::Sender<bool>,
}

impl GatewayTasks {
    pub(super) fn new() -> Self {
        Self { state: Mutex::new(State::default()), stop: watch::channel(false).0 }
    }

    /// Registration and closing share a lock: no task can escape the snapshot
    /// taken by shutdown, including a runner spawned by an in-flight connect.
    pub(super) fn spawn(&self, name: &str, future: impl Future<Output = ()> + Send + 'static) {
        let mut state = self.state.lock().expect("Gateway task registry poisoned");
        if state.closed {
            return;
        }
        state.tasks.retain(|task| !task.abort.is_finished());
        let mut stop = self.stop.subscribe();
        let join = spawn_named(name, async move {
            // This cancellation covers the whole operation, including connection
            // establishment, retry sleeps, reads and stalled close handshakes.
            tokio::select! {
                biased;
                _ = stop.changed() => {},
                () = future => {},
            }
        });
        state
            .tasks
            .push(Arc::new(Task { abort: join.abort_handle(), join: AsyncMutex::new(Some(join)) }));
    }

    pub(super) fn close(&self, force: bool) {
        let mut state = self.state.lock().expect("Gateway task registry poisoned");
        state.closed = true;
        self.stop.send_replace(true);
        if force {
            for task in &state.tasks {
                task.abort.abort();
            }
        }
    }

    /// Handles stay in the registry while awaiting them. Dropping this future
    /// releases only the waiter's locks, never the handles or the stop signal.
    /// Connection tasks do not take these per-task join locks.
    pub(super) async fn join(&self) {
        let tasks = self.state.lock().expect("Gateway task registry poisoned").tasks.clone();
        for task in tasks {
            let mut join = task.join.lock().await;
            if let Some(handle) = join.as_mut() {
                if let Err(error) = handle.await {
                    if !error.is_cancelled() {
                        tracing::warn!(%error, "Gateway connection task failed during shutdown");
                    }
                }
                *join = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct Dropped(Arc<AtomicUsize>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn terminal_join_drops_all_connection_futures_and_retains_no_unjoined_tasks() {
        let group = GatewayTasks::new();
        let dropped = Arc::new(AtomicUsize::new(0));
        for _ in 0..4 {
            let guard = Dropped(Arc::clone(&dropped));
            group.spawn("test connection", async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
        }
        tokio::task::yield_now().await;
        group.close(false);
        group.join().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 4);
        let tasks = group.state.lock().unwrap().tasks.clone();
        for task in tasks {
            assert!(task.abort.is_finished());
            assert!(task.join.lock().await.is_none());
        }
        // A late spawn is dropped without ever polling the connection future.
        let guard = Dropped(Arc::clone(&dropped));
        group.spawn("late connection", async move {
            let _guard = guard;
            panic!("connection started after terminal stop");
        });
        assert_eq!(dropped.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn force_stop_aborts_and_joins_every_owned_task() {
        let group = GatewayTasks::new();
        let dropped = Arc::new(AtomicUsize::new(0));
        for _ in 0..4 {
            let guard = Dropped(Arc::clone(&dropped));
            group.spawn("test connection", async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
        }
        group.close(true);
        group.join().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 4);
        group.join().await;
    }
}
