use std::time::Duration;

use rand::{rngs::OsRng, RngCore};
use tokio::{sync::watch, task::JoinHandle};

use crate::{SyncClient, SyncReport, Transport};

#[derive(Clone, Copy, Debug)]
pub struct BackgroundSyncOptions {
    pub interval: Duration,
    pub initial_retry: Duration,
    pub max_retry: Duration,
}

impl Default for BackgroundSyncOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            initial_retry: Duration::from_secs(1),
            max_retry: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackgroundSyncStatus {
    Starting,
    Succeeded(SyncReport),
    Retrying {
        attempt: u32,
        error: String,
        retry_in: Duration,
    },
    Blocked {
        error: String,
    },
    Stopped,
}

pub struct BackgroundSync {
    shutdown: watch::Sender<bool>,
    status: watch::Receiver<BackgroundSyncStatus>,
    task: Option<JoinHandle<()>>,
}

impl BackgroundSync {
    #[must_use]
    pub fn status(&self) -> BackgroundSyncStatus {
        self.status.borrow().clone()
    }

    /// Waits until the background loop publishes a different status.
    ///
    /// Returns `None` after the loop has stopped and closed its status channel.
    pub async fn changed(&mut self) -> Option<BackgroundSyncStatus> {
        self.status.changed().await.ok()?;
        Some(self.status())
    }

    pub async fn shutdown(mut self) {
        let _sent = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            let _joined = task.await;
        }
    }
}

impl Drop for BackgroundSync {
    fn drop(&mut self) {
        let _sent = self.shutdown.send(true);
    }
}

pub(crate) fn spawn<T>(client: SyncClient<T>, options: BackgroundSyncOptions) -> BackgroundSync
where
    T: Transport + Clone + 'static,
{
    let interval = nonzero(options.interval);
    let initial_retry = nonzero(options.initial_retry);
    let max_retry = nonzero(options.max_retry).max(initial_retry);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (status_tx, status_rx) = watch::channel(BackgroundSyncStatus::Starting);
    let task = tokio::spawn(async move {
        let mut attempt = 0_u32;
        let mut retry = initial_retry;
        let mut blocked = false;
        loop {
            let delay = match client.sync().await {
                Ok(report) => {
                    attempt = 0;
                    retry = initial_retry;
                    let _sent = status_tx.send(BackgroundSyncStatus::Succeeded(report));
                    jitter(interval)
                }
                Err(error) => {
                    if !error.is_retryable() {
                        let _sent = status_tx.send(BackgroundSyncStatus::Blocked {
                            error: error.to_string(),
                        });
                        blocked = true;
                        break;
                    }
                    attempt = attempt.saturating_add(1);
                    let delay = jitter(retry);
                    let _sent = status_tx.send(BackgroundSyncStatus::Retrying {
                        attempt,
                        error: error.to_string(),
                        retry_in: delay,
                    });
                    retry = retry.saturating_mul(2).min(max_retry);
                    delay
                }
            };
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() { break; }
                }
                () = tokio::time::sleep(delay) => {}
            }
        }
        if !blocked {
            let _sent = status_tx.send(BackgroundSyncStatus::Stopped);
        }
    });
    BackgroundSync {
        shutdown: shutdown_tx,
        status: status_rx,
        task: Some(task),
    }
}

fn nonzero(duration: Duration) -> Duration {
    if duration.is_zero() {
        Duration::from_millis(1)
    } else {
        duration
    }
}

fn jitter(duration: Duration) -> Duration {
    let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    let spread = millis / 5;
    if spread == 0 {
        return duration;
    }
    let width = spread.saturating_mul(2).saturating_add(1);
    let offset = OsRng.next_u64() % width;
    Duration::from_millis(millis.saturating_sub(spread).saturating_add(offset))
}
