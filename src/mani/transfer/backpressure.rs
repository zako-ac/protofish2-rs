use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct BackpressureBank {
    credits: Arc<AtomicUsize>,
    notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl BackpressureBank {
    pub fn new(initial_credits: usize) -> Self {
        Self {
            credits: Arc::new(AtomicUsize::new(initial_credits)),
            notify: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn increase_credits(&self, amount: usize) {
        tracing::trace!("Increasing credits by {}", amount);

        self.credits.fetch_add(amount, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn decrease_credits(&self, amount: usize) {
        tracing::trace!("Decreasing credits by {}", amount);
        self.credits.fetch_sub(amount, Ordering::SeqCst);
    }

    pub fn signal_shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Returns `true` if credit was obtained, `false` if the bank was shut down.
    pub async fn wait_for_credit(&self) -> bool {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return false;
            }
            let current_credits = self.credits.load(Ordering::SeqCst);
            tracing::trace!("Current credits: {}", current_credits);
            if current_credits > 0 {
                return true;
            }
            self.notify.notified().await;
        }
    }
}
