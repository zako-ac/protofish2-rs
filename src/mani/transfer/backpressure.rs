use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct BackpressureBank {
    credits: Arc<AtomicUsize>,

    notify: Arc<Notify>,
}

impl BackpressureBank {
    pub fn new(initial_credits: usize) -> Self {
        Self {
            credits: Arc::new(AtomicUsize::new(initial_credits)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn increase_credits(&self, amount: usize) {
        tracing::debug!("Increasing credits by {}", amount);

        self.credits.fetch_add(amount, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn decrease_credits(&self, amount: usize) {
        tracing::debug!("Decreasing credits by {}", amount);
        self.credits.fetch_sub(amount, Ordering::SeqCst);
    }

    pub async fn wait_for_credit(&self) {
        loop {
            let current_credits = self.credits.load(Ordering::SeqCst);
            tracing::trace!("Current credits: {}", current_credits);
            if current_credits > 0 {
                break;
            }
            self.notify.notified().await;
        }
    }
}
