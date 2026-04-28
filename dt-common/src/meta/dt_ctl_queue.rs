use std::sync::Arc;

use concurrent_queue::{ConcurrentQueue, PopError};
use tokio::sync::Notify;

use crate::meta::dt_ctl::DtCtl;

pub struct DtCtlQueue {
    queue: ConcurrentQueue<DtCtl>,
    not_empty: Arc<Notify>,
}

impl DtCtlQueue {
    pub fn new() -> Self {
        Self {
            queue: ConcurrentQueue::unbounded(),
            not_empty: Arc::new(Notify::new()),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn push(&self, ctl: DtCtl) -> anyhow::Result<()> {
        self.queue.push(ctl)?;
        self.not_empty.notify_one();
        Ok(())
    }

    pub fn try_pop(&self) -> Option<DtCtl> {
        self.queue.pop().ok()
    }

    #[allow(dead_code)]
    pub async fn pop(&self) -> anyhow::Result<DtCtl, PopError> {
        loop {
            match self.queue.pop() {
                Ok(ctl) => return Ok(ctl),
                Err(PopError::Empty) => self.not_empty.notified().await,
                Err(err) => return Err(err),
            }
        }
    }
}

impl Default for DtCtlQueue {
    fn default() -> Self {
        Self::new()
    }
}
