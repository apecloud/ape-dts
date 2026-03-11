use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::Notify;

use concurrent_queue::{ConcurrentQueue, PopError, PushError};

use crate::limiter::buffer_limiter::BufferLimiter;

use super::dt_data::DtItem;

pub struct DtQueue {
    queue: ConcurrentQueue<DtItem>,
    check_memory: bool,
    max_bytes: u64,
    cur_bytes: AtomicU64,
    not_full: Arc<Notify>,
    enqueue_limiter: Option<Arc<BufferLimiter>>,
    dequeue_limiter: Option<Arc<BufferLimiter>>,
}

impl DtQueue {
    pub fn new(
        capacity: usize,
        max_bytes: u64,
        enqueue_limiter: Option<Arc<BufferLimiter>>,
        dequeue_limiter: Option<Arc<BufferLimiter>>,
    ) -> Self {
        Self {
            queue: ConcurrentQueue::bounded(capacity),
            max_bytes,
            check_memory: max_bytes > 0,
            cur_bytes: AtomicU64::new(0),
            not_full: Arc::new(Notify::new()),
            enqueue_limiter,
            dequeue_limiter,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline(always)]
    pub fn get_curr_size(&self) -> u64 {
        self.cur_bytes.load(Ordering::Relaxed)
    }

    pub async fn push(&self, mut item: DtItem) -> anyhow::Result<()> {
        if let Some(enqueue_limiter) = &self.enqueue_limiter {
            enqueue_limiter.acquire(&item).await?;
        }
        let item_size = item.dt_data.get_data_size();
        loop {
            if !self.queue.is_full() && !self.is_mem_full() {
                let res = self.queue.push(item);
                match res {
                    Ok(_) => {
                        self.cur_bytes.fetch_add(item_size, Ordering::Release);
                        return Ok(());
                    }
                    Err(PushError::Full(returned_item)) => {
                        item = returned_item;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            self.not_full.notified().await;
        }
    }

    pub async fn pop(&self) -> anyhow::Result<DtItem, PopError> {
        let item = self.queue.pop()?;

        if let Some(enqueue_limiter) = &self.enqueue_limiter {
            enqueue_limiter.release(&item).await;
        }
        if let Some(dequeue_limiter) = &self.dequeue_limiter {
            // error can not be returned here, the item has been popped out,
            // and the limiter acquire should not fail.
            dequeue_limiter.acquire(&item).await.unwrap();
            dequeue_limiter.release(&item).await;
        }

        if self.queue.is_empty() {
            self.cur_bytes.store(0, Ordering::Release);
        } else {
            self.cur_bytes
                .fetch_sub(item.dt_data.get_data_size(), Ordering::Release);
        }

        self.not_full.notify_one();

        Ok(item)
    }

    #[inline(always)]
    fn is_mem_full(&self) -> bool {
        if self.check_memory {
            self.cur_bytes.load(Ordering::Acquire) > self.max_bytes
        } else {
            false
        }
    }
}
