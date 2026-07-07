use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

use concurrent_queue::{ConcurrentQueue, PopError, PushError};

use crate::limiter::buffer_limiter::BufferLimiter;

use super::dt_data::DtItem;

pub struct DtQueue {
    queue: ConcurrentQueue<DtItem>,
    check_memory: bool,
    max_bytes: u64,
    cur_bytes: AtomicU64,
    not_full: Notify,
    is_empty: Notify,
    enqueue_limiter: Option<BufferLimiter>,
    dequeue_limiter: Option<BufferLimiter>,
}

impl DtQueue {
    pub fn new(
        capacity: usize,
        max_bytes: u64,
        enqueue_limiter: Option<BufferLimiter>,
        dequeue_limiter: Option<BufferLimiter>,
    ) -> Self {
        Self {
            queue: ConcurrentQueue::bounded(capacity),
            check_memory: max_bytes > 0,
            max_bytes,
            cur_bytes: AtomicU64::new(0),
            not_full: Notify::new(),
            is_empty: Notify::new(),
            enqueue_limiter,
            dequeue_limiter,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub async fn wait_until_empty(&self) {
        while !self.queue.is_empty() {
            self.is_empty.notified().await;
        }
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
            self.is_empty.notify_one();
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

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn notify_one_before_notified_completes_next_waiter_once() {
        let notify = Notify::new();

        notify.notify_one();

        assert!(notify.notified().now_or_never().is_some());
        assert!(notify.notified().now_or_never().is_none());
    }
}
