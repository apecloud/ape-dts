use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::yield_now;

use concurrent_queue::{ConcurrentQueue, PopError, PushError};

use super::dt_data::DtItem;

pub struct DtQueue {
    queue: ConcurrentQueue<DtItem>,
    check_memory: bool,
    max_bytes: u64,
    cur_bytes: AtomicU64,
}

impl DtQueue {
    pub fn new(capacity: usize, max_bytes: u64) -> Self {
        Self {
            queue: ConcurrentQueue::bounded(capacity),
            max_bytes,
            check_memory: max_bytes > 0,
            cur_bytes: AtomicU64::new(0),
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

    #[inline(always)]
    pub async fn push(&self, mut item: DtItem) -> anyhow::Result<()> {
        while self.queue.is_full() {
            yield_now().await;
        }

        if self.check_memory {
            while self.cur_bytes.load(Ordering::Acquire) > self.max_bytes {
                yield_now().await;
            }
        }
        self.cur_bytes
            .fetch_add(item.dt_data.get_data_size(), Ordering::Release);

        loop {
            let res = self.queue.push(item);
            match res {
                Ok(_) => break,
                Err(PushError::Full(returned_item)) => {
                    item = returned_item;
                    yield_now().await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    #[inline(always)]
    pub fn pop(&self) -> anyhow::Result<DtItem, PopError> {
        let item = self.queue.pop()?;

        if self.queue.is_empty() {
            self.cur_bytes.store(0, Ordering::Release);
        } else {
            self.cur_bytes
                .fetch_sub(item.dt_data.get_data_size(), Ordering::Release);
        }

        Ok(item)
    }
}
