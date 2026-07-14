use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use tokio::{sync::Notify, time::timeout, time::Duration};

use super::dt_data::DtItem;

pub struct DtQueue {
    queue: ConcurrentQueue<DtItem>,
    check_memory: bool,
    max_bytes: u64,
    cur_bytes: AtomicU64,
    not_empty: Arc<Notify>,
    not_full: Arc<Notify>,
}

impl DtQueue {
    pub fn new(capacity: usize, max_bytes: u64) -> Self {
        Self {
            queue: ConcurrentQueue::bounded(capacity),
            max_bytes,
            check_memory: max_bytes > 0,
            cur_bytes: AtomicU64::new(0),
            not_empty: Arc::new(Notify::new()),
            not_full: Arc::new(Notify::new()),
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
        let item_size = item.dt_data.get_data_size();
        loop {
            if !self.queue.is_full() && !self.is_mem_full() {
                match self.queue.push(item) {
                    Ok(_) => {
                        self.cur_bytes.fetch_add(item_size, Ordering::Release);
                        self.not_empty.notify_one();
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

    #[inline(always)]
    pub fn pop(&self) -> anyhow::Result<DtItem, PopError> {
        let item = self.queue.pop()?;

        if self.queue.is_empty() {
            self.cur_bytes.store(0, Ordering::Release);
        } else {
            self.cur_bytes
                .fetch_sub(item.dt_data.get_data_size(), Ordering::Release);
        }

        self.not_full.notify_one();

        Ok(item)
    }

    pub async fn wait_for_data(&self, max_wait: Duration) {
        let _ = timeout(max_wait, self.not_empty.notified()).await;
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
    use std::{sync::Arc, time::Duration};

    use tokio::time::{sleep, timeout};

    use super::DtQueue;
    use crate::meta::dt_data::DtData;
    use crate::meta::{dt_data::DtItem, position::Position};

    #[tokio::test]
    async fn wait_for_data_wakes_after_push() {
        let queue = Arc::new(DtQueue::new(8, 0));
        let waiter_queue = queue.clone();
        let waiter = tokio::spawn(async move {
            waiter_queue.wait_for_data(Duration::from_secs(30)).await;
        });

        sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        queue
            .push(DtItem {
                dt_data: DtData::Heartbeat {},
                position: Position::None,
                data_origin_node: String::new(),
            })
            .await
            .unwrap();
        timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter should wake after push")
            .unwrap();
    }
}
