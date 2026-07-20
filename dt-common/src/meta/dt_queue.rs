use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use tokio::{sync::Notify, time::timeout, time::Duration};

use crate::limiter::buffer_limiter::BufferLimiter;

use super::dt_data::DtItem;

#[derive(Debug, thiserror::Error)]
pub enum DtQueuePopError {
    #[error("queue pop error: {0}")]
    Queue(#[from] PopError),

    #[error("dequeue limiter error: {0}")]
    DequeueLimiter(#[source] anyhow::Error),
}

pub struct DtQueue {
    queue: ConcurrentQueue<DtItem>,
    check_memory: bool,
    max_bytes: u64,
    cur_bytes: AtomicU64,
    not_empty: Arc<Notify>,
    not_full: Arc<Notify>,
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
            max_bytes,
            check_memory: max_bytes > 0,
            cur_bytes: AtomicU64::new(0),
            not_empty: Arc::new(Notify::new()),
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
                        self.not_empty.notify_one();
                        return Ok(());
                    }
                    Err(PushError::Full(returned_item)) => {
                        item = returned_item;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            crate::runtime_trace::instrument_wait(
                "dtqueue.not_full.wait",
                self.not_full.notified(),
            )
            .await;
        }
    }

    pub async fn pop(&self) -> Result<DtItem, DtQueuePopError> {
        let item = self.queue.pop()?;

        if let Some(enqueue_limiter) = &self.enqueue_limiter {
            enqueue_limiter.release(&item).await;
        }
        let dequeue_result = if let Some(dequeue_limiter) = &self.dequeue_limiter {
            match dequeue_limiter.acquire(&item).await {
                Ok(()) => {
                    dequeue_limiter.release(&item).await;
                    Ok(())
                }
                Err(error) => Err(DtQueuePopError::DequeueLimiter(error)),
            }
        } else {
            Ok(())
        };

        if self.queue.is_empty() {
            self.cur_bytes.store(0, Ordering::Release);
        } else {
            self.cur_bytes
                .fetch_sub(item.dt_data.get_data_size(), Ordering::Release);
        }

        self.not_full.notify_one();

        dequeue_result?;
        Ok(item)
    }

    pub async fn wait_for_data(&self, max_wait: Duration) {
        let notified = crate::runtime_trace::instrument_wait(
            "dtqueue.not_empty.wait",
            self.not_empty.notified(),
        );
        let _ = timeout(max_wait, notified).await;
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
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use futures::FutureExt;
    use tokio::{
        sync::Notify,
        time::{sleep, timeout},
    };

    use super::{DtQueue, DtQueuePopError};
    use crate::{
        config::limiter_config::RateLimiterConfig,
        limiter::buffer_limiter::BufferLimiter,
        meta::{
            col_value::ColValue,
            dt_data::{DtData, DtItem},
            position::Position,
            row_data::RowData,
            row_type::RowType,
        },
    };

    #[tokio::test]
    async fn notify_one_before_notified_completes_next_waiter_once() {
        let notify = Notify::new();

        notify.notify_one();

        assert!(notify.notified().now_or_never().is_some());
        assert!(notify.notified().now_or_never().is_none());
    }

    fn heartbeat_item() -> DtItem {
        DtItem {
            dt_data: DtData::Heartbeat {},
            position: Position::None,
            data_origin_node: String::new(),
        }
    }

    fn bytes_item(data_size: usize) -> DtItem {
        DtItem {
            dt_data: DtData::Dml {
                row_data: RowData {
                    schema: "db".to_string(),
                    tb: "tb".to_string(),
                    chunk_id: 0,
                    row_type: RowType::Insert,
                    before: None,
                    after: Some(HashMap::from([(
                        "c1".to_string(),
                        ColValue::RawString(Vec::new()),
                    )])),
                    data_size,
                    is_not_origin: false,
                },
            },
            position: Position::None,
            data_origin_node: String::new(),
        }
    }

    #[tokio::test]
    async fn wait_for_data_wakes_after_push() {
        let queue = Arc::new(DtQueue::new(8, 0, None, None));
        let waiter_queue = queue.clone();
        let waiter = tokio::spawn(async move {
            waiter_queue.wait_for_data(Duration::from_secs(30)).await;
        });

        sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        queue.push(heartbeat_item()).await.unwrap();
        timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter should wake after push")
            .unwrap();
    }

    #[tokio::test]
    async fn pop_returns_dequeue_limiter_error_without_panicking() {
        let rate_config = RateLimiterConfig {
            max_mbps: 1,
            max_rps: 0,
        };
        let dequeue_limiter = BufferLimiter::from_config(Some(&rate_config), None).unwrap();
        let queue = DtQueue::new(1, 0, None, Some(dequeue_limiter));
        queue.push(bytes_item(2 * 1024 * 1024)).await.unwrap();

        let error = queue.pop().await.unwrap_err();

        assert!(matches!(error, DtQueuePopError::DequeueLimiter(_)));
        assert!(queue.is_empty());
        assert_eq!(queue.get_curr_size(), 0);
    }
}
