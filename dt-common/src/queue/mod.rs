use std::sync::Arc;

use anyhow::bail;
use concurrent_queue::PopError;
use tokio::time::Duration;

use crate::meta::dt_data::DtItem;

pub mod basic_queue;
pub mod dependency_queue;

use basic_queue::BasicQueue;
use dependency_queue::{DependencyQueue, NodeId, ReadyNode};

enum QueueReceipt {
    Basic,
    Dependency(NodeId),
}

pub struct DtQueueItem {
    pub item: DtItem,
    receipt: QueueReceipt,
}

#[derive(Default)]
pub struct DtQueueAck {
    dependency_node_ids: Vec<NodeId>,
}

#[derive(Default)]
pub struct DtQueueBatch {
    pub items: Vec<DtItem>,
    ack: DtQueueAck,
}

impl DtQueueBatch {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Vec::with_capacity(capacity),
            ack: DtQueueAck {
                dependency_node_ids: Vec::with_capacity(capacity),
            },
        }
    }

    pub fn push(&mut self, queued: DtQueueItem) {
        self.items.push(queued.item);
        self.retain_receipt(queued.receipt);
    }

    /// Keeps completion ownership for an item intentionally consumed by a
    /// drain algorithm without forwarding its payload to a sinker.
    pub fn consume(&mut self, queued: DtQueueItem) {
        self.retain_receipt(queued.receipt);
    }

    fn retain_receipt(&mut self, receipt: QueueReceipt) {
        if let QueueReceipt::Dependency(id) = receipt {
            self.ack.dependency_node_ids.push(id);
        }
    }

    pub fn into_parts(self) -> (Vec<DtItem>, DtQueueAck) {
        (self.items, self.ack)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Queue implementation selected once when a task is created.
///
/// Keeping this as a concrete enum gives extractors and pipelines a common API
/// without a trait object or an async-trait allocation on hot paths.
#[derive(Clone)]
pub enum DtQueue {
    Basic(Arc<BasicQueue>),
    Dependency(Arc<DependencyQueue<DtItem>>),
}

impl DtQueue {
    #[inline]
    pub async fn push(&self, item: DtItem) -> anyhow::Result<()> {
        match self {
            Self::Basic(queue) => queue.push(item).await,
            Self::Dependency(queue) => queue.push_item(item).await.map(|_| ()),
        }
    }

    pub async fn push_batch(&self, items: Vec<DtItem>) -> anyhow::Result<()> {
        match self {
            Self::Basic(queue) => {
                for item in items {
                    queue.push(item).await?;
                }
                Ok(())
            }
            Self::Dependency(queue) => queue.push_items_batch(items).await.map(|_| ()),
        }
    }

    pub async fn wait_until_empty(&self) {
        match self {
            Self::Basic(queue) => queue.wait_until_empty().await,
            Self::Dependency(queue) => queue.wait_until_empty().await,
        }
    }

    /// Non-blocking consumer pop. Queue-specific receipt data stays private and
    /// is returned to the queue through `ack` or `nack`.
    pub async fn try_pop(&self) -> anyhow::Result<Option<DtQueueItem>> {
        match self {
            Self::Basic(queue) => match queue.pop().await {
                Ok(item) => Ok(Some(DtQueueItem {
                    item,
                    receipt: QueueReceipt::Basic,
                })),
                Err(DtQueuePopError::Queue(PopError::Empty)) => Ok(None),
                Err(error) => Err(error.into()),
            },
            Self::Dependency(queue) => Ok(queue.try_pop_ready().await?.map(|node| DtQueueItem {
                item: node.payload,
                receipt: QueueReceipt::Dependency(node.id),
            })),
        }
    }

    /// Basic queues are drained non-blockingly. Dependency queues wait until a
    /// ready frontier exists or the queue is closed.
    pub async fn pop_batch(&self, limit: usize) -> anyhow::Result<Option<DtQueueBatch>> {
        match self {
            Self::Basic(_) => {
                let mut batch = DtQueueBatch::with_capacity(limit);
                while batch.items.len() < limit {
                    let Some(item) = self.try_pop().await? else {
                        break;
                    };
                    batch.push(item);
                }
                Ok(Some(batch))
            }
            Self::Dependency(queue) => {
                let Some(nodes) = queue.pop_ready_batch(limit).await? else {
                    return Ok(None);
                };
                let mut batch = DtQueueBatch::with_capacity(nodes.len());
                for node in nodes {
                    batch.push(DtQueueItem {
                        item: node.payload,
                        receipt: QueueReceipt::Dependency(node.id),
                    });
                }
                Ok(Some(batch))
            }
        }
    }

    #[inline]
    pub async fn ack(&self, ack: DtQueueAck) -> anyhow::Result<()> {
        match self {
            Self::Basic(_) => {
                debug_assert!(ack.dependency_node_ids.is_empty());
                Ok(())
            }
            Self::Dependency(queue) => queue.ack_batch(ack.dependency_node_ids).await,
        }
    }

    pub async fn nack(&self, queued: DtQueueItem) -> anyhow::Result<()> {
        match (self, queued.receipt) {
            (Self::Basic(queue), QueueReceipt::Basic) => queue.push(queued.item).await,
            (Self::Dependency(queue), QueueReceipt::Dependency(id)) => {
                queue
                    .nack(ReadyNode {
                        id,
                        payload: queued.item,
                    })
                    .await
            }
            _ => bail!("queue item receipt does not belong to this queue"),
        }
    }

    pub async fn fail(&self, error: impl Into<Arc<str>>) {
        if let Self::Dependency(queue) = self {
            queue.fail(error).await;
        }
    }

    pub async fn close(&self) {
        if let Self::Dependency(queue) = self {
            queue.close().await;
        }
    }

    pub async fn is_empty(&self) -> bool {
        match self {
            Self::Basic(queue) => queue.is_empty(),
            Self::Dependency(queue) => queue.is_empty().await,
        }
    }

    pub async fn len(&self) -> usize {
        match self {
            Self::Basic(queue) => queue.len(),
            Self::Dependency(queue) => queue.len().await,
        }
    }

    pub fn is_full(&self) -> bool {
        match self {
            Self::Basic(queue) => queue.is_full(),
            Self::Dependency(queue) => queue.is_full(),
        }
    }

    pub fn get_curr_size(&self) -> u64 {
        match self {
            Self::Basic(queue) => queue.get_curr_size(),
            Self::Dependency(_) => 0,
        }
    }

    pub async fn wait_for_data(&self, max_wait: Duration) {
        match self {
            Self::Basic(queue) => queue.wait_for_data(max_wait).await,
            Self::Dependency(queue) => queue.wait_for_data(max_wait).await,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DtQueuePopError {
    #[error("queue pop error: {0}")]
    Queue(#[from] PopError),

    #[error("dequeue limiter error: {0}")]
    DequeueLimiter(#[source] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        meta::{dt_data::DtData, position::Position},
        queue::dependency_queue::{DependencyInput, DependencySpec},
    };

    fn item() -> DtItem {
        DtItem {
            dt_data: DtData::Heartbeat {},
            position: Position::None,
            data_origin_node: String::new(),
        }
    }

    #[tokio::test]
    async fn dependency_receipt_is_acked_through_queue_enum() {
        let dependency = Arc::new(DependencyQueue::new(1));
        dependency
            .push(DependencyInput::new(item(), DependencySpec::default()))
            .await
            .unwrap();
        let queue = DtQueue::Dependency(dependency.clone());

        let batch = queue.pop_batch(1).await.unwrap().unwrap();
        let (items, ack) = batch.into_parts();
        assert_eq!(items.len(), 1);
        assert_eq!(dependency.len().await, 1);

        queue.ack(ack).await.unwrap();
        assert_eq!(dependency.len().await, 0);
    }

    #[tokio::test]
    async fn dependency_item_can_be_nacked_through_queue_enum() {
        let dependency = Arc::new(DependencyQueue::new(1));
        dependency
            .push(DependencyInput::new(item(), DependencySpec::default()))
            .await
            .unwrap();
        let queue = DtQueue::Dependency(dependency);

        let queued = queue.try_pop().await.unwrap().unwrap();
        queue.nack(queued).await.unwrap();
        let retried = queue.try_pop().await.unwrap().unwrap();
        let mut batch = DtQueueBatch::default();
        batch.push(retried);
        let (_, ack) = batch.into_parts();
        queue.ack(ack).await.unwrap();
        assert!(queue.is_empty().await);
    }
}
