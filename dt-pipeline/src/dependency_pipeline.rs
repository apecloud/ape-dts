use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use tokio::task::JoinSet;
use tokio::time::{timeout, Duration};

use crate::Pipeline;
use dt_common::{
    limiter::buffer_limiter::BufferLimiter,
    log_info,
    meta::{
        dt_data::{DtData, DtItem},
        struct_meta::struct_data::StructData,
    },
    monitor::{counter_type::CounterType, task_monitor_handle::TaskMonitorHandle},
    queue::{DtQueue, DtQueueBatch},
};
use dt_connector::Sinker;

/// Pipeline for dependency-aware struct execution.
///
/// Queue admission and dependency resolution happen on the writer side. This
/// pipeline consumes only the ready frontier and acknowledges nodes after the
/// sink batch succeeds.
pub struct DependencyPipeline {
    pub queue: DtQueue,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: Arc<AtomicBool>,
    pub monitor: TaskMonitorHandle,
    pub max_batch_size: usize,
    pub dequeue_limiter: Option<BufferLimiter>,
}

#[async_trait]
impl Pipeline for DependencyPipeline {
    async fn start(&mut self) -> anyhow::Result<()> {
        log_info!(
            "DependencyPipeline starts, parallel_size: {}",
            self.sinkers.len()
        );

        loop {
            if self.shut_down.load(Ordering::Acquire) && self.queue.is_empty().await {
                break;
            }

            let Some(ready) = self.pop_ready_batch_with_timeout().await? else {
                continue;
            };

            let (ready, ack) = ready.into_parts();
            if let Some(dequeue_limiter) = &self.dequeue_limiter {
                for item in &ready {
                    if let Err(error) = dequeue_limiter.acquire(item).await {
                        self.queue
                            .fail(format!("dequeue rate limit failed: {error:#}"))
                            .await;
                        return Err(error);
                    }
                    dequeue_limiter.release(item).await;
                }
            }

            let data = match Self::take_struct_data(ready) {
                Ok(data) => data,
                Err(error) => {
                    self.queue
                        .fail(format!("invalid ready node: {error:#}"))
                        .await;
                    return Err(error);
                }
            };
            let count = data.len() as u64;

            if let Err(error) = self.sink_ready_batch(data).await {
                self.queue
                    .fail(format!("struct sink failed: {error:#}"))
                    .await;
                return Err(error);
            }

            self.queue.ack(ack).await?;
            self.monitor
                .add_counter(
                    self.monitor.default_task_id(),
                    CounterType::SinkedRecordTotal,
                    count,
                )
                .await;
        }

        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.queue.close().await;
        for sinker in &mut self.sinkers {
            sinker.lock().await.close_with_position(None).await?;
        }
        Ok(())
    }
}

impl DependencyPipeline {
    async fn pop_ready_batch_with_timeout(&self) -> anyhow::Result<Option<DtQueueBatch>> {
        match timeout(
            Duration::from_secs(1),
            self.queue.pop_batch(self.max_batch_size),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Ok(None),
        }
    }

    fn take_struct_data(ready: Vec<DtItem>) -> anyhow::Result<Vec<StructData>> {
        ready
            .into_iter()
            .map(|item| match item.dt_data {
                DtData::Struct { struct_data } => Ok(struct_data),
                _ => anyhow::bail!("dependency struct pipeline received a non-struct item"),
            })
            .collect()
    }

    async fn sink_ready_batch(&self, data: Vec<StructData>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if self.sinkers.is_empty() {
            anyhow::bail!("dependency pipeline requires at least one sinker");
        }

        let mut tasks = JoinSet::new();
        for (index, struct_data) in data.into_iter().enumerate() {
            let sinker = self.sinkers[index % self.sinkers.len()].clone();
            tasks.spawn(async move { sinker.lock().await.sink_struct(vec![struct_data]).await });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use dt_common::{
        meta::{
            dt_data::{DtData, DtItem},
            position::Position,
            struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
        },
        monitor::{task_monitor::MonitorType, task_monitor_handle::TaskMonitorHandle},
        queue::{
            dependency_queue::{DependencyInput, DependencyQueue, DependencySpec},
            DtQueue,
        },
    };
    use dt_connector::Sinker;
    use tokio::{
        sync::Barrier,
        time::{timeout, Duration},
    };

    use super::{DependencyPipeline, Pipeline};

    struct CountingSinker(Arc<AtomicUsize>);

    struct ConcurrentSinker {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
    }

    #[async_trait]
    impl Sinker for ConcurrentSinker {
        async fn sink_struct(&mut self, _data: Vec<StructData>) -> anyhow::Result<()> {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_active.fetch_max(active, Ordering::AcqRel);
            self.barrier.wait().await;
            self.active.fetch_sub(1, Ordering::AcqRel);
            Ok(())
        }
    }

    #[async_trait]
    impl Sinker for CountingSinker {
        async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
            self.0.fetch_add(data.len(), Ordering::Release);
            Ok(())
        }
    }

    fn struct_item() -> DtItem {
        DtItem {
            dt_data: DtData::Struct {
                struct_data: StructData {
                    schema: String::new(),
                    statement: StructStatement::Unknown,
                },
            },
            position: Position::None,
            data_origin_node: String::new(),
        }
    }

    #[tokio::test]
    async fn successful_sink_acknowledges_ready_nodes() {
        let queue = Arc::new(DependencyQueue::new(1));
        queue
            .push(DependencyInput::new(
                struct_item(),
                DependencySpec::default(),
            ))
            .await
            .unwrap();

        let sunk = Arc::new(AtomicUsize::new(0));
        let mut pipeline = DependencyPipeline {
            queue: DtQueue::Dependency(queue.clone()),
            sinkers: vec![Arc::new(async_mutex::Mutex::new(
                Box::new(CountingSinker(sunk.clone())) as Box<dyn Sinker + Send>,
            ))],
            shut_down: Arc::new(AtomicBool::new(true)),
            monitor: TaskMonitorHandle::noop(MonitorType::Pipeline),
            max_batch_size: 1,
            dequeue_limiter: None,
        };

        pipeline.start().await.unwrap();
        assert_eq!(sunk.load(Ordering::Acquire), 1);
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn missing_sinker_is_rejected_instead_of_silently_acking() {
        let queue = Arc::new(DependencyQueue::new(1));
        queue
            .push(DependencyInput::new(
                struct_item(),
                DependencySpec::default(),
            ))
            .await
            .unwrap();

        let mut pipeline = DependencyPipeline {
            queue: DtQueue::Dependency(queue.clone()),
            sinkers: Vec::new(),
            shut_down: Arc::new(AtomicBool::new(true)),
            monitor: TaskMonitorHandle::noop(MonitorType::Pipeline),
            max_batch_size: 1,
            dequeue_limiter: None,
        };

        assert!(pipeline.start().await.is_err());
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn ready_frontier_is_sunk_concurrently() {
        let queue = Arc::new(DependencyQueue::new(2));
        for _ in 0..2 {
            queue
                .push(DependencyInput::new(
                    struct_item(),
                    DependencySpec::default(),
                ))
                .await
                .unwrap();
        }

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let sinkers = (0..2)
            .map(|_| {
                Arc::new(async_mutex::Mutex::new(Box::new(ConcurrentSinker {
                    active: active.clone(),
                    max_active: max_active.clone(),
                    barrier: barrier.clone(),
                })
                    as Box<dyn Sinker + Send>))
            })
            .collect();
        let mut pipeline = DependencyPipeline {
            queue: DtQueue::Dependency(queue.clone()),
            sinkers,
            shut_down: Arc::new(AtomicBool::new(true)),
            monitor: TaskMonitorHandle::noop(MonitorType::Pipeline),
            max_batch_size: 2,
            dequeue_limiter: None,
        };

        timeout(Duration::from_secs(1), pipeline.start())
            .await
            .expect("ready nodes were not dispatched concurrently")
            .unwrap();
        assert_eq!(max_active.load(Ordering::Acquire), 2);
        assert_eq!(queue.len().await, 0);
    }
}
