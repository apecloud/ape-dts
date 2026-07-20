use async_trait::async_trait;
use dt_common::{
    meta::{
        dcl_meta::dcl_data::DclData, ddl_meta::ddl_data::DdlData, dt_data::DtItem,
        row_data::RowData, struct_meta::struct_data::StructData,
    },
    monitor::sinker_worker_metrics::SinkerWorkerRecorder,
};

use crate::Sinker;

pub struct BusyTrackingSinker {
    inner: Box<dyn Sinker + Send>,
    recorder: SinkerWorkerRecorder,
}

impl BusyTrackingSinker {
    /// Tracks the full trait call while this sinker is unavailable to other work.
    /// The outer mutex has already been acquired before any method is entered.
    pub fn new(inner: Box<dyn Sinker + Send>, recorder: SinkerWorkerRecorder) -> Self {
        Self { inner, recorder }
    }
}

#[async_trait]
impl Sinker for BusyTrackingSinker {
    async fn sink_dml(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        let _guard = self.recorder.enter();
        self.inner.sink_dml(data, batch).await
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, batch: bool) -> anyhow::Result<()> {
        let _guard = self.recorder.enter();
        self.inner.sink_ddl(data, batch).await
    }

    async fn sink_dcl(&mut self, data: Vec<DclData>, batch: bool) -> anyhow::Result<()> {
        let _guard = self.recorder.enter();
        self.inner.sink_dcl(data, batch).await
    }

    async fn sink_raw(&mut self, data: Vec<DtItem>, batch: bool) -> anyhow::Result<()> {
        let _guard = self.recorder.enter();
        self.inner.sink_raw(data, batch).await
    }

    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let _guard = self.recorder.enter();
        self.inner.sink_struct(data).await
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> anyhow::Result<()> {
        let _guard = self.recorder.enter();
        self.inner.refresh_meta(data).await
    }

    async fn handle_control_item(&mut self, item: &DtItem) -> anyhow::Result<()> {
        let _guard = self.recorder.enter();
        self.inner.handle_control_item(item).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.inner.close().await
    }

    fn get_id(&self) -> String {
        self.inner.get_id()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        hint::black_box,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Instant,
    };

    use anyhow::bail;
    use async_trait::async_trait;
    use dt_common::{
        meta::{
            dcl_meta::dcl_data::DclData,
            ddl_meta::ddl_data::DdlData,
            dt_data::{DtData, DtItem},
            position::Position,
            row_data::RowData,
            struct_meta::struct_data::StructData,
        },
        monitor::sinker_worker_metrics::SinkerWorkerMetrics,
    };
    use tokio::sync::Notify;

    use crate::Sinker;

    use super::BusyTrackingSinker;

    struct TestSinker {
        fail: Arc<AtomicBool>,
        metrics: Option<Arc<SinkerWorkerMetrics>>,
    }

    impl TestSinker {
        fn assert_busy(&self, expected: u64) {
            if let Some(metrics) = self.metrics.as_ref() {
                assert_eq!(metrics.snapshot().busy, expected);
            }
        }
    }

    #[async_trait]
    impl Sinker for TestSinker {
        async fn sink_dml(&mut self, _data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
            self.assert_busy(1);
            if self.fail.load(Ordering::Relaxed) {
                bail!("expected failure");
            }
            Ok(())
        }

        async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
            self.assert_busy(1);
            Ok(())
        }

        async fn sink_dcl(&mut self, _data: Vec<DclData>, _batch: bool) -> anyhow::Result<()> {
            self.assert_busy(1);
            Ok(())
        }

        async fn sink_raw(&mut self, _data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
            self.assert_busy(1);
            Ok(())
        }

        async fn sink_struct(&mut self, _data: Vec<StructData>) -> anyhow::Result<()> {
            self.assert_busy(1);
            Ok(())
        }

        async fn refresh_meta(&mut self, _data: Vec<DdlData>) -> anyhow::Result<()> {
            self.assert_busy(1);
            Ok(())
        }

        async fn handle_control_item(&mut self, _item: &DtItem) -> anyhow::Result<()> {
            self.assert_busy(1);
            Ok(())
        }

        async fn close(&mut self) -> anyhow::Result<()> {
            self.assert_busy(0);
            Ok(())
        }

        fn get_id(&self) -> String {
            self.assert_busy(0);
            "test-sinker".to_owned()
        }
    }

    #[tokio::test]
    async fn releases_busy_worker_after_success_and_error() {
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let recorder = metrics.register_worker();
        let fail = Arc::new(AtomicBool::new(false));
        let mut sinker = BusyTrackingSinker::new(
            Box::new(TestSinker {
                fail: fail.clone(),
                metrics: Some(metrics.clone()),
            }),
            recorder,
        );

        sinker.sink_dml(Vec::new(), false).await.unwrap();
        assert_eq!(metrics.snapshot().busy, 0);

        fail.store(true, Ordering::Relaxed);
        assert!(sinker.sink_dml(Vec::new(), false).await.is_err());
        assert_eq!(metrics.snapshot().busy, 0);
    }

    #[tokio::test]
    async fn get_id_and_close_are_not_counted_as_busy() {
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let recorder = metrics.register_worker();
        let mut sinker = BusyTrackingSinker::new(
            Box::new(TestSinker {
                fail: Arc::new(AtomicBool::new(false)),
                metrics: Some(metrics.clone()),
            }),
            recorder,
        );

        assert_eq!(sinker.get_id(), "test-sinker");
        sinker.close().await.unwrap();

        assert_eq!(metrics.snapshot().busy, 0);
    }

    #[tokio::test]
    async fn counts_every_operational_method() {
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let recorder = metrics.register_worker();
        let mut sinker = BusyTrackingSinker::new(
            Box::new(TestSinker {
                fail: Arc::new(AtomicBool::new(false)),
                metrics: Some(metrics.clone()),
            }),
            recorder,
        );
        let control_item = DtItem {
            dt_data: DtData::Heartbeat {},
            position: Position::None,
            data_origin_node: String::new(),
        };

        sinker.sink_dml(Vec::new(), false).await.unwrap();
        sinker.sink_ddl(Vec::<DdlData>::new(), false).await.unwrap();
        sinker.sink_dcl(Vec::<DclData>::new(), false).await.unwrap();
        sinker.sink_raw(Vec::new(), false).await.unwrap();
        sinker.sink_struct(Vec::new()).await.unwrap();
        sinker.refresh_meta(Vec::new()).await.unwrap();
        sinker.handle_control_item(&control_item).await.unwrap();
        assert_eq!(metrics.snapshot().busy, 0);
    }

    struct BlockingSinker {
        entered: Arc<Notify>,
    }

    #[async_trait]
    impl Sinker for BlockingSinker {
        async fn sink_dml(&mut self, _data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
            self.entered.notify_one();
            pending::<()>().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn cancellation_releases_busy_and_mutex_wait_is_not_counted() {
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let entered = Arc::new(Notify::new());
        let sinker: Box<dyn Sinker + Send> = Box::new(BusyTrackingSinker::new(
            Box::new(BlockingSinker {
                entered: entered.clone(),
            }),
            metrics.register_worker(),
        ));
        let sinker = Arc::new(async_mutex::Mutex::new(sinker));

        let first_sinker = sinker.clone();
        let first =
            tokio::spawn(
                async move { first_sinker.lock().await.sink_dml(Vec::new(), false).await },
            );
        entered.notified().await;
        assert_eq!(metrics.snapshot().busy, 1);

        let waiting_sinker = sinker.clone();
        let waiting = tokio::spawn(async move {
            waiting_sinker
                .lock()
                .await
                .sink_dml(Vec::new(), false)
                .await
        });
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert_eq!(metrics.snapshot().busy, 1);

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(metrics.snapshot().busy, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "manual release-mode end-to-end overhead measurement"]
    async fn measures_decorator_end_to_end_cost() {
        const ITERATIONS: u32 = 500_000;

        let fail = Arc::new(AtomicBool::new(false));
        let mut direct: Box<dyn Sinker + Send> = Box::new(TestSinker {
            fail: fail.clone(),
            metrics: None,
        });
        let metrics = Arc::new(SinkerWorkerMetrics::default());
        let mut tracked: Box<dyn Sinker + Send> = Box::new(BusyTrackingSinker::new(
            Box::new(TestSinker {
                fail,
                metrics: None,
            }),
            metrics.register_worker(),
        ));

        let started = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(direct.sink_dml(Vec::new(), false).await.unwrap());
        }
        let direct_ns = started.elapsed().as_nanos() as f64 / f64::from(ITERATIONS);

        let started = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(tracked.sink_dml(Vec::new(), false).await.unwrap());
        }
        let tracked_ns = started.elapsed().as_nanos() as f64 / f64::from(ITERATIONS);

        eprintln!(
            "sinker decorator: direct={direct_ns:.2} ns/call, tracked={tracked_ns:.2} ns/call, delta={:.2} ns/call",
            tracked_ns - direct_ns
        );
    }
}
