use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::select;

use crate::{log_info, monitor::FlushableMonitor, utils::time_util::TimeUtil};

pub struct MonitorUtil {}

impl MonitorUtil {
    pub async fn flush_monitors_generic<T1, T2>(
        interval_secs: u64,
        shut_down: Arc<AtomicBool>,
        t1_monitors: &[Arc<T1>],
        t2_monitors: &[Arc<T2>],
    ) where
        T1: FlushableMonitor + Send + Sync + 'static,
        T2: FlushableMonitor + Send + Sync + 'static,
    {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await;

        loop {
            if shut_down.load(Ordering::Acquire) {
                Self::do_flush_monitors(t1_monitors, t2_monitors).await;
                break;
            }

            select! {
                _ = interval.tick() => {
                    Self::do_flush_monitors(t1_monitors, t2_monitors).await;
                }
                _ = Self::wait_for_shutdown(shut_down.clone()) => {
                    log_info!("task shutdown detected, do final flush");
                    Self::do_flush_monitors(t1_monitors, t2_monitors).await;
                    break;
                }
            }
        }
    }

    async fn wait_for_shutdown(shut_down: Arc<AtomicBool>) {
        loop {
            if shut_down.load(Ordering::Acquire) {
                break;
            }
            TimeUtil::sleep_millis(100).await;
        }
    }

    async fn do_flush_monitors<T1, T2>(t1_monitors: &[Arc<T1>], t2_monitors: &[Arc<T2>])
    where
        T1: FlushableMonitor + Send + Sync + 'static,
        T2: FlushableMonitor + Send + Sync + 'static,
    {
        let t1_futures = t1_monitors
            .iter()
            .map(|monitor| {
                let monitor = monitor.clone();
                async move { monitor.flush().await }
            })
            .collect::<Vec<_>>();

        let t2_futures = t2_monitors
            .iter()
            .map(|monitor| {
                let monitor = monitor.clone();
                async move { monitor.flush().await }
            })
            .collect::<Vec<_>>();

        tokio::join!(
            futures::future::join_all(t1_futures),
            futures::future::join_all(t2_futures)
        );
    }
}
