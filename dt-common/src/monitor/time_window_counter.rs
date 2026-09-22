use std::collections::LinkedList;

use tokio::sync::RwLock;

use super::{counter::Counter, task_metrics::TaskMetricValue};
use crate::utils::limit_queue::LimitedQueue;

#[derive(Default)]
pub struct WindowCounterStatistics {
    pub latest: TaskMetricValue,
    pub sum: TaskMetricValue,
    pub max: TaskMetricValue,
    pub min: TaskMetricValue,
    pub avg_by_count: TaskMetricValue,
    pub max_by_sec: TaskMetricValue,
    pub min_by_sec: TaskMetricValue,
    pub avg_by_sec: TaskMetricValue,
    pub count: u64,
}

pub struct TimeWindowCounter {
    pub time_window_secs: u64,
    pub max_sub_count: u64,
    pub counters: RwLock<LinkedList<Counter>>,
}

impl TimeWindowCounter {
    pub fn new(time_window_secs: u64, max_sub_count: u64) -> Self {
        Self {
            time_window_secs,
            max_sub_count,
            counters: RwLock::new(LinkedList::new()),
        }
    }

    #[inline(always)]
    pub async fn adds(&self, values: &LimitedQueue<(u64, u64)>) -> &Self {
        if values.is_empty() {
            return self;
        }

        let mut counters = self.counters.write().await;
        while let Some(front) = counters.front() {
            if front.timestamp.elapsed().as_secs() >= self.time_window_secs {
                counters.pop_front();
            } else {
                break;
            }
        }

        while counters.len() as u64 + values.len() as u64 >= self.max_sub_count {
            counters.pop_front();
        }

        for (value, count) in values.iter() {
            if *count == 0 {
                continue;
            }
            counters.push_back(Counter::new(*value, *count));
        }
        self
    }

    #[inline(always)]
    pub async fn add(&self, value: impl Into<TaskMetricValue> + Send, count: u64) -> &Self {
        let value = value.into();
        let mut counters = self.counters.write().await;

        while let Some(front) = counters.front() {
            if front.timestamp.elapsed().as_secs() >= self.time_window_secs {
                counters.pop_front();
            } else {
                break;
            }
        }

        while counters.len() as u64 >= self.max_sub_count {
            counters.pop_front();
        }
        counters.push_back(Counter::new(value, count));
        self
    }

    #[inline(always)]
    pub async fn statistics(&self) -> WindowCounterStatistics {
        self.statistics_in_window(self.time_window_secs).await
    }

    #[inline(always)]
    pub async fn statistics_in_window(&self, time_window_secs: u64) -> WindowCounterStatistics {
        let counters = self.counters.read().await;
        if counters.is_empty() {
            return WindowCounterStatistics::default();
        }

        let mut statistics = WindowCounterStatistics {
            min: u64::MAX.into(),
            min_by_sec: u64::MAX.into(),
            ..Default::default()
        };

        let mut sum_in_current_sec = TaskMetricValue::default();
        let mut current_elapsed_secs = None;
        let mut sec_sums = LimitedQueue::new(1000);

        for counter in counters.iter() {
            if counter.timestamp.elapsed().as_secs() >= time_window_secs {
                continue;
            }

            let value = counter.value;
            statistics.latest = value;
            statistics.sum += value;
            statistics.count += counter.count;
            statistics.max = statistics.max.max(value);
            statistics.min = statistics.min.min(value);

            let counter_elapsed_secs = counter.timestamp.elapsed().as_secs();

            match current_elapsed_secs {
                None => {
                    // first counter
                    current_elapsed_secs = Some(counter_elapsed_secs);
                    sum_in_current_sec = value;
                }
                Some(elapsed_secs) if elapsed_secs == counter_elapsed_secs => {
                    // sum when in same second
                    sum_in_current_sec += value;
                }
                Some(_) => {
                    // new second
                    sec_sums.push(sum_in_current_sec);
                    current_elapsed_secs = Some(counter_elapsed_secs);
                    sum_in_current_sec = value;
                }
            }
        }

        // the last second
        if current_elapsed_secs.is_some() {
            sec_sums.push(sum_in_current_sec);
        }
        for &sec_sum in sec_sums.iter() {
            statistics.max_by_sec = statistics.max_by_sec.max(sec_sum);
            statistics.min_by_sec = statistics.min_by_sec.min(sec_sum);
        }

        if statistics.count > 0 {
            statistics.avg_by_count = statistics.sum / statistics.count;
            if !sec_sums.is_empty() {
                let sec_sum_total = sec_sums
                    .iter()
                    .fold(TaskMetricValue::default(), |sum, value| sum + *value);
                statistics.avg_by_sec = sec_sum_total / sec_sums.len() as u64;
            }
        }

        if statistics.min == u64::MAX.into() {
            statistics.min = 0.into();
        }
        if statistics.min_by_sec == u64::MAX.into() {
            statistics.min_by_sec = 0.into();
        }

        statistics
    }

    #[inline(always)]
    pub async fn has_live_data(&self) -> bool {
        self.has_live_data_in_window(self.time_window_secs).await
    }

    #[inline(always)]
    pub async fn has_live_data_in_window(&self, time_window_secs: u64) -> bool {
        let counters = self.counters.read().await;
        counters
            .iter()
            .any(|counter| counter.timestamp.elapsed().as_secs() < time_window_secs)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[tokio::test]
    async fn statistics_preserve_sampled_seconds_and_zero_values() {
        let window = TimeWindowCounter::new(60, 100);
        let now = Instant::now();
        for (age_ms, value) in [(60500, 999), (5500, 100), (3500, 0), (1600, 90), (1500, 30)] {
            window.add(value, 1).await;
            window.counters.write().await.back_mut().unwrap().timestamp =
                now - Duration::from_millis(age_ms);
        }
        let result = window.statistics().await;
        assert_eq!(result.sum.as_u64(), Some(220));
        assert_eq!(result.count, 4);
        assert_eq!(result.avg_by_count.as_u64(), Some(55));
        assert_eq!(
            (result.min.as_u64(), result.max.as_u64()),
            (Some(0), Some(100))
        );
        assert_eq!(result.avg_by_sec.as_u64(), Some(73)); // (100 + 0 + 120) / 3 sampled seconds
        assert_eq!(
            (result.min_by_sec.as_u64(), result.max_by_sec.as_u64()),
            (Some(0), Some(120))
        );
    }

    #[tokio::test]
    async fn fractional_samples_obey_window_and_sample_limits() {
        let counter = TimeWindowCounter::new(60, 3);
        for sample in [0.9, 0.1, 0.0, 0.2] {
            counter.add(sample, 1).await;
        }
        let stats = counter.statistics().await;
        assert_eq!(stats.count, 3);
        assert_eq!(stats.latest.as_f64(), 0.2);
        assert!((stats.sum.as_f64() - 0.3).abs() < 1e-12);
        assert!((stats.avg_by_count.as_f64() - 0.1).abs() < 1e-12);
        assert_eq!(stats.min.as_f64(), 0.0);
        assert_eq!(stats.max.as_f64(), 0.2);
        {
            let mut samples = counter.counters.write().await;
            samples.front_mut().unwrap().timestamp = Instant::now() - Duration::from_secs(61);
        }
        let stats = counter.statistics().await;
        assert_eq!(stats.count, 2);
        assert_eq!(stats.sum.as_f64(), 0.2);
        for sample in counter.counters.write().await.iter_mut() {
            sample.timestamp = Instant::now() - Duration::from_secs(61);
        }
        let stats = counter.statistics().await;
        assert_eq!(stats.count, 0);
        assert_eq!(stats.latest.as_f64(), 0.0);
        assert_eq!(stats.sum.as_f64(), 0.0);
        assert_eq!(stats.avg_by_count.as_f64(), 0.0);
    }
}
