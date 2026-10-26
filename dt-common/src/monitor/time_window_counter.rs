use std::collections::{BTreeMap, LinkedList};
use std::time::Instant;

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
    pub sample_count: u64,
    pub per_second: BTreeMap<u64, TaskMetricValue>,
}

impl WindowCounterStatistics {
    pub fn merge(&mut self, other: &Self) {
        self.latest += other.latest;
        self.sum += other.sum;
        self.count += other.count;
        if other.sample_count > 0 {
            if self.sample_count == 0 {
                self.min = other.min;
                self.max = other.max;
            } else {
                self.min = self.min.min(other.min);
                self.max = self.max.max(other.max);
            }
            self.sample_count += other.sample_count;
        }
        for (second, value) in &other.per_second {
            *self.per_second.entry(*second).or_default() += *value;
        }
        self.recalculate();
    }

    pub fn recalculate(&mut self) {
        self.avg_by_count = if self.count > 0 {
            self.sum / self.count
        } else {
            0.into()
        };
        if self.sample_count == 0 {
            self.min = 0.into();
            self.max = 0.into();
        }
        if self.per_second.is_empty() {
            self.min_by_sec = 0.into();
            self.max_by_sec = 0.into();
            self.avg_by_sec = 0.into();
        } else {
            self.min_by_sec = self
                .per_second
                .values()
                .copied()
                .reduce(TaskMetricValue::min)
                .unwrap();
            self.max_by_sec = self
                .per_second
                .values()
                .copied()
                .reduce(TaskMetricValue::max)
                .unwrap();
            let total = self
                .per_second
                .values()
                .copied()
                .fold(TaskMetricValue::default(), |sum, value| sum + value);
            self.avg_by_sec = total / self.per_second.len() as u64;
        }
    }
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
        self.statistics_at(Instant::now()).await
    }

    #[inline(always)]
    pub async fn statistics_at(&self, now: Instant) -> WindowCounterStatistics {
        self.statistics_in_window_at(self.time_window_secs, now)
            .await
    }

    #[inline(always)]
    pub async fn statistics_in_window(&self, time_window_secs: u64) -> WindowCounterStatistics {
        self.statistics_in_window_at(time_window_secs, Instant::now())
            .await
    }

    #[inline(always)]
    pub async fn statistics_in_window_at(
        &self,
        time_window_secs: u64,
        now: Instant,
    ) -> WindowCounterStatistics {
        let counters = self.counters.read().await;
        if counters.is_empty() {
            return WindowCounterStatistics::default();
        }

        let mut statistics = WindowCounterStatistics {
            min: u64::MAX.into(),
            min_by_sec: u64::MAX.into(),
            ..Default::default()
        };

        for counter in counters.iter() {
            let elapsed_secs = now.saturating_duration_since(counter.timestamp).as_secs();
            if elapsed_secs >= time_window_secs {
                continue;
            }

            let value = counter.value;
            statistics.latest = value;
            statistics.sum += value;
            statistics.count += counter.count;
            statistics.sample_count += 1;
            statistics.max = statistics.max.max(value);
            statistics.min = statistics.min.min(value);
            *statistics.per_second.entry(elapsed_secs).or_default() += value;
        }

        statistics.recalculate();
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
        let result = window.statistics_at(now).await;
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
    async fn merged_statistics_align_seconds_and_weight_sample_averages() {
        let now = Instant::now();
        let first = TimeWindowCounter::new(60, 100);
        let second = TimeWindowCounter::new(60, 100);
        for (counter, age_ms, value) in [
            (&first, 500, 10),
            (&first, 1500, 90),
            (&second, 300, 20),
            (&second, 2500, 40),
        ] {
            counter.add(value, 1).await;
            counter.counters.write().await.back_mut().unwrap().timestamp =
                now - Duration::from_millis(age_ms);
        }

        let mut merged = first.statistics_at(now).await;
        merged.merge(&second.statistics_at(now).await);
        assert_eq!(merged.avg_by_count.as_u64(), Some(40));
        assert_eq!(
            (merged.min.as_u64(), merged.max.as_u64()),
            (Some(10), Some(90))
        );
        assert_eq!(merged.avg_by_sec.as_u64(), Some(53));
        assert_eq!(
            (merged.min_by_sec.as_u64(), merged.max_by_sec.as_u64()),
            (Some(30), Some(90))
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
