use std::{fmt::Write, time::Instant};

use super::{
    counter_type::{AggregateType, CounterType},
    task_metrics::TaskMetricValue,
};
use crate::utils::limit_queue::LimitedQueue;

#[derive(Debug, Clone)]
pub struct Counter {
    pub timestamp: Instant,
    pub value: TaskMetricValue,
    pub count: u64,
}

impl Counter {
    pub fn new(value: impl Into<TaskMetricValue>, count: u64) -> Self {
        let value = value.into();
        Self {
            timestamp: Instant::now(),
            value,
            count,
        }
    }

    #[inline(always)]
    pub fn adds(&mut self, values: &LimitedQueue<(u64, u64)>) {
        for (value, count) in values.iter() {
            self.add(*value, *count);
        }
    }

    #[inline(always)]
    pub fn set(&mut self, value: impl Into<TaskMetricValue>, count: u64) {
        let value = value.into();
        self.value = value;
        self.count = count;
    }

    #[inline(always)]
    pub fn add(&mut self, value: impl Into<TaskMetricValue>, count: u64) {
        if count == 0 {
            return;
        }
        let value = value.into();
        if self.count == 0 {
            self.set(value, count);
        } else {
            self.value += value;
            self.count += count;
        }
    }

    /// Merge sums and counts so averages are weighted by sample count.
    pub fn merge(&mut self, other: &Self) {
        self.add(other.value, other.count);
    }

    #[inline(always)]
    pub fn avg_by_count(&self) -> TaskMetricValue {
        self.value / self.count.max(1)
    }

    pub fn aggregate(&self, aggregate: &AggregateType) -> TaskMetricValue {
        match aggregate {
            AggregateType::Latest | AggregateType::Sum => self.value,
            AggregateType::AvgByCount => self.avg_by_count(),
            AggregateType::Count => self.count.into(),
            _ => unreachable!("unsupported aggregation for a no-window counter"),
        }
    }

    pub(crate) fn log_line(
        &self,
        name: &str,
        description: &str,
        counter_type: &CounterType,
    ) -> String {
        let mut line = format!("{name} | {description} | {counter_type}");
        for aggregate in counter_type.get_aggregate_types() {
            write!(line, " | {aggregate}={}", self.aggregate(&aggregate)).unwrap();
        }
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_operations_keep_full_u64_precision() {
        let mut counter = Counter::new(0, 0);
        counter.add(u64::MAX - 1, 1);
        counter.add(1, 1);
        assert_eq!(counter.value, TaskMetricValue::Integer(u64::MAX));
        assert_eq!(
            counter.avg_by_count(),
            TaskMetricValue::Integer(u64::MAX / 2)
        );
        assert_eq!(
            serde_json::to_string(&counter.value).unwrap(),
            u64::MAX.to_string()
        );
        assert_eq!(counter.value.to_string(), u64::MAX.to_string());
    }

    #[test]
    fn merge_uses_sample_weights_and_ignores_empty_counters() {
        let mut first = Counter::new(0.0, 0);
        first.add(0.9, 1);
        let mut second = Counter::new(0.0, 0);
        for value in [0.1, 0.2, 0.0] {
            second.add(value, 1);
        }
        for (left, right) in [(&first, &second), (&second, &first)] {
            let mut merged = Counter::new(0.0, 0);
            merged.merge(left);
            merged.merge(&Counter::new(0.0, 0));
            merged.merge(right);
            assert_eq!(merged.count, 4);
            assert!((merged.value.as_f64() - 1.2).abs() < 1e-12);
            assert!((merged.avg_by_count().as_f64() - 0.3).abs() < 1e-12);
        }
    }
}
