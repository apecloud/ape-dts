use std::time::Instant;

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
    pub min: TaskMetricValue,
    pub max: TaskMetricValue,
}

impl Counter {
    pub fn new(value: impl Into<TaskMetricValue>, count: u64) -> Self {
        let value = value.into();
        Self {
            timestamp: Instant::now(),
            value,
            count,
            min: value,
            max: value,
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
        self.min = value;
        self.max = value;
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
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
    }

    /// Merge raw statistics, so averages are weighted by sample count and empty
    /// counters do not contribute a spurious zero to the minimum.
    pub fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
        } else {
            self.value += other.value;
            self.count += other.count;
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
    }

    #[inline(always)]
    pub fn avg_by_count(&self) -> TaskMetricValue {
        self.value / self.count.max(1)
    }

    pub fn aggregate(&self, aggregate: &AggregateType) -> TaskMetricValue {
        match aggregate {
            AggregateType::Latest | AggregateType::Sum => self.value,
            AggregateType::AvgByCount => self.avg_by_count(),
            AggregateType::MinByCount => self.min,
            AggregateType::MaxByCount => self.max,
            AggregateType::Count => self.count.into(),
            _ => unreachable!("per-second statistics require a time window"),
        }
    }

    pub(crate) fn log_line(
        &self,
        name: &str,
        description: &str,
        counter_type: &CounterType,
    ) -> String {
        use std::fmt::Write;
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
        assert_eq!(counter.min, TaskMetricValue::Integer(1));
        assert_eq!(counter.max, TaskMetricValue::Integer(u64::MAX - 1));
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
    fn merge_uses_sample_weights_and_ignores_empty_extrema() {
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
            assert_eq!(merged.min.as_f64(), 0.0);
            assert_eq!(merged.max.as_f64(), 0.9);
        }
        first.merge(&Counter::new(0.0, 0));
        assert_eq!(first.min.as_f64(), 0.9);
    }

    #[test]
    fn mixed_values_promote_to_float_and_set_resets_statistics() {
        let mut counter = Counter::new(2, 1);
        counter.add(0.5, 1);
        assert_eq!(counter.value, TaskMetricValue::Float(2.5));
        assert_eq!(counter.avg_by_count(), TaskMetricValue::Float(1.25));
        assert_eq!(counter.min, TaskMetricValue::Float(0.5));
        assert_eq!(counter.max, TaskMetricValue::Float(2.0));
        counter.set(7, 1);
        assert_eq!(counter.value, TaskMetricValue::Integer(7));
        assert_eq!(counter.min, counter.value);
        assert_eq!(counter.max, counter.value);
        assert_eq!(counter.avg_by_count(), counter.value);
        counter.add(100, 0);
        assert_eq!(counter.count, 1);
        assert_eq!(counter.value, TaskMetricValue::Integer(7));
    }
}
