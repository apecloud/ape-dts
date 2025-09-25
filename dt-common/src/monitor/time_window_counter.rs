use std::{cmp, collections::LinkedList};

use super::counter::Counter;
use crate::utils::limit_queue::LimitedQueue;

pub struct WindowCounterStatistics {
    pub sum: u64,
    pub max: u64,
    pub min: u64,
    pub avg_by_count: u64,
    pub max_by_sec: u64,
    pub min_by_sec: u64,
    pub avg_by_sec: u64,
    pub count: u64,
}

impl Default for WindowCounterStatistics {
    fn default() -> Self {
        Self {
            sum: 0,
            max: 0,
            min: 0,
            avg_by_count: 0,
            max_by_sec: 0,
            min_by_sec: 0,
            avg_by_sec: 0,
            count: 0,
        }
    }
}

#[derive(Clone)]
pub struct TimeWindowCounter {
    pub time_window_secs: u64,
    pub max_sub_count: u64,
    pub counters: LinkedList<Counter>,
    pub description: String,
}

impl TimeWindowCounter {
    pub fn new(time_window_secs: u64, max_sub_count: u64) -> Self {
        Self {
            time_window_secs,
            max_sub_count,
            counters: LinkedList::new(),
            description: String::new(),
        }
    }

    #[inline(always)]
    pub fn adds(&mut self, values: &LimitedQueue<(u64, u64)>) -> &Self {
        for (value, count) in values.iter() {
            if *count == 0 {
                continue;
            }
            self.add(*value, *count);
        }
        self
    }

    #[inline(always)]
    pub fn add(&mut self, value: u64, count: u64) -> &Self {
        while self.counters.len() as u64 > self.max_sub_count {
            self.counters.pop_front();
        }
        self.counters.push_back(Counter::new(value, count));
        self
    }

    #[inline(always)]
    pub fn statistics(&mut self) -> WindowCounterStatistics {
        self.refresh_window();

        if self.counters.is_empty() {
            return WindowCounterStatistics::default();
        }

        let mut statistics = WindowCounterStatistics::default();
        statistics.min = u64::MAX;
        statistics.min_by_sec = u64::MAX;

        let mut sum_in_current_sec = 0;
        let mut current_elapsed_secs = None;
        let mut sec_sums = LimitedQueue::new(1000);

        for counter in self.counters.iter() {
            statistics.sum += counter.value;
            statistics.count += counter.count;
            statistics.max = cmp::max(statistics.max, counter.value);
            statistics.min = cmp::min(statistics.min, counter.value);

            let counter_elapsed_secs = counter.timestamp.elapsed().as_secs();

            match current_elapsed_secs {
                None => {
                    // first counter
                    current_elapsed_secs = Some(counter_elapsed_secs);
                    sum_in_current_sec = counter.value;
                }
                Some(elapsed_secs) if elapsed_secs == counter_elapsed_secs => {
                    // sum when in same second
                    sum_in_current_sec += counter.value;
                }
                Some(_) => {
                    // new second
                    sec_sums.push(sum_in_current_sec);
                    current_elapsed_secs = Some(counter_elapsed_secs);
                    sum_in_current_sec = counter.value;
                }
            }
        }

        // the last second
        if current_elapsed_secs.is_some() {
            sec_sums.push(sum_in_current_sec);
        }
        for &sec_sum in sec_sums.iter() {
            statistics.max_by_sec = cmp::max(statistics.max_by_sec, sec_sum);
            statistics.min_by_sec = cmp::min(statistics.min_by_sec, sec_sum);
        }

        if statistics.count > 0 {
            statistics.avg_by_count = statistics.sum / statistics.count;
            if !sec_sums.is_empty() {
                let sec_sum_total: u64 = sec_sums.iter().sum();
                statistics.avg_by_sec = sec_sum_total / sec_sums.len() as u64;
            }
        }

        if statistics.min == u64::MAX {
            statistics.min = 0;
        }
        if statistics.min_by_sec == u64::MAX {
            statistics.min_by_sec = 0;
        }

        statistics
    }

    #[inline(always)]
    pub fn refresh_window(&mut self) {
        let mut outdate_count = 0;
        for counter in self.counters.iter() {
            if counter.timestamp.elapsed().as_secs() >= self.time_window_secs {
                outdate_count += 1;
            } else {
                break;
            }
        }

        for _ in 0..outdate_count {
            self.counters.pop_front();
        }
    }
}
