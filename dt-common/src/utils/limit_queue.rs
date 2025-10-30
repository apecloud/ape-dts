use std::collections::{vec_deque, VecDeque};

pub struct LimitedQueue<T> {
    data: VecDeque<T>,
    max_size: usize,
}

impl<T> LimitedQueue<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    pub fn push(&mut self, item: T) {
        if self.data.len() >= self.max_size {
            self.data.pop_front();
        }
        self.data.push_back(item);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn iter(&self) -> vec_deque::Iter<T> {
        self.data.iter()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}
