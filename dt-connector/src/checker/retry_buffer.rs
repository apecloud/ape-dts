use std::{collections::VecDeque, mem::size_of};

use tokio::time::Instant;

use dt_common::meta::row_data::RowData;

#[derive(Debug)]
pub(super) struct RetryItem {
    pub row: RowData,
    pub retries_left: u32,
    pub next_retry_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RetryBufferFull {
    Rows,
    Bytes,
}

pub(super) struct RetryBuffer {
    queue: VecDeque<RetryItem>,
    pending_bytes: usize,
    max_rows: usize,
    max_bytes: usize,
    overflow_count: u64,
}

impl RetryBuffer {
    pub fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            pending_bytes: 0,
            max_rows: max_rows.max(1),
            max_bytes: max_bytes.max(1),
            overflow_count: 0,
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "the rejected RetryItem must be returned without cloning or allocating"
    )]
    pub fn try_push(&mut self, item: RetryItem) -> Result<(), (RetryItem, RetryBufferFull)> {
        if self.queue.len() >= self.max_rows {
            self.overflow_count = self.overflow_count.saturating_add(1);
            return Err((item, RetryBufferFull::Rows));
        }

        let item_bytes = Self::item_bytes(&item);
        if self.pending_bytes.saturating_add(item_bytes) > self.max_bytes {
            self.overflow_count = self.overflow_count.saturating_add(1);
            return Err((item, RetryBufferFull::Bytes));
        }

        self.pending_bytes = self.pending_bytes.saturating_add(item_bytes);
        self.queue.push_back(item);
        Ok(())
    }

    pub fn push_existing(&mut self, item: RetryItem) {
        let item_bytes = Self::item_bytes(&item);
        debug_assert!(self.queue.len() < self.max_rows);
        debug_assert!(self.pending_bytes.saturating_add(item_bytes) <= self.max_bytes);
        self.pending_bytes = self.pending_bytes.saturating_add(item_bytes);
        self.queue.push_back(item);
    }

    pub fn pop_front(&mut self) -> Option<RetryItem> {
        let item = self.queue.pop_front()?;
        self.pending_bytes = self.pending_bytes.saturating_sub(Self::item_bytes(&item));
        Some(item)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &RetryItem> {
        self.queue.iter()
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub fn overflow_count(&self) -> u64 {
        self.overflow_count
    }

    fn item_bytes(item: &RetryItem) -> usize {
        usize::try_from(item.row.get_data_size())
            .unwrap_or(usize::MAX)
            .saturating_add(size_of::<RetryItem>())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dt_common::meta::{col_value::ColValue, row_data::RowData, row_type::RowType};

    use super::*;

    fn row(id: i32) -> RowData {
        RowData::new(
            "s1".to_string(),
            "t1".to_string(),
            0,
            RowType::Insert,
            None,
            Some(HashMap::from([("id".to_string(), ColValue::Long(id))])),
        )
    }

    fn item(id: i32) -> RetryItem {
        RetryItem {
            row: row(id),
            retries_left: 1,
            next_retry_at: Instant::now(),
        }
    }

    #[test]
    fn enforces_row_limit_and_releases_capacity_after_pop() {
        let mut buffer = RetryBuffer::new(1, usize::MAX);
        buffer.try_push(item(1)).unwrap();

        let (_, reason) = buffer.try_push(item(2)).unwrap_err();
        assert_eq!(reason, RetryBufferFull::Rows);
        assert_eq!(buffer.overflow_count(), 1);

        let popped = buffer.pop_front().unwrap();
        buffer.try_push(item(2)).unwrap();
        assert_eq!(
            popped.row.after.unwrap().get("id"),
            Some(&ColValue::Long(1))
        );
    }

    #[test]
    fn enforces_byte_limit() {
        let first = item(1);
        let max_bytes = RetryBuffer::item_bytes(&first);
        let mut buffer = RetryBuffer::new(2, max_bytes);
        buffer.try_push(first).unwrap();

        let (_, reason) = buffer.try_push(item(2)).unwrap_err();
        assert_eq!(reason, RetryBufferFull::Bytes);
        assert!(buffer.pending_bytes() <= max_bytes);
    }
}
