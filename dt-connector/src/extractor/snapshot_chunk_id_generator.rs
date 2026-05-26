#[derive(Debug, Clone)]
pub struct SnapshotChunkIdGenerator {
    chunk_id: u64,
    rows_in_chunk: usize,
    row_chunk_size: usize,
}

impl Default for SnapshotChunkIdGenerator {
    fn default() -> Self {
        Self::new(1)
    }
}

impl SnapshotChunkIdGenerator {
    pub fn new(row_chunk_size: usize) -> Self {
        Self {
            chunk_id: 0,
            rows_in_chunk: 0,
            row_chunk_size: row_chunk_size.max(1),
        }
    }

    #[inline(always)]
    pub fn next_chunk_id(&mut self) -> u64 {
        self.chunk_id += 1;
        self.chunk_id
    }

    #[inline(always)]
    pub fn peek_next_chunk_id(&self) -> u64 {
        self.chunk_id + 1
    }

    #[inline(always)]
    pub fn next_row_chunk_id(&mut self) -> u64 {
        if self.chunk_id == 0 {
            self.chunk_id = 1;
        } else if self.rows_in_chunk >= self.row_chunk_size {
            self.chunk_id += 1;
            self.rows_in_chunk = 0;
        }
        self.rows_in_chunk += 1;
        self.chunk_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_chunk_id_starts_at_one_and_increments() {
        let mut gen = SnapshotChunkIdGenerator::new(10);
        assert_eq!(gen.next_chunk_id(), 1);
        assert_eq!(gen.next_chunk_id(), 2);
        assert_eq!(gen.next_chunk_id(), 3);
    }

    #[test]
    fn peek_next_chunk_id_does_not_advance() {
        let mut gen = SnapshotChunkIdGenerator::new(10);
        assert_eq!(gen.peek_next_chunk_id(), 1);
        assert_eq!(gen.peek_next_chunk_id(), 1);
        assert_eq!(gen.next_chunk_id(), 1);
        assert_eq!(gen.peek_next_chunk_id(), 2);
        assert_eq!(gen.next_chunk_id(), 2);
    }

    #[test]
    fn default_uses_row_chunk_size_of_one() {
        let mut gen = SnapshotChunkIdGenerator::default();
        assert_eq!(gen.next_row_chunk_id(), 1);
        assert_eq!(gen.next_row_chunk_id(), 2);
        assert_eq!(gen.next_row_chunk_id(), 3);
    }

    #[test]
    fn zero_row_chunk_size_is_clamped_to_one() {
        let mut gen = SnapshotChunkIdGenerator::new(0);
        assert_eq!(gen.next_row_chunk_id(), 1);
        assert_eq!(gen.next_row_chunk_id(), 2);
        assert_eq!(gen.next_row_chunk_id(), 3);
    }

    #[test]
    fn next_row_chunk_id_groups_rows_by_chunk_size() {
        let mut gen = SnapshotChunkIdGenerator::new(3);
        let ids: Vec<u64> = (0..10).map(|_| gen.next_row_chunk_id()).collect();
        assert_eq!(ids, vec![1, 1, 1, 2, 2, 2, 3, 3, 3, 4]);
    }

    #[test]
    fn next_row_chunk_id_with_chunk_size_one_increments_every_call() {
        let mut gen = SnapshotChunkIdGenerator::new(1);
        let ids: Vec<u64> = (0..5).map(|_| gen.next_row_chunk_id()).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn next_row_chunk_id_first_call_initializes_from_zero() {
        let mut gen = SnapshotChunkIdGenerator::new(5);
        // First call must return 1, not 0, even though chunk_id starts at 0.
        assert_eq!(gen.next_row_chunk_id(), 1);
    }

    #[test]
    fn mixing_next_chunk_id_and_next_row_chunk_id_shares_counter() {
        let mut gen = SnapshotChunkIdGenerator::new(2);
        assert_eq!(gen.next_chunk_id(), 1);
        assert_eq!(gen.next_chunk_id(), 2);
        // next_row_chunk_id sees chunk_id == 2 (non-zero) and rows_in_chunk == 0,
        // so it stays on chunk 2 and starts counting rows there.
        assert_eq!(gen.next_row_chunk_id(), 2);
        assert_eq!(gen.next_row_chunk_id(), 2);
        assert_eq!(gen.next_row_chunk_id(), 3);
    }
}
