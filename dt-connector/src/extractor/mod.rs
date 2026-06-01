pub mod base_check_extractor;
pub mod base_extractor;
pub mod base_splitter;
pub mod extractor_monitor;
pub mod foxlake;
pub mod kafka;
pub mod mongo;
pub mod mysql;
pub mod pg;
pub mod rdb_snapshot_extract_statement;
pub mod redis;
pub mod resumer;
pub mod snapshot_chunk_id_generator;
pub mod snapshot_dispatcher;
pub mod snapshot_types;

fn estimated_sample_limit(sample_rate: Option<u8>, estimated_count: u64) -> Option<usize> {
    let sample_rate = sample_rate.filter(|rate| (1..100).contains(rate))?;
    if estimated_count == 0 {
        return None;
    }

    let limit = estimated_count
        .saturating_mul(u64::from(sample_rate))
        .saturating_add(99)
        / 100;
    usize::try_from(limit.max(1)).ok()
}
