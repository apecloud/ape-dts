use std::{
    fs,
    hint::black_box,
    path::PathBuf,
    time::{Duration, Instant},
};

use dt_common::{
    config::parallelizer_config::ChunkPartitionerRebalanceConfig, meta::row_data::RowData,
};

pub(crate) struct GroupPlan {
    row_indexes: Vec<usize>,
}

impl GroupPlan {
    pub(crate) fn new() -> Self {
        Self {
            row_indexes: Vec::new(),
        }
    }

    pub(crate) fn push_row(&mut self, row_index: usize) {
        self.row_indexes.push(row_index);
    }

    pub(crate) fn rows(&self) -> usize {
        self.row_indexes.len()
    }
}

pub(crate) type GroupFn = fn(&[RowData]) -> (usize, usize);

pub(crate) type PartitionFn =
    fn(Vec<RowData>, usize, &ChunkPartitionerRebalanceConfig) -> anyhow::Result<Vec<Vec<RowData>>>;

#[derive(Clone, Copy)]
pub(crate) struct PartitionSummary {
    pub(crate) partitions: usize,
    pub(crate) total_rows: usize,
    pub(crate) min_rows: usize,
    pub(crate) max_rows: usize,
    pub(crate) avg_rows: f64,
    pub(crate) row_variance: f64,
}

pub(crate) fn bench_grouping(data: &[RowData], group: GroupFn, iterations: usize) -> Duration {
    let start = Instant::now();
    let mut checksum = 0;
    for _ in 0..iterations {
        let (groups, rows) = black_box(group(data));
        checksum += groups + rows;
    }
    black_box(checksum);
    start.elapsed()
}

pub(crate) fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

pub(crate) fn write_report(file_name: &str, contents: &str) -> anyhow::Result<PathBuf> {
    let report_dir = project_root::get_project_root()?
        .join("dt-tests")
        .join("tests")
        .join("parallelizer")
        .join("chunk_partitioner");
    fs::create_dir_all(&report_dir)?;
    let path = report_dir.join(file_name);
    fs::write(&path, contents)?;
    Ok(path)
}

pub(crate) fn bench_partition(
    data: &[RowData],
    target_partitions: usize,
    config: &ChunkPartitionerRebalanceConfig,
    partition: PartitionFn,
    iterations: usize,
) -> anyhow::Result<(Duration, PartitionSummary)> {
    let mut elapsed = Duration::ZERO;
    let mut summary = None;

    for _ in 0..iterations {
        let input = data.to_vec();
        let start = Instant::now();
        let partitions = partition(input, target_partitions, config)?;
        elapsed += start.elapsed();
        let current_summary = summarize_partitions(&partitions);
        black_box(current_summary);
        summary = Some(current_summary);
    }

    Ok((elapsed, summary.unwrap_or_default()))
}

pub(crate) fn summarize_partitions(partitions: &[Vec<RowData>]) -> PartitionSummary {
    let partitions_count = partitions.len();
    let total_rows = partitions.iter().map(Vec::len).sum();
    let min_rows = partitions.iter().map(Vec::len).min().unwrap_or(0);
    let max_rows = partitions.iter().map(Vec::len).max().unwrap_or(0);
    let avg_rows = if partitions_count == 0 {
        0.0
    } else {
        total_rows as f64 / partitions_count as f64
    };
    let row_variance = if partitions_count == 0 {
        0.0
    } else {
        partitions
            .iter()
            .map(|partition| {
                let diff = partition.len() as f64 - avg_rows;
                diff * diff
            })
            .sum::<f64>()
            / partitions_count as f64
    };

    PartitionSummary {
        partitions: partitions_count,
        total_rows,
        min_rows,
        max_rows,
        avg_rows,
        row_variance,
    }
}

pub(crate) fn assert_rows_preserved(input: &[RowData], partitions: &[Vec<RowData>]) {
    let total_rows: usize = partitions.iter().map(Vec::len).sum();
    assert_eq!(total_rows, input.len());
    assert!(partitions.iter().all(|partition| !partition.is_empty()));
}

impl Default for PartitionSummary {
    fn default() -> Self {
        Self {
            partitions: 0,
            total_rows: 0,
            min_rows: 0,
            max_rows: 0,
            avg_rows: 0.0,
            row_variance: 0.0,
        }
    }
}
