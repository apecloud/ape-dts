use std::{collections::HashMap, fmt::Write};

use dt_common::meta::row_data::RowData;
use dt_parallelizer::chunk_partitioner::*;

use super::{
    bench_util::{bench_grouping, duration_ms, write_report, GroupPlan},
    test_data::default_data_cases,
};

struct GroupBenchRow {
    implementation: &'static str,
    rows: usize,
    iterations: usize,
    elapsed_ms: f64,
    speedup_vs_format_key: f64,
}

fn write_group_case(
    report: &mut String,
    case_name: &str,
    rows: &[GroupBenchRow],
) -> anyhow::Result<()> {
    writeln!(report, "## {}\n", case_name)?;
    writeln!(
        report,
        "| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |"
    )?;
    writeln!(report, "| :--- | ---: | ---: | ---: | ---: |")?;
    for row in rows {
        writeln!(
            report,
            "| {} | {} | {} | {:.3} | {:.2}x |",
            row.implementation, row.rows, row.iterations, row.elapsed_ms, row.speedup_vs_format_key,
        )?;
    }
    writeln!(report)?;
    Ok(())
}

fn group_for_bench_with_format_key(data: &[RowData]) -> (usize, usize) {
    let mut group_indexes: HashMap<String, usize> = HashMap::with_capacity(16);
    let mut groups: Vec<GroupPlan> = Vec::with_capacity(16);

    for (row_index, row_data) in data.iter().enumerate() {
        let key = format!("{}.{}.{}", row_data.schema, row_data.tb, row_data.chunk_id);

        let index = match group_indexes.get(&key).copied() {
            Some(index) => index,
            None => {
                let index = groups.len();
                group_indexes.insert(key, index);
                groups.push(GroupPlan::new());
                index
            }
        };

        groups[index].push_row(row_index);
    }

    let row_count = groups.iter().map(GroupPlan::rows).sum();
    (groups.len(), row_count)
}

fn group_for_bench_without_fast_path(data: &[RowData]) -> (usize, usize) {
    let mut group_indexes: HashMap<ChunkKey<'_>, usize> = HashMap::with_capacity(16);
    let mut groups: Vec<GroupPlan> = Vec::with_capacity(16);

    for (row_index, row_data) in data.iter().enumerate() {
        let key = ChunkKey {
            schema: row_data.schema.as_str(),
            tb: row_data.tb.as_str(),
            chunk_id: row_data.chunk_id,
        };

        let index = match group_indexes.get(&key).copied() {
            Some(index) => index,
            None => {
                let index = groups.len();
                group_indexes.insert(key, index);
                groups.push(GroupPlan::new());
                index
            }
        };

        groups[index].push_row(row_index);
    }

    let row_count = groups.iter().map(GroupPlan::rows).sum();
    (groups.len(), row_count)
}

fn group_for_bench_with_fast_path(data: &[RowData]) -> (usize, usize) {
    let mut group_indexes: HashMap<ChunkKey<'_>, usize> = HashMap::with_capacity(16);
    let mut groups: Vec<GroupPlan> = Vec::with_capacity(16);
    let mut last_group: Option<(ChunkKey<'_>, usize)> = None;

    for (row_index, row_data) in data.iter().enumerate() {
        let key = ChunkKey {
            schema: row_data.schema.as_str(),
            tb: row_data.tb.as_str(),
            chunk_id: row_data.chunk_id,
        };

        let index = match last_group {
            Some((last_key, last_index)) if last_key == key => last_index,
            _ => {
                let index = match group_indexes.get(&key).copied() {
                    Some(index) => index,
                    None => {
                        let index = groups.len();
                        group_indexes.insert(key, index);
                        groups.push(GroupPlan::new());
                        index
                    }
                };
                last_group = Some((key, index));
                index
            }
        };

        groups[index].push_row(row_index);
    }

    let row_count = groups.iter().map(GroupPlan::rows).sum();
    (groups.len(), row_count)
}

#[test]
#[ignore = "micro-benchmark; run with --ignored --nocapture, preferably with --release"]
fn bench_partition_dml_grouping_fast_path() -> anyhow::Result<()> {
    let mut report = String::new();
    writeln!(report, "# Chunk Partitioner Group Fast Path Benchmark\n")?;

    for bench_case in default_data_cases() {
        let with_format_key = bench_grouping(
            &bench_case.data,
            group_for_bench_with_format_key,
            bench_case.iterations,
        );
        let without_fast_path = bench_grouping(
            &bench_case.data,
            group_for_bench_without_fast_path,
            bench_case.iterations,
        );
        let with_fast_path = bench_grouping(
            &bench_case.data,
            group_for_bench_with_fast_path,
            bench_case.iterations,
        );

        let rows = [
            GroupBenchRow {
                implementation: "with_format_key",
                rows: bench_case.data.len(),
                iterations: bench_case.iterations,
                elapsed_ms: duration_ms(with_format_key),
                speedup_vs_format_key: 1.0,
            },
            GroupBenchRow {
                implementation: "without_fast_path",
                rows: bench_case.data.len(),
                iterations: bench_case.iterations,
                elapsed_ms: duration_ms(without_fast_path),
                speedup_vs_format_key: with_format_key.as_secs_f64()
                    / without_fast_path.as_secs_f64(),
            },
            GroupBenchRow {
                implementation: "with_fast_path",
                rows: bench_case.data.len(),
                iterations: bench_case.iterations,
                elapsed_ms: duration_ms(with_fast_path),
                speedup_vs_format_key: with_format_key.as_secs_f64() / with_fast_path.as_secs_f64(),
            },
        ];
        write_group_case(&mut report, bench_case.name, &rows)?;

        assert_eq!(
            group_for_bench_with_format_key(&bench_case.data),
            group_for_bench_without_fast_path(&bench_case.data)
        );
        assert_eq!(
            group_for_bench_without_fast_path(&bench_case.data),
            group_for_bench_with_fast_path(&bench_case.data)
        );
    }

    let path = write_report("group_fast_path_result.md", &report)?;
    println!("wrote {}", path.display());
    Ok(())
}
