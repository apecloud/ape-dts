use std::fmt::Write;

use dt_common::{
    config::parallelizer_config::{
        ChunkPartitionerRebalanceConfig, ChunkPartitionerRebalanceCost,
        ChunkPartitionerRebalanceStrategy,
    },
    meta::row_data::RowData,
};
use dt_parallelizer::chunk_partitioner::ChunkPartitioner;

use super::{
    bench_util::{assert_rows_preserved, bench_partition, duration_ms, write_report, PartitionFn},
    refs::{
        string_key_basic_partitioner::StringKeyBasicPartitioner,
        string_key_row_rebalance_partitioner::StringKeyRowRebalancePartitioner,
    },
    test_data::{default_data_cases, grouping_many_keys_contiguous},
};

struct PartitionerBenchRow {
    strategy: &'static str,
    implementation: &'static str,
    input_rows: usize,
    output_rows: usize,
    iterations: usize,
    elapsed_ms: f64,
    partitions: usize,
    min_rows: usize,
    max_rows: usize,
    avg_rows: f64,
    row_variance: f64,
}

fn write_partitioner_case(
    report: &mut String,
    case_name: &str,
    rows: &[PartitionerBenchRow],
) -> anyhow::Result<()> {
    writeln!(report, "## {}\n", case_name)?;
    writeln!(
        report,
        "| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |"
    )?;
    writeln!(
        report,
        "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )?;

    let mut last_strategy = None;
    for row in rows {
        if last_strategy.is_some() && last_strategy != Some(row.strategy) {
            writeln!(report, "|  |  |  |  |  |  |  |  |  |  |  |")?;
        }
        last_strategy = Some(row.strategy);

        writeln!(
            report,
            "| {} | {} | {} | {} | {} | {:.3} | {} | {} | {} | {:.2} | {:.2} |",
            row.strategy,
            row.implementation,
            row.input_rows,
            row.output_rows,
            row.iterations,
            row.elapsed_ms,
            row.partitions,
            row.min_rows,
            row.max_rows,
            row.avg_rows,
            row.row_variance,
        )?;
    }
    writeln!(report)?;
    writeln!(report)?;
    Ok(())
}

fn current_indexed_plan_partition(
    data: Vec<RowData>,
    target_partitions: usize,
    config: &ChunkPartitionerRebalanceConfig,
) -> anyhow::Result<Vec<Vec<RowData>>> {
    ChunkPartitioner::partition_dml(data, target_partitions, config)
}

fn string_key_row_rebalance_partition(
    data: Vec<RowData>,
    target_partitions: usize,
    config: &ChunkPartitionerRebalanceConfig,
) -> anyhow::Result<Vec<Vec<RowData>>> {
    StringKeyRowRebalancePartitioner::partition_dml(data, target_partitions, config)
}

fn string_key_basic_partition(
    data: Vec<RowData>,
    target_partitions: usize,
    _config: &ChunkPartitionerRebalanceConfig,
) -> anyhow::Result<Vec<Vec<RowData>>> {
    StringKeyBasicPartitioner::partition_dml(data, target_partitions)
}

fn config_for_case(
    case_name: &str,
    strategy: ChunkPartitionerRebalanceStrategy,
) -> ChunkPartitionerRebalanceConfig {
    ChunkPartitionerRebalanceConfig {
        strategy,
        cost: if case_name.starts_with("bytes_") {
            ChunkPartitionerRebalanceCost::Bytes
        } else {
            ChunkPartitionerRebalanceCost::Rows
        },
        max_partitions_per_sinker: 2,
        min_partition_rows: 200,
        split_skew_ratio: 2.0,
    }
}

fn strategies() -> Vec<(&'static str, ChunkPartitionerRebalanceStrategy)> {
    vec![
        ("none", ChunkPartitionerRebalanceStrategy::None),
        (
            "chunk_largest_first",
            ChunkPartitionerRebalanceStrategy::ChunkLargestFirst,
        ),
        ("auto_split", ChunkPartitionerRebalanceStrategy::AutoSplit),
        (
            "table_min_rows",
            ChunkPartitionerRebalanceStrategy::TableMinRows,
        ),
        ("table_even", ChunkPartitionerRebalanceStrategy::TableEven),
    ]
}

#[test]
#[ignore = "micro-benchmark; run with --ignored --nocapture, preferably with --release"]
fn bench_chunk_partitioner_versions() -> anyhow::Result<()> {
    let target_partitions = 16;
    let partitioners: [(&str, PartitionFn); 3] = [
        ("current_indexed_plan", current_indexed_plan_partition),
        (
            "string_key_row_rebalance",
            string_key_row_rebalance_partition,
        ),
        ("string_key_basic", string_key_basic_partition),
    ];

    let mut report = String::new();
    writeln!(report, "# Chunk Partitioner Version Benchmark\n")?;

    for bench_case in default_data_cases() {
        let mut rows = Vec::new();

        for (strategy_name, strategy) in strategies() {
            let config = config_for_case(bench_case.name, strategy);
            for (name, partition) in partitioners {
                let (elapsed, summary) = bench_partition(
                    &bench_case.data,
                    target_partitions,
                    &config,
                    partition,
                    bench_case.iterations,
                )?;
                let partitions = partition(bench_case.data.clone(), target_partitions, &config)?;
                assert_rows_preserved(&bench_case.data, &partitions);

                rows.push(PartitionerBenchRow {
                    strategy: strategy_name,
                    implementation: name,
                    input_rows: bench_case.data.len(),
                    output_rows: summary.total_rows,
                    iterations: bench_case.iterations,
                    elapsed_ms: duration_ms(elapsed),
                    partitions: summary.partitions,
                    min_rows: summary.min_rows,
                    max_rows: summary.max_rows,
                    avg_rows: summary.avg_rows,
                    row_variance: summary.row_variance,
                });
            }
        }
        write_partitioner_case(&mut report, bench_case.name, &rows)?;
    }

    let path = write_report("partitioner_compare_result.md", &report)?;
    println!("wrote {}", path.display());
    Ok(())
}

#[test]
#[ignore = "manual terminal output comparison"]
fn compare_grouping_many_keys_contiguous_chunks() -> anyhow::Result<()> {
    let data = grouping_many_keys_contiguous(160_000);
    let target_partitions = 16;
    let iterations = 1;
    let partitioners: [(&str, PartitionFn); 3] = [
        ("current_indexed_plan", current_indexed_plan_partition),
        (
            "string_key_row_rebalance",
            string_key_row_rebalance_partition,
        ),
        ("string_key_basic", string_key_basic_partition),
    ];

    println!();
    println!("case: grouping_many_keys_contiguous_chunks");
    println!(
        "{:<20} {:<28} {:>10} {:>10} {:>12} {:>10} {:>10} {:>10} {:>12}",
        "strategy",
        "impl",
        "elapsed_ms",
        "partitions",
        "output_rows",
        "min_rows",
        "max_rows",
        "avg_rows",
        "row_var",
    );
    println!("{}", "-".repeat(128));

    for (strategy_name, strategy) in strategies() {
        let config = config_for_case("grouping_many_keys_contiguous_chunks", strategy);
        for (name, partition) in partitioners {
            let (elapsed, summary) =
                bench_partition(&data, target_partitions, &config, partition, iterations)?;
            let partitions = partition(data.clone(), target_partitions, &config)?;
            assert_rows_preserved(&data, &partitions);

            println!(
                "{:<20} {:<28} {:>10.3} {:>10} {:>12} {:>10} {:>10} {:>10.2} {:>12.2}",
                strategy_name,
                name,
                duration_ms(elapsed),
                summary.partitions,
                summary.total_rows,
                summary.min_rows,
                summary.max_rows,
                summary.avg_rows,
                summary.row_variance,
            );
        }
        println!();
    }

    Ok(())
}
