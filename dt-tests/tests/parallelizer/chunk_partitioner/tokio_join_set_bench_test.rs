use std::{
    fmt::Write,
    time::{Duration, Instant},
};

use tokio::{runtime::Builder, task::JoinSet};

use super::bench_util::{duration_ms, write_report};

const TOTAL_TASKS: [usize; 3] = [128, 256, 512];
const TASK_SLEEP_MS: u64 = 20;
const IN_FLIGHT_TASKS: [usize; 6] = [1, 2, 4, 8, 16, 32];
const WORKER_THREADS: [usize; 4] = [1, 2, 4, 8];

struct JoinSetBenchRow {
    worker_threads: usize,
    in_flight_tasks: usize,
    total_tasks: usize,
    sleep_ms: u64,
    elapsed_ms: f64,
    ideal_ms: f64,
    overhead_ms: f64,
    overhead_per_wave_ms: f64,
}

async fn run_windowed_join_set(
    total_tasks: usize,
    in_flight_tasks: usize,
    task_sleep: Duration,
) -> anyhow::Result<(usize, usize, Duration)> {
    assert!(in_flight_tasks < total_tasks);

    let start = Instant::now();
    let mut join_set = JoinSet::new();
    let mut submitted = 0usize;
    let mut completed = 0usize;

    let initial_tasks = in_flight_tasks.min(total_tasks);
    for _ in 0..initial_tasks {
        join_set.spawn(async move {
            tokio::time::sleep(task_sleep).await;
        });
        submitted += 1;
    }

    while let Some(result) = join_set.join_next().await {
        result?;
        completed += 1;

        if submitted < total_tasks {
            join_set.spawn(async move {
                tokio::time::sleep(task_sleep).await;
            });
            submitted += 1;
        }
    }

    Ok((submitted, completed, start.elapsed()))
}

fn bench_case(
    worker_threads: usize,
    total_tasks: usize,
    in_flight_tasks: usize,
) -> anyhow::Result<JoinSetBenchRow> {
    assert!(in_flight_tasks < total_tasks);

    let task_sleep = Duration::from_millis(TASK_SLEEP_MS);
    let runtime = Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_time()
        .build()?;

    let (submitted, completed, elapsed) = runtime.block_on(run_windowed_join_set(
        total_tasks,
        in_flight_tasks,
        task_sleep,
    ))?;

    assert_eq!(submitted, total_tasks);
    assert_eq!(completed, total_tasks);

    let ideal_waves = total_tasks.div_ceil(in_flight_tasks);
    let ideal_ms = (ideal_waves as u64 * TASK_SLEEP_MS) as f64;
    let elapsed_ms = duration_ms(elapsed);
    let overhead_ms = elapsed_ms - ideal_ms;

    Ok(JoinSetBenchRow {
        worker_threads,
        in_flight_tasks,
        total_tasks,
        sleep_ms: TASK_SLEEP_MS,
        elapsed_ms,
        ideal_ms,
        overhead_ms,
        overhead_per_wave_ms: overhead_ms / ideal_waves as f64,
    })
}

fn write_join_set_report(rows: &[JoinSetBenchRow]) -> anyhow::Result<String> {
    let mut report = String::new();
    writeln!(report, "# Tokio JoinSet Window Benchmark\n")?;
    writeln!(
        report,
        "| worker_threads | in_flight_tasks | total_tasks | sleep_ms | elapsed_ms | ideal_ms | overhead_ms | overhead_per_wave_ms |"
    )?;
    writeln!(
        report,
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )?;

    for row in rows {
        writeln!(
            report,
            "| {} | {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} |",
            row.worker_threads,
            row.in_flight_tasks,
            row.total_tasks,
            row.sleep_ms,
            row.elapsed_ms,
            row.ideal_ms,
            row.overhead_ms,
            row.overhead_per_wave_ms,
        )?;
    }

    Ok(report)
}

#[test]
#[ignore = "micro-benchmark; run with --ignored --nocapture, preferably with --release"]
fn bench_tokio_join_set_windowed_tasks() -> anyhow::Result<()> {
    let mut rows =
        Vec::with_capacity(WORKER_THREADS.len() * TOTAL_TASKS.len() * IN_FLIGHT_TASKS.len());

    for worker_threads in WORKER_THREADS {
        for total_tasks in TOTAL_TASKS {
            for in_flight_tasks in IN_FLIGHT_TASKS {
                rows.push(bench_case(worker_threads, total_tasks, in_flight_tasks)?);
            }
        }
    }

    let report = write_join_set_report(&rows)?;
    let path = write_report("tokio_join_set_result.md", &report)?;
    println!("wrote {}", path.display());
    Ok(())
}
