use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use dt_connector::checker::check_log::{CheckLog, CheckSummaryLog};
use serde::Deserialize;

use super::{check_util::CheckUtil, redis_test_runner::RedisTestRunner};

#[derive(Deserialize)]
struct Expected {
    checked_count: usize,
    miss: BTreeSet<String>,
    diff: BTreeMap<String, BTreeSet<String>>,
    target_db: Option<String>,
}

pub struct RedisCheckTestRunner {
    pub base: RedisTestRunner,
    log_dir: String,
}

impl RedisCheckTestRunner {
    pub async fn new(relative: &str) -> anyhow::Result<Self> {
        let mut base = RedisTestRunner::new_default(relative).await?;
        let (_, log_dir) = CheckUtil::get_check_log_dir(&base.base, "");
        CheckUtil::clear_check_log(&log_dir);
        base.execute_prepare_sqls()?;
        base.execute_test_sqls()?;
        base.execute_target_test_sqls();
        Ok(Self { base, log_dir })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        self.base.base.start_task().await?;
        let expected: Expected = serde_json::from_str(&fs::read_to_string(
            Path::new(&self.base.base.test_dir).join("expected.json"),
        )?)?;
        let mut misses = BTreeSet::new();
        let mut diffs = BTreeMap::new();
        for (name, missing) in [("miss.log", true), ("diff.log", false)] {
            for line in read_lines(Path::new(&self.log_dir).join(name))? {
                let log: CheckLog = serde_json::from_str(&line)?;
                let key = log.id_col_values["key"].as_ref().unwrap();
                assert!(key.starts_with("hex:"));
                assert!(log.src_row.is_none() && log.dst_row.is_none());
                assert_eq!(log.target_db, expected.target_db);
                let identity = format!("{}:{}", log.db, key);
                if missing {
                    assert!(log.diff_col_values.is_empty());
                    assert!(misses.insert(identity), "duplicate missing key");
                } else {
                    let dimensions = log.diff_col_values.keys().cloned().collect::<BTreeSet<_>>();
                    assert!(!dimensions.is_empty());
                    assert!(
                        diffs.insert(identity, dimensions).is_none(),
                        "duplicate diff key"
                    );
                }
            }
        }
        assert_eq!(misses, expected.miss);
        assert_eq!(diffs, expected.diff);
        let summaries = read_lines(Path::new(&self.log_dir).join("summary.log"))?;
        assert_eq!(summaries.len(), 1);
        let summary: CheckSummaryLog = serde_json::from_str(&summaries[0])?;
        assert_eq!(summary.checked_count, expected.checked_count);
        assert_eq!(summary.miss_count, misses.len());
        assert_eq!(summary.diff_count, diffs.len());
        assert_eq!(summary.skip_count, 0);
        assert_eq!(summary.is_consistent, misses.is_empty() && diffs.is_empty());
        assert_eq!(
            summary
                .tables
                .iter()
                .map(|table| table.checked_count)
                .sum::<usize>(),
            summary.checked_count
        );
        assert_eq!(
            summary
                .tables
                .iter()
                .map(|table| table.miss_count)
                .sum::<usize>(),
            summary.miss_count
        );
        assert_eq!(
            summary
                .tables
                .iter()
                .map(|table| table.diff_count)
                .sum::<usize>(),
            summary.diff_count
        );
        assert!(!summary.start_time.is_empty() && !summary.end_time.is_empty());
        Ok(())
    }
}

fn read_lines(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }
    Ok(fs::read_to_string(path)?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect())
}
