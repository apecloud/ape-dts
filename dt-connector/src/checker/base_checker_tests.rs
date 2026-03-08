use super::*;
use dt_common::monitor::monitor::Monitor;
use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::mpsc;

struct MockChecker {
    calls: Arc<StdMutex<Vec<(String, String, usize)>>>,
}

struct MissingDstChecker;

fn build_test_tb_meta(schema: &str, tb: &str) -> Arc<CheckerTbMeta> {
    Arc::new(CheckerTbMeta::Mongo(RdbTbMeta {
        schema: schema.to_string(),
        tb: tb.to_string(),
        cols: vec!["id".to_string(), "v".to_string()],
        id_cols: vec!["id".to_string()],
        key_map: HashMap::from([("PRIMARY".to_string(), vec!["id".to_string()])]),
        ..Default::default()
    }))
}

#[async_trait]
impl Checker for MockChecker {
    async fn fetch(&mut self, src_rows: &[RowData]) -> anyhow::Result<FetchResult> {
        let first = src_rows
            .first()
            .context("mock checker requires non-empty source rows")?;
        self.calls
            .lock()
            .unwrap()
            .push((first.schema.clone(), first.tb.clone(), src_rows.len()));

        let tb_meta = build_test_tb_meta(&first.schema, &first.tb);

        let dst_rows = src_rows
            .iter()
            .map(|row| {
                let after = row.after.clone().or_else(|| row.before.clone());
                RowData::new(
                    row.schema.clone(),
                    row.tb.clone(),
                    RowType::Insert,
                    None,
                    after,
                )
            })
            .collect();

        Ok(FetchResult {
            tb_meta,
            src_rows: src_rows.to_vec(),
            dst_rows,
        })
    }
}

#[async_trait]
impl Checker for MissingDstChecker {
    async fn fetch(&mut self, src_rows: &[RowData]) -> anyhow::Result<FetchResult> {
        let first = src_rows
            .first()
            .context("mock checker requires non-empty source rows")?;
        let tb_meta = build_test_tb_meta(&first.schema, &first.tb);
        Ok(FetchResult {
            tb_meta,
            src_rows: src_rows.to_vec(),
            dst_rows: vec![],
        })
    }
}

fn build_context(monitor: Arc<Monitor>, is_cdc: bool, batch_size: usize) -> CheckContext {
    CheckContext {
        monitor,
        summary: CheckSummaryLog {
            start_time: "test".to_string(),
            ..Default::default()
        },
        output_revise_sql: false,
        extractor_meta_manager: None,
        reverse_router: RdbRouter {
            schema_map: HashMap::new(),
            tb_map: HashMap::new(),
            col_map: HashMap::new(),
            topic_map: HashMap::new(),
        },
        output_full_row: false,
        revise_match_full_row: false,
        global_summary: None,
        batch_size,
        retry_interval_secs: 0,
        max_retries: 0,
        is_cdc,
        check_log_dir: String::new(),
        cdc_check_log_max_file_size: 100 * 1024 * 1024,
        cdc_check_log_max_rows: 1000,
        s3_output: None,
        cdc_check_log_interval_secs: 1,
        state_store: None,
        expected_resume_position: None,
    }
}

fn build_insert_row(schema: &str, tb: &str, id: i32, v: &str) -> RowData {
    let mut after = HashMap::new();
    after.insert("id".to_string(), ColValue::Long(id));
    after.insert("v".to_string(), ColValue::String(v.to_string()));
    RowData::new(
        schema.to_string(),
        tb.to_string(),
        RowType::Insert,
        None,
        Some(after),
    )
}

fn build_update_key_changed_row(
    schema: &str,
    tb: &str,
    before_id: i32,
    after_id: i32,
    before_v: &str,
    after_v: &str,
) -> RowData {
    let mut before = HashMap::new();
    before.insert("id".to_string(), ColValue::Long(before_id));
    before.insert("v".to_string(), ColValue::String(before_v.to_string()));

    let mut after = HashMap::new();
    after.insert("id".to_string(), ColValue::Long(after_id));
    after.insert("v".to_string(), ColValue::String(after_v.to_string()));
    RowData::new(
        schema.to_string(),
        tb.to_string(),
        RowType::Update,
        Some(before),
        Some(after),
    )
}

fn build_buffer_test_log(id: i32) -> CheckLog {
    CheckLog {
        schema: "s1".to_string(),
        tb: "t1".to_string(),
        target_schema: None,
        target_tb: None,
        id_col_values: HashMap::from([("id".to_string(), Some(id.to_string()))]),
        diff_col_values: HashMap::new(),
        src_row: None,
        dst_row: None,
    }
}

fn collect_log_ids(buf: Vec<u8>) -> Vec<i32> {
    buf.split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let log: CheckLog =
                serde_json::from_slice(line).expect("line should be valid check log json");
            log.id_col_values
                .get("id")
                .and_then(|v| v.as_ref())
                .and_then(|v| v.parse::<i32>().ok())
                .expect("id should exist and be a valid integer")
        })
        .collect()
}

#[test]
fn bounded_ndjson_buffer_respects_row_limit() {
    let mut buffer = BoundedNdjsonBuffer::new(4096, 2);
    buffer.push(&build_buffer_test_log(1));
    buffer.push(&build_buffer_test_log(2));
    buffer.push(&build_buffer_test_log(3));

    let ids = collect_log_ids(buffer.into_bytes());
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn bounded_ndjson_buffer_respects_size_limit() {
    let first = build_buffer_test_log(1);
    let second = build_buffer_test_log(2);
    let third = build_buffer_test_log(3);
    let line_size = serde_json::to_vec(&first).unwrap().len() + 1;

    let mut buffer = BoundedNdjsonBuffer::new(line_size * 2, 10);
    buffer.push(&first);
    buffer.push(&second);
    buffer.push(&third);

    let ids = collect_log_ids(buffer.into_bytes());
    assert_eq!(ids, vec![2, 3]);
}

#[tokio::test]
async fn process_batch_groups_fetch_by_table() {
    let calls = Arc::new(StdMutex::new(Vec::new()));
    let checker = MockChecker {
        calls: calls.clone(),
    };

    let monitor = Arc::new(Monitor::new("checker", "test", 1, 10, 10));
    let ctx = build_context(monitor.clone(), false, 8);
    let (_tx, rx) = mpsc::channel(8);
    let mut data_checker = DataChecker::new(checker, "test-task".to_string(), ctx, rx, "test");

    let rows = vec![
        build_insert_row("s1", "t1", 1, "a"),
        build_insert_row("s1", "t2", 2, "b"),
    ];
    data_checker.process_batch(&rows, false).await.unwrap();

    let calls = calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    let tables: HashSet<(String, String)> = calls
        .iter()
        .map(|(schema, tb, _)| (schema.clone(), tb.clone()))
        .collect();
    assert!(tables.contains(&("s1".to_string(), "t1".to_string())));
    assert!(tables.contains(&("s1".to_string(), "t2".to_string())));
}

#[tokio::test]
async fn cdc_update_with_changed_key_should_not_mark_miss() {
    let checker = MockChecker {
        calls: Arc::new(StdMutex::new(Vec::new())),
    };
    let monitor = Arc::new(Monitor::new("checker", "test", 1, 10, 10));
    let ctx = build_context(monitor.clone(), true, 8);
    let (_tx, rx) = mpsc::channel(8);
    let mut data_checker = DataChecker::new(checker, "test-task".to_string(), ctx, rx, "test");

    let row = build_update_key_changed_row("s1", "t1", 1, 2, "before", "after");
    data_checker.process_batch(&[row], false).await.unwrap();

    assert!(data_checker.store.is_empty());
    assert_eq!(data_checker.ctx.summary.miss_count, 0);
    assert_eq!(data_checker.ctx.summary.diff_count, 0);
}
#[tokio::test]
async fn non_cdc_without_retry_should_record_miss_immediately() {
    let checker = MissingDstChecker;
    let monitor = Arc::new(Monitor::new("checker", "test", 1, 10, 10));
    let mut ctx = build_context(monitor.clone(), false, 8);
    ctx.max_retries = 0;
    ctx.retry_interval_secs = 10;
    let (_tx, rx) = mpsc::channel(8);
    let mut data_checker = DataChecker::new(checker, "test-task".to_string(), ctx, rx, "test");

    let row = build_insert_row("s1", "t1", 1, "a");
    data_checker.process_batch(&[row], false).await.unwrap();

    assert_eq!(data_checker.ctx.summary.miss_count, 1);
    assert_eq!(data_checker.ctx.summary.diff_count, 0);
    assert!(data_checker.retry_queue.is_empty());
    assert!(data_checker.store.is_empty());
}

#[test]
fn cdc_store_same_key_should_replace_entry_without_eviction() {
    let checker = MissingDstChecker;
    let monitor = Arc::new(Monitor::new("checker", "test", 1, 10, 10));
    let ctx = build_context(monitor.clone(), true, 8);
    let (_tx, rx) = mpsc::channel(8);
    let mut data_checker = DataChecker::new(checker, "test-task".to_string(), ctx, rx, "test");

    let mut id_col_values = HashMap::new();
    id_col_values.insert("id".to_string(), Some("1".to_string()));

    let first = CheckEntry {
        log: CheckLog {
            schema: "s1".to_string(),
            tb: "t1".to_string(),
            target_schema: None,
            target_tb: None,
            id_col_values: id_col_values.clone(),
            diff_col_values: HashMap::new(),
            src_row: None,
            dst_row: None,
        },
        revise_sql: None,
        is_miss: true,
        src_row_data: build_insert_row("s1", "t1", 1, "a"),
    };
    data_checker.store_entry(1, first);
    assert_eq!(data_checker.store.len(), 1);
    assert_eq!(data_checker.evicted_miss, 0);
    assert_eq!(data_checker.evicted_diff, 0);

    let mut diff_col_values = HashMap::new();
    diff_col_values.insert(
        "v".to_string(),
        DiffColValue {
            src: Some("a".to_string()),
            dst: Some("b".to_string()),
            src_type: None,
            dst_type: None,
        },
    );
    let second = CheckEntry {
        log: CheckLog {
            schema: "s1".to_string(),
            tb: "t1".to_string(),
            target_schema: None,
            target_tb: None,
            id_col_values,
            diff_col_values,
            src_row: None,
            dst_row: None,
        },
        revise_sql: None,
        is_miss: false,
        src_row_data: build_insert_row("s1", "t1", 1, "b"),
    };
    data_checker.store_entry(1, second);

    assert_eq!(data_checker.store.len(), 1);
    assert_eq!(data_checker.evicted_miss, 0);
    assert_eq!(data_checker.evicted_diff, 0);
    let stored = data_checker.store.get(&1).unwrap();
    assert!(!stored.is_miss);
    assert_eq!(stored.log.diff_col_values.len(), 1);
}

#[test]
fn remove_store_entry_should_keep_fifo_order() {
    let checker = MissingDstChecker;
    let monitor = Arc::new(Monitor::new("checker", "test", 1, 10, 10));
    let ctx = build_context(monitor.clone(), true, 8);
    let (_tx, rx) = mpsc::channel(8);
    let mut data_checker = DataChecker::new(checker, "test-task".to_string(), ctx, rx, "test");

    let make_entry = |id: i32| CheckEntry {
        log: CheckLog {
            schema: "s1".to_string(),
            tb: "t1".to_string(),
            target_schema: None,
            target_tb: None,
            id_col_values: HashMap::from([("id".to_string(), Some(id.to_string()))]),
            diff_col_values: HashMap::new(),
            src_row: None,
            dst_row: None,
        },
        revise_sql: None,
        is_miss: true,
        src_row_data: build_insert_row("s1", "t1", id, "v"),
    };

    data_checker.store_entry(1, make_entry(1));
    data_checker.store_entry(2, make_entry(2));
    data_checker.store_entry(3, make_entry(3));
    data_checker.remove_store_entry(1);

    let keys: Vec<u128> = data_checker.store.keys().copied().collect();
    assert_eq!(keys, vec![2, 3]);
}
