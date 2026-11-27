use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use chrono::Local;
use dt_common::{
    config::{checker_config::CheckerConfig, connection_auth_config::ConnectionAuthConfig},
    filter::DtFilter,
    log_diff, log_miss, log_summary, log_warn,
    meta::{
        dt_data::{DtData, DtItem},
        redis::{
            command::key_parser::KeyParser, redis_entry::RedisEntry, redis_object::RedisObject,
        },
    },
    monitor::{counter_type::CounterType, task_metrics::TaskMetricsType},
    resource::ResourceKind,
    rid,
    router::DtRouter,
    utils::redis_util::RedisUtil,
};
use redis::{aio::MultiplexedConnection, AsyncConnectionConfig, Cmd, FromRedisValue};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    time::sleep,
};

use super::{
    base_checker::{is_summary_consistent, CheckContext},
    check_log::{CheckLog, CheckTableSummaryLog, DiffColValue},
};

const STRING_CHUNK_BYTES: usize = 64 * 1024;
type Differences = HashMap<String, DiffColValue>;

#[derive(Clone, Serialize, Deserialize)]
enum Outcome {
    Equal,
    MissingTarget,
    Diff(Differences),
    Unstable,
}

#[derive(Clone, Debug)]
enum Expiry {
    Missing,
    Persistent,
    At { millis: i128, uncertainty: i128 },
}

impl Expiry {
    fn same(&self, other: &Self, tolerance: i128) -> bool {
        match (self, other) {
            (Self::Missing, Self::Missing) | (Self::Persistent, Self::Persistent) => true,
            (
                Self::At {
                    millis: a,
                    uncertainty: x,
                },
                Self::At {
                    millis: b,
                    uncertainty: y,
                },
            ) => (a - b).abs() <= tolerance + x + y,
            _ => false,
        }
    }

    fn label(&self) -> String {
        match self {
            Self::Missing => "missing".into(),
            Self::Persistent => "persistent".into(),
            Self::At { millis, .. } => format!("expires_at_check_elapsed_ms:{millis}"),
        }
    }
}

struct Metadata {
    kind: String,
    expiry: Expiry,
}

impl Metadata {
    fn exists(&self) -> bool {
        self.kind != "none" && !matches!(self.expiry, Expiry::Missing)
    }
}

/// Standalone Redis comparison of extracted source entries against the target.
pub struct RedisChecker {
    source: RedisCheckEndpoint,
    target: RedisCheckEndpoint,
    filter: DtFilter,
    router: Option<DtRouter>,
    page_size: usize,
    tolerance: i128,
    sample_percent: Option<u8>,
    clock: Instant,
    reverse_nodes: Option<VecDeque<(i64, i64, usize)>>,
    reverse_cursor: u64,
    ctx: CheckContext,
    results: ResultSpool,
    failed: bool,
    closed: bool,
}

impl RedisChecker {
    pub async fn new(
        source: RedisCheckEndpoint,
        target: RedisCheckEndpoint,
        filter: DtFilter,
        router: Option<DtRouter>,
        config: &CheckerConfig,
        ttl_tolerance_ms: u64,
        mut ctx: CheckContext,
    ) -> anyhow::Result<Self> {
        validate_routes(
            router.as_ref(),
            &filter,
            source.is_cluster(),
            target.is_cluster(),
        )?;
        ctx.summary.start_time = Local::now().to_rfc3339();
        let results = ResultSpool::new(
            Path::new(&ctx.check_log_dir),
            ctx.retry_buffer_max_rows,
            ctx.retry_buffer_max_bytes,
        )
        .await?;
        Ok(Self {
            source,
            target,
            filter,
            router,
            page_size: config.batch_size.clamp(1, 1000),
            tolerance: i128::from(ttl_tolerance_ms),
            sample_percent: config.sample_percent,
            clock: Instant::now(),
            reverse_nodes: None,
            reverse_cursor: 0,
            ctx,
            results,
            failed: false,
            closed: false,
        })
    }

    fn selected(&self, key: &CheckKey) -> anyhow::Result<bool> {
        Ok(!self.filter.filter(rid!(key.db.clone()))?
            && sampled(key.db.parse()?, &key.key, self.sample_percent))
    }
    fn target_database(&self, db: i64) -> anyhow::Result<i64> {
        map_db(self.router.as_ref(), db)
    }
    fn source_db(&self, db: i64) -> anyhow::Result<Option<i64>> {
        source_database(self.router.as_ref(), &self.filter, db)
    }
    async fn compare_snapshot(&mut self, entry: &RedisEntry) -> anyhow::Result<Outcome> {
        let db = self.target_database(entry.db_id)?;
        let key = entry.key.as_bytes();
        let source_expiry = match entry.expire_at_ms {
            Some(deadline) => {
                let remaining =
                    i128::from(deadline) - i128::from(chrono::Utc::now().timestamp_millis());
                if remaining <= 0 {
                    let target = metadata(&mut self.target, db, key, self.clock).await?;
                    return Ok(if target.exists() {
                        Outcome::Diff(HashMap::from([(
                            "existence".into(),
                            difference(None, Some("present".into())),
                        )]))
                    } else {
                        Outcome::Equal
                    });
                }
                Expiry::At {
                    millis: self.clock.elapsed().as_millis() as i128 + remaining,
                    uncertainty: 1,
                }
            }
            None if entry.expire_ms == 0 => Expiry::Persistent,
            None => {
                bail!("Redis checker requires an absolute expiration deadline for expiring entries")
            }
        };
        let before = metadata(&mut self.target, db, key, self.clock).await?;
        if !before.exists() {
            return Ok(Outcome::MissingTarget);
        }
        let kind = entry.get_type();
        let mut differences = Differences::new();
        if kind != before.kind {
            differences.insert(
                "type".into(),
                difference(Some(kind.clone()), Some(before.kind.clone())),
            );
        } else if !self.snapshot_content_equal(entry, db).await? {
            differences.insert(
                "value".into(),
                DiffColValue {
                    src: None,
                    dst: None,
                    src_type: Some(kind),
                    dst_type: Some(before.kind.clone()),
                },
            );
        }
        if !source_expiry.same(&before.expiry, self.tolerance) {
            differences.insert(
                "ttl".into(),
                difference(Some(source_expiry.label()), Some(before.expiry.label())),
            );
        }
        let after = metadata(&mut self.target, db, key, self.clock).await?;
        if before.kind != after.kind || !before.expiry.same(&after.expiry, self.tolerance) {
            return Ok(Outcome::Unstable);
        }
        Ok(if differences.is_empty() {
            Outcome::Equal
        } else {
            Outcome::Diff(differences)
        })
    }

    async fn snapshot_content_equal(
        &mut self,
        entry: &RedisEntry,
        db: i64,
    ) -> anyhow::Result<bool> {
        let key = entry.key.as_bytes();
        let (command, expected_len) = match &entry.value {
            RedisObject::String(value) => ("STRLEN", value.value.bytes.len()),
            RedisObject::List(value) => ("LLEN", value.elements.len()),
            RedisObject::Hash(value) => ("HLEN", value.value.len()),
            RedisObject::Set(value) => ("SCARD", value.elements.len()),
            RedisObject::Zset(value) => ("ZCARD", value.elements.len()),
            RedisObject::Stream(value) => (
                "XLEN",
                value.cmds.iter().filter(|cmd| is_stream_entry(cmd)).count(),
            ),
            _ => bail!(
                "Redis checker does not support value type: {}",
                entry.get_type()
            ),
        };
        let length: usize = self
            .target
            .query(db, key, redis::cmd(command).arg(key))
            .await?;
        if length != expected_len {
            return Ok(false);
        }
        match &entry.value {
            RedisObject::String(value) => {
                for (page, expected) in value.value.bytes.chunks(STRING_CHUNK_BYTES).enumerate() {
                    let start = page * STRING_CHUNK_BYTES;
                    let found: Vec<u8> = self
                        .target
                        .query(
                            db,
                            key,
                            redis::cmd("GETRANGE")
                                .arg(key)
                                .arg(start)
                                .arg(start + expected.len() - 1),
                        )
                        .await?;
                    if found != expected {
                        return Ok(false);
                    }
                }
            }
            RedisObject::List(value) => {
                for (page, expected) in value.elements.chunks(self.page_size).enumerate() {
                    let start = page * self.page_size;
                    let found: Vec<Vec<u8>> = self
                        .target
                        .query(
                            db,
                            key,
                            redis::cmd("LRANGE")
                                .arg(key)
                                .arg(start)
                                .arg(start + expected.len() - 1),
                        )
                        .await?;
                    if found.len() != expected.len()
                        || found.iter().zip(expected).any(|(a, b)| a != &b.bytes)
                    {
                        return Ok(false);
                    }
                }
            }
            RedisObject::Hash(value) => {
                let mut fields = value.value.iter();
                loop {
                    let page: Vec<_> = fields.by_ref().take(self.page_size).collect();
                    if page.is_empty() {
                        break;
                    }
                    let mut cmd = redis::cmd("HMGET");
                    cmd.arg(key);
                    for (field, (_, expiry)) in &page {
                        anyhow::ensure!(
                            expiry.is_none(),
                            "Redis checker does not support hash-field expiration"
                        );
                        cmd.arg(field.as_bytes());
                    }
                    let found: Vec<Option<Vec<u8>>> = self.target.query(db, key, &cmd).await?;
                    if found.len() != page.len()
                        || found
                            .iter()
                            .zip(page)
                            .any(|(a, (_, (b, _)))| a.as_deref() != Some(b.as_bytes()))
                    {
                        return Ok(false);
                    }
                }
            }
            RedisObject::Set(value) => {
                for page in value.elements.chunks(self.page_size) {
                    let mut pipeline = redis::pipe();
                    for member in page {
                        pipeline.cmd("SISMEMBER").arg(key).arg(member.as_bytes());
                    }
                    let found: Vec<bool> = self.target.pipeline(db, key, &pipeline).await?;
                    if found.len() != page.len() || found.iter().any(|v| !v) {
                        return Ok(false);
                    }
                }
            }
            RedisObject::Zset(value) => {
                for page in value.elements.chunks(self.page_size) {
                    let mut pipeline = redis::pipe();
                    for item in page {
                        pipeline.cmd("ZSCORE").arg(key).arg(item.member.as_bytes());
                    }
                    let found: Vec<Option<f64>> = self.target.pipeline(db, key, &pipeline).await?;
                    anyhow::ensure!(found.len() == page.len(), "invalid Redis score response");
                    for (score, expected) in found.iter().zip(page) {
                        let expected: f64 =
                            std::str::from_utf8(expected.score.as_bytes())?.parse()?;
                        if *score != Some(expected) {
                            return Ok(false);
                        }
                    }
                }
            }
            RedisObject::Stream(value) => {
                let mut entries = value.cmds.iter().filter(|cmd| is_stream_entry(cmd));
                let mut start = "-".to_string();
                loop {
                    let page: Vec<_> = entries.by_ref().take(self.page_size).collect();
                    if page.is_empty() {
                        break;
                    }
                    let found: Vec<(String, Vec<Vec<u8>>)> = self
                        .target
                        .query(
                            db,
                            key,
                            redis::cmd("XRANGE")
                                .arg(key)
                                .arg(&start)
                                .arg("+")
                                .arg("COUNT")
                                .arg(page.len()),
                        )
                        .await?;
                    if found.len() != page.len()
                        || found.iter().zip(&page).any(|((id, fields), cmd)| {
                            id.as_bytes() != cmd.args[2] || fields.as_slice() != &cmd.args[3..]
                        })
                    {
                        return Ok(false);
                    }
                    let Some((last, _)) = found.last() else {
                        bail!("empty Redis stream page");
                    };
                    let Some(next) = next_stream_id(last)? else {
                        break;
                    };
                    start = next;
                }
                let expected_id = value
                    .cmds
                    .iter()
                    .find(|cmd| cmd.get_name().eq_ignore_ascii_case("XSETID"))
                    .and_then(|cmd| cmd.args.get(2))
                    .context("RDB stream lacks its last-generated-id")?;
                let info: Vec<redis::Value> = self
                    .target
                    .query(db, key, redis::cmd("XINFO").arg("STREAM").arg(key))
                    .await?;
                if stream_last_id(&info)?.as_bytes() != expected_id {
                    return Ok(false);
                }
            }
            _ => bail!("unsupported Redis value"),
        }
        Ok(true)
    }
}

impl RedisChecker {
    fn key(&self, item: &DtItem) -> anyhow::Result<Option<CheckKey>> {
        let DtData::Redis { entry } = &item.dt_data else {
            bail!("Redis checker received non-Redis data")
        };
        let key = CheckKey {
            db: entry.db_id.to_string(),
            key: entry.key.bytes.clone(),
        };
        Ok(self.selected(&key)?.then_some(key))
    }
    fn target_db(&self, db: &str) -> anyhow::Result<String> {
        Ok(self.target_database(db.parse()?)?.to_string())
    }
    pub fn sink_ids(&self) -> Vec<String> {
        self.target
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect()
    }
    async fn compare_item(&mut self, item: &DtItem) -> anyhow::Result<Outcome> {
        let DtData::Redis { entry } = &item.dt_data else {
            bail!("Redis checker received non-Redis data")
        };
        match self.compare_snapshot(entry).await {
            Err(error) if unstable_read(&error) => Ok(Outcome::Unstable),
            result => result,
        }
    }
    async fn compare_key(&mut self, key: &CheckKey) -> anyhow::Result<Outcome> {
        let source_db: i64 = key.db.parse()?;
        let exists: bool = self
            .source
            .query(source_db, &key.key, redis::cmd("EXISTS").arg(&key.key))
            .await?;
        if exists {
            return Ok(Outcome::Equal);
        }
        let db = self.target_database(source_db)?;
        let target = metadata(&mut self.target, db, &key.key, self.clock).await?;
        Ok(if target.exists() {
            Outcome::Diff(HashMap::from([(
                "existence".into(),
                difference(None, Some("present".into())),
            )]))
        } else {
            Outcome::Equal
        })
    }
    async fn next_extra_keys(&mut self) -> anyhow::Result<Option<Vec<CheckKey>>> {
        if self.reverse_nodes.is_none() {
            let mut nodes = VecDeque::new();
            for db in self.target.databases().await? {
                if let Some(source_db) = self.source_db(db)? {
                    if !self.filter.filter(rid!(source_db.to_string()))? {
                        for node in 0..self.target.node_count() {
                            nodes.push_back((db, source_db, node));
                        }
                    }
                }
            }
            self.reverse_nodes = Some(nodes);
        }
        let nodes = self
            .reverse_nodes
            .as_mut()
            .context("missing reverse scan state")?;
        let Some(&(db, source_db, node)) = nodes.front() else {
            return Ok(None);
        };
        let (cursor, keys) = self
            .target
            .scan(node, db, self.reverse_cursor, self.page_size)
            .await?;
        if cursor == 0 {
            nodes.pop_front();
        }
        self.reverse_cursor = cursor;
        let mut extra = Vec::new();
        for bytes in keys {
            let key = CheckKey {
                db: source_db.to_string(),
                key: bytes,
            };
            if self.selected(&key)? {
                let exists: bool = self
                    .source
                    .query(source_db, &key.key, redis::cmd("EXISTS").arg(&key.key))
                    .await?;
                if !exists {
                    extra.push(key);
                }
            }
        }
        Ok(Some(extra))
    }
}

impl RedisChecker {
    pub async fn check_raw(&mut self, data: Vec<DtItem>) -> anyhow::Result<()> {
        let result = self.check_items(data).await;
        self.failed |= result.is_err();
        result
    }
    async fn check_items(&mut self, items: Vec<DtItem>) -> Result<()> {
        let mut pending = Vec::new();
        let mut checked = 0;
        for item in items {
            if matches!(
                item.dt_data,
                DtData::Heartbeat {} | DtData::Begin { .. } | DtData::Commit { .. }
            ) {
                continue;
            }
            let Some(key) = self.key(&item)? else {
                continue;
            };
            if matches!(&item.dt_data, DtData::Redis { entry } if matches!(entry.value, RedisObject::Module(_)))
            {
                log_warn!(
                    "skip Redis module value check: db={}, key_hex={}",
                    key.db,
                    hex::encode(&key.key)
                );
                self.ctx.summary.skip_count += 1;
                let mut table = self.table(&key.db)?;
                table.skip_count = 1;
                self.ctx.summary.merge_table(table);
                continue;
            }
            let outcome = self.compare_item(&item).await?;
            self.record_checked(&key.db, 1)?;
            checked += 1;
            if !matches!(outcome, Outcome::Equal) {
                pending.push((item, key, outcome));
            }
        }
        // Retain the incoming snapshot only for this bounded batch, including retries.
        for _ in 0..self.ctx.max_retries {
            if pending.is_empty() {
                break;
            }
            sleep(Duration::from_secs(self.ctx.retry_interval_secs)).await;
            let mut remaining = Vec::new();
            for (item, key, _) in pending {
                let outcome = self.compare_item(&item).await?;
                if !matches!(outcome, Outcome::Equal) {
                    remaining.push((item, key, outcome));
                }
            }
            pending = remaining;
        }
        for (_, key, outcome) in pending {
            self.results
                .push(&ResultRecord {
                    key,
                    outcome,
                    source_observed: true,
                })
                .await?;
        }
        self.ctx
            .base_sinker
            .update_serial_monitor(checked, 0)
            .await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        while let Some(keys) = self.next_extra_keys().await? {
            let mut pending = Vec::new();
            for key in keys {
                let outcome = self.compare_key(&key).await?;
                if !matches!(outcome, Outcome::Equal) {
                    pending.push((key, outcome));
                }
            }
            for _ in 0..self.ctx.max_retries {
                if pending.is_empty() {
                    break;
                }
                sleep(Duration::from_secs(self.ctx.retry_interval_secs)).await;
                let mut remaining = Vec::new();
                for (key, _) in pending {
                    let outcome = self.compare_key(&key).await?;
                    if !matches!(outcome, Outcome::Equal) {
                        remaining.push((key, outcome));
                    }
                }
                pending = remaining;
            }
            for (key, outcome) in pending {
                self.results
                    .push(&ResultRecord {
                        key,
                        outcome,
                        source_observed: false,
                    })
                    .await?;
            }
        }
        let path = self.results.sorted().await?;
        let mut reader = BufReader::new(File::open(path).await?).lines();
        while let Some(record) = read_result(&mut reader).await? {
            self.record_result(&record.key, record.outcome).await?;
        }
        Ok(())
    }

    fn table(&self, db: &str) -> anyhow::Result<CheckTableSummaryLog> {
        let target = self.target_db(db)?;
        Ok(CheckTableSummaryLog {
            db: db.to_string(),
            target_db: (db != target).then(|| target.to_string()),
            ..Default::default()
        })
    }

    fn record_checked(&mut self, db: &str, count: usize) -> anyhow::Result<()> {
        self.ctx.summary.checked_count += count;
        let mut table = self.table(db)?;
        table.checked_count = count;
        self.ctx.summary.merge_table(table);
        Ok(())
    }

    async fn record_result(&mut self, key: &CheckKey, outcome: Outcome) -> anyhow::Result<()> {
        let mut table = self.table(&key.db)?;
        let differences = match outcome {
            Outcome::Equal => return Ok(()),
            Outcome::MissingTarget => {
                self.ctx.summary.miss_count += 1;
                table.miss_count = 1;
                self.ctx
                    .add_checker_counter(CounterType::CheckerMissCount, 1)
                    .await;
                self.ctx
                    .add_checker_metric(TaskMetricsType::CheckerMissCount, 1);
                Differences::new()
            }
            Outcome::Diff(differences) => {
                self.ctx.summary.diff_count += 1;
                table.diff_count = 1;
                self.ctx
                    .add_checker_counter(CounterType::CheckerDiffCount, 1)
                    .await;
                self.ctx
                    .add_checker_metric(TaskMetricsType::CheckerDiffCount, 1);
                differences
            }
            Outcome::Unstable => {
                self.ctx.summary.skip_count += 1;
                table.skip_count = 1;
                HashMap::from([("unstable".into(), difference(None, None))])
            }
        };
        self.ctx.summary.merge_table(table);
        let target = self.target_db(&key.db)?;
        let log = CheckLog {
            db: key.db.to_string(),
            schema: String::new(),
            tb: String::new(),
            target_db: (target != key.db).then(|| target.to_string()),
            target_schema: None,
            target_tb: None,
            id_col_values: HashMap::from([(
                "key".into(),
                Some(format!("hex:{}", hex::encode(&key.key))),
            )]),
            diff_col_values: differences,
            src_row: None,
            dst_row: None,
        };
        let json = serde_json::to_string(&log)?;
        if log.diff_col_values.is_empty() {
            log_miss!("{}", json);
        } else {
            log_diff!("{}", json);
        }
        Ok(())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        if self.closed {
            return Ok(());
        }
        let result = if self.failed {
            Err(anyhow::anyhow!("Redis checker failed before finalization"))
        } else {
            self.finish().await
        };
        self.failed |= result.is_err();
        if self.failed {
            self.ctx.summary.skip_count += 1;
        }
        self.ctx.summary.end_time = Local::now().to_rfc3339();
        self.ctx.summary.is_consistent = is_summary_consistent(&self.ctx.summary, self.failed);
        self.ctx.summary.sort_tables();
        if let Some(summary) = &self.ctx.global_summary {
            summary.lock().await.merge(&self.ctx.summary);
        } else {
            log_summary!("{}", serde_json::to_string(&self.ctx.summary)?);
        }
        self.closed = true;
        let cleanup = self.results.cleanup().await;
        result.and(cleanup)
    }
}

fn is_stream_entry(cmd: &dt_common::meta::redis::redis_object::RedisCmd) -> bool {
    cmd.get_name().eq_ignore_ascii_case("XADD")
        && cmd.args.len() >= 3
        && !cmd.args[2].eq_ignore_ascii_case(b"MAXLEN")
}
fn unstable_read(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<redis::RedisError>()
        .is_some_and(|e| e.code() == Some("WRONGTYPE") || e.detail() == Some("no such key"))
}
fn difference(src: Option<String>, dst: Option<String>) -> DiffColValue {
    DiffColValue {
        src,
        dst,
        src_type: None,
        dst_type: None,
    }
}

fn validate_routes(
    router: Option<&DtRouter>,
    filter: &DtFilter,
    source_cluster: bool,
    target_cluster: bool,
) -> anyhow::Result<()> {
    let Some(router) = router else {
        return Ok(());
    };
    let mut targets = BTreeSet::new();
    if let Some(databases) = router.resource_routers().get(&ResourceKind::Database) {
        for (source, target) in databases.mappings() {
            let source: i64 = source.segments()[0]
                .parse()
                .context("invalid source Redis database")?;
            let target: i64 = target.segments()[0]
                .parse()
                .context("invalid target Redis database")?;
            if filter.filter(rid!(source.to_string()))? {
                continue;
            }
            anyhow::ensure!(
                source >= 0 && target >= 0,
                "Redis databases must be nonnegative"
            );
            anyhow::ensure!(
                !source_cluster || source == 0,
                "Redis Cluster source only supports db 0"
            );
            anyhow::ensure!(
                !target_cluster || target == 0,
                "Redis Cluster target only supports db 0"
            );
            anyhow::ensure!(
                targets.insert(target),
                "Redis check requires one-to-one database routes"
            );
            if source != target
                && !filter.filter(rid!(target.to_string()))?
                && map_db(Some(router), target)? == target
            {
                bail!("Redis check route collides with an unrenamed source database; select source databases explicitly");
            }
        }
    }
    Ok(())
}

fn source_database(
    router: Option<&DtRouter>,
    filter: &DtFilter,
    db: i64,
) -> anyhow::Result<Option<i64>> {
    if let Some(routes) =
        router.and_then(|router| router.resource_routers().get(&ResourceKind::Database))
    {
        for (source, target) in routes.mappings() {
            let source: i64 = source.segments()[0].parse()?;
            let target: i64 = target.segments()[0].parse()?;
            if target == db && !filter.filter(rid!(source.to_string()))? {
                return Ok(Some(source));
            }
        }
    }
    Ok((map_db(router, db)? == db).then_some(db))
}

fn map_db(router: Option<&DtRouter>, db: i64) -> anyhow::Result<i64> {
    let Some(router) = router else {
        return Ok(db);
    };
    let mapped: String = router.forward(rid!(db.to_string()))?;
    mapped
        .parse()
        .context("invalid Redis checker database mapping")
}

fn sampled(db: i64, key: &[u8], percent: Option<u8>) -> bool {
    // Stable FNV-1a identity sampling applies identically in both scan directions.
    let hash = db
        .to_le_bytes()
        .iter()
        .chain(key)
        .fold(0xcbf29ce484222325u64, |hash, b| {
            (hash ^ u64::from(*b)).wrapping_mul(0x100000001b3)
        });
    percent.is_none_or(|percent| hash % 100 < u64::from(percent))
}

async fn metadata(
    endpoint: &mut RedisCheckEndpoint,
    db: i64,
    key: &[u8],
    clock: Instant,
) -> anyhow::Result<Metadata> {
    let before = clock.elapsed().as_millis() as i128;
    let mut pipeline = redis::pipe();
    pipeline.cmd("TYPE").arg(key).cmd("PTTL").arg(key);
    let (kind, ttl): (String, i64) = endpoint.pipeline(db, key, &pipeline).await?;
    let after = clock.elapsed().as_millis() as i128;
    let expiry = match ttl {
        -2 => Expiry::Missing,
        -1 => Expiry::Persistent,
        n if n >= 0 => Expiry::At {
            millis: (before + after) / 2 + i128::from(n),
            uncertainty: (after - before + 1) / 2,
        },
        _ => bail!("invalid Redis PTTL response: {ttl}"),
    };
    Ok(Metadata { kind, expiry })
}

fn next_stream_id(id: &str) -> anyhow::Result<Option<String>> {
    let (millis, sequence) = id.split_once('-').context("invalid Redis stream ID")?;
    let millis: u64 = millis.parse()?;
    let sequence: u64 = sequence.parse()?;
    Ok(if let Some(next) = sequence.checked_add(1) {
        Some(format!("{millis}-{next}"))
    } else {
        millis.checked_add(1).map(|next| format!("{next}-0"))
    })
}

fn stream_last_id(values: &[redis::Value]) -> anyhow::Result<String> {
    anyhow::ensure!(values.len() % 2 == 0, "invalid Redis XINFO STREAM response");
    for pair in values.chunks_exact(2) {
        if redis::from_redis_value::<String>(&pair[0])? == "last-generated-id" {
            return redis::from_redis_value(&pair[1]).map_err(Into::into);
        }
    }
    bail!("Redis XINFO STREAM response lacks last-generated-id")
}

struct Node {
    id: String,
    conn: MultiplexedConnection,
    db: i64,
}

/// Read-only connections shared by the standalone scan and comparison paths.
pub struct RedisCheckEndpoint {
    nodes: Vec<Node>,
    slots: Vec<usize>,
}

impl RedisCheckEndpoint {
    pub async fn new(
        url: &str,
        auth: &ConnectionAuthConfig,
        is_cluster: Option<bool>,
    ) -> anyhow::Result<Self> {
        let mut seed = Self::connect(url, auth).await?;
        let cluster = match is_cluster {
            Some(cluster) => cluster,
            None => {
                let info: String = redis::cmd("INFO")
                    .arg("cluster")
                    .query_async(&mut seed)
                    .await
                    .context("failed to read Redis cluster configuration")?;
                info.lines().any(|line| line.trim() == "cluster_enabled:1")
            }
        };
        if !cluster {
            // URL database selection must not leak into scans of other logical databases.
            redis::cmd("SELECT")
                .arg(0)
                .query_async::<()>(&mut seed)
                .await?;
            return Ok(Self {
                nodes: vec![Node {
                    id: String::new(),
                    conn: seed,
                    db: 0,
                }],
                slots: Vec::new(),
            });
        }

        let topology: String = redis::cmd("CLUSTER")
            .arg("NODES")
            .query_async(&mut seed)
            .await
            .context("failed to read Redis master topology")?;
        let mut masters = RedisUtil::parse_cluster_nodes(&topology)?
            .into_iter()
            .filter(|node| node.is_master)
            .collect::<Vec<_>>();
        masters.sort_by(|a, b| a.address.cmp(&b.address));
        let mut slots = vec![usize::MAX; 16384];
        let mut nodes = Vec::with_capacity(masters.len());
        for master in masters {
            for slot in master.slots {
                let destination = &mut slots[usize::from(slot)];
                anyhow::ensure!(
                    *destination == usize::MAX,
                    "Redis slot has multiple masters"
                );
                *destination = nodes.len();
            }
            let url = RedisUtil::replace_url_address(url, &master.host, master.port.parse()?)?;
            nodes.push(Node {
                id: master.address,
                conn: Self::connect(&url, auth).await?,
                db: 0,
            });
        }
        anyhow::ensure!(
            !nodes.is_empty() && !slots.contains(&usize::MAX),
            "Redis master topology does not cover every slot"
        );
        Ok(Self { nodes, slots })
    }

    async fn connect(
        url: &str,
        auth: &ConnectionAuthConfig,
    ) -> anyhow::Result<MultiplexedConnection> {
        let resolved = RedisUtil::resolve_connection_config(url, auth)?;
        let client = resolved.ssl_config.apply_redis(&resolved.url)?;
        client
            .get_multiplexed_async_connection_with_config(
                &AsyncConnectionConfig::new()
                    .set_connection_timeout(Duration::from_secs(30))
                    .set_response_timeout(Duration::from_secs(30)),
            )
            .await
            .context("failed to connect to Redis checker endpoint")
    }

    pub fn is_cluster(&self) -> bool {
        !self.slots.is_empty()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub async fn databases(&mut self) -> anyhow::Result<BTreeSet<i64>> {
        if self.is_cluster() {
            return Ok(BTreeSet::from([0]));
        }
        let info: String = self
            .on_node(0, 0, redis::cmd("INFO").arg("keyspace"))
            .await?;
        let mut databases = BTreeSet::new();
        for line in info.lines() {
            if let Some((db, _)) = line
                .trim()
                .strip_prefix("db")
                .and_then(|s| s.split_once(':'))
            {
                databases.insert(db.parse().context("invalid Redis keyspace database")?);
            }
        }
        Ok(databases)
    }

    pub async fn scan(
        &mut self,
        node: usize,
        db: i64,
        cursor: u64,
        count: usize,
    ) -> anyhow::Result<(u64, Vec<Vec<u8>>)> {
        self.on_node(
            node,
            db,
            redis::cmd("SCAN").arg(cursor).arg("COUNT").arg(count),
        )
        .await
    }

    pub async fn query<T: FromRedisValue>(
        &mut self,
        db: i64,
        key: &[u8],
        cmd: &Cmd,
    ) -> anyhow::Result<T> {
        let node = if self.is_cluster() {
            self.slots[usize::from(KeyParser::calc_slot(key))]
        } else {
            0
        };
        self.on_node(node, db, cmd).await
    }

    pub async fn pipeline<T: FromRedisValue>(
        &mut self,
        db: i64,
        key: &[u8],
        pipeline: &redis::Pipeline,
    ) -> anyhow::Result<T> {
        let node = if self.is_cluster() {
            self.slots[usize::from(KeyParser::calc_slot(key))]
        } else {
            0
        };
        pipeline
            .query_async(self.connection(node, db).await?)
            .await
            .context("Redis checker pipeline read failed")
    }

    async fn on_node<T: FromRedisValue>(
        &mut self,
        node: usize,
        db: i64,
        cmd: &Cmd,
    ) -> anyhow::Result<T> {
        cmd.query_async(self.connection(node, db).await?)
            .await
            .context("Redis checker read failed (topology changes require a new check)")
    }

    async fn connection(
        &mut self,
        node: usize,
        db: i64,
    ) -> anyhow::Result<&mut MultiplexedConnection> {
        if db < 0 || (self.is_cluster() && db != 0) {
            bail!("invalid logical database for Redis checker endpoint: {db}");
        }
        let node = self
            .nodes
            .get_mut(node)
            .context("missing Redis master connection")?;
        if node.db != db {
            redis::cmd("SELECT")
                .arg(db)
                .query_async::<()>(&mut node.conn)
                .await
                .context("failed to select Redis checker database")?;
            node.db = db;
        }
        Ok(&mut node.conn)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct CheckKey {
    db: String,
    key: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ResultRecord {
    key: CheckKey,
    outcome: Outcome,
    source_observed: bool,
}

impl ResultRecord {
    fn memory_size(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>() + self.key.db.len() + self.key.key.len();
        if let Outcome::Diff(values) = &self.outcome {
            bytes += values.capacity()
                * (std::mem::size_of::<String>() + std::mem::size_of::<DiffColValue>());
            for (name, value) in values {
                bytes += name.len();
                for text in [&value.src, &value.dst, &value.src_type, &value.dst_type]
                    .into_iter()
                    .flatten()
                {
                    bytes += text.len();
                }
            }
        }
        bytes
    }

    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| other.source_observed.cmp(&self.source_observed))
    }
}

type KeyReader = tokio::io::Lines<BufReader<File>>;

async fn read_result(reader: &mut KeyReader) -> anyhow::Result<Option<ResultRecord>> {
    reader
        .next_line()
        .await?
        .map(|line| serde_json::from_str(&line).context("invalid Redis checker spool record"))
        .transpose()
}

async fn write_result(writer: &mut BufWriter<File>, key: &ResultRecord) -> anyhow::Result<()> {
    let mut encoded = serde_json::to_vec(key)?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await?;
    Ok(())
}

/// Only identities and final differences are spooled; source payloads are never serialized.
struct ResultSpool {
    dir: PathBuf,
    writer: BufWriter<File>,
    max_rows: usize,
    max_bytes: usize,
}

impl ResultSpool {
    async fn new(base: &Path, max_rows: usize, max_bytes: usize) -> anyhow::Result<Self> {
        let dir = base.join(format!(".redis-check-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).await?;
        let writer = BufWriter::new(File::create(dir.join("candidates")).await?);
        Ok(Self {
            dir,
            writer,
            max_rows: max_rows.max(1),
            max_bytes: max_bytes.max(1),
        })
    }

    async fn push(&mut self, key: &ResultRecord) -> anyhow::Result<()> {
        write_result(&mut self.writer, key).await
    }

    fn run(&self, pass: usize, run: usize) -> PathBuf {
        self.dir.join(format!("sorted-{pass}-{run}"))
    }

    async fn sorted(&mut self) -> anyhow::Result<PathBuf> {
        self.writer.flush().await?;
        let mut reader = BufReader::new(File::open(self.dir.join("candidates")).await?).lines();
        let mut keys = Vec::new();
        let mut bytes = 0;
        let mut runs = 0;
        while let Some(key) = read_result(&mut reader).await? {
            bytes += key.memory_size();
            keys.push(key);
            if keys.len() >= self.max_rows || bytes >= self.max_bytes {
                self.flush_run(&mut keys, runs).await?;
                runs += 1;
                bytes = 0;
            }
        }
        if !keys.is_empty() || runs == 0 {
            self.flush_run(&mut keys, runs).await?;
            runs += 1;
        }
        // Bound open files and merge state independently of the number of candidates.
        let mut pass = 0;
        while runs > 1 {
            let mut next_runs = 0;
            for start in (0..runs).step_by(16) {
                let end = (start + 16).min(runs);
                let mut readers = Vec::new();
                let mut heads = Vec::new();
                for run in start..end {
                    let mut reader = BufReader::new(File::open(self.run(pass, run)).await?).lines();
                    heads.push(read_result(&mut reader).await?);
                    readers.push(reader);
                }
                let mut writer = BufWriter::new(File::create(self.run(pass + 1, next_runs)).await?);
                let mut previous = None;
                loop {
                    let next = heads
                        .iter()
                        .enumerate()
                        .filter_map(|(i, key)| key.as_ref().map(|key| (i, key)))
                        .min_by(|(_, a), (_, b)| a.compare(b))
                        .map(|(i, _)| i);
                    let Some(index) = next else { break };
                    let key = heads[index].take().context("missing merge key")?;
                    if previous.as_ref() != Some(&key.key) {
                        write_result(&mut writer, &key).await?;
                    }
                    previous = Some(key.key);
                    heads[index] = read_result(&mut readers[index]).await?;
                }
                writer.flush().await?;
                drop(readers);
                for run in start..end {
                    fs::remove_file(self.run(pass, run)).await?;
                }
                next_runs += 1;
            }
            runs = next_runs;
            pass += 1;
        }
        Ok(self.run(pass, 0))
    }

    async fn flush_run(&self, keys: &mut Vec<ResultRecord>, run: usize) -> anyhow::Result<()> {
        keys.sort_by(ResultRecord::compare);
        keys.dedup_by(|a, b| a.key == b.key);
        let mut writer = BufWriter::new(File::create(self.run(0, run)).await?);
        for key in keys.drain(..) {
            write_result(&mut writer, &key).await?;
        }
        writer.flush().await?;
        Ok(())
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        fs::remove_dir_all(&self.dir).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn external_merge_deduplicates_binary_keys_across_multiple_passes() {
        let mut spool = ResultSpool::new(&std::env::temp_dir(), 2, 128)
            .await
            .unwrap();
        for i in (0..100).rev() {
            let key = CheckKey {
                db: (i % 3).to_string(),
                key: vec![0, 255, i as u8],
            };
            spool
                .push(&ResultRecord {
                    key: key.clone(),
                    outcome: Outcome::MissingTarget,
                    source_observed: false,
                })
                .await
                .unwrap();
            spool
                .push(&ResultRecord {
                    key: key.clone(),
                    outcome: Outcome::Unstable,
                    source_observed: true,
                })
                .await
                .unwrap();
        }
        let path = spool.sorted().await.unwrap();
        let mut reader = BufReader::new(File::open(path).await.unwrap()).lines();
        let mut keys = Vec::new();
        while let Some(key) = read_result(&mut reader).await.unwrap() {
            keys.push(key);
        }
        assert_eq!(keys.len(), 100);
        assert!(keys
            .iter()
            .all(|record| record.source_observed && matches!(record.outcome, Outcome::Unstable)));
        assert!(keys.windows(2).all(|pair| pair[0].key < pair[1].key));
        spool.cleanup().await.unwrap();
    }
}
