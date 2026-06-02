use std::{collections::BTreeMap, path::Path};

use anyhow::{anyhow, bail, Result};
use clap::ValueEnum;
use url::Url;

const SERVER_ID_MIN: u64 = 10001;
const SERVER_ID_MAX: u64 = 4_294_836_224;

#[derive(Debug, Clone, PartialEq, Eq, Default, ValueEnum)]
pub enum Mode {
    Struct,
    #[default]
    Snapshot,
    Cdc,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Struct => "struct",
            Self::Snapshot => "snapshot",
            Self::Cdc => "cdc",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum DbType {
    Mysql,
    #[value(alias = "postgres", alias = "postgresql")]
    Pg,
    #[value(alias = "mongodb")]
    Mongo,
    Redis,
}

impl DbType {
    pub fn from_scheme(scheme: &str) -> Result<Self> {
        match scheme {
            "mysql" => Ok(Self::Mysql),
            "pg" | "postgres" | "postgresql" => Ok(Self::Pg),
            "mongo" | "mongodb" => Ok(Self::Mongo),
            "redis" => Ok(Self::Redis),
            _ => bail!(
                "unsupported URL scheme '{scheme}', expected mysql/pg/postgres/postgresql/mongo/mongodb/redis"
            ),
        }
    }

    pub fn as_config_value(&self) -> &'static str {
        match self {
            Self::Mysql => "mysql",
            Self::Pg => "pg",
            Self::Mongo => "mongo",
            Self::Redis => "redis",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CreateConfig {
    pub task_name: String,
    pub mode: Mode,
    pub preflight: bool,
    pub source_url: String,
    pub target_url: String,
    pub source_db: Option<DbType>,
    pub target_db: Option<DbType>,
    pub source_user: Option<String>,
    pub source_password: Option<String>,
    pub target_user: Option<String>,
    pub target_password: Option<String>,
    pub filter_do: Option<String>,
    pub filter_ignore: Option<String>,
    pub do_events: Option<String>,
    pub mysql_server_id: Option<String>,
    pub pg_slot_name: Option<String>,
    pub set: Vec<String>,
}

pub fn infer_db_type(url: &str, explicit: Option<DbType>) -> Result<DbType> {
    let scheme = url
        .split_once("://")
        .map(|(scheme, _)| scheme)
        .ok_or_else(|| anyhow!("invalid url '{url}', expected '<scheme>://...'"))?;
    let inferred = DbType::from_scheme(scheme)?;
    if let Some(value) = explicit {
        if value != inferred {
            bail!(
                "explicit db type '{}' does not match url scheme '{}'",
                value.as_config_value(),
                scheme
            );
        }
    } else if matches!(inferred, DbType::Pg | DbType::Mongo) {
        let parsed = Url::parse(url).map_err(|err| anyhow!("invalid url '{url}': {err}"))?;
        if parsed.path().trim_matches('/').is_empty() {
            bail!(
                "invalid {} url '{url}', database is required when db type is inferred",
                inferred.as_config_value()
            );
        }
    }
    Ok(inferred)
}

pub fn build_task_config(
    create: &CreateConfig,
    source_db: &DbType,
    target_db: &DbType,
    runtime_log_dir: &Path,
    runtime_log4rs_file: &Path,
) -> Result<String> {
    let mut ini = IniDoc::new();
    ini.set("global", "task_id", &create.task_name);
    ini.set("extractor", "db_type", source_db.as_config_value());
    ini.set("extractor", "extract_type", extract_type(&create.mode));
    ini.set("extractor", "url", &create.source_url);
    set_optional(&mut ini, "extractor", "username", &create.source_user);
    set_optional(&mut ini, "extractor", "password", &create.source_password);

    ini.set("sinker", "db_type", target_db.as_config_value());
    ini.set("sinker", "sink_type", sink_type(&create.mode));
    ini.set(
        "sinker",
        "batch_size",
        &default_sinker_batch_size(&create.mode).to_string(),
    );
    ini.set("sinker", "url", &create.target_url);
    set_optional(&mut ini, "sinker", "username", &create.target_user);
    set_optional(&mut ini, "sinker", "password", &create.target_password);

    if matches!(create.mode, Mode::Snapshot | Mode::Cdc) {
        match source_db {
            DbType::Mysql | DbType::Pg => ini.set("resumer", "resume_type", "from_target"),
            // TODO: Mongo and Redis do not support resume_type=from_target yet; add it after support lands.
            DbType::Mongo | DbType::Redis => {}
        }
    }

    if matches!(create.mode, Mode::Cdc) {
        match source_db {
            DbType::Mysql => ini.set(
                "extractor",
                "server_id",
                &create
                    .mysql_server_id
                    .clone()
                    .unwrap_or_else(|| random_mysql_server_id().to_string()),
            ),
            DbType::Pg => {
                let slot_name = create
                    .pg_slot_name
                    .clone()
                    .unwrap_or_else(|| pg_slot_name_from_task_name(&create.task_name));
                ini.set("extractor", "slot_name", &slot_name);
                ini.set(
                    "extractor",
                    "pub_name",
                    &format!("{}_publication_for_all_tables", slot_name),
                );
            }
            DbType::Redis => {
                ini.set("extractor", "repl_port", "10008");
            }
            DbType::Mongo => {}
        }
    }

    let filter_do = split_filter_patterns(create.filter_do.as_deref().unwrap_or(""), source_db)?;
    let filter_ignore =
        split_filter_patterns(create.filter_ignore.as_deref().unwrap_or(""), source_db)?;
    ini.set("filter", "do_dbs", &filter_do.dbs);
    ini.set("filter", "ignore_dbs", &filter_ignore.dbs);
    ini.set("filter", "do_tbs", &filter_do.tbs);
    ini.set("filter", "ignore_tbs", &filter_ignore.tbs);
    ini.set(
        "filter",
        "do_events",
        create.do_events.as_deref().unwrap_or(""),
    );
    ini.set("router", "db_map", "");
    ini.set("router", "tb_map", "");
    ini.set("router", "col_map", "");
    let parallelizer = default_parallelizer(&create.mode, source_db);
    ini.set("parallelizer", "parallel_type", parallelizer.parallel_type);
    ini.set(
        "parallelizer",
        "parallel_size",
        &parallelizer.parallel_size.to_string(),
    );
    ini.set("pipeline", "buffer_size", "16000");
    ini.set("pipeline", "checkpoint_interval_secs", "10");
    ini.set("runtime", "log_level", "info");
    ini.set(
        "runtime",
        "log4rs_file",
        &runtime_log4rs_file.display().to_string(),
    );
    ini.set("runtime", "log_dir", &runtime_log_dir.display().to_string());

    if create.preflight {
        ini.set(
            "precheck",
            "do_struct_init",
            if matches!(create.mode, Mode::Struct) {
                "true"
            } else {
                "false"
            },
        );
        ini.set(
            "precheck",
            "do_cdc",
            if matches!(create.mode, Mode::Cdc) {
                "true"
            } else {
                "false"
            },
        );
    }

    for item in &create.set {
        let (path, value) = item
            .split_once('=')
            .ok_or_else(|| anyhow!("--set must use section.key=value, got '{item}'"))?;
        let (section, key) = path
            .split_once('.')
            .ok_or_else(|| anyhow!("--set must use section.key=value, got '{item}'"))?;
        ini.set(section, key, value);
    }

    Ok(ini.render())
}

fn set_optional(ini: &mut IniDoc, section: &str, key: &str, value: &Option<String>) {
    if let Some(value) = value {
        ini.set(section, key, value);
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct FilterPatterns {
    dbs: String,
    tbs: String,
}

fn split_filter_patterns(patterns: &str, db_type: &DbType) -> Result<FilterPatterns> {
    if patterns.trim().is_empty() {
        return Ok(FilterPatterns::default());
    }

    let escape = identifier_escape(db_type);
    let mut dbs = Vec::new();
    let mut tbs = Vec::new();

    for pattern in split_unescaped(patterns, ',', escape)? {
        match split_unescaped(pattern, '.', escape)?.len() {
            1 => dbs.push(pattern),
            2 => tbs.push(pattern),
            _ => bail!(
                "invalid filter expression '{pattern}', expected db or db.table; enclose names containing '.' or ',' with the database identifier escape"
            ),
        }
    }

    Ok(FilterPatterns {
        dbs: dbs.join(","),
        tbs: tbs.join(","),
    })
}

// Keep this mapping aligned with dt-common's SqlUtil::get_escape_pairs.
fn identifier_escape(db_type: &DbType) -> Option<char> {
    match db_type {
        DbType::Mysql => Some('`'),
        DbType::Pg | DbType::Redis => Some('"'),
        DbType::Mongo => None,
    }
}

fn split_unescaped(value: &str, delimiter: char, escape: Option<char>) -> Result<Vec<&str>> {
    let mut tokens = Vec::new();
    let mut start = 0;
    let mut in_escape = false;
    let mut in_regex = false;
    let mut chars = value.char_indices().peekable();

    while let Some((index, ch)) = chars.next() {
        if in_regex {
            if ch == '#' {
                in_regex = false;
            }
            continue;
        }
        if Some(ch) == escape {
            in_escape = !in_escape;
        } else if !in_escape
            && ch == 'r'
            && value[index..].starts_with("r#")
            && is_filter_token_start(value, index)
        {
            in_regex = true;
            chars.next();
        } else if ch == delimiter && !in_escape {
            push_filter_token(&mut tokens, &value[start..index], value)?;
            start = index + ch.len_utf8();
        }
    }
    if in_escape {
        bail!("unclosed identifier escape in filter expression '{value}'");
    }
    if in_regex {
        bail!("unclosed regex escape in filter expression '{value}'");
    }
    push_filter_token(&mut tokens, &value[start..], value)?;
    Ok(tokens)
}

fn is_filter_token_start(value: &str, index: usize) -> bool {
    value[..index]
        .chars()
        .rev()
        .find(|ch| !ch.is_whitespace())
        .map(|ch| matches!(ch, ',' | '.'))
        .unwrap_or(true)
}

fn push_filter_token<'a>(tokens: &mut Vec<&'a str>, token: &'a str, value: &str) -> Result<()> {
    let token = token.trim();
    if token.is_empty() {
        bail!("empty filter expression in '{value}'");
    }
    tokens.push(token);
    Ok(())
}

fn extract_type(mode: &Mode) -> &'static str {
    match mode {
        Mode::Struct => "struct",
        Mode::Snapshot => "snapshot",
        Mode::Cdc => "cdc",
    }
}

fn sink_type(mode: &Mode) -> &'static str {
    match mode {
        Mode::Struct => "struct",
        _ => "write",
    }
}

fn default_sinker_batch_size(mode: &Mode) -> usize {
    match mode {
        Mode::Snapshot | Mode::Cdc => 100,
        Mode::Struct => 1,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ParallelizerDefaults {
    parallel_type: &'static str,
    parallel_size: usize,
}

fn default_parallelizer(mode: &Mode, db_type: &DbType) -> ParallelizerDefaults {
    match mode {
        Mode::Struct => ParallelizerDefaults {
            parallel_type: "serial",
            parallel_size: 1,
        },
        Mode::Snapshot => match db_type {
            DbType::Redis => ParallelizerDefaults {
                parallel_type: "redis",
                parallel_size: 8,
            },
            _ => ParallelizerDefaults {
                parallel_type: "snapshot",
                parallel_size: 8,
            },
        },
        Mode::Cdc => match db_type {
            DbType::Mysql | DbType::Pg => ParallelizerDefaults {
                parallel_type: "rdb_merge",
                parallel_size: 8,
            },
            DbType::Mongo => ParallelizerDefaults {
                parallel_type: "mongo",
                parallel_size: 8,
            },
            DbType::Redis => ParallelizerDefaults {
                parallel_type: "redis",
                parallel_size: 8,
            },
        },
    }
}

#[derive(Debug, Default)]
struct IniDoc {
    sections: Vec<String>,
    values: BTreeMap<String, Vec<(String, String)>>,
}

impl IniDoc {
    fn new() -> Self {
        Self::default()
    }

    fn set(&mut self, section: &str, key: &str, value: &str) {
        if !self.values.contains_key(section) {
            self.sections.push(section.to_string());
            self.values.insert(section.to_string(), Vec::new());
        }
        let entries = self.values.get_mut(section).expect("section exists");
        if let Some((_, old)) = entries.iter_mut().find(|(item_key, _)| item_key == key) {
            *old = value.to_string();
        } else {
            entries.push((key.to_string(), value.to_string()));
        }
    }

    fn render(&self) -> String {
        let mut output = String::new();
        for section in &self.sections {
            output.push_str(&format!("[{}]\n", section));
            if let Some(entries) = self.values.get(section) {
                for (key, value) in entries {
                    output.push_str(&format!("{}={}\n", key, value));
                }
            }
            output.push('\n');
        }
        output
    }
}

fn pg_slot_name_from_task_name(task_name: &str) -> String {
    let mut normalized = String::from("ape_dts_");
    for ch in task_name.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    while normalized.contains("__") {
        normalized = normalized.replace("__", "_");
    }
    normalized = normalized.trim_matches('_').to_string();
    if normalized.is_empty() {
        normalized = "ape_dts_task".to_string();
    }
    if normalized.len() <= 63 {
        return normalized;
    }
    let hash = stable_hash(task_name);
    let suffix = format!("_{hash:08x}");
    let keep = 63 - suffix.len();
    format!("{}{}", &normalized[..keep], suffix)
}

fn stable_hash(value: &str) -> u32 {
    let mut hash = 2_166_136_261u32;
    for byte in value.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

fn random_mysql_server_id() -> u64 {
    let span = SERVER_ID_MAX - SERVER_ID_MIN + 1;
    SERVER_ID_MIN + (randomish_u64() % span)
}

fn randomish_u64() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ ((std::process::id() as u64) << 32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths() -> (&'static Path, &'static Path) {
        (
            Path::new("/tmp/dts/logs/order_task"),
            Path::new("/opt/ape-dts/log4rs.yaml"),
        )
    }

    #[test]
    fn preflight_flags_follow_task_mode() {
        let (log_dir, log4rs_file) = paths();
        let cases = [
            (Mode::Struct, "true", "false"),
            (Mode::Snapshot, "false", "false"),
            (Mode::Cdc, "false", "true"),
        ];

        for (mode, do_struct_init, do_cdc) in cases {
            let create = CreateConfig {
                task_name: "order_preflight".to_string(),
                mode,
                preflight: true,
                source_url: "mysql://src:3306".to_string(),
                target_url: "mysql://dst:3307".to_string(),
                mysql_server_id: Some("20001".to_string()),
                ..CreateConfig::default()
            };

            let actual = build_task_config(
                &create,
                &DbType::Mysql,
                &DbType::Mysql,
                log_dir,
                log4rs_file,
            )
            .unwrap();

            assert!(actual.contains(&format!(
                "[precheck]\ndo_struct_init={do_struct_init}\ndo_cdc={do_cdc}\n"
            )));
        }
    }

    #[test]
    fn filter_patterns_split_dbs_and_tbs_without_splitting_mysql_escapes() {
        assert_eq!(
            split_filter_patterns(
                "db_1,db_2.tb_2,`heh.e`.`ta,ble`,`db,3`,bar#foo,r#db[.]4#.r#tb[,]4#",
                &DbType::Mysql,
            )
            .unwrap(),
            FilterPatterns {
                dbs: "db_1,`db,3`,bar#foo".to_string(),
                tbs: "db_2.tb_2,`heh.e`.`ta,ble`,r#db[.]4#.r#tb[,]4#".to_string(),
            }
        );
    }

    #[test]
    fn filter_patterns_split_pg_escapes_and_reject_invalid_expressions() {
        assert_eq!(
            split_filter_patterns(r#""heh.e"."ta,ble",public"#, &DbType::Pg).unwrap(),
            FilterPatterns {
                dbs: "public".to_string(),
                tbs: r#""heh.e"."ta,ble""#.to_string(),
            }
        );
        assert!(split_filter_patterns("db.tb.column", &DbType::Mysql).is_err());
        assert!(split_filter_patterns("`db.tb", &DbType::Mysql).is_err());
        assert!(split_filter_patterns("db,,db.tb", &DbType::Mysql).is_err());
    }

    #[test]
    fn mode_2_struct_config_matches_expected() {
        let (log_dir, log4rs_file) = paths();
        let create = CreateConfig {
            task_name: "order_struct".to_string(),
            mode: Mode::Struct,
            source_url: "postgres://src:5432".to_string(),
            target_url: "postgres://dst:5433".to_string(),
            filter_do: Some("public".to_string()),
            ..CreateConfig::default()
        };

        let actual =
            build_task_config(&create, &DbType::Pg, &DbType::Pg, log_dir, log4rs_file).unwrap();

        let expected = r#"[global]
task_id=order_struct

[extractor]
db_type=pg
extract_type=struct
url=postgres://src:5432

[sinker]
db_type=pg
sink_type=struct
batch_size=1
url=postgres://dst:5433

[filter]
do_dbs=public
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=/opt/ape-dts/log4rs.yaml
log_dir=/tmp/dts/logs/order_task

"#;
        assert_eq!(actual, expected);
    }

    #[test]
    fn mode_3_snapshot_config_matches_expected() {
        let (log_dir, log4rs_file) = paths();
        let create = CreateConfig {
            task_name: "order_snapshot".to_string(),
            mode: Mode::Snapshot,
            source_url: "mysql://src:3306".to_string(),
            target_url: "mysql://dst:3307".to_string(),
            filter_do: Some("test_db.*".to_string()),
            filter_ignore: Some("test_db.tmp_*".to_string()),
            ..CreateConfig::default()
        };

        let actual = build_task_config(
            &create,
            &DbType::Mysql,
            &DbType::Mysql,
            log_dir,
            log4rs_file,
        )
        .unwrap();

        let expected = r#"[global]
task_id=order_snapshot

[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://src:3306

[sinker]
db_type=mysql
sink_type=write
batch_size=100
url=mysql://dst:3307

[resumer]
resume_type=from_target

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db.*
ignore_tbs=test_db.tmp_*
do_events=

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=/opt/ape-dts/log4rs.yaml
log_dir=/tmp/dts/logs/order_task

"#;
        assert_eq!(actual, expected);
    }

    #[test]
    fn mode_4_cdc_config_matches_expected() {
        let (log_dir, log4rs_file) = paths();
        let create = CreateConfig {
            task_name: "order_cdc".to_string(),
            mode: Mode::Cdc,
            source_url: "postgres://src:5432".to_string(),
            target_url: "postgres://dst:5433".to_string(),
            filter_do: Some("public.orders".to_string()),
            do_events: Some("insert,update,delete".to_string()),
            pg_slot_name: Some("ape_dts_order_cdc".to_string()),
            ..CreateConfig::default()
        };

        let actual =
            build_task_config(&create, &DbType::Pg, &DbType::Pg, log_dir, log4rs_file).unwrap();

        let expected = r#"[global]
task_id=order_cdc

[extractor]
db_type=pg
extract_type=cdc
url=postgres://src:5432
slot_name=ape_dts_order_cdc
pub_name=ape_dts_order_cdc_publication_for_all_tables

[sinker]
db_type=pg
sink_type=write
batch_size=100
url=postgres://dst:5433

[resumer]
resume_type=from_target

[filter]
do_dbs=
ignore_dbs=
do_tbs=public.orders
ignore_tbs=
do_events=insert,update,delete

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=/opt/ape-dts/log4rs.yaml
log_dir=/tmp/dts/logs/order_task

"#;
        assert_eq!(actual, expected);
    }

    #[test]
    fn from_target_resumer_is_generated_only_for_supported_modes_and_sources() {
        let (log_dir, log4rs_file) = paths();
        let cases = [
            (Mode::Struct, DbType::Mysql, false),
            (Mode::Struct, DbType::Pg, false),
            (Mode::Snapshot, DbType::Mysql, true),
            (Mode::Snapshot, DbType::Pg, true),
            (Mode::Cdc, DbType::Mysql, true),
            (Mode::Cdc, DbType::Pg, true),
            (Mode::Snapshot, DbType::Mongo, false),
            (Mode::Cdc, DbType::Mongo, false),
            (Mode::Snapshot, DbType::Redis, false),
            (Mode::Cdc, DbType::Redis, false),
        ];

        for (mode, source_db, expected) in cases {
            let create = CreateConfig {
                task_name: "resumer_task".to_string(),
                mode,
                source_url: format!("{}://src", source_db.as_config_value()),
                target_url: "mysql://dst".to_string(),
                mysql_server_id: Some("20001".to_string()),
                ..CreateConfig::default()
            };

            let actual =
                build_task_config(&create, &source_db, &DbType::Mysql, log_dir, log4rs_file)
                    .unwrap();

            assert_eq!(
                actual.contains("[resumer]\nresume_type=from_target\n"),
                expected,
                "unexpected resumer config for mode={} source_db={}",
                create.mode.as_str(),
                source_db.as_config_value(),
            );
        }
    }

    #[test]
    fn infer_db_type_rejects_invalid_url_or_mismatch() {
        assert_eq!(
            infer_db_type("mysql://u:p@127.0.0.1:3306", None).unwrap(),
            DbType::Mysql
        );
        assert_eq!(
            infer_db_type("postgres://u:p@127.0.0.1:5432/postgres", None).unwrap(),
            DbType::Pg
        );
        assert_eq!(
            infer_db_type("mongodb://127.0.0.1:27017/admin", None).unwrap(),
            DbType::Mongo
        );
        assert_eq!(
            infer_db_type("redis://127.0.0.1:6379", None).unwrap(),
            DbType::Redis
        );
        assert!(infer_db_type("postgres://u:p@127.0.0.1:5432", None).is_err());
        assert!(infer_db_type("postgres://u:p@127.0.0.1:5432/", None).is_err());
        assert!(infer_db_type("mongodb://127.0.0.1:27017", None).is_err());
        assert!(infer_db_type("mongodb://127.0.0.1:27017/", None).is_err());
        assert_eq!(
            infer_db_type("postgres://u:p@127.0.0.1:5432", Some(DbType::Pg)).unwrap(),
            DbType::Pg
        );
        assert_eq!(
            infer_db_type("mongodb://127.0.0.1:27017", Some(DbType::Mongo)).unwrap(),
            DbType::Mongo
        );
        assert!(infer_db_type("mysql:bad", None).is_err());
        assert!(infer_db_type("mysql://127.0.0.1:3306", Some(DbType::Pg)).is_err());
    }

    #[test]
    fn pg_slot_name_is_valid_and_bounded() {
        let slot = pg_slot_name_from_task_name("Order.Sync-Task");
        assert_eq!(slot, "ape_dts_order_sync_task");
        let long_slot = pg_slot_name_from_task_name("a".repeat(100).as_str());
        assert!(long_slot.len() <= 63);
        assert!(long_slot
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_'));
    }

    #[test]
    fn parallelizer_defaults_follow_transfer_definition_shapes() {
        assert_eq!(
            default_parallelizer(&Mode::Struct, &DbType::Mysql),
            ParallelizerDefaults {
                parallel_type: "serial",
                parallel_size: 1,
            }
        );
        assert_eq!(
            default_parallelizer(&Mode::Snapshot, &DbType::Mysql),
            ParallelizerDefaults {
                parallel_type: "snapshot",
                parallel_size: 8,
            }
        );
        assert_eq!(
            default_parallelizer(&Mode::Snapshot, &DbType::Redis),
            ParallelizerDefaults {
                parallel_type: "redis",
                parallel_size: 8,
            }
        );
        assert_eq!(
            default_parallelizer(&Mode::Cdc, &DbType::Pg),
            ParallelizerDefaults {
                parallel_type: "rdb_merge",
                parallel_size: 8,
            }
        );
        assert_eq!(
            default_parallelizer(&Mode::Cdc, &DbType::Mongo),
            ParallelizerDefaults {
                parallel_type: "mongo",
                parallel_size: 8,
            }
        );
        assert_eq!(
            default_parallelizer(&Mode::Cdc, &DbType::Redis),
            ParallelizerDefaults {
                parallel_type: "redis",
                parallel_size: 8,
            }
        );
    }

    #[test]
    fn sinker_batch_size_defaults_follow_transfer_definition_shapes() {
        assert_eq!(default_sinker_batch_size(&Mode::Struct), 1);
        assert_eq!(default_sinker_batch_size(&Mode::Snapshot), 100);
        assert_eq!(default_sinker_batch_size(&Mode::Cdc), 100);
    }
}
