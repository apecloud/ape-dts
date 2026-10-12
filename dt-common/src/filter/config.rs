use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use anyhow::{ensure, Context, Result};

use super::{CommonFilter, CommonFilterKind, DtFilter, ExpressionFilter, ResourceFilter};
use crate::{
    config::{
        config_enums::DbType,
        filter_config::{ExpressionFilterConfig, FilterConfig, FilterSectionConfig, RULES},
    },
    meta::{
        dcl_meta::dcl_type::DclType, ddl_meta::ddl_type::DdlType, row_type::RowType,
        struct_meta::structure::structure_type::StructureType,
    },
    resource::{config_parser as config, validate_rid, ResourceKind, REGEX_PREFIX, RID},
    utils::sql_util::SqlUtil,
};

fn patterns(value: &str, db_type: &DbType) -> Result<HashSet<RID>> {
    config::paths(value, db_type)?
        .into_iter()
        .map(|path| {
            for token in &path {
                config::literal(token, db_type)?;
            }
            RID::new(path)
        })
        .collect()
}

fn common_values(value: &str, kind: CommonFilterKind) -> Result<HashSet<String>> {
    ensure!(
        kind == CommonFilterKind::Cmd || !value.trim_end().ends_with(','),
        "trailing comma in {} filter values",
        kind.as_ref()
    );
    let mut rules = HashSet::new();
    let mut remaining = value.trim();
    while !remaining.is_empty() {
        // Keep commas inside r#...# in the same literal command token.
        let (name, rest) = if kind == CommonFilterKind::Cmd && remaining.starts_with(REGEX_PREFIX) {
            let (name, rest) = config::token(remaining, &[])?;
            ensure!(
                rest.is_empty() || rest.starts_with(','),
                "expected comma after command value"
            );
            (name, rest.strip_prefix(',').unwrap_or(rest))
        } else {
            remaining.split_once(',').unwrap_or((remaining, ""))
        };
        let name = name.trim();
        remaining = rest.trim_start();
        if kind == CommonFilterKind::Cmd && name.is_empty() {
            continue;
        }
        if name == "*" {
            rules.insert(name.to_owned());
            continue;
        }
        let valid = match kind {
            CommonFilterKind::Event => RowType::from_str(name).is_ok(),
            CommonFilterKind::Ddl => DdlType::from_str(name).is_ok(),
            CommonFilterKind::Dcl => DclType::from_str(name).is_ok(),
            CommonFilterKind::Structure => StructureType::from_str(name).is_ok(),
            CommonFilterKind::Cmd => true,
        };
        ensure!(valid, "invalid {} filter value {name}", kind.as_ref());
        rules.insert(name.to_owned());
    }
    Ok(rules)
}

fn common_filter(kind: CommonFilterKind, base: &FilterSectionConfig) -> Result<CommonFilter> {
    let context = format!("[{RULES}.{}]", kind.as_ref());
    Ok(CommonFilter {
        do_set: common_values(
            base.do_rules
                .as_deref()
                .unwrap_or_else(|| kind.default_do()),
            kind,
        )
        .with_context(|| format!("{context}.do"))?,
        ignore_set: common_values(base.ignore_rules.as_deref().unwrap_or_default(), kind)
            .with_context(|| format!("{context}.ignore"))?,
    })
}

fn where_conditions(value: &str, kind: ResourceKind, db_type: &DbType) -> Result<ExpressionFilter> {
    let pairs = SqlUtil::get_escape_pairs(db_type);
    let mut remaining = value.trim();
    let mut conditions = HashMap::new();
    while !remaining.is_empty() {
        let mut segments = Vec::new();
        loop {
            let (token, rest) = config::token(remaining, &pairs)?;
            segments.push(token.to_owned());
            if let Some(rest) = rest.strip_prefix('.') {
                remaining = rest;
            } else {
                remaining = rest
                    .strip_prefix(':')
                    .context("expected ':' after WHERE resource ID")?
                    .trim_start();
                break;
            }
        }
        let id = config::exact_path(segments, db_type)?;
        validate_rid(db_type, kind, &id)?;
        ensure!(
            remaining.starts_with('"'),
            "WHERE expression must be enclosed in JSON double quotes"
        );
        // Decode one JSON string, then resume at its byte offset.
        let mut strings = serde_json::Deserializer::from_str(remaining).into_iter::<String>();
        let condition = strings
            .next()
            .context("missing WHERE expression")?
            .context("invalid JSON string escape in WHERE expression")?;
        remaining = remaining[strings.byte_offset()..].trim_start();
        conditions.entry(id).or_insert(condition);
        if !remaining.is_empty() {
            remaining = remaining
                .strip_prefix(',')
                .context("expected ',' after WHERE expression")?
                .trim_start();
            ensure!(!remaining.is_empty(), "trailing comma in WHERE conditions");
        }
    }
    Ok(ExpressionFilter::WhereConditions { conditions })
}

impl DtFilter {
    pub fn from_config(config: &FilterConfig, db_type: DbType) -> Result<Self> {
        let sections = match config {
            FilterConfig::Legacy(options) => return Self::from_legacy_config(options, db_type),
            FilterConfig::Basic(sections) => sections,
        };
        let mut filter = Self::new(db_type.clone());
        for (name, base) in sections {
            if let Ok(kind) = name.parse::<CommonFilterKind>() {
                ensure!(
                    base.expression_filters.is_empty(),
                    "[{RULES}.{name}].where_conditions requires a resource section"
                );
                filter.set_common_filter(kind, common_filter(kind, base)?);
                continue;
            }
            let empty = [&base.do_rules, &base.ignore_rules]
                .iter()
                .all(|value| value.as_deref().unwrap_or_default().trim().is_empty())
                && base.expression_filters.is_empty();
            let Some(kind) = config::section_kind(name, &db_type, empty)? else {
                continue;
            };
            filter.set_resource_filter(
                kind,
                ResourceFilter {
                    do_set: patterns(base.do_rules.as_deref().unwrap_or_default(), &db_type)
                        .with_context(|| format!("{kind:?}.do"))?,
                    ignore_set: patterns(
                        base.ignore_rules.as_deref().unwrap_or_default(),
                        &db_type,
                    )
                    .with_context(|| format!("{kind:?}.ignore"))?,
                },
            )?;
            for expression in &base.expression_filters {
                let expression = match expression {
                    ExpressionFilterConfig::WhereConditions(value) => {
                        where_conditions(value, kind, &db_type)
                            .with_context(|| format!("{kind:?}.where_conditions"))?
                    }
                };
                filter.set_expression_filter(kind, expression)?;
            }
        }
        Ok(filter)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use configparser::ini::Ini;

    use super::*;
    use crate::{resource::resource_levels, rid};

    fn load_filter(ini: &Ini, db_type: DbType) -> Result<DtFilter> {
        DtFilter::from_config(&FilterConfig::from_ini(ini)?, db_type)
    }

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    fn filter(text: &str) -> DtFilter {
        load_filter(&ini(text), DbType::Mysql).unwrap()
    }

    #[test]
    fn from_config_accepts_resource_maps_and_checks_direct_callers() {
        let mut bases = BTreeMap::from([
            (
                "table".to_owned(),
                FilterSectionConfig {
                    ignore_rules: Some("market.inventory.secret".into()),
                    ..Default::default()
                },
            ),
            (
                "schema".to_owned(),
                FilterSectionConfig {
                    do_rules: Some("market.inventory".into()),
                    ..Default::default()
                },
            ),
        ]);
        bases.insert(
            "event".into(),
            FilterSectionConfig {
                do_rules: Some("insert".into()),
                ..Default::default()
            },
        );
        let filter =
            DtFilter::from_config(&FilterConfig::Basic(bases.clone()), DbType::Mssql).unwrap();
        assert!(!filter
            .filter_event(&rid!("market", "inventory", "order"), &RowType::Insert)
            .unwrap());
        assert!(filter
            .filter_event(&rid!("market", "inventory", "order"), &RowType::Update)
            .unwrap());
        assert!(filter
            .filter(&rid!("market", "inventory", "secret"))
            .unwrap());
        assert!(filter.filter(&rid!("other", "inventory", "order")).unwrap());
        bases.insert(
            "collection".to_owned(),
            FilterSectionConfig {
                do_rules: Some("db.c".into()),
                ..Default::default()
            },
        );
        assert!(DtFilter::from_config(&FilterConfig::Basic(bases.clone()), DbType::Mssql).is_err());
        let mut sections = BTreeMap::from([("table".to_owned(), FilterSectionConfig::default())]);
        for value in [None, Some(String::new())] {
            sections.get_mut("table").unwrap().do_rules = value;
            let filter =
                DtFilter::from_config(&FilterConfig::Basic(sections.clone()), DbType::Mysql)
                    .unwrap();
            assert!(filter.filter(&rid!("db", "t")).unwrap());
        }
        sections.clear();
        let filter = DtFilter::from_config(&FilterConfig::Basic(sections), DbType::Mysql).unwrap();
        assert!(filter.filter(&rid!("db", "t")).unwrap());
    }

    #[test]
    fn resource_sections_replace_legacy_rules_and_expressions() {
        for legacy in [
            r#"do_dbs=legacy
ignore_dbs=db
do_tbs=legacy.t
ignore_tbs=db.t
ignore_cols=json:[{"db":"db","tb":"t","ignore_cols":["id"]}]
where_conditions=json:[{"db":"db","tb":"t","condition":"legacy"}]"#,
            "do_dbs=r#[#\nignore_dbs=r#[#\ndo_tbs=invalid\nignore_tbs=invalid\nignore_cols=json:invalid\nwhere_conditions=json:invalid",
        ] {
            for (section, excluded, condition) in [
                ("[rules.table]\ndo=db.t", false, None),
                ("[rules.table]", true, None),
                ("[rules.table]\nroute=db.t:archive.t", true, None),
                ("[rules.db]\ndo=", true, None),
                ("[rules.table]\ndo=db.t\nwhere_conditions=", false, None),
                ("[rules.table]\ndo=db.t\nwhere_conditions=db.t:\"new\"", false, Some("new")),
                ("[rules.db]\nwhere_conditions=db:\"database\"\n[rules.table]\ndo=db.t", false, None),
            ] {
                let f = filter(&format!("[filter]\n{legacy}\ndo_events=insert\n{section}"));
                assert_eq!(f.filter(&rid!("db", "t", "id")).unwrap(), excluded, "{section}");
                assert!(f.filter(&rid!("legacy", "t")).unwrap());
                for event in [RowType::Insert, RowType::Update] {
                    assert_eq!(f.filter_event(&rid!("db", "t"), &event).unwrap(), excluded, "{section}");
                }
                assert_eq!(f.where_condition(&rid!("db", "t")).unwrap(), condition, "{section}");
                assert_eq!(f.where_condition(&rid!("db", "t", "id")).unwrap(), None);
            }
        }
        let f = load_filter(&ini("[filter]\ndo_tbs=db.t\n[rules.db]\ndo="), DbType::Pg).unwrap();
        assert!(f.filter(&rid!("db", "t")).unwrap());
    }

    #[test]
    fn raw_resource_names_are_resolved_for_the_consuming_engine() {
        for (db_type, section) in [(DbType::Mysql, "db"), (DbType::Pg, "schema")] {
            let config =
                FilterConfig::from_ini(&ini(&format!("[rules.{section}]\ndo=inventory"))).unwrap();
            let filter = DtFilter::from_config(&config, db_type).unwrap();
            assert!(!filter.filter(&rid!("inventory", "order")).unwrap());
            assert!(filter.filter(&rid!("other", "order")).unwrap());
        }
        for (db_type, text) in [
            (DbType::Mysql, "[rules.unknown]\ndo=grant"),
            (DbType::Mysql, "[rules.database]\ndo=db"),
            (DbType::Mysql, "[rules.schema]\ndo=schema"),
            (DbType::Kafka, "[rules.db]\ndo=db"),
            (DbType::Pg, "[rules.db]\ndo=db"),
            (DbType::Mongo, "[rules.column]\nignore=db.c.field"),
            (DbType::Mysql, "[rules.collection]\ndo=db.c"),
            (DbType::Mongo, "[rules.document]\ndo=db.c"),
        ] {
            let config = FilterConfig::from_ini(&ini(text)).unwrap();
            assert!(DtFilter::from_config(&config, db_type).is_err(), "{text}");
        }
        let filter = load_filter(
            &ini("[rules.schema]\ndo=inventory\n[rules.db]\ndo="),
            DbType::Pg,
        )
        .unwrap();
        assert!(!filter.filter(&rid!("inventory", "order")).unwrap());
    }

    #[test]
    fn new_and_legacy_configs_keep_union_ignore_and_question_mark_semantics() {
        let old = filter("[filter]\ndo_dbs=test_db_*\nignore_dbs=test_db_3\ndo_tbs=other_test_db_1.*\nignore_tbs=test_db_4.*,test_db_?.one_pk_no_uk_2,*_test_db_1.one_pk_no_uk_2\ndo_events=insert");
        let new = filter("[rules.event]\ndo=insert\n[rules.db]\ndo=test_db_*\nignore=test_db_3\n[rules.table]\ndo=other_test_db_1.*\nignore=test_db_4.*,test_db_?.one_pk_no_uk_2,*_test_db_1.one_pk_no_uk_2");
        for (db, table, excluded) in [
            ("test_db_1", "t1", false),
            ("other_test_db_1", "t1", false),
            ("test_db_3", "t1", true),
            ("test_db_4", "t1", true),
            ("test_db_1", "one_pk_no_uk_2", true),
            ("test_db_", "one_pk_no_uk_2", true),
            ("test_db_10", "one_pk_no_uk_2", false),
            ("other_test_db_1", "one_pk_no_uk_2", true),
            ("unselected", "t1", true),
        ] {
            let id = RID::new([db, table]).unwrap();
            for f in [&old, &new] {
                assert_eq!(f.filter(&id).unwrap(), excluded, "{id}");
                assert_eq!(f.filter_event(&id, &RowType::Insert).unwrap(), excluded);
                assert!(f.filter_event(&id, &RowType::Update).unwrap());
                // Event rejection must not poison the cached object decision.
                assert_eq!(f.filter(&id).unwrap(), excluded);
            }
        }
    }

    #[test]
    fn common_sections_support_do_and_ignore_for_every_engine() {
        let config = FilterConfig::from_ini(&ini(r#"[filter]
do_events=r#[#
do_ddls=r#[#
do_dcls=r#[#
do_structures=r#[#
ignore_cmds=GET
[rules.event]
do=insert,update
ignore=update
[rules.ddl]
do=create_table,drop_table
ignore=drop_table
[rules.dcl]
do=grant,revoke
ignore=revoke
[rules.structure]
do=table,index
ignore=index
[rules.cmd]
do=GET,SET,FT.SEARCH
ignore=SET"#))
        .unwrap();
        for engine in [
            DbType::Mysql,
            DbType::Tidb,
            DbType::Pg,
            DbType::Mssql,
            DbType::Mongo,
            DbType::Redis,
            DbType::Kafka,
            DbType::ClickHouse,
            DbType::StarRocks,
            DbType::Doris,
        ] {
            let depth = resource_levels(&engine)
                .iter()
                .take_while(|&&kind| kind != ResourceKind::Column)
                .count();
            let id = RID::new(vec!["object"; depth]).unwrap();
            let mut f = DtFilter::from_config(&config, engine).unwrap();
            f.add_do(&id).unwrap();
            assert!(!f.filter_event(&id, &RowType::Insert).unwrap());
            assert!(f.filter_event(&id, &RowType::Update).unwrap());
            assert!(f.filter_event(&id, &RowType::Delete).unwrap());
            assert!(!f.filter_ddl(&id, &DdlType::CreateTable).unwrap());
            assert!(f.filter_ddl(&id, &DdlType::DropTable).unwrap());
            assert!(!f.filter_dcl(&DclType::Grant));
            assert!(f.filter_dcl(&DclType::Revoke));
            assert!(!f.filter_structure(&StructureType::Table));
            assert!(f.filter_structure(&StructureType::Index));
            assert!(!f.filter_cmd("GET"));
            assert!(!f.filter_cmd("FT.SEARCH"));
            assert!(f.filter_cmd("SET"));
            assert!(f.filter_cmd("DEL"));
            assert!(!f.filter(&id).unwrap());
        }
    }

    #[test]
    fn common_sections_select_basic_without_any_legacy_fallback() {
        let mut f = filter(
            r#"[filter]
do_tbs=db.t
do_events=insert
do_ddls=create_table
do_dcls=grant
do_structures=table
ignore_cmds=SET
where_conditions=json:[{"db":"db","tb":"t","condition":"id > 1"}]
[rules.event]
ignore=update"#,
        );
        assert!(f.filter(&rid!("db", "t")).unwrap());
        assert_eq!(f.where_condition(&rid!("db", "t")).unwrap(), None);
        f.add_do(&rid!("db", "t")).unwrap();
        assert!(!f.filter_event(&rid!("db", "t"), &RowType::Delete).unwrap());
        assert!(f.filter_event(&rid!("db", "t"), &RowType::Update).unwrap());
        assert!(f
            .filter_ddl(&rid!("db", "t"), &DdlType::CreateTable)
            .unwrap());
        assert!(f.filter_dcl(&DclType::Grant));
        assert!(!f.filter_structure(&StructureType::Table));
        assert!(!f.filter_structure(&StructureType::Index));
        assert!(!f.filter_cmd("SET"));
    }

    #[test]
    fn common_defaults_empty_rules_and_literal_ignores_are_distinct() {
        let defaults = filter("");
        let constructed = DtFilter::new(DbType::Mysql);
        for (kind, value, excluded) in [
            (CommonFilterKind::Event, "insert", false),
            (CommonFilterKind::Ddl, "create_table", true),
            (CommonFilterKind::Dcl, "grant", true),
            (CommonFilterKind::Structure, "table", false),
            (CommonFilterKind::Cmd, "GET", false),
        ] {
            let absent = filter(&format!("[rules.{}]", kind.as_ref()));
            let empty = filter(&format!("[rules.{}]\ndo=", kind.as_ref()));
            let ignored = filter(&format!("[rules.{}]\ndo=*\nignore=*", kind.as_ref()));
            assert_eq!(defaults.filter_common(kind, value), excluded);
            assert_eq!(constructed.filter_common(kind, value), excluded);
            assert_eq!(absent.filter_common(kind, value), excluded);
            assert!(empty.filter_common(kind, value));
            assert!(!ignored.filter_common(kind, value));
            assert!(ignored.filter_common(kind, "*"));
            let mixed = filter(&format!("[rules.{}]\ndo=*,{value}", kind.as_ref()));
            assert!(!mixed.filter_common(kind, value));
            assert!(mixed.filter_common(kind, "other"));
            let all = filter(&format!("[rules.{}]\ndo=*,*", kind.as_ref()));
            assert!(!all.filter_common(kind, "other"));
        }
        let legacy = filter("[filter]\nignore_cmds=*,SET");
        assert!(!legacy.filter_cmd("GET"));
        assert!(legacy.filter_cmd("SET"));
        assert!(legacy.filter_cmd("*"));
    }

    #[test]
    fn expressions_support_each_engine_resource_level() {
        for engine in [
            DbType::Mysql,
            DbType::Pg,
            DbType::Mssql,
            DbType::Mongo,
            DbType::Redis,
            DbType::Kafka,
        ] {
            let mut ini = ini("");
            let mut ids = Vec::new();
            for &kind in resource_levels(&engine) {
                let name = match kind {
                    ResourceKind::Database => "db",
                    ResourceKind::Schema => "schema",
                    ResourceKind::Table => "table",
                    ResourceKind::Column => "column",
                    ResourceKind::Collection => "collection",
                    ResourceKind::Topic => "topic",
                };
                let mut segments: Vec<_> = ids
                    .last()
                    .map(|(id, _): &(RID, &str)| id.segments().to_vec())
                    .unwrap_or_default();
                segments.push(name.into());
                let id = RID::new(segments).unwrap();
                ini.set(
                    &format!("rules.{name}"),
                    "where_conditions",
                    Some(format!("{}:\"{name}\"", id.segments().join("."))),
                );
                ids.push((id, name));
            }
            let filter = load_filter(&ini, engine).unwrap();
            assert_eq!(filter.expression_filters.len(), ids.len());
            for (id, condition) in ids {
                assert_eq!(filter.where_condition(&id).unwrap(), Some(condition));
                let mut sibling = id.segments().to_vec();
                *sibling.last_mut().unwrap() = "other".into();
                assert_eq!(
                    filter.where_condition(&RID::new(sibling).unwrap()).unwrap(),
                    None
                );
            }
        }
    }

    #[test]
    fn expression_paths_decode_quotes_and_preserve_segment_boundaries() {
        for (engine, section, value, id) in [
            (
                DbType::Mysql,
                "table",
                r#"`a.b,c:d`.`t*`:"id > 1""#,
                rid!("a.b,c:d", "t*"),
            ),
            (
                DbType::Pg,
                "table",
                r#""a.b,c:d"."t""x":"id > 1""#,
                rid!("a.b,c:d", "t\"x"),
            ),
            (
                DbType::Mssql,
                "table",
                r#"[db]]:x].[s,1].[t]:"id > 1""#,
                rid!("db]:x", "s,1", "t"),
            ),
            (DbType::Kafka, "topic", r#"orders:"id > 1""#, rid!("orders")),
            (
                DbType::Mongo,
                "collection",
                r#"inventory.events:"id > 1""#,
                rid!("inventory", "events"),
            ),
        ] {
            let filter = load_filter(
                &ini(&format!("[rules.{section}]\nwhere_conditions={value}")),
                engine,
            )
            .unwrap();
            assert_eq!(filter.where_condition(&id).unwrap(), Some("id > 1"));
        }
    }

    #[test]
    fn where_conditions_keep_the_first_exact_rid_without_inheriting_parents() {
        let f = filter(
            r#"[rules.db]
where_conditions=inventory:"database condition"
[rules.table]
where_conditions=inventory.orders:"id > 100",`inventory`.`orders`:"discarded",inventory.events:"id > 1",inventory.empty:"",inventory.empty:"discarded""#,
        );
        for (id, expected) in [
            (rid!("inventory"), Some("database condition")),
            (rid!("inventory", "orders"), Some("id > 100")),
            (rid!("inventory", "events"), Some("id > 1")),
            (rid!("inventory", "empty"), Some("")),
            (rid!("inventory", "other"), None),
            (rid!("other", "orders"), None),
            (rid!("inventory2"), None),
            (rid!("inventory", "orders", "id"), None),
        ] {
            assert_eq!(f.where_condition(&id).unwrap(), expected, "{id}");
        }
        let ExpressionFilter::WhereConditions { conditions } =
            &f.expression_filters[&ResourceKind::Table];
        assert_eq!(conditions.len(), 3);
    }

    #[test]
    fn expression_strings_use_json_escapes_independently_of_sql_quotes() {
        let value = r#"inventory.orders:"\"名字\" = '订单,事件:1' AND path = 'C:\\tmp' AND mark = '\u4e2d\ud83d\ude00'\nAND id IN (1,2)\t/* #; */",inventory.events:"id > 1",inventory.empty:"""#;
        let expected = "\"名字\" = '订单,事件:1' AND path = 'C:\\tmp' AND mark = '中😀'\nAND id IN (1,2)\t/* #; */";
        for engine in [DbType::Mysql, DbType::Pg, DbType::Mssql] {
            let value = if engine == DbType::Mssql {
                value.replace("inventory.", "inventory.dbo.")
            } else {
                value.to_owned()
            };
            let f = load_filter(
                &ini(&format!("[rules.table]\nwhere_conditions={value}")),
                engine.clone(),
            )
            .unwrap();
            for (table, expected) in [("orders", expected), ("events", "id > 1"), ("empty", "")] {
                let id = if engine == DbType::Mssql {
                    RID::new(["inventory", "dbo", table]).unwrap()
                } else {
                    RID::new(["inventory", table]).unwrap()
                };
                assert_eq!(f.where_condition(&id).unwrap(), Some(expected));
            }
        }
    }

    #[test]
    fn invalid_expressions_fail_when_compiled_for_the_engine() {
        for (engine, section, value) in [
            (DbType::Mysql, "table", r#"json:{"db.t":"replaced format"}"#),
            (DbType::Mysql, "table", r#"db:"wrong depth""#),
            (DbType::Mysql, "db", r#"db.t:"wrong depth""#),
            (DbType::Mssql, "table", r#"db.t:"missing schema""#),
            (DbType::Mysql, "table", r#"db.*:"wildcard unsupported""#),
            (DbType::Mysql, "table", r#"db.t?:"wildcard unsupported""#),
            (
                DbType::Mysql,
                "table",
                r#"db.r#t[0-9]+#:"regex unsupported""#,
            ),
            (DbType::Mysql, "table", r#"db.r#[#:"invalid regex""#),
            (DbType::Mysql, "table", r#"`db.t:"unclosed identifier""#),
            (DbType::Mysql, "table", r#"db.r#table:"unclosed regex""#),
            (DbType::Mysql, "table", r#"db..t:"empty segment""#),
            (DbType::Mysql, "table", r#"db.t,db.other:"multiple paths""#),
            (DbType::Mysql, "event", ""),
            (DbType::Mysql, "schema", ""),
            (DbType::Pg, "db", ""),
            (DbType::Mongo, "column", ""),
        ] {
            let config = FilterConfig::from_ini(&ini(&format!(
                "[rules.{section}]\nwhere_conditions={value}"
            )))
            .unwrap();
            assert!(
                DtFilter::from_config(&config, engine).is_err(),
                "{section}: {value}"
            );
        }
        // Bypass INI parsing so raw control characters reach the JSON decoder.
        for value in [
            r#"db.t"#,
            r#"db.t:"#,
            r#"db.t:id > 1"#,
            r#"db.t:'id > 1'"#,
            r#"db.t:123"#,
            r#"db.t:"unterminated"#,
            r#"db.t:"bad\q""#,
            r#"db.t:"\uD800""#,
            r#"db.t:"\u12xx""#,
            "db.t:\"raw\ncontrol\"",
            r#"db.t:"unescaped "name"""#,
            r#"db.t:"SQL doubled ""name""""#,
            r#"db.t:"ok"suffix"#,
            r#"db.t:"ok" db.other:"missing comma""#,
            r#"db.t:"ok", "#,
            r#"db.t:"ok",,db.other:"extra comma""#,
        ] {
            assert!(
                where_conditions(value, ResourceKind::Table, &DbType::Mysql).is_err(),
                "{value}"
            );
        }
    }

    #[test]
    fn legacy_mssql_and_new_resource_rules_use_separate_modes() {
        let f = load_filter(&ini("[filter]\ndo_tbs=market.orders"), DbType::Mssql).unwrap();
        assert!(!f.filter(&rid!("market", "dbo", "orders")).unwrap());
        assert!(f.filter(&rid!("market", "other", "orders")).unwrap());
        let f = load_filter(&ini("[rules.schema]\ndo=inventory"), DbType::Pg).unwrap();
        assert!(!f.filter(&rid!("inventory", "order")).unwrap());
        let f = filter("[filter]\ndo_tbs=db.t2,db.t1\n[rules.table]\ndo=db.t1,db.t2");
        assert!(!f.filter(&rid!("db", "t1")).unwrap());
        let f = filter("[filter]\ndo_tbs=db.t1\n[rules.table]\ndo=db.t2");
        assert!(f.filter(&rid!("db", "t1")).unwrap());
        assert!(!f.filter(&rid!("db", "t2")).unwrap());
    }

    #[test]
    fn non_relational_resources_use_their_own_sections() {
        for (db_type, text, selected, excluded) in [
            (
                DbType::Mongo,
                "[rules.collection]\ndo=db.c",
                rid!("db", "c"),
                rid!("db", "other"),
            ),
            (DbType::Redis, "[rules.db]\ndo=0", rid!("0"), rid!("1")),
            (
                DbType::Kafka,
                "[rules.topic]\ndo=orders*",
                rid!("orders.eu"),
                rid!("other"),
            ),
        ] {
            let f = load_filter(&ini(text), db_type).unwrap();
            assert!(!f.filter_event(&selected, &RowType::Insert).unwrap());
            assert!(f.filter(&excluded).unwrap());
        }
        assert!(load_filter(&ini("[filter]\ndo_dbs=topic"), DbType::Kafka).is_err());
    }

    #[test]
    fn malformed_rules_fail_during_compilation() {
        for text in [
            "[rules.table]\ndo=db",
            "[rules.table]\ndo=db..table",
            "[rules.table]\ndo=db.table,",
            "[rules.table]\ndo=`db.table",
            "[rules.table]\ndo=db.r#[#",
            "[rules.table]\ndo=db.t\n[rules.event]\ndo=r#[#",
            "[rules.event]\ndo=typo",
            "[rules.event]\nignore=typo",
            "[rules.ddl]\nignore=typo",
            "[rules.dcl]\nignore=typo",
            "[rules.structure]\nignore=typo",
            "[rules.events]\ndo=insert",
            r#"[filter]
    where_conditions=json:[{"id":["db","t"],"db":"other","tb":"t","condition":"id > 1"}]"#,
        ] {
            let ini = ini(text);
            let config = FilterConfig::from_ini(&ini).unwrap();
            assert!(
                DtFilter::from_config(&config, DbType::Mysql).is_err(),
                "{text}"
            );
        }
        assert!(filter("").filter(&rid!("db", "t")).unwrap());
    }
    #[test]
    fn tokenization_preserves_patterns_quotes_and_segment_boundaries() {
        for (value, expected) in [
            ("`db*`.order?*", rid!("`db*`", "order?*")),
            ("db.r#order[0-9]+#", rid!("db", "r#order[0-9]+#")),
            ("db.order+*", rid!("db", "order+*")),
            ("`a.b`.`order+*`", rid!("`a.b`", "`order+*`")),
        ] {
            assert_eq!(
                patterns(value, &DbType::Mysql).unwrap(),
                HashSet::from([expected]),
                "{value}"
            );
        }
        assert_eq!(
            patterns("`db`.`^.*$`,db.*,db.*", &DbType::Mysql).unwrap(),
            HashSet::from([rid!("`db`", "`^.*$`"), rid!("db", "*")])
        );
    }

    #[test]
    fn explicit_regex_groups_anchor_alternatives_and_quoted_wildcards_are_literal() {
        let f = filter("[rules.table]\ndo=`a.b`.`t*`,r#(?:a|b)#.r#order[0-9]+#,unicode.订单\n");
        for id in [
            rid!("a.b", "t*"),
            rid!("a", "order10"),
            rid!("b", "order2"),
            rid!("unicode", "订单"),
        ] {
            assert!(!f.filter(&id).unwrap(), "{id}");
        }
        for id in [
            rid!("a.b", "t123"),
            rid!("ab", "order2"),
            rid!("a", "order2tail"),
        ] {
            assert!(f.filter(&id).unwrap(), "{id}");
        }
        let mssql = load_filter(
            &ini("[rules.table]\ndo=[a.b].[inv]]entory].[ord.er]"),
            DbType::Mssql,
        )
        .unwrap();
        assert!(!mssql.filter(&rid!("a.b", "inv]entory", "ord.er")).unwrap());
    }
}
