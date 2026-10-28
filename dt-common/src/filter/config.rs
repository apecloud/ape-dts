use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use anyhow::{ensure, Context, Result};

use super::{CommonFilter, CommonFilterKind, DtFilter, ExpressionFilter, ResourceFilter};
use crate::{
    config::{
        config_enums::DbType,
        config_token_parser::ConfigTokenParser,
        filter_config::{ExpressionFilterConfig, FilterConfig, FilterSectionConfig, SOURCE},
    },
    meta::{
        dcl_meta::dcl_type::DclType, ddl_meta::ddl_type::DdlType, row_type::RowType,
        struct_meta::structure::structure_type::StructureType,
    },
    resource::{
        ResourceConfigParser, ResourceKind, MAPPING_SEPARATOR, REGEX_PREFIX, RESOURCE_DELIMITERS,
        RESOURCE_SEPARATOR, RID, RULE_SEPARATOR, WILDCARD,
    },
};

const VALUE_DELIMITERS: &[char] = &[','];

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
                    "[{SOURCE}.{name}].where_conditions requires a resource section"
                );
                filter.set_common_filter(kind, CommonFilter::from_config(kind, base)?);
                continue;
            }
            let Some(kind) = ResourceKind::from_section(name, &db_type)? else {
                continue;
            };
            let resource_filter = ResourceFilter {
                include_set: ResourceFilter::parse_patterns(
                    base.include_rules.as_deref().unwrap_or_default(),
                    &db_type,
                )
                .with_context(|| format!("{kind:?}.include"))?,
                exclude_set: ResourceFilter::parse_patterns(
                    base.exclude_rules.as_deref().unwrap_or_default(),
                    &db_type,
                )
                .with_context(|| format!("{kind:?}.exclude"))?,
            };
            let depth = kind.depth(&db_type).context("unsupported resource kind")?;
            resource_filter.validate(depth + 1, &filter.matcher)?;
            filter.set_resource_filter(kind, resource_filter)?;
            for expression in &base.expression_filters {
                let expression = match expression {
                    ExpressionFilterConfig::WhereConditions(value) => {
                        ExpressionFilter::parse_where_conditions(value, kind, &db_type)
                            .with_context(|| format!("{kind:?}.where_conditions"))?
                    }
                };
                filter.set_expression_filter(kind, expression)?;
            }
        }
        Ok(filter)
    }
}

impl CommonFilter {
    fn from_config(kind: CommonFilterKind, base: &FilterSectionConfig) -> Result<Self> {
        let context = format!("[{SOURCE}.{}]", kind.as_ref());
        Ok(Self {
            include_set: Self::parse_values(
                base.include_rules
                    .as_deref()
                    .unwrap_or_else(|| kind.default_include()),
                kind,
            )
            .with_context(|| format!("{context}.include"))?,
            exclude_set: Self::parse_values(
                base.exclude_rules.as_deref().unwrap_or_default(),
                kind,
            )
            .with_context(|| format!("{context}.exclude"))?,
        })
    }

    fn parse_values(value: &str, kind: CommonFilterKind) -> Result<HashSet<String>> {
        ensure!(
            kind == CommonFilterKind::Cmd || !value.trim_end().ends_with(RULE_SEPARATOR),
            "trailing comma in {} filter values",
            kind.as_ref()
        );
        let pairs = ResourceConfigParser::escape_pairs(&[]);
        let mut rules = HashSet::new();
        let mut remaining = value.trim();
        while !remaining.is_empty() {
            // Keep commas inside r#...# in the same literal command token.
            let (name, rest) = if kind == CommonFilterKind::Cmd
                && remaining.starts_with(REGEX_PREFIX)
            {
                let (name, rest) = ConfigTokenParser::token(remaining, VALUE_DELIMITERS, &pairs)?;
                ensure!(
                    rest.is_empty() || rest.starts_with(RULE_SEPARATOR),
                    "expected comma after command value"
                );
                (name, rest.strip_prefix(RULE_SEPARATOR).unwrap_or(rest))
            } else {
                remaining
                    .split_once(RULE_SEPARATOR)
                    .unwrap_or((remaining, ""))
            };
            let name = name.trim();
            remaining = rest.trim_start();
            if kind == CommonFilterKind::Cmd && name.is_empty() {
                continue;
            }
            if name == WILDCARD {
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
}

impl ExpressionFilter {
    fn parse_where_conditions(value: &str, kind: ResourceKind, db_type: &DbType) -> Result<Self> {
        let pairs = ResourceConfigParser::escape_pairs(&[db_type]);
        let mut remaining = value.trim();
        let mut conditions = HashMap::new();
        while !remaining.is_empty() {
            let mut segments = Vec::new();
            loop {
                let (token, rest) =
                    ConfigTokenParser::token(remaining, RESOURCE_DELIMITERS, &pairs)?;
                segments.push(token.to_owned());
                if let Some(rest) = rest.strip_prefix(RESOURCE_SEPARATOR) {
                    remaining = rest;
                } else {
                    remaining = rest
                        .strip_prefix(MAPPING_SEPARATOR)
                        .context("expected ':' after WHERE resource ID")?
                        .trim_start();
                    break;
                }
            }
            let id = ResourceConfigParser::exact_path(segments, db_type)?;
            kind.validate_rid(db_type, &id)?;
            let mut strings = serde_json::Deserializer::from_str(remaining).into_iter::<String>();
            let condition = strings
                .next()
                .context("missing WHERE expression")?
                .context("invalid JSON string escape in WHERE expression")?;
            remaining = remaining[strings.byte_offset()..].trim_start();
            conditions.entry(id).or_insert(condition);
            if !remaining.is_empty() {
                remaining = remaining
                    .strip_prefix(RULE_SEPARATOR)
                    .context("expected ',' after WHERE expression")?
                    .trim_start();
                ensure!(!remaining.is_empty(), "trailing comma in WHERE conditions");
            }
        }
        Ok(Self::WhereConditions { conditions })
    }
}

impl ResourceFilter {
    fn parse_patterns(value: &str, db_type: &DbType) -> Result<HashSet<RID>> {
        ResourceConfigParser::paths(value, db_type)?
            .into_iter()
            .map(|path| {
                for token in &path {
                    ResourceConfigParser::literal(token, db_type)?;
                }
                Ok(RID::new(path))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use configparser::ini::Ini;

    use super::*;
    use crate::rid;

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
                ("[source.table]\ninclude=db.t", false, None),
                ("[source.table]", true, None),
                ("[source.table]\nroute=db.t:archive.t", true, None),
                ("[source.db]\ninclude=", true, None),
                ("[source.table]\ninclude=db.t\nwhere_conditions=", false, None),
                ("[source.table]\ninclude=db.t\nwhere_conditions=db.t:\"new\"", false, Some("new")),
                ("[source.db]\nwhere_conditions=db:\"database\"\n[source.table]\ninclude=db.t", false, None),
            ] {
                let f = filter(&format!("[filter]\n{legacy}\ndo_events=insert\n{section}"));
                assert_eq!(f.filter(rid!("db", "t", "id")).unwrap(), excluded, "{section}");
                assert!(f.filter(rid!("legacy", "t")).unwrap());
                for event in [RowType::Insert, RowType::Update] {
                    assert_eq!(f.filter_event(rid!("db", "t"), &event).unwrap(), excluded, "{section}");
                }
                assert_eq!(f.where_condition(rid!("db", "t")).unwrap(), condition, "{section}");
                assert_eq!(f.where_condition(rid!("db", "t", "id")).unwrap(), None);
            }
        }
        let f = load_filter(
            &ini("[filter]\ndo_tbs=db.t\n[source.db]\ninclude="),
            DbType::Pg,
        )
        .unwrap();
        assert!(f.filter(rid!("db", "t")).unwrap());
    }

    #[test]
    fn raw_resource_names_are_resolved_for_the_consuming_engine() {
        for (db_type, section) in [
            (DbType::Mysql, "db"),
            (DbType::Mysql, "database"),
            (DbType::Pg, "schema"),
        ] {
            let config =
                FilterConfig::from_ini(&ini(&format!("[source.{section}]\ninclude=inventory")))
                    .unwrap();
            let filter = DtFilter::from_config(&config, db_type).unwrap();
            assert!(!filter.filter(rid!("inventory", "order")).unwrap());
            assert!(filter.filter(rid!("other", "order")).unwrap());
        }
        for (db_type, text) in [
            (DbType::Mysql, "[source.unknown]\ninclude=grant"),
            (DbType::Mongo, "[source.document]\ninclude=db.c"),
        ] {
            let config = FilterConfig::from_ini(&ini(text)).unwrap();
            assert!(DtFilter::from_config(&config, db_type).is_err(), "{text}");
        }
        let filter = load_filter(
            &ini("[source.schema]\ninclude=inventory\n[source.db]\ninclude="),
            DbType::Pg,
        )
        .unwrap();
        assert!(!filter.filter(rid!("inventory", "order")).unwrap());
    }

    #[test]
    fn unsupported_resource_sections_are_skipped_before_parsing_values() {
        for (engine, supported, unsupported) in [
            (DbType::Mysql, "db", "schema"),
            (DbType::Mysql, "db", "collection"),
            (DbType::Pg, "schema", "database"),
            (DbType::Mssql, "db", "topic"),
            (DbType::Mongo, "db", "column"),
            (DbType::Redis, "db", "table"),
            (DbType::Kafka, "topic", "db"),
        ] {
            let text = format!("[source.{supported}]\ninclude=selected\n[source.{unsupported}]\ninclude=r#[#\nexclude=invalid..path\nwhere_conditions=invalid");
            let filter = load_filter(&ini(&text), engine).unwrap();
            assert!(!filter.filter(rid!("selected")).unwrap(), "{text}");
            assert!(filter.filter(rid!("other")).unwrap(), "{text}");
            assert_eq!(filter.resource_filters().len(), 1);
            assert!(filter.expression_filters.is_empty());
        }
    }

    #[test]
    fn new_and_legacy_configs_keep_union_exclude_and_question_mark_semantics() {
        let old = filter("[filter]\ndo_dbs=test_db_*\nignore_dbs=test_db_3\ndo_tbs=other_test_db_1.*\nignore_tbs=test_db_4.*,test_db_?.one_pk_no_uk_2,*_test_db_1.one_pk_no_uk_2\ndo_events=insert");
        let new = filter("[source.event]\ninclude=insert\n[source.db]\ninclude=test_db_*\nexclude=test_db_3\n[source.table]\ninclude=other_test_db_1.*\nexclude=test_db_4.*,test_db_?.one_pk_no_uk_2,*_test_db_1.one_pk_no_uk_2");
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
            let id = RID::new([db, table]);
            for f in [&old, &new] {
                assert_eq!(f.filter(&id).unwrap(), excluded, "{id}");
                assert_eq!(f.filter_event(&id, &RowType::Insert).unwrap(), excluded);
                assert!(f.filter_event(&id, &RowType::Update).unwrap());
                assert_eq!(f.filter(&id).unwrap(), excluded);
            }
        }
    }

    #[test]
    fn common_sections_support_include_and_exclude_for_every_engine() {
        let config = FilterConfig::from_ini(&ini(r#"[filter]
do_events=r#[#
do_ddls=r#[#
do_dcls=r#[#
do_structures=r#[#
ignore_cmds=GET
[source.event]
include=insert,update
exclude=update
[source.ddl]
include=create_table,drop_table
exclude=drop_table
[source.dcl]
include=grant,revoke
exclude=revoke
[source.structure]
include=table,index
exclude=index
[source.cmd]
include=GET,SET,FT.SEARCH
exclude=SET"#))
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
            let depth = ResourceKind::levels(&engine)
                .iter()
                .take_while(|&&kind| kind != ResourceKind::Column)
                .count();
            let id = RID::new(vec!["object"; depth]);
            let mut f = DtFilter::from_config(&config, engine).unwrap();
            f.add_include(&id).unwrap();
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
[source.event]
exclude=update"#,
        );
        assert!(f.filter(rid!("db", "t")).unwrap());
        assert_eq!(f.where_condition(rid!("db", "t")).unwrap(), None);
        f.add_include(&rid!("db", "t")).unwrap();
        assert!(!f.filter_event(rid!("db", "t"), &RowType::Delete).unwrap());
        assert!(f.filter_event(rid!("db", "t"), &RowType::Update).unwrap());
        assert!(f
            .filter_ddl(rid!("db", "t"), &DdlType::CreateTable)
            .unwrap());
        assert!(f.filter_dcl(&DclType::Grant));
        assert!(!f.filter_structure(&StructureType::Table));
        assert!(!f.filter_structure(&StructureType::Index));
        assert!(!f.filter_cmd("SET"));
    }

    #[test]
    fn common_defaults_empty_rules_and_literal_excludes_are_distinct() {
        let defaults = filter("");
        let constructed = DtFilter::new(DbType::Mysql);
        for (kind, value, excluded) in [
            (CommonFilterKind::Event, "insert", false),
            (CommonFilterKind::Ddl, "create_table", true),
            (CommonFilterKind::Dcl, "grant", true),
            (CommonFilterKind::Structure, "table", false),
            (CommonFilterKind::Cmd, "GET", false),
        ] {
            let absent = filter(&format!("[source.{}]", kind.as_ref()));
            let empty = filter(&format!("[source.{}]\ninclude=", kind.as_ref()));
            let with_excludes =
                filter(&format!("[source.{}]\ninclude=*\nexclude=*", kind.as_ref()));
            assert_eq!(defaults.filter_common(kind, value), excluded);
            assert_eq!(constructed.filter_common(kind, value), excluded);
            assert_eq!(absent.filter_common(kind, value), excluded);
            assert!(empty.filter_common(kind, value));
            assert!(!with_excludes.filter_common(kind, value));
            assert!(with_excludes.filter_common(kind, "*"));
            let mixed = filter(&format!("[source.{}]\ninclude=*,{value}", kind.as_ref()));
            assert!(!mixed.filter_common(kind, value));
            assert!(mixed.filter_common(kind, "other"));
            let all = filter(&format!("[source.{}]\ninclude=*,*", kind.as_ref()));
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
            for &kind in ResourceKind::levels(&engine) {
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
                let id = RID::new(segments);
                ini.set(
                    &format!("source.{name}"),
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
                assert_eq!(filter.where_condition(RID::new(sibling)).unwrap(), None);
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
                &ini(&format!("[source.{section}]\nwhere_conditions={value}")),
                engine,
            )
            .unwrap();
            assert_eq!(filter.where_condition(&id).unwrap(), Some("id > 1"));
        }
    }

    #[test]
    fn where_conditions_keep_the_first_exact_rid_without_inheriting_parents() {
        let f = filter(
            r#"[source.db]
where_conditions=inventory:"database condition"
[source.table]
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
                &ini(&format!("[source.table]\nwhere_conditions={value}")),
                engine.clone(),
            )
            .unwrap();
            for (table, expected) in [("orders", expected), ("events", "id > 1"), ("empty", "")] {
                let id = if engine == DbType::Mssql {
                    RID::new(["inventory", "dbo", table])
                } else {
                    RID::new(["inventory", table])
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
            (
                DbType::Mysql,
                "table",
                r#"[db].t:"wrong identifier quotes""#,
            ),
            (DbType::Pg, "table", r#"`db`.t:"wrong identifier quotes""#),
            (
                DbType::Mssql,
                "table",
                r#""db".dbo.t:"wrong identifier quotes""#,
            ),
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
        ] {
            let config = FilterConfig::from_ini(&ini(&format!(
                "[source.{section}]\nwhere_conditions={value}"
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
                ExpressionFilter::parse_where_conditions(
                    value,
                    ResourceKind::Table,
                    &DbType::Mysql
                )
                .is_err(),
                "{value}"
            );
        }
    }

    #[test]
    fn legacy_mssql_and_new_resource_rules_use_separate_modes() {
        let f = load_filter(&ini("[filter]\ndo_tbs=market.orders"), DbType::Mssql).unwrap();
        assert!(!f.filter(rid!("market", "dbo", "orders")).unwrap());
        assert!(f.filter(rid!("market", "other", "orders")).unwrap());
        let f = load_filter(&ini("[source.schema]\ninclude=inventory"), DbType::Pg).unwrap();
        assert!(!f.filter(rid!("inventory", "order")).unwrap());
        let f = filter("[filter]\ndo_tbs=db.t2,db.t1\n[source.table]\ninclude=db.t1,db.t2");
        assert!(!f.filter(rid!("db", "t1")).unwrap());
        let f = filter("[filter]\ndo_tbs=db.t1\n[source.table]\ninclude=db.t2");
        assert!(f.filter(rid!("db", "t1")).unwrap());
        assert!(!f.filter(rid!("db", "t2")).unwrap());
    }

    #[test]
    fn non_relational_resources_use_their_own_sections() {
        for (db_type, text, selected, excluded) in [
            (
                DbType::Mongo,
                "[source.collection]\ninclude=db.c",
                rid!("db", "c"),
                rid!("db", "other"),
            ),
            (
                DbType::Redis,
                "[source.db]\ninclude=0",
                rid!("0"),
                rid!("1"),
            ),
            (
                DbType::Kafka,
                "[source.topic]\ninclude=orders*",
                rid!("orders.eu"),
                rid!("other"),
            ),
        ] {
            let f = load_filter(&ini(text), db_type).unwrap();
            assert!(!f.filter_event(&selected, &RowType::Insert).unwrap());
            assert!(f.filter(&excluded).unwrap());
        }
        assert!(load_filter(&ini("[filter]\ndo_dbs=topic"), DbType::Kafka)
            .unwrap()
            .resource_filters()
            .is_empty());
    }

    #[test]
    fn malformed_rules_fail_during_compilation() {
        for text in [
            "[source.table]\ninclude=db",
            "[source.table]\ninclude=db..table",
            "[source.table]\ninclude=db.table,",
            "[source.table]\ninclude=`db.table",
            "[source.table]\ninclude=db.r#[#",
            "[source.table]\ninclude=db.t\n[source.event]\ninclude=r#[#",
            "[source.event]\ninclude=typo",
            "[source.event]\nexclude=typo",
            "[source.ddl]\nexclude=typo",
            "[source.dcl]\nexclude=typo",
            "[source.structure]\nexclude=typo",
            "[source.events]\ninclude=insert",
            r#"[filter]
    where_conditions=json:[{"id":["db","t"],"condition":"id > 1"}]"#,
        ] {
            let ini = ini(text);
            let config = FilterConfig::from_ini(&ini).unwrap();
            assert!(
                DtFilter::from_config(&config, DbType::Mysql).is_err(),
                "{text}"
            );
        }
        assert!(filter("").filter(rid!("db", "t")).unwrap());
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
                ResourceFilter::parse_patterns(value, &DbType::Mysql).unwrap(),
                HashSet::from([expected]),
                "{value}"
            );
        }
        assert_eq!(
            ResourceFilter::parse_patterns("`db`.`^.*$`,db.*,db.*", &DbType::Mysql).unwrap(),
            HashSet::from([rid!("`db`", "`^.*$`"), rid!("db", "*")])
        );
    }

    #[test]
    fn explicit_regex_groups_anchor_alternatives_and_quoted_wildcards_are_literal() {
        let f =
            filter("[source.table]\ninclude=`a.b`.`t*`,r#(?:a|b)#.r#order[0-9]+#,unicode.订单\n");
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
            &ini("[source.table]\ninclude=[a.b].[inv]]entory].[ord.er]"),
            DbType::Mssql,
        )
        .unwrap();
        assert!(!mssql.filter(rid!("a.b", "inv]entory", "ord.er")).unwrap());
    }
}
