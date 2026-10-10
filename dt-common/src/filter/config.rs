use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

use super::{pattern::MatcherSet, BaseFilter, DtFilter, IdPattern, ValueFilter};
use crate::{
    config::{
        config_enums::DbType,
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
        filter_config::{BaseFilterConfig, FilterConfig},
    },
    meta::{
        dcl_meta::dcl_type::DclType, ddl_meta::ddl_type::DdlType, row_type::RowType,
        struct_meta::structure::structure_type::StructureType,
    },
    resource::{config, resource_depth, resource_levels, ResourceKind, RID},
};

fn patterns(value: &str, db_type: &DbType, legacy: bool, table: bool) -> Result<MatcherSet> {
    let paths = if legacy && (!table || !matches!(db_type, DbType::Mssql)) {
        // The old non-MSSQL grammar flattened both commas and dots. Preserve that
        // behavior here; only new resource sections require explicit path boundaries.
        let pairs = [TokenEscapePair::from(("r#".to_string(), "#".to_string()))];
        let tokens =
            ConfigTokenParser::parse_config(value.trim(), db_type, &[',', '.'], Some(&pairs))?;
        let depth = if table { 2 } else { 1 };
        ensure!(
            tokens.len() % depth == 0,
            "incomplete legacy resource pattern"
        );
        tokens.chunks(depth).map(|chunk| chunk.to_vec()).collect()
    } else {
        config::paths(value, db_type)?
    };
    MatcherSet::new(
        paths
            .into_iter()
            .map(|path| {
                let path = if table {
                    config::legacy_table(path, db_type)?
                } else {
                    path
                };
                IdPattern::from_tokens(&path, db_type, legacy)
            })
            .collect::<Result<_>>()?,
    )
}

fn merge(
    old: MatcherSet,
    old_present: bool,
    new: Option<&str>,
    db_type: &DbType,
) -> Result<MatcherSet> {
    let Some(new) = new else {
        return Ok(old);
    };
    let new = patterns(new, db_type, false, false)?;
    ensure!(
        !old_present || old.equivalent(&new),
        "conflicting legacy and resource filter rules"
    );
    Ok(new)
}

fn category<T: FromStr>(value: &str, key: &str, default: &str) -> Result<ValueFilter> {
    let value = if value.trim().is_empty() {
        default
    } else {
        value
    };
    let mut values = HashSet::new();
    if !value.trim().is_empty() {
        for name in value.split(',').map(str::trim) {
            ensure!(
                name == "*" || T::from_str(name).is_ok(),
                "invalid [filter].{key} value {name}"
            );
            values.insert(name.to_string());
        }
    }
    Ok(ValueFilter { values })
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WhereRule {
    Resource(ResourceWhere),
    Legacy(LegacyWhere),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceWhere {
    id: RID,
    condition: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyWhere {
    db: String,
    tb: String,
    condition: String,
}

#[derive(Deserialize)]
struct IgnoreColumns {
    db: String,
    tb: String,
    ignore_cols: Vec<String>,
}

impl DtFilter {
    /// Compile resource strings independently of their configuration source.
    /// `options` retains [filter] common options and legacy rules for compatibility.
    pub fn from_config(
        bases: &HashMap<ResourceKind, BaseFilterConfig>,
        options: &FilterConfig,
        db_type: DbType,
    ) -> Result<Self> {
        for kind in bases.keys() {
            ensure!(
                resource_depth(&db_type, *kind).is_some(),
                "unsupported {kind:?} for {db_type}"
            );
        }
        if matches!(db_type, DbType::Kafka) {
            for (key, value) in [
                ("do_dbs", &options.do_schemas),
                ("ignore_dbs", &options.ignore_schemas),
            ] {
                ensure!(
                    value.trim().is_empty(),
                    "[filter].{key} is not supported by {db_type}; use [filter.topic]"
                );
            }
        }
        let mut filter = Self::new(db_type.clone());
        let mut old_columns = MatcherSet::default();
        let ignore_cols = &options.ignore_cols;
        if !ignore_cols.trim().is_empty() {
            ensure!(
                resource_depth(&db_type, ResourceKind::Column).is_some(),
                "{db_type} does not support column filtering"
            );
            let mut tables = HashMap::new();
            for row in
                config::json::<Vec<IgnoreColumns>>(ignore_cols).context("[filter].ignore_cols")?
            {
                let table = config::legacy_json_table(&row.db, &row.tb, &db_type)?;
                tables.insert(table, row.ignore_cols);
            }
            for (table, columns) in tables {
                for col in columns {
                    old_columns.insert(IdPattern::literal(table.child(col)?))?;
                }
            }
        }
        let empty_base = BaseFilterConfig::default();
        for (index, &kind) in resource_levels(&db_type).iter().enumerate() {
            let base = bases.get(&kind).unwrap_or(&empty_base);
            let legacy_keys = if index == 0 {
                Some((
                    "do_dbs",
                    &options.do_schemas,
                    "ignore_dbs",
                    &options.ignore_schemas,
                    false,
                ))
            } else if matches!(kind, ResourceKind::Table | ResourceKind::Collection) {
                Some((
                    "do_tbs",
                    &options.do_tbs,
                    "ignore_tbs",
                    &options.ignore_tbs,
                    true,
                ))
            } else {
                None
            };
            let mut old_do = MatcherSet::default();
            let mut old_ignore = MatcherSet::default();
            let mut do_present = false;
            let mut ignore_present = false;
            if let Some((do_key, do_value, ignore_key, ignore_value, table)) = legacy_keys {
                do_present = !do_value.trim().is_empty();
                ignore_present = !ignore_value.trim().is_empty();
                old_do = patterns(do_value, &db_type, true, table)
                    .with_context(|| format!("[filter].{do_key}"))?;
                old_ignore = patterns(ignore_value, &db_type, true, table)
                    .with_context(|| format!("[filter].{ignore_key}"))?;
            }
            if kind == ResourceKind::Column {
                old_ignore = old_columns.clone();
                ignore_present = !ignore_cols.trim().is_empty();
            }
            let compiled = BaseFilter {
                do_set: merge(old_do, do_present, base.do_rules.as_deref(), &db_type)
                    .with_context(|| format!("{kind:?}.do"))?,
                ignore_set: merge(
                    old_ignore,
                    ignore_present,
                    base.ignore_rules.as_deref(),
                    &db_type,
                )
                .with_context(|| format!("{kind:?}.ignore"))?,
            };
            filter.set_base_filter(kind, compiled)?;
        }
        // Do not silently swallow table options on engines which have no tables.
        if matches!(db_type, DbType::Redis | DbType::Kafka) {
            for (key, value) in [
                ("do_tbs", &options.do_tbs),
                ("ignore_tbs", &options.ignore_tbs),
            ] {
                ensure!(
                    value.trim().is_empty(),
                    "[filter].{key} is not supported by {db_type}"
                );
            }
        }
        filter.events = category::<RowType>(&options.do_events, "do_events", "*")?;
        filter.ddls = category::<DdlType>(&options.do_ddls, "do_ddls", "")?;
        filter.dcls = category::<DclType>(&options.do_dcls, "do_dcls", "")?;
        filter.structures =
            category::<StructureType>(&options.do_structures, "do_structures", "*")?;
        let commands = &options.ignore_cmds;
        filter.ignore_cmds = commands
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        let conditions = &options.where_conditions;
        if !conditions.trim().is_empty() {
            for rule in
                config::json::<Vec<WhereRule>>(conditions).context("[filter].where_conditions")?
            {
                match rule {
                    WhereRule::Resource(ResourceWhere { id, condition }) => {
                        filter.set_where_condition(id, condition)?
                    }
                    WhereRule::Legacy(LegacyWhere { db, tb, condition }) => {
                        let id = config::legacy_json_table(&db, &tb, &db_type)?;
                        crate::resource::validate_rid(&db_type, ResourceKind::Table, &id)?;
                        // Legacy duplicate entries keep their existing last-value behavior.
                        filter.where_conditions.insert(id, condition);
                    }
                }
            }
        }
        Ok(filter)
    }
}

#[cfg(test)]
mod tests {
    use configparser::ini::Ini;

    use super::*;
    use crate::rid;

    fn load_filter(ini: &Ini, db_type: DbType) -> Result<DtFilter> {
        let bases = BaseFilterConfig::from_ini(ini, &db_type)?;
        DtFilter::from_config(&bases, &FilterConfig::from_ini(ini), db_type)
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
        let mut bases = HashMap::from([
            (
                ResourceKind::Table,
                BaseFilterConfig {
                    ignore_rules: Some("market.inventory.secret".into()),
                    ..Default::default()
                },
            ),
            (
                ResourceKind::Schema,
                BaseFilterConfig {
                    do_rules: Some("market.inventory".into()),
                    ..Default::default()
                },
            ),
        ]);
        let options = FilterConfig {
            do_events: "insert".into(),
            ..Default::default()
        };
        let filter = DtFilter::from_config(&bases, &options, DbType::Mssql).unwrap();
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
        // Direct maps must not silently drop unsupported kinds.
        bases.insert(
            ResourceKind::Collection,
            BaseFilterConfig {
                do_rules: Some("db.c".into()),
                ..Default::default()
            },
        );
        assert!(DtFilter::from_config(&bases, &options, DbType::Mssql).is_err());
        // A missing field inherits legacy rules; an explicitly empty field conflicts.
        let mut bases = HashMap::from([(ResourceKind::Table, BaseFilterConfig::default())]);
        let options = FilterConfig {
            do_tbs: "db.t".into(),
            ..Default::default()
        };
        assert!(!DtFilter::from_config(&bases, &options, DbType::Mysql)
            .unwrap()
            .filter(&rid!("db", "t"))
            .unwrap());
        bases.get_mut(&ResourceKind::Table).unwrap().do_rules = Some(String::new());
        assert!(DtFilter::from_config(&bases, &options, DbType::Mysql).is_err());
    }

    #[test]
    fn new_and_legacy_configs_keep_union_ignore_and_question_mark_semantics() {
        let old = filter("[filter]\ndo_dbs=test_db_*\nignore_dbs=test_db_3\ndo_tbs=other_test_db_1.*\nignore_tbs=test_db_4.*,test_db_?.one_pk_no_uk_2,*_test_db_1.one_pk_no_uk_2\ndo_events=insert");
        let new = filter("[filter]\ndo_events=insert\nwhere_conditions=\n[filter.database]\ndo=test_db_*\nignore=test_db_3\n[filter.table]\ndo=other_test_db_1.*\nignore=test_db_4.*,test_db_?.one_pk_no_uk_2,*_test_db_1.one_pk_no_uk_2");
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
    fn where_and_categories_stay_in_filter_with_existing_defaults() {
        let f = filter(
            r#"[filter]
    do_events=
    do_structures=
    do_dcls=grant
    do_ddls=create_table
    ignore_cmds=FLUSHDB
    where_conditions=json:[{"id":["db","t"],"condition":"id > 1"}]
    [filter.database]
    do=db
    "#,
        );
        assert!(!f.filter_event(&rid!("db", "t"), &RowType::Delete).unwrap());
        assert!(!f.filter_structure(&StructureType::Index));
        assert!(!f.filter_dcl(&DclType::Grant));
        assert!(f.filter_dcl(&DclType::Revoke));
        assert!(!f
            .filter_ddl(&rid!("db", "t"), &DdlType::CreateTable)
            .unwrap());
        assert!(f.filter_ddl(&rid!("db", "t"), &DdlType::DropTable).unwrap());
        assert!(f.filter_cmd("FLUSHDB"));
        assert!(!f.filter_cmd("GET"));
        assert_eq!(f.where_condition(&rid!("db", "t")).unwrap(), Some("id > 1"));
        assert!(f.where_condition(&rid!("db")).is_err());
        assert!(f.filter_event(&rid!("db"), &RowType::Insert).is_err());
        let defaults = filter("[filter.database]\ndo=db");
        assert!(defaults.filter_dcl(&DclType::Grant));
        assert!(defaults
            .filter_ddl(&rid!("db", "t"), &DdlType::CreateTable)
            .unwrap());
    }

    #[test]
    fn legacy_json_columns_and_where_keep_exact_names_and_last_condition() {
        let f = filter(
            r#"[filter]
    do_dbs=*
    ignore_cols=json:[{"db":"a.b","tb":"t","ignore_cols":["id"]},{"db":"a.b","tb":"t","ignore_cols":["secret"]}]
    where_conditions=json:[{"db":"a.b","tb":"t","condition":"id > 1"},{"db":"a.b","tb":"t","condition":"id > 2"}]
    "#,
        );
        assert!(f.filter(&rid!("a.b", "t", "secret")).unwrap());
        assert!(!f.filter(&rid!("a.b", "t", "id")).unwrap());
        assert_eq!(
            f.where_condition(&rid!("a.b", "t")).unwrap(),
            Some("id > 2")
        );
        let mut f = f;
        assert!(f
            .set_where_condition(rid!("a.b", "t"), "other".into())
            .is_err());
    }

    #[test]
    fn aliases_legacy_mssql_and_mixed_rules_are_checked() {
        let f = load_filter(&ini("[filter]\ndo_tbs=market.orders"), DbType::Mssql).unwrap();
        assert!(!f.filter(&rid!("market", "dbo", "orders")).unwrap());
        assert!(f.filter(&rid!("market", "other", "orders")).unwrap());
        let f = load_filter(&ini("[filter.db]\ndo=inventory"), DbType::Pg).unwrap();
        assert!(!f.filter(&rid!("inventory", "order")).unwrap());
        let f = load_filter(&ini("[filter.document]\ndo=db.c"), DbType::Mongo).unwrap();
        assert!(!f.filter(&rid!("db", "c")).unwrap());
        let f = filter("[filter]\ndo_tbs=db.t2,db.t1\n[filter.table]\ndo=db.t1,db.t2");
        assert!(!f.filter(&rid!("db", "t1")).unwrap());
        assert!(load_filter(
            &ini("[filter]\ndo_tbs=db.t1\n[filter.table]\ndo=db.t2"),
            DbType::Mysql
        )
        .is_err());
    }

    #[test]
    fn legacy_scope_matches_existing_filter_for_flattened_names_and_regexes() {
        use crate::{config::filter_config::FilterConfig, rdb_filter::RdbFilter};
        for db_type in [DbType::Mysql, DbType::Mssql] {
            let do_dbs = "db.name,store[0-9]+,r#inventory|market#";
            let old = RdbFilter::from_config(
                &FilterConfig {
                    do_schemas: do_dbs.into(),
                    ..Default::default()
                },
                &db_type,
            )
            .unwrap();
            let new = load_filter(&ini(&format!("[filter]\ndo_dbs={do_dbs}")), db_type).unwrap();
            for name in ["db", "name", "db.name", "store12", "inventory", "other"] {
                assert_eq!(
                    new.filter(&RID::new([name]).unwrap()).unwrap(),
                    old.filter_schema(name),
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn non_relational_resources_use_their_own_sections() {
        for (db_type, text, selected, excluded) in [
            (
                DbType::Mongo,
                "[filter.collection]\ndo=db.c",
                rid!("db", "c"),
                rid!("db", "other"),
            ),
            (
                DbType::Redis,
                "[filter.database]\ndo=0",
                rid!("0"),
                rid!("1"),
            ),
            (
                DbType::Kafka,
                "[filter.topic]\ndo=orders*",
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
            "[filter.table]\ndo=db",
            "[filter.table]\ndo=db..table",
            "[filter.table]\ndo=db.table,",
            "[filter.table]\ndo=`db.table",
            "[filter.table]\ndo=db.r#[#",
            "[filter.table]\ndo=db.t\n[filter]\ndo_events=typo",
            r#"[filter]
    where_conditions=json:[{"id":["db","t"],"db":"other","tb":"t","condition":"id > 1"}]"#,
        ] {
            let ini = ini(text);
            let bases = BaseFilterConfig::from_ini(&ini, &DbType::Mysql).unwrap();
            assert!(
                DtFilter::from_config(&bases, &FilterConfig::from_ini(&ini), DbType::Mysql)
                    .is_err(),
                "{text}"
            );
        }
        assert!(filter("").filter(&rid!("db", "t")).unwrap());
    }
}
