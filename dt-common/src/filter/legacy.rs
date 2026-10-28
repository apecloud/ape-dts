use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use strum::IntoEnumIterator;

use super::{CommonFilter, CommonFilterKind, DtFilter, ExpressionFilter, ResourceFilter};
use crate::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        filter_config::LegacyFilterConfig,
    },
    resource::{ResourceConfigParser, ResourceKind, REGEX_PREFIX, REGEX_SUFFIX, RID, WILDCARD},
};

const FILTER_DELIMITERS: &[char] = &['.', ','];

#[derive(Deserialize)]
struct IgnoreColumns {
    db: String,
    tb: String,
    ignore_cols: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyWhere {
    db: String,
    tb: String,
    condition: String,
}

impl DtFilter {
    pub fn from_legacy_config(options: &LegacyFilterConfig, db_type: DbType) -> Result<Self> {
        let mut filter = Self::new(db_type.clone());
        filter.load_legacy_resource_filters(options, &db_type)?;
        for kind in CommonFilterKind::iter() {
            if let Some(common) = CommonFilter::from_legacy_config(kind, options, &db_type)? {
                filter.set_common_filter(kind, common);
            }
        }
        if ResourceKind::Table.depth(&db_type).is_some()
            && !options.where_conditions.trim().is_empty()
        {
            let expression = ExpressionFilter::parse_legacy_where_conditions(
                &options.where_conditions,
                &db_type,
            )
            .context("[filter].where_conditions")?;
            let ExpressionFilter::WhereConditions { conditions } = &expression;
            if !conditions.is_empty() {
                filter.set_expression_filter(ResourceKind::Table, expression)?;
            }
        }
        Ok(filter)
    }

    fn load_legacy_resource_filters(
        &mut self,
        options: &LegacyFilterConfig,
        db_type: &DbType,
    ) -> Result<()> {
        for (index, &kind) in ResourceKind::levels(db_type).iter().enumerate() {
            let (include_key, include_value, exclude_key, exclude_value, table) =
                if index == 0 && matches!(kind, ResourceKind::Database | ResourceKind::Schema) {
                    (
                        "do_dbs",
                        &options.do_schemas,
                        "ignore_dbs",
                        &options.ignore_schemas,
                        false,
                    )
                } else if matches!(kind, ResourceKind::Table | ResourceKind::Collection) {
                    (
                        "do_tbs",
                        &options.do_tbs,
                        "ignore_tbs",
                        &options.ignore_tbs,
                        true,
                    )
                } else {
                    continue;
                };
            let resource_filter = ResourceFilter::new(
                ResourceFilter::parse_legacy_patterns(include_value, db_type, table)
                    .with_context(|| format!("[filter].{include_key}"))?,
                ResourceFilter::parse_legacy_patterns(exclude_value, db_type, table)
                    .with_context(|| format!("[filter].{exclude_key}"))?,
            );
            resource_filter.validate(index + 1, &self.matcher)?;
            self.set_resource_filter(kind, resource_filter)?;
        }
        if let Some(depth) = ResourceKind::Column
            .depth(db_type)
            .filter(|_| !options.ignore_cols.trim().is_empty())
        {
            let mut tables = BTreeMap::new();
            for row in ResourceConfigParser::legacy_json::<Vec<IgnoreColumns>>(&options.ignore_cols)
                .context("[filter].ignore_cols")?
            {
                tables.insert(
                    ResourceConfigParser::legacy_json_table(&row.db, &row.tb, db_type)?,
                    row.ignore_cols,
                );
            }
            let mut columns = HashSet::new();
            for (table, names) in tables {
                for name in names {
                    columns.insert(ResourceFilter::literal_pattern(table.child(name)));
                }
            }
            let resource_filter = ResourceFilter::new(HashSet::new(), columns);
            resource_filter.validate(depth + 1, &self.matcher)?;
            self.set_resource_filter(ResourceKind::Column, resource_filter)?;
        }
        Ok(())
    }
}

impl CommonFilter {
    fn from_legacy_config(
        kind: CommonFilterKind,
        options: &LegacyFilterConfig,
        db_type: &DbType,
    ) -> Result<Option<Self>> {
        let (key, value) = match kind {
            CommonFilterKind::Event => ("do_events", options.do_events.as_str()),
            CommonFilterKind::Ddl => ("do_ddls", options.do_ddls.as_str()),
            CommonFilterKind::Dcl => ("do_dcls", options.do_dcls.as_str()),
            CommonFilterKind::Structure => ("do_structures", options.do_structures.as_str()),
            CommonFilterKind::Cmd => ("ignore_cmds", WILDCARD),
        };
        let value = if value.trim().is_empty() {
            kind.default_include()
        } else {
            value
        };
        let exclude_set = if kind == CommonFilterKind::Cmd {
            Self::parse_legacy_values(&options.ignore_cmds, db_type)
                .context("[filter].ignore_cmds")?
        } else {
            HashSet::new()
        };
        if value == kind.default_include() && exclude_set.is_empty() {
            return Ok(None);
        }
        let include_set =
            Self::parse_legacy_values(value, db_type).with_context(|| format!("[filter].{key}"))?;
        Ok(Some(Self::new(include_set, exclude_set)))
    }

    fn parse_legacy_values(value: &str, db_type: &DbType) -> Result<HashSet<String>> {
        let pairs = ResourceConfigParser::escape_pairs(&[]);
        ConfigTokenParser::parse_config(value, db_type, FILTER_DELIMITERS, Some(&pairs))?
            .into_iter()
            .map(|token| {
                // Legacy parsing validates regex syntax even though common values match literally.
                if let Some(pattern) = token
                    .strip_prefix(REGEX_PREFIX)
                    .and_then(|inner| inner.strip_suffix(REGEX_SUFFIX))
                {
                    regex::Regex::new(pattern)
                        .with_context(|| format!("invalid filter regex {token}"))?;
                }
                Ok(token)
            })
            .collect()
    }
}

impl ExpressionFilter {
    fn parse_legacy_where_conditions(value: &str, db_type: &DbType) -> Result<Self> {
        let mut conditions = HashMap::new();
        for rule in ResourceConfigParser::legacy_json::<Vec<LegacyWhere>>(value)? {
            let id = ResourceConfigParser::legacy_json_table(&rule.db, &rule.tb, db_type)?;
            ResourceKind::Table.validate_rid(db_type, &id)?;
            conditions.entry(id).or_insert(rule.condition);
        }
        Ok(Self::WhereConditions { conditions })
    }
}

impl ResourceFilter {
    fn parse_legacy_patterns(value: &str, db_type: &DbType, table: bool) -> Result<HashSet<RID>> {
        let paths = if table && matches!(db_type, DbType::Mssql) {
            ResourceConfigParser::paths(value, db_type)?
        } else {
            // Legacy non-MSSQL table rules and namespace rules flatten commas and dots.
            let pairs = ResourceConfigParser::escape_pairs(&[]);
            let tokens = ConfigTokenParser::parse_config(
                value.trim(),
                db_type,
                FILTER_DELIMITERS,
                Some(&pairs),
            )?;
            let depth = if table { 2 } else { 1 };
            ensure!(
                tokens.len() % depth == 0,
                "incomplete legacy resource pattern"
            );
            tokens.chunks(depth).map(|chunk| chunk.to_vec()).collect()
        };
        paths
            .into_iter()
            .map(|path| {
                let path = if table {
                    ResourceConfigParser::legacy_table(path, db_type)?
                } else {
                    path
                };
                for token in &path {
                    ResourceConfigParser::quoted_literal(token, db_type)?;
                }
                Ok(RID::new(path))
            })
            .collect()
    }

    /// Escape exact legacy JSON names for use as resource patterns.
    fn literal_pattern(id: RID) -> RID {
        RID::new(
            id.segments().iter().map(|segment| {
                format!("{REGEX_PREFIX}(?:{}){REGEX_SUFFIX}", regex::escape(segment))
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use configparser::ini::Ini;

    use super::*;
    use crate::{
        config::filter_config::FilterConfig,
        filter::DtFilter,
        meta::{
            dcl_meta::dcl_type::DclType, ddl_meta::ddl_type::DdlType, row_type::RowType,
            struct_meta::structure::structure_type::StructureType,
        },
        rid,
    };

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    fn load_filter(ini: &Ini, db_type: DbType) -> Result<DtFilter> {
        DtFilter::from_config(&FilterConfig::from_ini(ini)?, db_type)
    }

    fn filter(text: &str) -> DtFilter {
        load_filter(&ini(text), DbType::Mysql).unwrap()
    }

    #[test]
    fn direct_legacy_filter_preserves_literal_delimiters_and_wildcard_rules() {
        for name in ["a.b,c:订单", "r#.*#", r"a\#b", "`quote`\"[]", "*"] {
            let condition = "name = \"a,b\" AND id > 1";
            let input = FilterConfig::Legacy(LegacyFilterConfig {
                do_schemas: "*".into(),
                ignore_tbs: "excluded.*".into(),
                ignore_cols: format!(
                    "json:{}",
                    serde_json::json!([
                        {"db":name, "tb":name, "ignore_cols":[name]}
                    ])
                ),
                where_conditions: format!(
                    "json:{}",
                    serde_json::json!([
                        {"db":name, "tb":name, "condition":condition}
                    ])
                ),
                ..Default::default()
            });
            let FilterConfig::Legacy(options) = &input else {
                unreachable!()
            };
            let mut f = DtFilter::from_legacy_config(options, DbType::Mysql).unwrap();
            let table = RID::new([name, name]);
            assert_eq!(f.where_condition(&table).unwrap(), Some(condition));
            f.set_where_condition(table.clone(), "discarded".into())
                .unwrap();
            assert_eq!(f.where_condition(&table).unwrap(), Some(condition));
            assert!(f.filter(&table.child(name)).unwrap());
            assert!(!f.filter(&table.child("other")).unwrap());
            assert_eq!(f.where_condition(rid!("other", "other")).unwrap(), None);
            assert!(f.filter(rid!("excluded")).unwrap());
        }
    }

    #[test]
    fn legacy_where_and_categories_keep_existing_defaults() {
        assert!(load_filter(&ini("[filter]\nwhere_conditions=json:[]"), DbType::Kafka).is_ok());
        let f = filter(
            r#"[filter]
    do_events=
    do_structures=
    do_dcls=grant
    do_ddls=create_table
    ignore_cmds=FLUSHDB
    where_conditions=json:[{"db":"db","tb":"t","condition":"id > 1"}]
    do_dbs=db
    "#,
        );
        assert!(!f.filter_event(rid!("db", "t"), &RowType::Delete).unwrap());
        assert!(!f.filter_structure(&StructureType::Index));
        assert!(!f.filter_dcl(&DclType::Grant));
        assert!(f.filter_dcl(&DclType::Revoke));
        assert!(!f
            .filter_ddl(rid!("db", "t"), &DdlType::CreateTable)
            .unwrap());
        assert!(f.filter_ddl(rid!("db", "t"), &DdlType::DropTable).unwrap());
        assert!(f.filter_cmd("FLUSHDB"));
        assert!(!f.filter_cmd("GET"));
        assert_eq!(f.where_condition(rid!("db", "t")).unwrap(), Some("id > 1"));
        assert_eq!(f.where_condition(rid!("db")).unwrap(), None);
        assert_eq!(
            f.filter_event(rid!("db"), &RowType::Insert).unwrap(),
            f.filter(rid!("db")).unwrap()
        );
        let defaults = filter("[filter]\ndo_dbs=db");
        assert!(defaults.filter_dcl(&DclType::Grant));
        assert!(defaults
            .filter_ddl(rid!("db", "t"), &DdlType::CreateTable)
            .unwrap());
    }

    #[test]
    fn common_filters_preserve_legacy_selection_and_command_semantics() {
        for db_type in [DbType::Mysql, DbType::Pg, DbType::Mssql, DbType::Redis] {
            for value in [
                "",
                "*",
                "*,",
                "*,insert,create_table,grant,table",
                "insert,create_table,grant,table",
                "ins*,create*,gr*,ta*",
                "r#insert|update#,r#create_table|drop_table#,r#grant|revoke#,r#table|index#",
                "INSERT,CREATE_TABLE,GRANT,TABLE",
            ] {
                let config = FilterConfig::from_ini(&ini(&format!(
                    "[filter]\ndo_dbs=*\ndo_events={value}\ndo_ddls={value}\ndo_dcls={value}\ndo_structures={value}\nignore_cmds=*,SET,r#GET|DEL#,r#GET{{1,2}}#,SET*,GET?,FT.SEARCH"
                )))
                .unwrap();
                let all = value == "*";
                let listed = matches!(
                    value,
                    "*,insert,create_table,grant,table" | "insert,create_table,grant,table"
                );
                let new = DtFilter::from_config(&config, db_type.clone()).unwrap();
                let id = match db_type {
                    DbType::Mssql => rid!("db", "schema", "table"),
                    DbType::Redis => rid!("schema"),
                    _ => rid!("schema", "table"),
                };
                for event in [RowType::Insert, RowType::Update, RowType::Delete] {
                    assert_eq!(
                        new.filter_event(&id, &event).unwrap(),
                        !(all || value.is_empty() || listed && event == RowType::Insert),
                        "{db_type} {value} {event}"
                    );
                }
                for ddl in [DdlType::CreateTable, DdlType::DropTable] {
                    assert_eq!(
                        new.filter_ddl(&id, &ddl).unwrap(),
                        !(all || listed && ddl == DdlType::CreateTable),
                        "{db_type} {value} {ddl}"
                    );
                }
                for dcl in [DclType::Grant, DclType::Revoke] {
                    assert_eq!(
                        new.filter_dcl(&dcl),
                        !(all || listed && dcl == DclType::Grant)
                    );
                }
                for structure in [StructureType::Table, StructureType::Index] {
                    assert_eq!(
                        new.filter_structure(&structure),
                        !(all || value.is_empty() || listed && structure == StructureType::Table)
                    );
                }
                for command in [
                    "*",
                    "SET",
                    "set",
                    "GET",
                    "DEL",
                    "r#GET|DEL#",
                    "r#GET{1,2}#",
                    "SET*",
                    "GET?",
                    "SETEX",
                    "FT.SEARCH",
                    "FT",
                    "SEARCH",
                    "",
                    "line1\nline2",
                ] {
                    assert_eq!(
                        new.filter_cmd(command),
                        matches!(
                            command,
                            "*" | "SET"
                                | "r#GET|DEL#"
                                | "r#GET{1,2}#"
                                | "SET*"
                                | "GET?"
                                | "FT"
                                | "SEARCH"
                        ),
                        "{db_type} {value} {command}"
                    );
                }
            }
            for key in [
                "do_events",
                "do_ddls",
                "do_dcls",
                "do_structures",
                "ignore_cmds",
            ] {
                let config =
                    FilterConfig::from_ini(&ini(&format!("[filter]\n{key}=r#[#"))).unwrap();
                assert!(DtFilter::from_config(&config, db_type.clone()).is_err());
            }
        }
    }

    #[test]
    fn legacy_json_columns_keep_existing_overrides_and_where_keeps_first_condition() {
        let f = filter(
            r#"[filter]
    do_dbs=*
    ignore_cols=json:[{"db":"a.b","tb":"t","ignore_cols":["id"]},{"db":"a.b","tb":"t","ignore_cols":["secret"]}]
    where_conditions=json:[{"db":"a.b","tb":"t","condition":"id > 1"},{"db":"a.b","tb":"t","condition":"id > 2"}]
    "#,
        );
        assert!(f.filter(rid!("a.b", "t", "secret")).unwrap());
        assert!(!f.filter(rid!("a.b", "t", "id")).unwrap());
        assert_eq!(f.where_condition(rid!("a.b", "t")).unwrap(), Some("id > 1"));
        let mut f = f;
        f.set_where_condition(rid!("a.b", "t"), "other".into())
            .unwrap();
        assert_eq!(f.where_condition(rid!("a.b", "t")).unwrap(), Some("id > 1"));
    }

    #[test]
    fn legacy_json_columns_and_commands_do_not_become_patterns() {
        let f = filter(
            r#"[filter]
do_dbs=*
ignore_cols=json:[{"db":"db*","tb":"r#.*#","ignore_cols":["*","r#.*#"]}]
where_conditions=json:[{"db":"db*","tb":"r#.*#","condition":"id > 1"}]
ignore_cmds=*,r#GET|SET#,`GET`,SET*,GET|SET"#,
        );
        for name in ["*", "r#.*#"] {
            assert!(f.filter(RID::new(["db*", "r#.*#", name])).unwrap());
        }
        for id in [rid!("db*", "r#.*#", "id"), rid!("db1", "table", "*")] {
            assert!(!f.filter(&id).unwrap());
        }
        assert_eq!(
            f.where_condition(rid!("db*", "r#.*#")).unwrap(),
            Some("id > 1")
        );
        assert_eq!(f.where_condition(rid!("db1", "table")).unwrap(), None);
        for command in ["*", "r#GET|SET#", "`GET`", "SET*", "GET|SET"] {
            assert!(f.filter_cmd(command), "{command}");
        }
        for command in ["GET", "SET", "SETEX"] {
            assert!(!f.filter_cmd(command), "{command}");
        }
    }

    #[test]
    fn unsupported_legacy_options_are_skipped_before_parsing_values() {
        for (engine, options, selected) in [
            (DbType::Mongo,
             "do_tbs=db.c\nignore_cols=json:invalid\nwhere_conditions=json:invalid",
             Some(rid!("db", "c"))),
            (DbType::Redis,
             "do_dbs=0\ndo_tbs=r#[#\nignore_tbs=invalid\nignore_cols=json:invalid\nwhere_conditions=json:invalid",
             Some(rid!("0"))),
            (DbType::Kafka,
             "do_dbs=r#[#\nignore_dbs=r#[#\ndo_tbs=r#[#\nignore_tbs=r#[#\nignore_cols=json:invalid\nwhere_conditions=json:invalid",
             None),
        ] {
            let f = load_filter(&ini(&format!("[filter]\ndo_events=insert\n{options}")), engine).unwrap();
            if let Some(id) = selected {
                assert!(!f.filter(id).unwrap(), "{options}");
            } else {
                assert!(f.resource_filters().is_empty());
            }
            assert!(f.expression_filters.is_empty());
            assert!(!f.filter_common(CommonFilterKind::Event, "insert"));
            assert!(f.filter_common(CommonFilterKind::Event, "update"));
        }
    }

    #[test]
    fn legacy_scope_matches_existing_filter_for_flattened_names_and_regexes() {
        for db_type in [DbType::Mysql, DbType::Mssql] {
            let include_rules = "db.name,store[0-9]+,r#inventory|market#";
            let new =
                load_filter(&ini(&format!("[filter]\ndo_dbs={include_rules}")), db_type).unwrap();
            for name in ["db", "name", "db.name", "store12", "inventory", "other"] {
                assert_eq!(
                    new.filter(RID::new([name])).unwrap(),
                    matches!(name, "db.name" | "other"),
                    "{name}"
                );
            }
        }
        for rule in [
            "do_tbs=db",
            "ignore_tbs=db.t.extra",
            "do_dbs=r#[#",
            "ignore_tbs=db.r#[#",
        ] {
            assert!(
                load_filter(&ini(&format!("[filter]\n{rule}")), DbType::Mysql).is_err(),
                "{rule}"
            );
        }
    }
}
