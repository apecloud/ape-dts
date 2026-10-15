use std::collections::{BTreeMap, HashMap};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

use super::DtRouter;
use crate::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        router_config::LegacyRouterConfig,
    },
    resource::{
        ResourceConfigParser, ResourceKind, MAPPING_SEPARATOR, RESOURCE_SEPARATOR, RID,
        RULE_SEPARATOR, WILDCARD,
    },
};

const MAPPING_DELIMITERS: &[char] = &[',', ':'];

#[derive(Deserialize)]
struct ColumnRule {
    db: String,
    tb: String,
    col_map: BTreeMap<String, String>,
}

impl DtRouter {
    pub fn from_legacy_config(
        options: &LegacyRouterConfig,
        src: DbType,
        dst: DbType,
    ) -> Result<Self> {
        let to_maps = Self::parse_legacy_topic_maps(&options.topic_map, &src, &dst)?;
        let destination = dst;
        let (src, dst) = (
            &src,
            if to_maps.is_empty() {
                &destination
            } else {
                &src
            },
        );
        let mut rules = Vec::new();
        for (index, &kind) in ResourceKind::levels(src).iter().enumerate() {
            if !Self::supports_route(kind, dst) {
                continue;
            }
            let (key, value, table) =
                if index == 0 && matches!(kind, ResourceKind::Database | ResourceKind::Schema) {
                    ("db_map", &options.schema_map, false)
                } else if matches!(kind, ResourceKind::Table | ResourceKind::Collection) {
                    ("tb_map", &options.tb_map, true)
                } else {
                    continue;
                };
            // Legacy duplicate sources keep the last target.
            let mappings: BTreeMap<_, _> = Self::parse_legacy_routes(value, src, dst, table)
                .with_context(|| format!("[router].{key}"))?
                .into_iter()
                .collect();
            for (from, to) in mappings {
                kind.validate_rid(src, &from)?;
                rules.push((from, to));
            }
        }
        if ResourceKind::Column.depth(src).is_some()
            && ResourceKind::Column.depth(dst).is_some()
            && !options.col_map.trim().is_empty()
        {
            let parent_router = DtRouter::new(src.clone(), dst.clone(), rules.clone())?;
            let mut tables = BTreeMap::new();
            for entry in ResourceConfigParser::legacy_json::<Vec<ColumnRule>>(&options.col_map)
                .context("[router].col_map")?
            {
                tables.insert(
                    ResourceConfigParser::legacy_json_table(&entry.db, &entry.tb, src)?,
                    entry.col_map,
                );
            }
            for (source_table, columns) in tables {
                let target_table = parent_router.forward::<RID>(&source_table)?;
                for (from, to) in columns {
                    let from = source_table.child(from);
                    ResourceKind::Column.validate_rid(src, &from)?;
                    rules.push((from, target_table.child(to)));
                }
            }
        }
        Self::build_router(src.clone(), destination, rules, to_maps)
    }

    /// Legacy default/namespace selectors cover namespace events and objects, excluding columns.
    fn parse_legacy_topic_maps(
        value: &str,
        src: &DbType,
        dst: &DbType,
    ) -> Result<HashMap<ResourceKind, HashMap<RID, RID>>> {
        if ResourceKind::Topic.depth(dst).is_none() || value.trim().is_empty() {
            return Ok(HashMap::new());
        }
        let mut rules = BTreeMap::new();
        for (mut from, to) in ResourceConfigParser::pairs(value, src, dst)? {
            let default = from.iter().all(|segment| segment == WILDCARD);
            // Legacy MSSQL selectors lack the schema; only the default applies.
            if matches!(src, DbType::Mssql) && !default {
                continue;
            }
            let names = to
                .into_iter()
                .map(
                    |token| Ok(ResourceConfigParser::quoted_literal(&token, src)?.unwrap_or(token)),
                )
                .collect::<Result<Vec<_>>>()?;
            let topic = RID::new([names.join(RESOURCE_SEPARATOR)]);
            let descendants = !default && from.last().is_some_and(|segment| segment == WILDCARD);
            if descendants {
                from.pop();
            }
            let source = if default {
                None
            } else {
                Some(ResourceConfigParser::exact_path(from, src)?)
            };
            rules.insert((source, descendants), topic);
        }
        // Apply defaults, namespace scopes, then exact rules; exact rules don't affect descendants.
        let mut rules: Vec<_> = rules.into_iter().collect();
        rules.sort_by_key(|((source, descendants), _)| {
            (source.as_ref().map_or(0, RID::len), !descendants)
        });
        let levels = ResourceKind::levels(src);
        let scope_depth = levels
            .iter()
            .take_while(|&&kind| kind != ResourceKind::Column)
            .count();
        let mut maps = HashMap::<ResourceKind, HashMap<RID, RID>>::new();
        for ((source, descendants), target) in rules {
            ResourceKind::Topic.validate_rid(dst, &target)?;
            let (prefix, min_depth, max_depth) = if let Some(source) = source {
                let kind = ResourceKind::from_rid(src, &source)
                    .with_context(|| format!("invalid {src} resource: {source}"))?;
                ensure!(
                    kind != ResourceKind::Column,
                    "topic dispatch does not accept columns"
                );
                if descendants {
                    ensure!(
                        source.len() < scope_depth,
                        "topic prefix must identify a namespace"
                    );
                }
                let min = source.len();
                (
                    source.segments().to_vec(),
                    min,
                    if descendants { scope_depth } else { min },
                )
            } else {
                (Vec::new(), 1, scope_depth)
            };
            for (index, &kind) in levels
                .iter()
                .enumerate()
                .take(max_depth)
                .skip(min_depth - 1)
            {
                let mut path = prefix.clone();
                path.resize(index + 1, WILDCARD.into());
                let pattern = RID::new(path);
                let map = maps.entry(kind).or_default();
                map.insert(pattern, target.clone());
            }
        }
        Ok(maps)
    }

    fn parse_legacy_routes(
        value: &str,
        src: &DbType,
        dst: &DbType,
        table: bool,
    ) -> Result<Vec<(RID, RID)>> {
        if !table && matches!(src, DbType::Mssql) && matches!(dst, DbType::Mssql) {
            // Legacy MSSQL database names may contain unquoted dots.
            let tokens = ConfigTokenParser::parse_config_with_delimiters(
                value.trim(),
                src,
                MAPPING_DELIMITERS,
                None,
            )?;
            if tokens.is_empty() {
                return Ok(Vec::new());
            }
            return tokens
                .split(|token| token == RULE_SEPARATOR)
                .map(|entry| {
                    ensure!(
                        entry.len() == 3 && entry[1] == MAPPING_SEPARATOR,
                        "expected source:target database mapping"
                    );
                    Ok((
                        ResourceConfigParser::legacy_exact_path(vec![entry[0].clone()], src)?,
                        ResourceConfigParser::legacy_exact_path(vec![entry[2].clone()], dst)?,
                    ))
                })
                .collect();
        }
        ResourceConfigParser::pairs(value, src, dst)?
            .into_iter()
            .map(|(from, to)| {
                let (from, to) = if table {
                    (
                        ResourceConfigParser::legacy_table(from, src)?,
                        ResourceConfigParser::legacy_table(to, dst)?,
                    )
                } else {
                    (from, to)
                };
                Ok((
                    ResourceConfigParser::legacy_exact_path(from, src)?,
                    ResourceConfigParser::legacy_exact_path(to, dst)?,
                ))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use configparser::ini::Ini;

    use super::*;
    use crate::{config::router_config::RouterConfig, rid};

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    fn load_router(ini: &Ini, src: DbType, dst: DbType) -> Result<DtRouter> {
        DtRouter::from_config(&RouterConfig::from_ini(ini)?, src, dst)
    }

    #[test]
    fn clickhouse_table_routes_override_database_routes_in_both_directions() {
        let config = ini("[router]\ndb_map=test_db_2:dst_test_db_2\ntb_map=test_db_2.router_test_2:dst_test_db_2.dst_router_test_2");
        for src in [DbType::Mysql, DbType::Pg] {
            let router = load_router(&config, src, DbType::ClickHouse).unwrap();
            for (source, target) in [
                ("router_test_1", "router_test_1"),
                ("router_test_2", "dst_router_test_2"),
            ] {
                assert_eq!(
                    router
                        .forward::<(String, String)>(rid!("test_db_2", source))
                        .unwrap(),
                    ("dst_test_db_2".into(), target.into())
                );
                assert_eq!(
                    router
                        .reverse::<(String, String)>(rid!("dst_test_db_2", target))
                        .unwrap(),
                    ("test_db_2".into(), source.into())
                );
            }
        }
    }

    #[test]
    fn legacy_topic_maps_preserve_priority_duplicates_and_dotted_names() {
        let router = load_router(&ini("[router]\ntopic_map=*.*:old,*.*:default.topic,db.*:old,db.*:shared.topic,db.orders:orders.topic,db.events:shared.topic"), DbType::Mysql, DbType::Kafka).unwrap();
        for (source, target) in [
            (rid!("db", "orders"), rid!("orders.topic")),
            (rid!("db", "events"), rid!("shared.topic")),
            (rid!("db", "other"), rid!("shared.topic")),
            (rid!("db"), rid!("shared.topic")),
            (rid!("other", "t"), rid!("default.topic")),
        ] {
            let topic: String = router.to(source).unwrap();
            assert_eq!(topic, String::from(target));
        }
        assert!(router.to::<RID>(rid!("db", "orders", "id")).is_err());
        let quoted = load_router(
            &ini("[router]\ntopic_map=*.*:`a.b`"),
            DbType::Mysql,
            DbType::Kafka,
        )
        .unwrap();
        assert_eq!(quoted.to::<RID>(rid!("db", "t")).unwrap(), rid!("a.b"));
        for (engine, source) in [
            (DbType::Pg, rid!("public", "t")),
            (DbType::Mongo, rid!("db", "c")),
            (DbType::Redis, rid!("0")),
            (DbType::Kafka, rid!("orders.eu")),
            (DbType::Mssql, rid!("db", "sales", "orders")),
        ] {
            let router = load_router(
                &ini("[router]\ntopic_map=*.*:events"),
                engine,
                DbType::Kafka,
            )
            .unwrap();
            assert_eq!(router.to::<RID>(&source).unwrap(), rid!("events"));
        }
        for (engine, mapping) in [
            (DbType::Mysql, "*.t:topic"),
            (
                DbType::Mysql,
                r#"json:[{"from":["db","orders"],"to":["events"]}]"#,
            ),
        ] {
            assert!(load_router(
                &ini(&format!("[router]\ntopic_map={mapping}")),
                engine,
                DbType::Kafka
            )
            .is_err());
        }
    }

    #[test]
    fn legacy_config_builds_payload_routes_and_destination_dispatch() {
        let input = RouterConfig::from_ini(&ini(
            "[router]\ntb_map=db.t:archive.old,db.t:archive.new\ntopic_map=*.*:old,*.*:events",
        ))
        .unwrap();
        let RouterConfig::Legacy(mut options) = input else {
            unreachable!()
        };
        let router = DtRouter::from_legacy_config(&options, DbType::Mysql, DbType::Kafka).unwrap();
        let routed = router.forward::<RID>(rid!("db", "t")).unwrap();
        assert_eq!(routed, rid!("archive", "new"));
        assert_eq!(router.to::<RID>(&routed).unwrap(), rid!("events"));
        for mapping in [
            "invalid",
            "db.t:archive",
            r#"json:[{"from":["db","t"],"to":["archive","t"]}]"#,
        ] {
            options.tb_map = mapping.into();
            assert!(DtRouter::from_legacy_config(&options, DbType::Mysql, DbType::Kafka).is_err());
        }
        options.tb_map.clear();
        options.topic_map = "invalid".into();
        assert!(DtRouter::from_legacy_config(&options, DbType::Mysql, DbType::Kafka).is_err());
        assert!(load_router(
            &ini("[router]\ndb_map=db:schema"),
            DbType::Mssql,
            DbType::Pg,
        )
        .is_err());
    }

    #[test]
    fn unsupported_legacy_options_do_not_change_payload_or_dispatch_routes() {
        for (src, dst, options, source, target, destination) in [
            (
                DbType::Redis,
                DbType::Redis,
                "db_map=0:1\ntb_map=invalid\ncol_map=json:invalid\ntopic_map=invalid",
                rid!("0"),
                rid!("1"),
                rid!("1"),
            ),
            (
                DbType::Mongo,
                DbType::Mongo,
                "tb_map=db.c:archive.c\ncol_map=json:invalid\ntopic_map=invalid",
                rid!("db", "c"),
                rid!("archive", "c"),
                rid!("archive", "c"),
            ),
            (
                DbType::Mysql,
                DbType::Pg,
                "db_map=db:archive\ntopic_map=invalid",
                rid!("db", "t"),
                rid!("archive", "t"),
                rid!("archive", "t"),
            ),
            (
                DbType::Mysql,
                DbType::Kafka,
                "db_map=invalid\ntb_map=invalid\ncol_map=json:invalid",
                rid!("db", "t"),
                rid!("db", "t"),
                rid!("db", "t"),
            ),
            (
                DbType::Mysql,
                DbType::Mongo,
                "tb_map=invalid\ncol_map=json:invalid",
                rid!("db", "t"),
                rid!("db", "t"),
                rid!("db", "t"),
            ),
            (
                DbType::Kafka,
                DbType::Kafka,
                "db_map=invalid\ntb_map=invalid\ncol_map=json:invalid\ntopic_map=*.*:events",
                rid!("orders"),
                rid!("orders"),
                rid!("events"),
            ),
            (
                DbType::Mssql,
                DbType::Kafka,
                "topic_map=inventory.orders:ignored,*.*:events",
                rid!("db", "sales", "orders"),
                rid!("db", "sales", "orders"),
                rid!("events"),
            ),
            (
                DbType::Mssql,
                DbType::Kafka,
                "topic_map=inventory.orders:ignored",
                rid!("db", "sales", "orders"),
                rid!("db", "sales", "orders"),
                rid!("db", "sales", "orders"),
            ),
        ] {
            let router = load_router(&ini(&format!("[router]\n{options}")), src, dst).unwrap();
            assert_eq!(router.forward::<RID>(&source).unwrap(), target, "{options}");
            assert_eq!(router.reverse::<RID>(&target).unwrap(), source, "{options}");
            assert_eq!(router.to::<RID>(&target).unwrap(), destination, "{options}");
        }
    }

    #[test]
    fn legacy_column_parents_dotted_databases_and_duplicate_maps_are_preserved() {
        let r = load_router(
            &ini(r#"[router]
    db_map=market:archive
    tb_map=market.order:archive.history.order_2026
    col_map=json:[{"db":"market","tb":"order","col_map":{"value":"old_value"}},{"db":"market","tb":"order","col_map":{"id":"order_id"}}]
    "#),
            DbType::Mssql,
            DbType::Mssql,
        )
        .unwrap();
        assert_eq!(
            r.forward::<RID>(rid!("market", "dbo", "order", "id"))
                .unwrap(),
            rid!("archive", "history", "order_2026", "order_id")
        );
        assert_eq!(
            r.forward::<RID>(rid!("market", "dbo", "order", "value"))
                .unwrap(),
            rid!("archive", "history", "order_2026", "value")
        );
        let dotted = load_router(
            &ini("[router]\ndb_map=source.db:ignored.db,source.db:target.db"),
            DbType::Mssql,
            DbType::Mssql,
        )
        .unwrap();
        assert_eq!(
            dotted
                .forward::<RID>(rid!("source.db", "dbo", "t"))
                .unwrap(),
            rid!("target.db", "dbo", "t")
        );
    }
}
