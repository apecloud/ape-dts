use std::collections::{BTreeMap, HashMap};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

use super::{
    config::{build_router, RouteRule},
    resource_router::WILDCARD,
    DtRouter,
};
use crate::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        router_config::LegacyRouterConfig,
    },
    resource::{
        config_parser as config, legacy, resource_depth, resource_kind, resource_levels,
        validate_rid, ResourceKind, RID,
    },
};

impl DtRouter {
    pub fn from_legacy_config(
        options: &LegacyRouterConfig,
        src: DbType,
        dst: DbType,
    ) -> Result<Self> {
        let to_maps = topic_maps(&options.topic_map, &src, &dst)?;
        let destination = dst;
        let (src, dst) = (
            &src,
            if to_maps.is_empty() {
                &destination
            } else {
                &src
            },
        );
        if matches!(src, DbType::Kafka) {
            ensure!(
                options.schema_map.trim().is_empty(),
                "Kafka does not support [router].db_map; use [rules.topic].route"
            );
        }
        let mut rules = Vec::new();
        for (index, &kind) in resource_levels(src).iter().enumerate() {
            let (key, value, table) = if index == 0 {
                ("db_map", &options.schema_map, false)
            } else if matches!(kind, ResourceKind::Table | ResourceKind::Collection) {
                ("tb_map", &options.tb_map, true)
            } else {
                continue;
            };
            // Legacy duplicate sources keep the last target.
            let mappings: BTreeMap<_, _> = routes(value, src, dst, table)
                .with_context(|| format!("[router].{key}"))?
                .into_iter()
                .collect();
            for (from, to) in mappings {
                validate_rid(src, kind, &from)?;
                rules.push((from, to));
            }
        }
        if matches!(src, DbType::Redis | DbType::Kafka) {
            ensure!(
                options.tb_map.trim().is_empty(),
                "{src} does not support [router].tb_map"
            );
        }
        if !options.col_map.trim().is_empty() {
            ensure!(
                resource_depth(src, ResourceKind::Column).is_some(),
                "{src} does not support column routing"
            );
            let parent_router = DtRouter::new(src.clone(), dst.clone(), rules.clone())?;
            let mut tables = BTreeMap::new();
            for entry in
                config::json::<Vec<ColumnRule>>(&options.col_map).context("[router].col_map")?
            {
                tables.insert(
                    legacy::json_table(&entry.db, &entry.tb, src)?,
                    entry.col_map,
                );
            }
            for (source_table, columns) in tables {
                let target_table = parent_router.forward::<RID>(&source_table)?;
                for (from, to) in columns {
                    let from = source_table.child(from);
                    validate_rid(src, ResourceKind::Column, &from)?;
                    rules.push((from, target_table.child(to)));
                }
            }
        }
        build_router(src.clone(), destination, rules, to_maps)
    }
}

fn routes(value: &str, src: &DbType, dst: &DbType, table: bool) -> Result<Vec<(RID, RID)>> {
    if value.trim().starts_with("json:") {
        return Ok(config::json::<Vec<RouteRule>>(value)?
            .into_iter()
            .map(|r| (r.from, r.to))
            .collect());
    }
    if !table && matches!(src, DbType::Mssql) && matches!(dst, DbType::Mssql) {
        // Legacy MSSQL database names may contain unquoted dots.
        let tokens =
            ConfigTokenParser::parse_config_with_delimiters(value.trim(), src, &[',', ':'], None)?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        return tokens
            .split(|token| token == ",")
            .map(|entry| {
                ensure!(
                    entry.len() == 3 && entry[1] == ":",
                    "expected source:target database mapping"
                );
                Ok((
                    legacy::exact_path(vec![entry[0].clone()], src)?,
                    legacy::exact_path(vec![entry[2].clone()], dst)?,
                ))
            })
            .collect();
    }
    config::pairs(value, src, dst)?
        .into_iter()
        .map(|(from, to)| {
            let (from, to) = if table {
                (legacy::table(from, src)?, legacy::table(to, dst)?)
            } else {
                (from, to)
            };
            Ok((legacy::exact_path(from, src)?, legacy::exact_path(to, dst)?))
        })
        .collect()
}

#[derive(Deserialize)]
struct ColumnRule {
    db: String,
    tb: String,
    col_map: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TopicRule {
    from: Option<RID>,
    #[serde(default)]
    descendants: bool,
    to: RID,
}

/// Legacy default/namespace selectors cover namespace events and objects, excluding columns.
fn topic_maps(
    value: &str,
    src: &DbType,
    dst: &DbType,
) -> Result<HashMap<ResourceKind, HashMap<RID, RID>>> {
    if value.trim().is_empty() {
        return Ok(HashMap::new());
    }
    ensure!(
        resource_depth(dst, ResourceKind::Topic).is_some(),
        "legacy topic_map requires a destination with topic resources: {dst}"
    );
    let json = value.trim().starts_with("json:");
    let mut rules = if json {
        let mut unique = BTreeMap::new();
        for rule in config::json::<Vec<TopicRule>>(value).context("[router].topic_map")? {
            let selector = (rule.from, rule.descendants);
            if let Some(previous) = unique.insert(selector.clone(), rule.to.clone()) {
                ensure!(
                    previous == rule.to,
                    "conflicting topic routes for {selector:?}"
                );
            }
        }
        unique
            .into_iter()
            .map(|((from, descendants), to)| TopicRule {
                from,
                descendants,
                to,
            })
            .collect::<Vec<_>>()
    } else {
        let mut rules = BTreeMap::new();
        for (mut from, to) in config::pairs(value, src, dst)? {
            let names = to
                .into_iter()
                .map(|token| Ok(config::quoted_literal(&token, src)?.unwrap_or(token)))
                .collect::<Result<Vec<_>>>()?;
            let topic = RID::new([names.join(".")]);
            let default = from.iter().all(|s| s == WILDCARD);
            ensure!(!matches!(src, DbType::Mssql) || default, "legacy MSSQL topic_map lacks database identity; use JSON topic_map with a complete source path");
            let descendants = !default && from.last().is_some_and(|s| s == WILDCARD);
            if descendants {
                from.pop();
            }
            let source = if default {
                None
            } else {
                Some(config::exact_path(from, src)?)
            };
            rules.insert((source, descendants), topic);
        }
        rules
            .into_iter()
            .map(|((from, descendants), to)| TopicRule {
                from,
                descendants,
                to,
            })
            .collect()
    };
    // Apply defaults, namespace scopes, then exact rules; exact rules don't affect descendants.
    rules.sort_by_key(|rule| (rule.from.as_ref().map_or(0, RID::len), !rule.descendants));
    let levels = resource_levels(src);
    let scope_depth = levels
        .iter()
        .take_while(|&&kind| kind != ResourceKind::Column)
        .count();
    let mut maps = HashMap::<ResourceKind, HashMap<RID, RID>>::new();
    for rule in rules {
        validate_rid(dst, ResourceKind::Topic, &rule.to)?;
        let (prefix, min_depth, max_depth) = if let Some(from) = rule.from {
            let kind = resource_kind(src, &from)
                .with_context(|| format!("invalid {src} resource: {from}"))?;
            ensure!(
                kind != ResourceKind::Column,
                "topic dispatch does not accept columns"
            );
            if rule.descendants {
                ensure!(
                    from.len() < scope_depth,
                    "topic prefix must identify a namespace"
                );
            }
            let min = from.len();
            (
                from.segments().to_vec(),
                min,
                if rule.descendants { scope_depth } else { min },
            )
        } else {
            ensure!(
                !rule.descendants,
                "default topic cannot specify descendants"
            );
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
            map.insert(pattern, rule.to.clone());
        }
    }
    Ok(maps)
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
            (DbType::Mssql, "inventory.order:topic"),
            (DbType::Mysql, "json:[{\"from\":null,\"descendants\":true,\"to\":[\"topic\"]}]"),
            (DbType::Mysql, "json:[{\"from\":[\"db\",\"t\"],\"to\":[\"one\"]},{\"from\":[\"db\",\"t\"],\"to\":[\"two\"]}]"),
        ] {
            assert!(load_router(&ini(&format!("[router]\ntopic_map={mapping}")), engine, DbType::Kafka).is_err());
        }
    }

    #[test]
    fn legacy_json_topic_scopes_keep_namespace_events_separate_from_descendants() {
        let router = load_router(&ini(r#"[router]
topic_map=json:[{"from":null,"to":["default.topic"]},{"from":["db"],"descendants":true,"to":["database.topic"]},{"from":["db","sales"],"to":["schema.ddl.topic"]},{"from":["db","sales"],"descendants":true,"to":["sales.topic"]},{"from":["db","sales","order"],"to":["order.topic"]}]
"#), DbType::Mssql, DbType::Kafka).unwrap();
        for (source, target) in [
            (rid!("db", "sales"), rid!("schema.ddl.topic")),
            (rid!("db", "sales", "order"), rid!("order.topic")),
            (rid!("db", "sales", "other"), rid!("sales.topic")),
            (rid!("db", "audit", "other"), rid!("database.topic")),
            (rid!("other", "dbo", "t"), rid!("default.topic")),
        ] {
            assert_eq!(router.to::<RID>(&source).unwrap(), target);
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
            r#"json:[{"from":["db","t"],"to":["a","b","c","d"]}]"#,
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
