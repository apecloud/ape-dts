use std::collections::HashMap;

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

use super::{resource_router::WILDCARD, DtRouter};
use crate::{
    config::{
        config_enums::DbType,
        filter_config::RULES,
        router_config::{RouterConfig, TO_PREFIX},
    },
    filter::CommonFilterKind,
    resource::{config_parser as config, resource_depth, validate_rid, ResourceKind, RID},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRule {
    pub from: RID,
    pub to: RID,
}

fn routes(value: &str, src: &DbType, dst: &DbType) -> Result<Vec<(RID, RID)>> {
    if value.trim().starts_with("json:") {
        return Ok(config::json::<Vec<RouteRule>>(value)?
            .into_iter()
            .map(|r| (r.from, r.to))
            .collect());
    }
    config::pairs(value, src, dst)?
        .into_iter()
        .map(|(from, to)| Ok((config::exact_path(from, src)?, config::exact_path(to, dst)?)))
        .collect()
}

fn to_routes(
    value: &str,
    src: &DbType,
    dst: &DbType,
    dst_kind: ResourceKind,
) -> Result<Vec<(RID, RID)>> {
    if value.trim().starts_with("json:") {
        return Ok(config::json::<Vec<RouteRule>>(value)?
            .into_iter()
            .map(|r| (r.from, r.to))
            .collect());
    }
    config::pairs(value, src, dst)?
        .into_iter()
        .map(|(from, to)| {
            let source = RID::new(
                from.into_iter()
                    .map(|segment| {
                        if segment == WILDCARD {
                            return Ok(segment);
                        }
                        let id = config::exact_path(vec![segment], src)?;
                        Ok(id.segments().join(""))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            let source = if matches!(src, DbType::Kafka) {
                RID::new([source.segments().join(".")])
            } else {
                source
            };
            let target = config::exact_path(to, dst)?;
            // Dots belong to the topic name.
            let target = if dst_kind == ResourceKind::Topic {
                RID::new([target.segments().join(".")])
            } else {
                target
            };
            Ok((source, target))
        })
        .collect()
}

impl DtRouter {
    pub fn from_config(config: &RouterConfig, src: DbType, dst: DbType) -> Result<Self> {
        let sections = match config {
            RouterConfig::Legacy(options) => return Self::from_legacy_config(options, src, dst),
            RouterConfig::Basic(sections) => sections,
        };
        let mut bases = Vec::new();
        for (name, base) in sections {
            if name.parse::<CommonFilterKind>().is_ok() {
                ensure!(
                    base.route.is_none() && base.to.is_empty(),
                    "[{RULES}.{name}] does not support routing"
                );
                continue;
            }
            let empty =
                base.route.as_deref().unwrap_or_default().trim().is_empty() && base.to.is_empty();
            if let Some(kind) = config::section_kind(name, &src, empty)? {
                bases.push((kind, base));
            }
        }
        let mut to_maps = HashMap::new();
        for &(kind, base) in &bases {
            for (key, value) in &base.to {
                let name = key
                    .strip_prefix(TO_PREFIX)
                    .with_context(|| format!("invalid dispatch field {key}"))?;
                let dst_kind = name
                    .parse::<ResourceKind>()
                    .with_context(|| format!("unknown destination resource kind in {key}"))?;
                ensure!(
                    resource_depth(&dst, dst_kind).is_some(),
                    "{dst} does not support [{key}]"
                );
                let map = to_maps.entry(kind).or_insert_with(HashMap::new);
                for (from, to) in to_routes(value, &src, &dst, dst_kind)
                    .with_context(|| format!("{kind:?}.{key}"))?
                {
                    validate_rid(&dst, dst_kind, &to)?;
                    if let Some(previous) = map.insert(from.clone(), to.clone()) {
                        ensure!(
                            previous == to,
                            "conflicting destination routes for {from}: {previous} and {to}"
                        );
                    }
                }
            }
        }
        let route_dst = if to_maps.is_empty() { &dst } else { &src };
        let mut rules = Vec::new();
        for (kind, base) in bases {
            let mappings = routes(base.route.as_deref().unwrap_or_default(), &src, route_dst)
                .with_context(|| format!("{kind:?}.route"))?;
            for (from, to) in mappings {
                validate_rid(&src, kind, &from)?;
                rules.push((from, to));
            }
        }
        Self::new_with_to_maps(src, dst, rules, to_maps)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use configparser::ini::Ini;
    use strum::IntoEnumIterator;

    use super::*;
    use crate::{config::router_config::RouterSectionConfig, rid};

    fn load_router(ini: &Ini, src: DbType, dst: DbType) -> Result<DtRouter> {
        DtRouter::from_config(&RouterConfig::from_ini(ini)?, src, dst)
    }

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    #[test]
    fn destination_dispatch_reverses_names_and_keeps_payload_engine_context() {
        let config = RouterConfig::from_ini(&ini("[router]\ntopic_map=invalid\n[rules.db]\nroute=db:archive\n[rules.table]\nroute=db.orders:cold.history\nto_topic=db.orders:orders.topic,db.*:shared.topic,*.*:default.topic\n[rules.column]\nroute=db.orders.id:cold.history.new_id")).unwrap();
        let router = DtRouter::from_config(&config, DbType::Mysql, DbType::Kafka).unwrap();
        assert_eq!(router.route_db_type(), &DbType::Mysql);
        assert_eq!(router.dst_db_type(), &DbType::Kafka);
        for (source, payload, destination) in [
            (
                rid!("db", "orders"),
                rid!("cold", "history"),
                rid!("orders.topic"),
            ),
            (
                rid!("db", "events"),
                rid!("archive", "events"),
                rid!("shared.topic"),
            ),
            (
                rid!("db", "logs"),
                rid!("archive", "logs"),
                rid!("shared.topic"),
            ),
            (
                rid!("other", "t"),
                rid!("other", "t"),
                rid!("default.topic"),
            ),
        ] {
            assert_eq!(router.route(&source).unwrap(), payload);
            assert_eq!(router.to(&payload).unwrap(), destination);
        }
        assert_eq!(
            router.route(&rid!("db", "orders", "id")).unwrap(),
            rid!("cold", "history", "new_id")
        );
        assert!(router.to(&rid!("cold", "history", "new_id")).is_err());
    }

    #[test]
    fn dispatch_targets_are_generic_destination_resources() {
        for (src, dst, text, source, routed, target) in [
            (
                DbType::Mysql,
                DbType::Pg,
                "[rules.table]\nroute=db.t:archive.t\nto_table=db.*:public.queue",
                rid!("db", "t"),
                rid!("archive", "t"),
                rid!("public", "queue"),
            ),
            (
                DbType::Mssql,
                DbType::Kafka,
                "[rules.table]\nroute=db.sales.t:archive.history.t\nto_topic=db.sales.*:events",
                rid!("db", "sales", "t"),
                rid!("archive", "history", "t"),
                rid!("events"),
            ),
            (
                DbType::Mongo,
                DbType::Kafka,
                "[rules.collection]\nto_topic=db.*:events",
                rid!("db", "c"),
                rid!("db", "c"),
                rid!("events"),
            ),
            (
                DbType::Redis,
                DbType::Kafka,
                "[rules.db]\nto_topic=*:events",
                rid!("0"),
                rid!("0"),
                rid!("events"),
            ),
            (
                DbType::Kafka,
                DbType::Redis,
                "[rules.topic]\nto_db=orders.eu:0",
                rid!("orders.eu"),
                rid!("orders.eu"),
                rid!("0"),
            ),
        ] {
            let router = load_router(&ini(text), src.clone(), dst).unwrap();
            assert_eq!(router.route_db_type(), &src);
            assert_eq!(router.route(&source).unwrap(), routed);
            assert_eq!(router.to(&routed).unwrap(), target);
        }
        let router = load_router(
            &ini("[rules.table]\nroute=db.t:archive.t\nto_topic="),
            DbType::Mysql,
            DbType::Kafka,
        )
        .unwrap();
        assert_eq!(router.route_db_type(), &DbType::Mysql);
        assert!(router.to(&rid!("archive", "t")).is_err());
    }

    #[test]
    fn dispatch_rejects_invalid_kinds_depths_patterns_and_conflicts() {
        for (dst, text) in [
            (DbType::Pg, "[rules.table]\nto_topic=db.t:events"),
            (DbType::Kafka, "[rules.table]\nto_future=db.t:events"),
            (DbType::Kafka, "[rules.table]\nto_=db.t:events"),
            (DbType::Kafka, "[rules.schema]\nto_topic="),
            (DbType::Kafka, "[rules.event]\nto_topic="),
            (DbType::Kafka, "[rules.table]\nto_topic=db:events"),
            (DbType::Kafka, "[rules.table]\nto_topic=*.t:events"),
            (DbType::Kafka, "[rules.table]\nto_topic=db.order*:events"),
            (DbType::Kafka, "[rules.table]\nto_topic=db.t:one,db.t:two"),
            (DbType::Pg, "[rules.table]\nto_table=db.t:queue"),
        ] {
            assert!(
                load_router(&ini(text), DbType::Mysql, dst).is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn from_config_validates_resource_maps_and_compiles_columns_after_parents() {
        let mut bases = BTreeMap::from([
            (
                "column".to_owned(),
                RouterSectionConfig {
                    route: Some("db.t.id:archive.orders.order_id".into()),
                    ..Default::default()
                },
            ),
            (
                "table".to_owned(),
                RouterSectionConfig {
                    route: Some("db.t:archive.orders".into()),
                    ..Default::default()
                },
            ),
        ]);
        let router = DtRouter::from_config(
            &RouterConfig::Basic(bases.clone()),
            DbType::Mysql,
            DbType::Pg,
        )
        .unwrap();
        assert_eq!(
            router.route(&rid!("db", "t", "id")).unwrap(),
            rid!("archive", "orders", "order_id")
        );
        assert_eq!(
            router.route(&rid!("db", "t", "other")).unwrap(),
            rid!("archive", "orders", "other")
        );
        bases.insert(
            "schema".to_owned(),
            RouterSectionConfig {
                route: Some("db.schema:archive".into()),
                ..Default::default()
            },
        );
        assert!(DtRouter::from_config(
            &RouterConfig::Basic(bases.clone()),
            DbType::Mysql,
            DbType::Pg
        )
        .is_err());
        let mut sections = BTreeMap::from([("table".to_owned(), RouterSectionConfig::default())]);
        for value in [None, Some(String::new())] {
            sections.get_mut("table").unwrap().route = value;
            let router = DtRouter::from_config(
                &RouterConfig::Basic(sections.clone()),
                DbType::Mysql,
                DbType::Pg,
            )
            .unwrap();
            assert_eq!(router.route(&rid!("db", "t")).unwrap(), rid!("db", "t"));
        }
        sections.clear();
        let router =
            DtRouter::from_config(&RouterConfig::Basic(sections), DbType::Mysql, DbType::Pg)
                .unwrap();
        assert_eq!(router.route(&rid!("db", "t")).unwrap(), rid!("db", "t"));
    }

    #[test]
    fn resource_sections_ignore_all_legacy_mappings() {
        for legacy in [
            r#"db_map=db:legacy
tb_map=db.t:legacy.old
col_map=json:[{"db":"db","tb":"t","col_map":{"id":"old_id","value":"old_value"}}]"#,
            "db_map=invalid\ntb_map=invalid\ncol_map=json:invalid",
        ] {
            let config = RouterConfig::from_ini(&ini(&format!("[router]\n{legacy}\ntopic_map=*.*:events\n[rules.table]\nroute=db.t:archive.orders\n[rules.column]\nroute=db.t.id:archive.orders.new_id"))).unwrap();
            let router = DtRouter::from_config(&config, DbType::Mysql, DbType::Pg).unwrap();
            assert_eq!(
                router.route(&rid!("db", "t", "id")).unwrap(),
                rid!("archive", "orders", "new_id")
            );
            assert_eq!(
                router.route(&rid!("db", "t", "value")).unwrap(),
                rid!("archive", "orders", "value")
            );
            assert_eq!(
                router.route(&rid!("db", "other")).unwrap(),
                rid!("db", "other")
            );
            assert_eq!(router.route_db_type(), &DbType::Pg);
            assert_eq!(
                router.to(&rid!("archive", "orders")).unwrap(),
                rid!("archive", "orders")
            );
        }
        for section in [
            "[rules.table]",
            "[rules.table]\ndo=db.t",
            "[rules.db]\nroute=",
        ] {
            let config = RouterConfig::from_ini(&ini(&format!(
                "[router]\ntb_map=db.t:archive.orders\n{section}"
            )))
            .unwrap();
            for engine in [DbType::Mysql, DbType::Pg] {
                let router = DtRouter::from_config(&config, engine.clone(), engine).unwrap();
                assert_eq!(router.route(&rid!("db", "t")).unwrap(), rid!("db", "t"));
            }
        }
    }

    #[test]
    fn common_filter_sections_select_basic_and_reject_router_fields() {
        let config = RouterConfig::from_ini(&ini("[router]\ntb_map=db.t:archive.orders\ntopic_map=*.*:events\n[rules.event]\ndo=insert\nignore=delete\n[rules.cmd]\nignore=SET")).unwrap();
        let router = DtRouter::from_config(&config, DbType::Mysql, DbType::Pg).unwrap();
        assert_eq!(router.route(&rid!("db", "t")).unwrap(), rid!("db", "t"));
        assert_eq!(router.route_db_type(), &DbType::Pg);
        assert_eq!(router.to(&rid!("db", "t")).unwrap(), rid!("db", "t"));
        for kind in CommonFilterKind::iter() {
            for mapping in ["", "a:b"] {
                let config = RouterConfig::from_ini(&ini(&format!(
                    "[rules.{}]\nroute={mapping}",
                    kind.as_ref()
                )))
                .unwrap();
                assert!(DtRouter::from_config(&config, DbType::Mysql, DbType::Mysql).is_err());
            }
        }
    }

    #[test]
    fn raw_resource_names_are_resolved_for_the_source_engine() {
        for (db_type, section) in [
            (DbType::Mysql, "db"),
            (DbType::Mysql, "database"),
            (DbType::Pg, "schema"),
        ] {
            let config = RouterConfig::from_ini(&ini(&format!(
                "[rules.{section}]\nroute=inventory:archive"
            )))
            .unwrap();
            let router = DtRouter::from_config(&config, db_type, DbType::Pg).unwrap();
            assert_eq!(
                router.route(&rid!("inventory", "order")).unwrap(),
                rid!("archive", "order")
            );
        }
        for (db_type, text) in [
            (DbType::Mysql, "[rules.future]\nroute=a:b"),
            (DbType::Pg, "[rules.database]\nroute=a:b"),
            (DbType::Mysql, "[rules.schema]\nroute=a:b"),
            (DbType::Kafka, "[rules.db]\nroute=a:b"),
            (DbType::Pg, "[rules.db]\nroute=a:b"),
            (DbType::Mongo, "[rules.column]\nroute=db.c.field:db.c.other"),
            (DbType::Mysql, "[rules.collection]\nroute=db.c:db.other"),
            (DbType::Mongo, "[rules.document]\nroute=db.c:db.other"),
        ] {
            let config = RouterConfig::from_ini(&ini(text)).unwrap();
            assert!(
                DtRouter::from_config(&config, db_type.clone(), db_type).is_err(),
                "{text}"
            );
        }
        for (db_type, text) in [
            (
                DbType::Pg,
                "[rules.schema]\nroute=db:archive\n[rules.db]\nroute=",
            ),
            (DbType::Mongo, "[rules.collection]\nroute=db.c:archive.c"),
        ] {
            let router = load_router(&ini(text), db_type.clone(), db_type).unwrap();
            assert_eq!(
                router.route(&rid!("db", "c")).unwrap(),
                rid!("archive", "c")
            );
        }
    }

    #[test]
    fn ini_handles_endpoint_dialects_and_json() {
        let r = load_router(
            &ini("[rules.table]\nroute=`a.b`.`order`:\"archive\".\"order.2026\""),
            DbType::Mysql,
            DbType::Pg,
        )
        .unwrap();
        assert_eq!(
            r.route(&rid!("a.b", "order")).unwrap(),
            rid!("archive", "order.2026")
        );
        let r = load_router(
            &ini(r#"[rules.table]
    route=json:[{"from":["a.b","order"],"to":["archive","order.2026"]}]
    "#),
            DbType::Mysql,
            DbType::Pg,
        )
        .unwrap();
        assert_eq!(
            r.route(&rid!("a.b", "order")).unwrap(),
            rid!("archive", "order.2026")
        );
        for text in [
            "[rules.table]\nroute=a.t:b.t:c.t",
            "[rules.table]\nroute=a.*:b.t",
            "[rules.table]\nroute=a.t:b.t,",
        ] {
            let ini = ini(text);
            let config = RouterConfig::from_ini(&ini).unwrap();
            assert!(
                DtRouter::from_config(&config, DbType::Mysql, DbType::Mysql).is_err(),
                "{text}"
            );
        }
    }
}
