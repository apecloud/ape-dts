use std::collections::HashMap;

use anyhow::{ensure, Context, Result};

use super::DtRouter;
use crate::{
    config::{
        config_enums::DbType,
        filter_config::RULES,
        router_config::{RouterConfig, TO_PREFIX},
    },
    filter::CommonFilterKind,
    resource::{ResourceConfigParser, ResourceKind, RESOURCE_SEPARATOR, RID, WILDCARD},
};

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
            if let Some(kind) = ResourceKind::from_section(name, &src)? {
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
                if dst_kind.depth(&dst).is_none() {
                    continue;
                }
                let map = to_maps.entry(kind).or_insert_with(HashMap::new);
                for (from, to) in Self::parse_to_routes(value, &src, &dst, dst_kind)
                    .with_context(|| format!("{kind:?}.{key}"))?
                {
                    dst_kind.validate_rid(&dst, &to)?;
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
            if !Self::supports_route(kind, route_dst) {
                continue;
            }
            let mappings =
                Self::parse_routes(base.route.as_deref().unwrap_or_default(), &src, route_dst)
                    .with_context(|| format!("{kind:?}.route"))?;
            for (from, to) in mappings {
                kind.validate_rid(&src, &from)?;
                rules.push((from, to));
            }
        }
        Self::build_router(src, dst, rules, to_maps)
    }

    pub fn build_router(
        src: DbType,
        dst: DbType,
        rules: Vec<(RID, RID)>,
        to_maps: HashMap<ResourceKind, HashMap<RID, RID>>,
    ) -> Result<Self> {
        let route_dst = if to_maps.is_empty() { &dst } else { &src };
        for (source, target) in &rules {
            let src_kind = ResourceKind::from_rid(&src, source)
                .with_context(|| format!("invalid {src} resource: {source}"))?;
            let dst_kind = ResourceKind::from_rid(route_dst, target)
                .with_context(|| format!("invalid {route_dst} resource: {target}"))?;
            let namespace = |kind| matches!(kind, ResourceKind::Database | ResourceKind::Schema);
            ensure!(
                src_kind == dst_kind || namespace(src_kind) && namespace(dst_kind),
                "cannot route {src_kind:?} to {dst_kind:?}: {source} -> {target}"
            );
            ensure!(
                ResourceKind::levels(&src).iter().skip(source.len())
                    .eq(ResourceKind::levels(route_dst).iter().skip(target.len())),
                "incompatible resource suffixes for {source} -> {target}; bind the complete namespace or object"
            );
        }
        for (&kind, map) in &to_maps {
            for source in map.keys() {
                kind.validate_rid(&src, source)?;
                let mut wildcard = false;
                for segment in source.segments() {
                    if segment == WILDCARD {
                        wildcard = true;
                    } else {
                        ensure!(
                            !wildcard,
                            "dispatch wildcards must form a trailing suffix: {source}"
                        );
                    }
                }
            }
        }
        let router = Self::new_with_to_maps(src, dst, rules, to_maps)?;
        if let Some(columns) = router.resource_routers().get(&ResourceKind::Column) {
            for (source, target) in columns.mappings() {
                ensure!(
                    router.forward::<RID>(source.prefix(source.len() - 1))?
                        == target.prefix(target.len() - 1),
                    "column route changes its parent table: {source} -> {target}"
                );
            }
        }
        Ok(router)
    }

    fn parse_routes(value: &str, src: &DbType, dst: &DbType) -> Result<Vec<(RID, RID)>> {
        ResourceConfigParser::pairs(value, src, dst)?
            .into_iter()
            .map(|(from, to)| {
                Ok((
                    ResourceConfigParser::exact_path(from, src)?,
                    ResourceConfigParser::exact_path(to, dst)?,
                ))
            })
            .collect()
    }

    fn parse_to_routes(
        value: &str,
        src: &DbType,
        dst: &DbType,
        dst_kind: ResourceKind,
    ) -> Result<Vec<(RID, RID)>> {
        ResourceConfigParser::pairs(value, src, dst)?
            .into_iter()
            .map(|(from, to)| {
                let source = RID::new(
                    from.into_iter()
                        .map(|segment| {
                            if segment == WILDCARD {
                                return Ok(segment);
                            }
                            ResourceConfigParser::exact_token(segment, src)
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
                let source = if matches!(src, DbType::Kafka) {
                    RID::new([source.segments().join(RESOURCE_SEPARATOR)])
                } else {
                    source
                };
                let target = ResourceConfigParser::exact_path(to, dst)?;
                // Dots belong to the topic name.
                let target = if dst_kind == ResourceKind::Topic {
                    RID::new([target.segments().join(RESOURCE_SEPARATOR)])
                } else {
                    target
                };
                Ok((source, target))
            })
            .collect()
    }

    pub fn supports_route(kind: ResourceKind, dst: &DbType) -> bool {
        ResourceKind::levels(dst).iter().any(|&target| {
            kind == target
                || matches!(kind, ResourceKind::Database | ResourceKind::Schema)
                    && matches!(target, ResourceKind::Database | ResourceKind::Schema)
        })
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
            assert_eq!(router.forward::<RID>(&source).unwrap(), payload);
            assert_eq!(router.to::<RID>(&payload).unwrap(), destination);
        }
        assert_eq!(
            router.forward::<RID>(rid!("db", "orders", "id")).unwrap(),
            rid!("cold", "history", "new_id")
        );
        assert!(router.to::<RID>(rid!("cold", "history", "new_id")).is_err());
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
            assert_eq!(router.forward::<RID>(&source).unwrap(), routed);
            assert_eq!(router.to::<RID>(&routed).unwrap(), target);
        }
        let router = load_router(
            &ini("[rules.table]\nroute=db.t:archive.t\nto_topic="),
            DbType::Mysql,
            DbType::Kafka,
        )
        .unwrap();
        assert_eq!(router.route_db_type(), &DbType::Mysql);
        assert!(router.to::<RID>(rid!("archive", "t")).is_err());
    }

    #[test]
    fn dispatch_rejects_invalid_kinds_depths_patterns_and_conflicts() {
        for (dst, text) in [
            (DbType::Kafka, "[rules.table]\nto_future=db.t:events"),
            (DbType::Kafka, "[rules.table]\nto_=db.t:events"),
            (DbType::Kafka, "[rules.event]\nto_topic="),
            (DbType::Kafka, "[rules.table]\nto_topic=db:events"),
            (DbType::Kafka, "[rules.table]\nto_topic=*.t:events"),
            (DbType::Kafka, "[rules.table]\nto_topic=db.order*:events"),
            (DbType::Kafka, "[rules.table]\nto_topic=db.t:one,db.t:two"),
            (DbType::Pg, "[rules.table]\nto_table=db.t:queue"),
            (
                DbType::Kafka,
                r#"[rules.table]
to_topic=json:[{"from":["db","t"],"to":["events"]}]"#,
            ),
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
            router.forward::<RID>(rid!("db", "t", "id")).unwrap(),
            rid!("archive", "orders", "order_id")
        );
        assert_eq!(
            router.forward::<RID>(rid!("db", "t", "other")).unwrap(),
            rid!("archive", "orders", "other")
        );
        bases.insert(
            "schema".to_owned(),
            RouterSectionConfig {
                route: Some("db.schema:archive".into()),
                ..Default::default()
            },
        );
        let router = DtRouter::from_config(
            &RouterConfig::Basic(bases.clone()),
            DbType::Mysql,
            DbType::Pg,
        )
        .unwrap();
        assert_eq!(
            router.forward::<RID>(rid!("db", "t", "id")).unwrap(),
            rid!("archive", "orders", "order_id")
        );
        let mut sections = BTreeMap::from([("table".to_owned(), RouterSectionConfig::default())]);
        for value in [None, Some(String::new())] {
            sections.get_mut("table").unwrap().route = value;
            let router = DtRouter::from_config(
                &RouterConfig::Basic(sections.clone()),
                DbType::Mysql,
                DbType::Pg,
            )
            .unwrap();
            assert_eq!(
                router.forward::<RID>(rid!("db", "t")).unwrap(),
                rid!("db", "t")
            );
        }
        sections.clear();
        let router =
            DtRouter::from_config(&RouterConfig::Basic(sections), DbType::Mysql, DbType::Pg)
                .unwrap();
        assert_eq!(
            router.forward::<RID>(rid!("db", "t")).unwrap(),
            rid!("db", "t")
        );
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
                router.forward::<RID>(rid!("db", "t", "id")).unwrap(),
                rid!("archive", "orders", "new_id")
            );
            assert_eq!(
                router.forward::<RID>(rid!("db", "t", "value")).unwrap(),
                rid!("archive", "orders", "value")
            );
            assert_eq!(
                router.forward::<RID>(rid!("db", "other")).unwrap(),
                rid!("db", "other")
            );
            assert_eq!(router.route_db_type(), &DbType::Pg);
            assert_eq!(
                router.to::<RID>(rid!("archive", "orders")).unwrap(),
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
                assert_eq!(
                    router.forward::<RID>(rid!("db", "t")).unwrap(),
                    rid!("db", "t")
                );
            }
        }
    }

    #[test]
    fn common_filter_sections_select_basic_and_reject_router_fields() {
        let config = RouterConfig::from_ini(&ini("[router]\ntb_map=db.t:archive.orders\ntopic_map=*.*:events\n[rules.event]\ndo=insert\nignore=delete\n[rules.cmd]\nignore=SET")).unwrap();
        let router = DtRouter::from_config(&config, DbType::Mysql, DbType::Pg).unwrap();
        assert_eq!(
            router.forward::<RID>(rid!("db", "t")).unwrap(),
            rid!("db", "t")
        );
        assert_eq!(router.route_db_type(), &DbType::Pg);
        assert_eq!(router.to::<RID>(rid!("db", "t")).unwrap(), rid!("db", "t"));
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
                router.forward::<RID>(rid!("inventory", "order")).unwrap(),
                rid!("archive", "order")
            );
        }
        for (db_type, text) in [
            (DbType::Mysql, "[rules.future]\nroute=a:b"),
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
                router.forward::<RID>(rid!("db", "c")).unwrap(),
                rid!("archive", "c")
            );
        }
    }

    #[test]
    fn ini_routes_preserve_endpoint_quotes_and_literal_names() {
        for (src, dst, text, source, target) in [
            (
                DbType::Mysql,
                DbType::Pg,
                r#"[rules.table]
route=`a.b`.`order`:"archive"."order.2026""#,
                rid!("a.b", "order"),
                rid!("archive", "order.2026"),
            ),
            (
                DbType::Mssql,
                DbType::Mysql,
                r#"[rules.table]
route=[db].[sales].[a]]b]:`archive`.`order*`"#,
                rid!("db", "sales", "a]b"),
                rid!("archive", "order*"),
            ),
            (
                DbType::Mysql,
                DbType::Mysql,
                "[rules.db]\nroute=json:archive",
                rid!("json"),
                rid!("archive"),
            ),
            (
                DbType::Mysql,
                DbType::Mysql,
                "[router]\ndb_map=json:archive",
                rid!("json"),
                rid!("archive"),
            ),
        ] {
            let router = load_router(&ini(text), src, dst).unwrap();
            assert_eq!(router.forward::<RID>(&source).unwrap(), target);
            assert_eq!(router.reverse::<RID>(&target).unwrap(), source);
        }
        for text in [
            "[rules.table]\nroute=a.t:b.t:c.t",
            "[rules.table]\nroute=a.*:b.t",
            "[rules.table]\nroute=a.t:b.t,",
            "[rules.table]\nroute=a:b.t",
            "[rules.table]\nroute=a.t:b",
            "[rules.db]\nroute=a:b.t.id.extra",
            "[rules.column]\nroute=a.t.id:b.other.new_id",
            r#"[rules.table]
route=json:[{"from":["a","t"],"to":["b","t"]}]"#,
            r#"[router]
db_map=json:[{"from":["a"],"to":["b"]}]"#,
        ] {
            let ini = ini(text);
            let config = RouterConfig::from_ini(&ini).unwrap();
            assert!(
                DtRouter::from_config(&config, DbType::Mysql, DbType::Mysql).is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn unsupported_routes_are_skipped_without_changing_the_destination_engine() {
        for (src, dst, text, source, target) in [
            (DbType::Mysql, DbType::Pg,
             "[rules.table]\nroute=db.t:archive.t\nto_topic=invalid\n[rules.schema]\nroute=invalid\nto_table=invalid",
             rid!("db", "t"), rid!("archive", "t")),
            (DbType::Pg, DbType::Mysql,
             "[rules.table]\nroute=db.t:archive.t\nto_schema=invalid\n[rules.database]\nroute=invalid",
             rid!("db", "t"), rid!("archive", "t")),
            (DbType::Mongo, DbType::Mongo,
             "[rules.collection]\nroute=db.c:archive.c\n[rules.column]\nroute=invalid",
             rid!("db", "c"), rid!("archive", "c")),
            (DbType::Redis, DbType::Redis,
             "[rules.db]\nroute=0:1\n[rules.table]\nroute=invalid",
             rid!("0"), rid!("1")),
            (DbType::Kafka, DbType::Kafka,
             "[rules.topic]\nroute=orders:archive\n[rules.db]\nroute=invalid",
             rid!("orders"), rid!("archive")),
            (DbType::Mysql, DbType::Kafka,
             "[rules.table]\nroute=invalid\nto_db=invalid",
             rid!("db", "t"), rid!("db", "t")),
            (DbType::Mysql, DbType::Mongo,
             "[rules.column]\nroute=invalid",
             rid!("db", "t", "id"), rid!("db", "t", "id")),
        ] {
            let router = load_router(&ini(text), src, dst.clone()).unwrap();
            assert_eq!(router.route_db_type(), &dst, "{text}");
            assert_eq!(router.forward::<RID>(&source).unwrap(), target, "{text}");
            assert_eq!(router.reverse::<RID>(&target).unwrap(), source, "{text}");
            assert_eq!(router.to::<RID>(&target).unwrap(), target, "{text}");
            assert_eq!(router.has_rules(), source != target, "{text}");
        }
    }

    #[test]
    fn configured_routes_require_compatible_resource_layouts() {
        for (src, dst, text) in [
            (DbType::Mssql, DbType::Pg, "[rules.db]\nroute=db:schema"),
            (DbType::Pg, DbType::Mssql, "[rules.schema]\nroute=schema:db"),
        ] {
            assert!(load_router(&ini(text), src, dst).is_err(), "{text}");
        }
    }
}
