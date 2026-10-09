use anyhow::{ensure, Context, Result};
use configparser::ini::Ini;
use serde::Deserialize;

use super::{base::RouteOutput, BaseRouter};
use crate::{
    config::config_enums::DbType,
    resource::{config, resource_kind, resource_levels, validate_rid, ResourceKind, ToRid, RID},
};

/// Payload routing is distinct from topic selection. Supply the already-routed payload RID.
#[derive(Debug, Clone)]
pub struct TopicRouter {
    src_db_type: DbType,
    dst_db_type: DbType,
    routers: Vec<BaseRouter>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchRule {
    from: Option<RID>,
    #[serde(default)]
    descendants: bool,
    to: RID,
}

impl TopicRouter {
    pub fn new(src_db_type: DbType) -> Self {
        Self {
            routers: vec![BaseRouter::default(); resource_levels(&src_db_type).len()],
            src_db_type,
            dst_db_type: DbType::Kafka,
        }
    }

    pub fn src_db_type(&self) -> &DbType {
        &self.src_db_type
    }
    pub fn dst_db_type(&self) -> &DbType {
        &self.dst_db_type
    }

    pub fn from_ini(ini: &Ini, src_db_type: DbType) -> Result<Self> {
        config::base_configs(ini, &src_db_type)?;
        let old = ini.get("router", "topic_map").unwrap_or_default();
        let new = ini.get("router", "dispatch");
        let parse = |value: &str, legacy: bool| -> Result<Self> {
            let mut router = Self::new(src_db_type.clone());
            if !legacy && value.trim().starts_with("json:") {
                for rule in config::json::<Vec<DispatchRule>>(value)? {
                    router.insert(rule.from, rule.descendants, rule.to, false)?;
                }
                return Ok(router);
            }
            for (mut from, to) in config::pairs(value, &src_db_type, &DbType::Kafka)? {
                // Legacy topic quoting follows the payload engine's dialect. Dots are
                // joined only at this boundary: a topic is a single protocol name.
                let names = to
                    .iter()
                    .map(|token| {
                        Ok(config::literal(token, &src_db_type, legacy)?
                            .unwrap_or_else(|| token.clone()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let topic = RID::new([names.join(".")])?;
                let default = from.iter().all(|s| s == "*");
                ensure!(!legacy || !matches!(src_db_type, DbType::Mssql) || default,
                    "legacy MSSQL topic_map lacks database identity; use dispatch with a complete source path");
                let prefix = !default && from.last().is_some_and(|s| s == "*");
                if prefix {
                    from.pop();
                }
                let source = if default {
                    None
                } else {
                    Some(config::exact_path(from, &src_db_type, false)?)
                };
                router.insert(source, prefix, topic, legacy)?;
            }
            Ok(router)
        };
        let old_router = parse(&old, true).context("[router].topic_map")?;
        if let Some(new) = new {
            let new_router = parse(&new, false).context("[router].dispatch")?;
            ensure!(
                old.trim().is_empty() || old_router.routers == new_router.routers,
                "conflicting topic_map and dispatch"
            );
            Ok(new_router)
        } else {
            Ok(old_router)
        }
    }

    pub fn add_route(&mut self, source: Option<RID>, descendants: bool, target: RID) -> Result<()> {
        self.insert(source, descendants, target, false)
    }

    fn insert(
        &mut self,
        source: Option<RID>,
        descendants: bool,
        target: RID,
        legacy: bool,
    ) -> Result<()> {
        ensure!(
            source.is_some() || !descendants,
            "default topic cannot specify descendants"
        );
        validate_rid(&self.dst_db_type, ResourceKind::Topic, &target)?;
        if let Some(source) = &source {
            let kind = resource_kind(&self.src_db_type, source)?;
            ensure!(
                kind != ResourceKind::Column,
                "topic dispatch does not accept column resources"
            );
            if descendants {
                let scope_depth = resource_levels(&self.src_db_type)
                    .iter()
                    .take_while(|&&k| k != ResourceKind::Column)
                    .count();
                ensure!(
                    source.len() < scope_depth,
                    "topic prefix must identify a namespace"
                );
            }
        }
        let index = source.as_ref().map_or(0, |id| id.len() - 1);
        self.routers[index].insert(source, descendants, target, legacy)?;
        Ok(())
    }

    pub fn dispatch<I: ToRid + ?Sized>(&self, source: &I) -> Result<RID> {
        let source = source.to_rid(&self.src_db_type)?;
        ensure!(
            resource_kind(&self.src_db_type, &source)? != ResourceKind::Column,
            "topic dispatch does not accept columns"
        );
        BaseRouter::route(&self.routers, &source, RouteOutput::Replace)?
            .context("no topic dispatch rule for payload resource")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rid;

    fn ini(text: &str) -> Ini {
        let mut ini = Ini::new();
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(text.into()).unwrap();
        ini
    }

    #[test]
    fn namespace_exact_rules_do_not_hide_descendant_prefixes() {
        let mut router = TopicRouter::new(DbType::Mssql);
        for (source, descendants, target) in [
            (None, false, rid!("default.topic")),
            (Some(rid!("db")), true, rid!("database.topic")),
            (Some(rid!("db", "sales")), true, rid!("sales.topic")),
            (Some(rid!("db", "sales")), false, rid!("schema.ddl.topic")),
            (
                Some(rid!("db", "sales", "order")),
                false,
                rid!("order.topic"),
            ),
        ] {
            router.add_route(source, descendants, target).unwrap();
        }
        for (source, target) in [
            (rid!("db", "sales"), rid!("schema.ddl.topic")),
            (rid!("db", "sales", "order"), rid!("order.topic")),
            (rid!("db", "sales", "other"), rid!("sales.topic")),
            (rid!("db", "audit", "other"), rid!("database.topic")),
            (rid!("other", "dbo", "table"), rid!("default.topic")),
        ] {
            assert_eq!(router.dispatch(&source).unwrap(), target);
        }
        assert!(router
            .add_route(Some(rid!("db", "sales")), true, rid!("conflict"))
            .is_err());
        assert!(router.add_route(None, false, rid!("conflict")).is_err());
        assert_eq!(
            router.dispatch(&rid!("db", "sales", "other")).unwrap(),
            rid!("sales.topic")
        );
        assert_eq!(
            router.dispatch(&rid!("other")).unwrap(),
            rid!("default.topic")
        );
        let legacy = TopicRouter::from_ini(
            &ini("[router]\ntopic_map=*.*:old,*.*:new,db.*:first,db.*:last"),
            DbType::Mysql,
        )
        .unwrap();
        assert_eq!(
            legacy.dispatch(&rid!("other", "table")).unwrap(),
            rid!("new")
        );
        assert_eq!(legacy.dispatch(&rid!("db", "table")).unwrap(), rid!("last"));
    }

    #[test]
    fn topic_dispatch_uses_exact_prefix_default_and_keeps_dotted_topics() {
        let router = TopicRouter::from_ini(&ini("[router]\ndispatch=*.*:default.topic,inventory.*:inventory.topic,inventory.order:order.topic"), DbType::Mysql).unwrap();
        for (source, target) in [
            (rid!("inventory", "order"), rid!("order.topic")),
            (rid!("inventory", "other"), rid!("inventory.topic")),
            (rid!("other", "table"), rid!("default.topic")),
            (rid!("inventory"), rid!("inventory.topic")),
        ] {
            assert_eq!(router.dispatch(&source).unwrap(), target);
        }
        assert!(router.dispatch(&rid!("inventory", "order", "id")).is_err());
        assert!(TopicRouter::new(DbType::Mysql)
            .dispatch(&rid!("db", "table"))
            .is_err());
        assert!(
            TopicRouter::from_ini(&ini("[router]\ndispatch=*.t:topic"), DbType::Mysql).is_err()
        );
        let quoted = TopicRouter::from_ini(
            &ini("[router]\ntopic_map=*.*:`a.b`\ndispatch=*.*:a.b"),
            DbType::Mysql,
        )
        .unwrap();
        assert_eq!(quoted.dispatch(&rid!("db", "t")).unwrap(), rid!("a.b"));
        let structured = TopicRouter::from_ini(&ini(r#"[router]
    dispatch=json:[{"from":null,"to":["default.topic"]},{"from":["inventory"],"descendants":true,"to":["inventory.topic"]},{"from":["inventory","order"],"to":["order.topic"]}]
    "#), DbType::Mysql).unwrap();
        for source in [
            rid!("inventory", "order"),
            rid!("inventory", "other"),
            rid!("other", "table"),
        ] {
            assert_eq!(
                structured.dispatch(&source).unwrap(),
                router.dispatch(&source).unwrap()
            );
        }
        for text in [
            "[filter.topic]\ndispatch=*.*:topic",
            "[router]\ndispatch=json:[{\"from\":null,\"descendants\":true,\"to\":[\"topic\"]}]",
        ] {
            assert!(TopicRouter::from_ini(&ini(text), DbType::Mysql).is_err());
        }
        assert!(TopicRouter::from_ini(
            &ini("[router]\ntopic_map=inventory.order:topic"),
            DbType::Mssql
        )
        .is_err());
    }
}
