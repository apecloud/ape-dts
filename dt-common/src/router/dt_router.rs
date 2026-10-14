use std::collections::{HashMap, HashSet};

use anyhow::{ensure, Result};

use super::ResourceRouter;
use crate::{
    config::config_enums::DbType,
    resource::{resource_kind, ResourceKind, RID},
};

#[derive(Debug, Clone)]
pub struct DtRouter {
    src_db_type: DbType,
    dst_db_type: DbType,
    resource_routers: HashMap<ResourceKind, ResourceRouter>,
}

impl DtRouter {
    pub fn new(
        src_db_type: DbType,
        dst_db_type: DbType,
        rules: impl IntoIterator<Item = (RID, RID)>,
    ) -> Result<Self> {
        Self::new_with_to_maps(src_db_type, dst_db_type, rules, HashMap::new())
    }

    pub fn new_with_to_maps(
        src_db_type: DbType,
        dst_db_type: DbType,
        rules: impl IntoIterator<Item = (RID, RID)>,
        to_maps: HashMap<ResourceKind, HashMap<RID, RID>>,
    ) -> Result<Self> {
        let mut router = Self {
            resource_routers: HashMap::new(),
            src_db_type,
            dst_db_type,
        };
        for (kind, map) in to_maps {
            router
                .resource_routers
                .insert(kind, ResourceRouter::new(Some(map)));
        }
        for (src, dst) in rules {
            if let Some(kind) = resource_kind(&router.src_db_type, &src) {
                router
                    .resource_routers
                    .entry(kind)
                    .or_default()
                    .insert(src, dst)?;
            }
        }
        Ok(router)
    }

    pub fn src_db_type(&self) -> &DbType {
        &self.src_db_type
    }
    pub fn resource_routers(&self) -> &HashMap<ResourceKind, ResourceRouter> {
        &self.resource_routers
    }

    pub fn has_rules(&self) -> bool {
        self.resource_routers
            .values()
            .any(|router| !router.mappings().is_empty() || router.has_to_map())
    }

    pub fn dst_db_type(&self) -> &DbType {
        &self.dst_db_type
    }

    /// Dispatch keeps payload RIDs in the source engine's layout.
    pub fn route_db_type(&self) -> &DbType {
        ResourceRouter::route_db_type(&self.resource_routers, &self.src_db_type, &self.dst_db_type)
    }

    pub fn validate_unique_targets(&self, selected_sources: &HashSet<RID>) -> Result<()> {
        ResourceRouter::validate_unique_targets(
            &self.resource_routers,
            &self.src_db_type,
            selected_sources,
        )
    }

    pub fn forward<T: From<RID>>(&self, source: impl Into<RID>) -> Result<T> {
        let source = source.into();
        ResourceRouter::forward(&self.resource_routers, &self.src_db_type, &source).map(Into::into)
    }

    pub fn reverse<T: From<RID>>(&self, target: impl Into<RID>) -> Result<T> {
        let target = target.into();
        ResourceRouter::reverse(&self.resource_routers, &self.src_db_type, &target).map(Into::into)
    }

    pub fn reverse_route_in<T: From<RID>>(
        &self,
        target: impl Into<RID>,
        selected_sources: &HashSet<RID>,
    ) -> Result<T> {
        let source: RID = self.reverse(target)?;
        ensure!(
            selected_sources.contains(&source),
            "unmapped reverse route: {source} is not a selected source"
        );
        Ok(source.into())
    }

    /// Reverse the sinker's routed RID, then match dispatch rules in the source namespace.
    pub fn to<T: From<RID>>(&self, id: impl Into<RID>) -> Result<T> {
        let id = id.into();
        ResourceRouter::to(&self.resource_routers, &self.src_db_type, &id).map(Into::into)
    }

    pub fn has_resource_router(&self, kind: ResourceKind) -> bool {
        self.resource_routers.contains_key(&kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rid;

    struct EngineResource {
        id: RID,
    }

    impl From<RID> for EngineResource {
        fn from(id: RID) -> Self {
            Self { id }
        }
    }

    #[test]
    fn borrowed_rids_route_to_engine_resources() {
        for (src, dst, source, payload, destination) in [
            (
                DbType::Pg,
                DbType::Mssql,
                rid!("inventory", "order"),
                rid!("archive", "history", "order"),
                None,
            ),
            (
                DbType::Mysql,
                DbType::Pg,
                rid!("db", "t"),
                rid!("archive", "t"),
                Some(rid!("public", "queue")),
            ),
        ] {
            let maps = destination
                .as_ref()
                .map(|id| {
                    (
                        ResourceKind::Table,
                        HashMap::from([(source.clone(), id.clone())]),
                    )
                })
                .into_iter()
                .collect();
            let router = DtRouter::new_with_to_maps(
                src.clone(),
                dst.clone(),
                [(source.clone(), payload.clone())],
                maps,
            )
            .unwrap();
            let routed: EngineResource = router.forward(&source).unwrap();
            assert_eq!(routed.id, payload);
            let restored: EngineResource = router
                .reverse_route_in(&routed.id, &HashSet::from([source.clone()]))
                .unwrap();
            assert_eq!(restored.id, source);
            let rerouted: EngineResource = router.forward(restored.id).unwrap();
            assert_eq!(rerouted.id, payload);
            let target: EngineResource = router.to(routed.id).unwrap();
            assert_eq!(target.id, destination.unwrap_or(payload));
        }
    }

    #[test]
    fn longest_original_prefix_and_column_rules_do_not_cascade() {
        let router = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [
                (rid!("a"), rid!("b")),
                (rid!("b"), rid!("c")),
                (rid!("a", "t"), rid!("archive", "t9")),
                (rid!("a", "t", "id"), rid!("archive", "t9", "new_id")),
            ],
        )
        .unwrap();
        for (src, dst) in [
            (rid!("a"), rid!("b")),
            (rid!("a", "other"), rid!("b", "other")),
            (rid!("a", "t", "id"), rid!("archive", "t9", "new_id")),
            (rid!("a", "t", "value"), rid!("archive", "t9", "value")),
        ] {
            assert_eq!(router.forward::<RID>(&src).unwrap(), dst);
        }
    }

    #[test]
    fn engine_layout_changes_use_explicit_bindings() {
        let topics = DtRouter::new(
            DbType::Kafka,
            DbType::Kafka,
            [(rid!("orders.eu"), rid!("archive.eu"))],
        )
        .unwrap();
        let topic: String = topics.forward(rid!("orders.eu")).unwrap();
        assert_eq!(topic, "archive.eu");
        let router = DtRouter::new(
            DbType::Pg,
            DbType::Mssql,
            [(rid!("inventory"), rid!("archive", "history"))],
        )
        .unwrap();
        let src = rid!("inventory", "order");
        let dst = rid!("archive", "history", "order");
        let forward: (String, String, String) = router.forward(src).unwrap();
        assert_eq!(
            forward,
            ("archive".into(), "history".into(), "order".into())
        );
        let reverse: (String, String) = router.reverse(dst).unwrap();
        assert_eq!(reverse, ("inventory".into(), "order".into()));
        assert_eq!(
            router.forward::<RID>(rid!("unbound", "order")).unwrap(),
            rid!("unbound", "order")
        );
    }

    #[test]
    fn reverse_routing_prefers_explicit_rules_and_checks_selected_domain() {
        let a = rid!("a", "t");
        let b = rid!("b", "t");
        let router = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(a.clone(), b.clone()), (a.clone(), b.clone())],
        )
        .unwrap();
        assert_eq!(router.reverse::<RID>(&b).unwrap(), a);
        let sources = HashSet::from([a.clone()]);
        assert_eq!(router.reverse_route_in::<RID>(&b, &sources).unwrap(), a);
        assert!(router
            .reverse_route_in::<RID>(&b, &HashSet::from([b.clone()]))
            .is_err());
        assert!(router
            .validate_unique_targets(&HashSet::from([a, b]))
            .is_err());
    }

    #[test]
    fn conflicting_reverse_rules_return_errors_during_construction() {
        for (a, b, target) in [
            (rid!("a"), rid!("b"), rid!("x")),
            (rid!("a", "t"), rid!("b", "t"), rid!("x", "t")),
        ] {
            let error = DtRouter::new(
                DbType::Mysql,
                DbType::Mysql,
                [(a.clone(), target.clone()), (b.clone(), target.clone())],
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains("conflicting reverse routes"));
            for id in [&a, &b, &target] {
                assert!(error.contains(&id.to_string()), "{error}");
            }
        }
    }

    #[test]
    fn reverse_routing_checks_overrides_and_swaps() {
        let a = rid!("a", "t");
        let target = rid!("x", "t");
        let router = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a"), rid!("x")), (a.clone(), rid!("y", "u"))],
        )
        .unwrap();
        let sources = HashSet::from([a.clone()]);
        assert!(router
            .reverse_route_in::<RID>(&target, &sources)
            .unwrap_err()
            .to_string()
            .contains("unmapped"));
        assert_eq!(
            router
                .reverse_route_in::<RID>(rid!("y", "u"), &sources)
                .unwrap(),
            a
        );
        let swap = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a"), rid!("b")), (rid!("b"), rid!("a"))],
        )
        .unwrap();
        assert_eq!(swap.reverse::<RID>(rid!("b", "t")).unwrap(), rid!("a", "t"));
    }

    #[test]
    fn reverse_routing_stops_at_the_longest_match_across_engines() {
        for (src_type, dst_type, src_namespace, dst_namespace) in [
            (DbType::Mysql, DbType::Pg, rid!("a"), rid!("x")),
            (DbType::Pg, DbType::Mssql, rid!("a"), rid!("db", "x")),
            (DbType::Mssql, DbType::Pg, rid!("db", "a"), rid!("x")),
        ] {
            let inherited = src_namespace.child("t");
            let explicit = src_namespace.child("other");
            let target = dst_namespace.child("t");
            let router = DtRouter::new(
                src_type,
                dst_type,
                [
                    (src_namespace, dst_namespace),
                    (explicit.clone(), target.clone()),
                ],
            )
            .unwrap();
            for source in [&inherited, &explicit] {
                assert_eq!(router.forward::<RID>(source).unwrap(), target);
            }
            assert_eq!(router.reverse::<RID>(&target).unwrap(), explicit);
            for sources in [
                HashSet::from([explicit.clone()]),
                HashSet::from([inherited.clone(), explicit.clone()]),
            ] {
                assert_eq!(
                    router.reverse_route_in::<RID>(&target, &sources).unwrap(),
                    explicit
                );
            }
            assert!(router
                .reverse_route_in::<RID>(&target, &HashSet::from([inherited]))
                .is_err());
        }
    }

    #[test]
    fn duplicate_routes_and_actual_column_collisions_return_errors() {
        assert!(DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a"), rid!("b")), (rid!("a"), rid!("c"))]
        )
        .is_err());
        let router = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a", "t", "id"), rid!("a", "t", "value"))],
        )
        .unwrap();
        assert!(router
            .validate_unique_targets(&HashSet::from([
                rid!("a", "t", "id"),
                rid!("a", "t", "value")
            ]))
            .is_err());
    }

    #[test]
    fn routing_does_not_restrict_rid_depths_or_kinds() {
        for (src, dst, source, target) in [
            (
                DbType::Mysql,
                DbType::Pg,
                rid!("db"),
                rid!("a", "b", "c", "d"),
            ),
            (
                DbType::Mysql,
                DbType::Kafka,
                rid!("db", "table"),
                rid!("topic"),
            ),
            (DbType::Mssql, DbType::Pg, rid!("db"), rid!("schema")),
            (
                DbType::Mysql,
                DbType::Mysql,
                rid!("db", "t", "id"),
                rid!("other", "table", "col"),
            ),
        ] {
            let router = DtRouter::new(src, dst, [(source.clone(), target.clone())]).unwrap();
            assert_eq!(router.forward::<RID>(&source).unwrap(), target);
            assert_eq!(router.reverse::<RID>(&target).unwrap(), source);
            let descendant = source.child("suffix");
            let routed = target.child("suffix");
            assert_eq!(router.forward::<RID>(&descendant).unwrap(), routed);
            assert_eq!(router.reverse::<RID>(&routed).unwrap(), descendant);
        }
        let router = DtRouter::new(DbType::Mysql, DbType::Mssql, []).unwrap();
        for id in [rid!(""), rid!("db", "schema", "table", "column", "extra")] {
            assert_eq!(router.forward::<RID>(&id).unwrap(), id);
            assert_eq!(router.reverse::<RID>(&id).unwrap(), id);
        }
    }
}
