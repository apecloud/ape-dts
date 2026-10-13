use std::collections::{HashMap, HashSet};

use anyhow::Result;

use super::ResourceRouter;
use crate::{
    config::config_enums::DbType,
    resource::{ResourceKind, RID},
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
            let resource_router = ResourceRouter::new(Some(map));
            resource_router.validate_to_map(&router.src_db_type, &router.dst_db_type, kind)?;
            router.resource_routers.insert(kind, resource_router);
        }
        for (src, dst) in rules {
            let kind = ResourceRouter::validate_mapping(
                &router.src_db_type,
                router.route_db_type(),
                &src,
                &dst,
            )?;
            router
                .resource_routers
                .entry(kind)
                .or_default()
                .insert(src, dst)?;
        }
        ResourceRouter::validate_columns(
            &router.resource_routers,
            &router.src_db_type,
            &router.dst_db_type,
        )?;
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

    /// Known routed payloads prefer explicit inverse mappings over unchanged names.
    pub fn reverse_payload(&self, target: &RID) -> Result<RID> {
        ResourceRouter::reverse_rid(
            &self.resource_routers,
            &self.src_db_type,
            &self.dst_db_type,
            target,
            None,
            true,
        )
    }
    pub fn dst_db_type(&self) -> &DbType {
        &self.dst_db_type
    }

    /// Dispatch keeps payload RIDs in the source engine's layout.
    pub fn route_db_type(&self) -> &DbType {
        ResourceRouter::route_db_type(&self.resource_routers, &self.src_db_type, &self.dst_db_type)
    }

    /// Reverse the sinker's routed RID, then match dispatch rules in the source namespace.
    pub fn to(&self, id: &RID) -> Result<RID> {
        ResourceRouter::to(
            &self.resource_routers,
            &self.src_db_type,
            &self.dst_db_type,
            id,
        )
    }

    pub fn to_as<O>(&self, resource: &RID) -> Result<O>
    where
        O: for<'a> From<&'a RID>,
    {
        Ok(O::from(&self.to(resource)?))
    }

    pub fn route(&self, source: &RID) -> Result<RID> {
        ResourceRouter::forward(
            &self.resource_routers,
            &self.src_db_type,
            &self.dst_db_type,
            source,
        )
    }

    pub fn route_as<O>(&self, source: &RID) -> Result<O>
    where
        O: for<'a> From<&'a RID>,
    {
        Ok(O::from(&self.route(source)?))
    }

    /// Unchanged names are also inverse candidates; collisions return an error.
    pub fn reverse_route(&self, target: &RID) -> Result<RID> {
        self.reverse_rid(target, None)
    }

    pub fn reverse_route_in(&self, target: &RID, selected_sources: &HashSet<RID>) -> Result<RID> {
        self.reverse_rid(target, Some(selected_sources))
    }

    pub fn reverse_route_as<O>(&self, target: &RID, selected_sources: &HashSet<RID>) -> Result<O>
    where
        O: for<'a> From<&'a RID>,
    {
        Ok(O::from(&self.reverse_route_in(target, selected_sources)?))
    }

    fn reverse_rid(&self, target: &RID, selected_sources: Option<&HashSet<RID>>) -> Result<RID> {
        ResourceRouter::reverse(
            &self.resource_routers,
            &self.src_db_type,
            &self.dst_db_type,
            target,
            selected_sources,
        )
    }

    pub fn validate_unique_targets(&self, selected_sources: &HashSet<RID>) -> Result<()> {
        ResourceRouter::validate_unique_targets(
            &self.resource_routers,
            &self.src_db_type,
            &self.dst_db_type,
            selected_sources,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rid;

    struct EngineResource {
        id: RID,
    }

    impl From<&RID> for EngineResource {
        fn from(id: &RID) -> Self {
            Self { id: id.clone() }
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
            let routed: EngineResource = router.route_as(&source).unwrap();
            assert_eq!(routed.id, payload);
            let restored: EngineResource = router
                .reverse_route_as(&routed.id, &HashSet::from([source.clone()]))
                .unwrap();
            assert_eq!(restored.id, source);
            let rerouted: EngineResource = router.route_as(&restored.id).unwrap();
            assert_eq!(rerouted.id, payload);
            let target: EngineResource = router.to_as(&routed.id).unwrap();
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
            assert_eq!(router.route(&src).unwrap(), dst);
        }
    }

    #[test]
    fn engine_layout_changes_require_explicit_bindings() {
        let topics = DtRouter::new(
            DbType::Kafka,
            DbType::Kafka,
            [(rid!("orders.eu"), rid!("archive.eu"))],
        )
        .unwrap();
        assert_eq!(
            topics.route(&rid!("orders.eu")).unwrap(),
            rid!("archive.eu")
        );
        let router = DtRouter::new(
            DbType::Pg,
            DbType::Mssql,
            [(rid!("inventory"), rid!("archive", "history"))],
        )
        .unwrap();
        let src = rid!("inventory", "order");
        let dst = rid!("archive", "history", "order");
        assert_eq!(router.route(&src).unwrap(), dst);
        assert_eq!(router.reverse_route(&dst).unwrap(), src);
        assert!(router.route(&rid!("unbound", "order")).is_err());
        assert!(DtRouter::new(DbType::Mssql, DbType::Pg, [(rid!("db"), rid!("schema"))]).is_err());
        assert!(DtRouter::new(
            DbType::Mysql,
            DbType::Kafka,
            [(rid!("db", "table"), rid!("topic"))]
        )
        .is_err());
    }

    #[test]
    fn reverse_routing_checks_identity_collisions_and_selected_domain() {
        let a = rid!("a", "t");
        let b = rid!("b", "t");
        let router = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(a.clone(), b.clone()), (a.clone(), b.clone())],
        )
        .unwrap();
        assert!(router
            .reverse_route(&b)
            .unwrap_err()
            .to_string()
            .contains("ambiguous"));
        let sources = HashSet::from([a.clone()]);
        assert_eq!(router.reverse_route_in(&b, &sources).unwrap(), a);
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
            .reverse_route_in(&target, &sources)
            .unwrap_err()
            .to_string()
            .contains("unmapped"));
        assert_eq!(
            router.reverse_route_in(&rid!("y", "u"), &sources).unwrap(),
            a
        );
        let swap = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a"), rid!("b")), (rid!("b"), rid!("a"))],
        )
        .unwrap();
        assert_eq!(swap.reverse_route(&rid!("b", "t")).unwrap(), rid!("a", "t"));
    }

    #[test]
    fn reverse_routing_collects_all_target_levels_across_engines() {
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
                assert_eq!(router.route(source).unwrap(), target);
                assert_eq!(
                    router
                        .reverse_route_in(&target, &HashSet::from([source.clone()]))
                        .unwrap(),
                    *source
                );
            }
            assert!(router
                .reverse_route_in(&target, &HashSet::from([inherited, explicit]))
                .unwrap_err()
                .to_string()
                .contains("ambiguous"));
        }
    }

    #[test]
    fn column_parents_duplicates_and_actual_column_collisions_are_validated() {
        assert!(DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a", "t", "id"), rid!("b", "t", "id"))]
        )
        .is_err());
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
    fn invalid_resource_depths_propagate_errors() {
        let invalid = rid!("db", "schema", "table", "column");
        let valid = rid!("db", "table");
        assert!(DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(invalid.clone(), valid.clone())]
        )
        .is_err());
        let router = DtRouter::new(DbType::Mysql, DbType::Mysql, []).unwrap();
        assert!(router.route(&invalid).is_err());
        assert!(router.reverse_route(&invalid).is_err());
        assert!(router
            .reverse_route_in(&valid, &HashSet::from([invalid]))
            .is_err());
    }
}
