//! Immutable RID routing with separate source/target engine contexts.
mod base;
mod config;
mod topic;

use std::collections::{HashMap, HashSet};

use anyhow::{bail, ensure, Result};
pub use base::BaseRouter;
use base::RouteOutput;
pub use topic::TopicRouter;

use crate::{
    config::config_enums::DbType,
    resource::{resource_kind, resource_levels, FromRid, ResourceKind, ToRid, RID},
};

#[derive(Debug, Clone)]
pub struct Router {
    src_db_type: DbType,
    dst_db_type: DbType,
    forward_routers: Vec<BaseRouter>,
    // Keep every source candidate: a single-valued reverse map loses collisions.
    reverse_routers: Vec<HashMap<RID, Vec<RID>>>,
}

fn namespace(kind: ResourceKind) -> bool {
    matches!(kind, ResourceKind::Database | ResourceKind::Schema)
}

fn compatible_kind(src: ResourceKind, dst: ResourceKind) -> bool {
    src == dst || (namespace(src) && namespace(dst))
}

impl Router {
    pub fn new(
        src_db_type: DbType,
        dst_db_type: DbType,
        rules: impl IntoIterator<Item = (RID, RID)>,
    ) -> Result<Self> {
        let mut router = Self {
            forward_routers: vec![BaseRouter::default(); resource_levels(&src_db_type).len()],
            reverse_routers: vec![HashMap::new(); resource_levels(&dst_db_type).len()],
            src_db_type,
            dst_db_type,
        };
        for (src, dst) in rules {
            let src_kind = resource_kind(&router.src_db_type, &src)?;
            let dst_kind = resource_kind(&router.dst_db_type, &dst)?;
            ensure!(
                compatible_kind(src_kind, dst_kind),
                "cannot route {src_kind:?} to {dst_kind:?}: {src} -> {dst}"
            );
            ensure!(resource_levels(&router.src_db_type)[src.len()..] == resource_levels(&router.dst_db_type)[dst.len()..], "incompatible resource suffixes for {src} -> {dst}; bind the complete namespace or object");
            if !router.forward_routers[src.len() - 1].insert(
                Some(src.clone()),
                true,
                dst.clone(),
                false,
            )? {
                continue;
            }
            router.reverse_routers[dst.len() - 1]
                .entry(dst)
                .or_default()
                .push(src);
        }
        // All parent rules are present before validating column routes.
        for base in &router.forward_routers {
            for (src, dst) in base.prefixes() {
                if resource_kind(&router.src_db_type, src)? == ResourceKind::Column {
                    let src_parent = src.parent().expect("column has a parent");
                    ensure!(
                        Some(router.route_rid(&src_parent)?) == dst.parent(),
                        "column route changes its parent table: {src} -> {dst}"
                    );
                }
            }
        }
        Ok(router)
    }

    pub fn src_db_type(&self) -> &DbType {
        &self.src_db_type
    }
    pub fn dst_db_type(&self) -> &DbType {
        &self.dst_db_type
    }

    pub fn route<I: ToRid + ?Sized>(&self, source: &I) -> Result<RID> {
        self.route_rid(&source.to_rid(&self.src_db_type)?)
    }

    pub fn route_as<I: ToRid + ?Sized, O: FromRid>(&self, source: &I) -> Result<O> {
        O::from_rid(&self.route(source)?, &self.dst_db_type)
    }

    pub fn route_rid(&self, source: &RID) -> Result<RID> {
        let source_kind = resource_kind(&self.src_db_type, source)?;
        if let Some(target) =
            BaseRouter::route(&self.forward_routers, source, RouteOutput::PreserveSuffix)?
        {
            ensure!(
                compatible_kind(source_kind, resource_kind(&self.dst_db_type, &target)?),
                "route changes resource kind: {source} -> {target}"
            );
            return Ok(target);
        }
        let src_levels = resource_levels(&self.src_db_type);
        let dst_levels = resource_levels(&self.dst_db_type);
        ensure!(
            src_levels.len() == dst_levels.len()
                && src_levels
                    .iter()
                    .zip(dst_levels)
                    .all(|(&src, &dst)| compatible_kind(src, dst)),
            "no namespace binding for {} -> {}: {source}",
            self.src_db_type,
            self.dst_db_type
        );
        Ok(source.clone())
    }

    /// Without a selected source domain, identity fallback is also a reverse candidate.
    /// A rename into an unchanged name is consequently ambiguous.
    pub fn reverse_route<I: ToRid + ?Sized>(&self, target: &I) -> Result<RID> {
        self.reverse_rid(&target.to_rid(&self.dst_db_type)?, None)
    }

    pub fn reverse_route_in<I: ToRid + ?Sized>(
        &self,
        target: &I,
        selected_sources: &HashSet<RID>,
    ) -> Result<RID> {
        for source in selected_sources {
            resource_kind(&self.src_db_type, source)?;
        }
        self.reverse_rid(&target.to_rid(&self.dst_db_type)?, Some(selected_sources))
    }

    pub fn reverse_route_as<I: ToRid + ?Sized, O: FromRid>(
        &self,
        target: &I,
        selected_sources: &HashSet<RID>,
    ) -> Result<O> {
        O::from_rid(
            &self.reverse_route_in(target, selected_sources)?,
            &self.src_db_type,
        )
    }

    fn reverse_rid(&self, target: &RID, selected_sources: Option<&HashSet<RID>>) -> Result<RID> {
        let target_kind = resource_kind(&self.dst_db_type, target)?;
        let mut candidates = HashSet::new();
        let mut consider = |candidate: RID| {
            if selected_sources.is_some_and(|sources| !sources.contains(&candidate)) {
                return;
            }
            if resource_kind(&self.src_db_type, &candidate)
                .is_ok_and(|kind| compatible_kind(kind, target_kind))
                && self
                    .route_rid(&candidate)
                    .is_ok_and(|routed| &routed == target)
            {
                candidates.insert(candidate);
            }
        };
        consider(target.clone());
        for depth in 1..=target.len() {
            let prefix = target.prefix(depth).expect("validated prefix");
            if let Some(sources) = self.reverse_routers[depth - 1].get(&prefix) {
                for source in sources {
                    consider(target.replace_prefix(depth, source)?);
                }
            }
        }
        let mut candidates: Vec<_> = candidates.into_iter().collect();
        candidates.sort();
        match candidates.len() {
            0 => bail!("unmapped reverse route for {target}"),
            1 => Ok(candidates.remove(0)),
            _ => bail!("ambiguous reverse route for {target}: {candidates:?}"),
        }
    }

    /// Validate actual selected objects/columns before a caller enables reverse lookup.
    pub fn validate_unique_targets(&self, selected_sources: &HashSet<RID>) -> Result<()> {
        let mut targets = HashMap::new();
        for source in selected_sources {
            let target = self.route_rid(source)?;
            if let Some(previous) = targets.insert(target.clone(), source) {
                ensure!(
                    previous == source,
                    "route collision: {previous} and {source} -> {target}"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{resource::validate_rid, rid};

    struct EngineResource {
        id: RID,
        db_type: DbType,
    }
    impl ToRid for EngineResource {
        fn to_rid(&self, db_type: &DbType) -> Result<RID> {
            assert_eq!(&self.db_type, db_type);
            validate_rid(db_type, ResourceKind::Table, &self.id)?;
            Ok(self.id.clone())
        }
    }
    impl FromRid for EngineResource {
        fn from_rid(id: &RID, db_type: &DbType) -> Result<Self> {
            validate_rid(db_type, ResourceKind::Table, id)?;
            Ok(Self {
                id: id.clone(),
                db_type: db_type.clone(),
            })
        }
    }

    #[test]
    fn longest_original_prefix_and_column_rules_do_not_cascade() {
        let router = Router::new(
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
        let topics = Router::new(
            DbType::Kafka,
            DbType::Kafka,
            [(rid!("orders.eu"), rid!("archive.eu"))],
        )
        .unwrap();
        assert_eq!(
            topics.route(&rid!("orders.eu")).unwrap(),
            rid!("archive.eu")
        );
        let router = Router::new(
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
        assert!(Router::new(DbType::Mssql, DbType::Pg, [(rid!("db"), rid!("schema"))]).is_err());
        assert!(Router::new(
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
        let router = Router::new(DbType::Mysql, DbType::Mysql, [(a.clone(), b.clone())]).unwrap();
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
    fn reverse_routing_preserves_all_candidates_and_checks_overrides() {
        let a = rid!("a", "t");
        let b = rid!("b", "t");
        let target = rid!("x", "t");
        let router = Router::new(
            DbType::Mysql,
            DbType::Mysql,
            [(a.clone(), target.clone()), (b.clone(), target.clone())],
        )
        .unwrap();
        assert!(router
            .reverse_route_in(&target, &HashSet::from([a.clone(), b]))
            .is_err());
        let router = Router::new(
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
        let swap = Router::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a"), rid!("b")), (rid!("b"), rid!("a"))],
        )
        .unwrap();
        assert_eq!(swap.reverse_route(&rid!("b", "t")).unwrap(), rid!("a", "t"));
    }

    #[test]
    fn column_parents_duplicates_and_actual_column_collisions_are_validated() {
        assert!(Router::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a", "t", "id"), rid!("b", "t", "id"))]
        )
        .is_err());
        assert!(Router::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a"), rid!("b")), (rid!("a"), rid!("c"))]
        )
        .is_err());
        let router = Router::new(
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
    fn conversion_traits_receive_the_correct_engine_in_both_directions() {
        let router = Router::new(
            DbType::Pg,
            DbType::Mssql,
            [(rid!("inventory"), rid!("archive", "history"))],
        )
        .unwrap();
        let source = EngineResource {
            id: rid!("inventory", "order"),
            db_type: DbType::Pg,
        };
        let target: EngineResource = router.route_as(&source).unwrap();
        assert_eq!(target.db_type, DbType::Mssql);
        let recovered: EngineResource = router
            .reverse_route_as(&target, &HashSet::from([source.id.clone()]))
            .unwrap();
        assert_eq!(recovered.id, source.id);
        assert_eq!(recovered.db_type, DbType::Pg);
    }
}
