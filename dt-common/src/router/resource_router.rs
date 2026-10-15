use std::collections::{HashMap, HashSet};

use anyhow::{bail, ensure, Context, Result};

use crate::{
    config::config_enums::DbType,
    resource::{ResourceKind, RID, WILDCARD},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceRouter {
    forward_map: HashMap<RID, RID>,
    reverse_map: HashMap<RID, RID>,
    /// Original source RID -> destination; multiple sources may share a target.
    /// Only trailing whole-segment wildcards: `db.orders` > `db.*` > `*.*`.
    /// `None` uses cross-engine payload routing; `Some`, even empty, keeps payloads in the source layout.
    to_map: Option<HashMap<RID, RID>>,
}

impl ResourceRouter {
    pub fn new(to_map: Option<HashMap<RID, RID>>) -> Self {
        Self {
            to_map,
            ..Self::default()
        }
    }

    pub fn has_to_map(&self) -> bool {
        self.to_map.is_some()
    }

    pub fn mappings(&self) -> &HashMap<RID, RID> {
        &self.forward_map
    }

    /// Recover the source RID, preferring explicit inverse routes over unchanged names.
    pub fn to(routers: &HashMap<ResourceKind, Self>, src: &DbType, routed: &RID) -> Result<RID> {
        if !routers.values().any(Self::has_to_map) {
            return Ok(routed.clone());
        }
        let source = Self::reverse(routers, src, routed)?;
        let router = ResourceKind::from_rid(src, &source)
            .and_then(|kind| routers.get(&kind))
            .filter(|router| router.has_to_map())
            .with_context(|| format!("no destination dispatch rules for {source}"))?;
        router.dispatch(&source)
    }

    /// Any dispatch map makes all payload mappings source -> source.
    pub fn route_db_type<'a>(
        routers: &HashMap<ResourceKind, Self>,
        src: &'a DbType,
        dst: &'a DbType,
    ) -> &'a DbType {
        if routers.values().any(Self::has_to_map) {
            src
        } else {
            dst
        }
    }

    pub fn insert(&mut self, source: RID, target: RID) -> Result<bool> {
        if let Some(previous) = self.forward_map.get(&source) {
            ensure!(
                previous == &target,
                "conflicting routes for {source}: {previous} and {target}"
            );
            return Ok(false);
        }
        if let Some(previous) = self.reverse_map.get(&target) {
            ensure!(
                previous == &source,
                "conflicting reverse routes for {target}: {previous} and {source}"
            );
        }
        self.forward_map.insert(source.clone(), target.clone());
        self.reverse_map.insert(target, source);
        Ok(true)
    }

    pub fn route(&self, source: &RID) -> Option<&RID> {
        self.forward_map.get(source)
    }

    pub fn reverse_route(&self, target: &RID) -> Option<&RID> {
        self.reverse_map.get(target)
    }

    /// Match the longest original prefix; routes do not cascade.
    pub fn forward(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        source: &RID,
    ) -> Result<RID> {
        for (index, kind) in ResourceKind::levels(src_db_type)
            .iter()
            .take(source.len())
            .enumerate()
            .rev()
        {
            let Some(router) = routers.get(kind) else {
                continue;
            };
            let depth = index + 1;
            let prefix = source.prefix(depth);
            if let Some(target) = router.route(&prefix) {
                return Ok(source.replace_prefix(depth, target));
            }
        }
        Ok(source.clone())
    }

    /// Match the longest target prefix; only fall back to identity if no rule matches.
    pub fn reverse(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        target: &RID,
    ) -> Result<RID> {
        let restore = |source: RID| -> Result<RID> {
            ensure!(
                Self::forward(routers, src_db_type, &source)? == *target,
                "unmapped reverse route for {target}"
            );
            Ok(source)
        };
        for depth in (1..=target.len()).rev() {
            let prefix = target.prefix(depth);
            let mut source = None;
            for router in routers.values() {
                if let Some(candidate) = router.reverse_route(&prefix) {
                    ensure!(
                        source.is_none_or(|previous| previous == candidate),
                        "conflicting reverse routes for {prefix}"
                    );
                    source = Some(candidate);
                }
            }
            if let Some(source) = source {
                return restore(target.replace_prefix(depth, source));
            }
        }
        restore(target.clone())
    }

    pub fn validate_unique_targets(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        selected_sources: &HashSet<RID>,
    ) -> Result<()> {
        let mut targets = HashMap::new();
        for source in selected_sources {
            let target = Self::forward(routers, src_db_type, source)?;
            if let Some(previous) = targets.insert(target.clone(), source) {
                ensure!(
                    previous == source,
                    "route collision: {previous} and {source} -> {target}"
                );
            }
        }
        Ok(())
    }

    fn dispatch(&self, source: &RID) -> Result<RID> {
        let Some(map) = &self.to_map else {
            return Ok(source.clone());
        };
        if let Some(target) = map.get(source) {
            return Ok(target.clone());
        }
        let mut segments = source.segments().to_vec();
        for index in (0..segments.len()).rev() {
            segments[index] = WILDCARD.into();
            if let Some(target) = map.get(&RID::new(segments.clone())) {
                return Ok(target.clone());
            }
        }
        bail!("no destination dispatch rule for {source}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rid;

    #[test]
    fn to_map_uses_source_names_after_reversing_payload_routes() {
        let source_map = HashMap::from([
            (rid!("db", "orders"), rid!("orders.topic")),
            (rid!("db", "order*"), rid!("literal.topic")),
            (rid!("db", "*"), rid!("database.topic")),
            (rid!("*", "*"), rid!("default.topic")),
        ]);
        let mut table = ResourceRouter::new(Some(source_map.clone()));
        table
            .insert(rid!("db", "orders"), rid!("cold", "history"))
            .unwrap();
        table
            .insert(rid!("db", "events"), rid!("cold", "events"))
            .unwrap();
        let mut db = ResourceRouter::default();
        db.insert(rid!("db"), rid!("archive")).unwrap();
        let routers = HashMap::from([(ResourceKind::Table, table), (ResourceKind::Database, db)]);
        for (source, expected) in [
            (rid!("db", "orders"), rid!("orders.topic")),
            (rid!("db", "order*"), rid!("literal.topic")),
            (rid!("db", "order1"), rid!("database.topic")),
            (rid!("db", "events"), rid!("database.topic")),
            (rid!("db", "other"), rid!("database.topic")),
            (rid!("other", "t"), rid!("default.topic")),
        ] {
            let routed = ResourceRouter::forward(&routers, &DbType::Mysql, &source).unwrap();
            assert_eq!(
                ResourceRouter::to(&routers, &DbType::Mysql, &routed).unwrap(),
                expected
            );
        }
        assert_eq!(
            routers[&ResourceKind::Table].to_map.as_ref(),
            Some(&source_map)
        );
    }

    #[test]
    fn to_map_uses_the_longest_inverse_before_dispatch() {
        let id = rid!("schema", "t");
        assert_eq!(
            ResourceRouter::to(&HashMap::new(), &DbType::Mysql, &id).unwrap(),
            id
        );
        let mut routers = HashMap::from([(
            ResourceKind::Table,
            ResourceRouter::new(Some(HashMap::new())),
        )]);
        assert!(ResourceRouter::to(&routers, &DbType::Mysql, &id).is_err());
        let mut table = ResourceRouter::new(Some(HashMap::from([
            (rid!("a", "other"), rid!("explicit")),
            (rid!("a", "*"), rid!("inherited")),
            (rid!("*", "*"), rid!("shared")),
        ])));
        table.insert(rid!("a", "other"), rid!("x", "t")).unwrap();
        let mut db = ResourceRouter::default();
        db.insert(rid!("a"), rid!("x")).unwrap();
        routers.insert(ResourceKind::Table, table);
        routers.insert(ResourceKind::Database, db);
        assert_eq!(
            ResourceRouter::to(&routers, &DbType::Mysql, &rid!("x", "t")).unwrap(),
            rid!("explicit")
        );
    }

    #[test]
    fn routes_match_complete_rids_literally() {
        let mut router = ResourceRouter::default();
        for source in [
            rid!("inventory"),
            rid!("inventory", "order*"),
            rid!("inventory", "order?"),
            rid!("inventory", "r#order.*#"),
        ] {
            let target = source.replace_prefix(1, &rid!("archive"));
            router.insert(source.clone(), target.clone()).unwrap();
            assert_eq!(router.route(&source), Some(&target));
            assert_eq!(router.reverse_route(&target), Some(&source));
        }
        for (source, target) in [
            (rid!("inventory", "order"), rid!("archive", "order")),
            (rid!("inventory", "order1"), rid!("archive", "order1")),
            (
                rid!("inventory", "order*", "id"),
                rid!("archive", "order*", "id"),
            ),
            (rid!("other"), rid!("other")),
        ] {
            assert_eq!(router.route(&source), None);
            assert_eq!(router.reverse_route(&target), None);
        }
    }

    #[test]
    fn conflicts_in_either_direction_leave_both_maps_unchanged() {
        let mut router = ResourceRouter::default();
        let source = rid!("inventory", "orders");
        let target = rid!("archive", "orders");
        assert!(router.insert(source.clone(), target.clone()).unwrap());
        assert!(!router.insert(source.clone(), target.clone()).unwrap());
        let original = router.clone();
        for (from, to) in [
            (source.clone(), rid!("other", "orders")),
            (rid!("other", "orders"), target.clone()),
        ] {
            assert!(router.insert(from, to).is_err());
            assert_eq!(router, original);
        }
        assert_eq!(router.route(&source), Some(&target));
        assert_eq!(router.reverse_route(&target), Some(&source));
    }
}
