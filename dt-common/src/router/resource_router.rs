use std::collections::{HashMap, HashSet};

use anyhow::{bail, ensure, Context, Result};

use crate::{
    config::config_enums::DbType,
    resource::{resource_depth, resource_kind, resource_levels, validate_rid, ResourceKind, RID},
};

pub const WILDCARD: &str = "*";

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

    /// Recover the source RID, preferring explicit inverse routes over unchanged names.
    pub fn to(
        routers: &HashMap<ResourceKind, Self>,
        src: &DbType,
        dst: &DbType,
        routed: &RID,
    ) -> Result<RID> {
        if !routers.values().any(Self::has_to_map) {
            resource_kind(dst, routed)?;
            return Ok(routed.clone());
        }
        let source = Self::reverse(routers, src, dst, routed, None)?;
        let kind = resource_kind(src, &source)?;
        let router = routers
            .get(&kind)
            .filter(|router| router.has_to_map())
            .with_context(|| format!("no destination dispatch rules for {kind:?}: {source}"))?;
        let target = router.dispatch(&source)?;
        resource_kind(dst, &target)?;
        Ok(target)
    }

    pub fn validate_to_map(&self, src: &DbType, dst: &DbType, kind: ResourceKind) -> Result<()> {
        if let Some(map) = &self.to_map {
            ensure!(
                resource_depth(src, kind).is_some(),
                "{src} does not support {kind:?} dispatch sources"
            );
            for (source, target) in map {
                validate_rid(src, kind, source)?;
                resource_kind(dst, target)?;
                Self::validate_to_pattern(source)?;
            }
        }
        Ok(())
    }

    fn validate_to_pattern(pattern: &RID) -> Result<()> {
        let mut wildcard = false;
        for segment in pattern.segments() {
            if segment == WILDCARD {
                wildcard = true;
            } else {
                ensure!(
                    !wildcard,
                    "dispatch wildcards must form a trailing suffix: {pattern}"
                );
            }
        }
        Ok(())
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

    pub fn validate_mapping(
        src_db_type: &DbType,
        dst_db_type: &DbType,
        source: &RID,
        target: &RID,
    ) -> Result<ResourceKind> {
        let src_kind = resource_kind(src_db_type, source)?;
        let dst_kind = resource_kind(dst_db_type, target)?;
        ensure!(
            compatible_kind(src_kind, dst_kind),
            "cannot route {src_kind:?} to {dst_kind:?}: {source} -> {target}"
        );
        ensure!(
            resource_levels(src_db_type)
                .iter()
                .skip(source.len())
                .eq(resource_levels(dst_db_type).iter().skip(target.len())),
            "incompatible resource suffixes for {source} -> {target}; bind the complete namespace or object"
        );
        Ok(src_kind)
    }

    /// Match the longest original prefix; routes do not cascade.
    pub fn forward(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        dst_db_type: &DbType,
        source: &RID,
    ) -> Result<RID> {
        let dst_db_type = Self::route_db_type(routers, src_db_type, dst_db_type);
        let source_kind = resource_kind(src_db_type, source)?;
        for (index, kind) in resource_levels(src_db_type)
            .iter()
            .take(source.len())
            .enumerate()
            .rev()
        {
            let Some(router) = routers.get(kind) else {
                continue;
            };
            let depth = index + 1;
            let prefix = source
                .prefix(depth)
                .with_context(|| format!("invalid route prefix depth {depth}: {source}"))?;
            if let Some(target) = router.route(&prefix) {
                let target = source.replace_prefix(depth, target)?;
                ensure!(
                    compatible_kind(source_kind, resource_kind(dst_db_type, &target)?),
                    "route changes resource kind: {source} -> {target}"
                );
                return Ok(target);
            }
        }
        ensure!(
            identity_compatible(src_db_type, dst_db_type),
            "no namespace binding for {src_db_type} -> {dst_db_type}: {source}"
        );
        Ok(source.clone())
    }

    /// Match the longest target prefix; only fall back to identity if no rule matches.
    pub fn reverse(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        dst_db_type: &DbType,
        target: &RID,
        selected_sources: Option<&HashSet<RID>>,
    ) -> Result<RID> {
        let dst_db_type = Self::route_db_type(routers, src_db_type, dst_db_type);
        if let Some(sources) = selected_sources {
            for source in sources {
                resource_kind(src_db_type, source)?;
            }
        }
        let target_kind = resource_kind(dst_db_type, target)?;
        let validate = |candidate: RID| -> Result<RID> {
            ensure!(
                selected_sources.is_none_or(|sources| sources.contains(&candidate))
                    && compatible_kind(resource_kind(src_db_type, &candidate)?, target_kind)
                    && Self::forward(routers, src_db_type, dst_db_type, &candidate)? == *target,
                "unmapped reverse route for {target}"
            );
            Ok(candidate)
        };
        let src_levels = resource_levels(src_db_type);
        let dst_levels = resource_levels(dst_db_type);
        for (index, kind) in src_levels.iter().enumerate().rev() {
            let Some(router) = routers.get(kind) else {
                continue;
            };
            // Align unchanged suffixes across layouts, e.g. PG schema -> MSSQL db.schema.
            let suffix_len = src_levels.len() - index - 1;
            let Some(depth) = dst_levels
                .len()
                .checked_sub(suffix_len)
                .filter(|&depth| depth > 0 && depth <= target.len())
            else {
                continue;
            };
            let prefix = target
                .prefix(depth)
                .with_context(|| format!("invalid reverse route prefix depth {depth}: {target}"))?;
            if let Some(source) = router.reverse_route(&prefix) {
                return validate(target.replace_prefix(depth, source)?);
            }
        }
        ensure!(
            identity_compatible(src_db_type, dst_db_type),
            "unmapped reverse route for {target}"
        );
        validate(target.clone())
    }

    /// Validate after all parent rules have been inserted.
    pub fn validate_columns(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        dst_db_type: &DbType,
    ) -> Result<()> {
        if let Some(columns) = routers.get(&ResourceKind::Column) {
            for (source, target) in &columns.forward_map {
                let parent = source
                    .parent()
                    .with_context(|| format!("column resource has no parent: {source}"))?;
                ensure!(
                    Some(Self::forward(routers, src_db_type, dst_db_type, &parent)?)
                        == target.parent(),
                    "column route changes its parent table: {source} -> {target}"
                );
            }
        }
        Ok(())
    }

    pub fn validate_unique_targets(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        dst_db_type: &DbType,
        selected_sources: &HashSet<RID>,
    ) -> Result<()> {
        let mut targets = HashMap::new();
        for source in selected_sources {
            let target = Self::forward(routers, src_db_type, dst_db_type, source)?;
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

fn compatible_kind(src: ResourceKind, dst: ResourceKind) -> bool {
    let namespace = |kind| matches!(kind, ResourceKind::Database | ResourceKind::Schema);
    src == dst || (namespace(src) && namespace(dst))
}

fn identity_compatible(src_db_type: &DbType, dst_db_type: &DbType) -> bool {
    let src_levels = resource_levels(src_db_type);
    let dst_levels = resource_levels(dst_db_type);
    src_levels.len() == dst_levels.len()
        && src_levels
            .iter()
            .zip(dst_levels)
            .all(|(&src, &dst)| compatible_kind(src, dst))
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
            let routed =
                ResourceRouter::forward(&routers, &DbType::Mysql, &DbType::Kafka, &source).unwrap();
            assert_eq!(
                ResourceRouter::to(&routers, &DbType::Mysql, &DbType::Kafka, &routed).unwrap(),
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
            ResourceRouter::to(&HashMap::new(), &DbType::Mysql, &DbType::Pg, &id).unwrap(),
            id
        );
        let mut routers = HashMap::from([(
            ResourceKind::Table,
            ResourceRouter::new(Some(HashMap::new())),
        )]);
        assert!(ResourceRouter::to(&routers, &DbType::Mysql, &DbType::Kafka, &id).is_err());
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
            ResourceRouter::to(&routers, &DbType::Mysql, &DbType::Kafka, &rid!("x", "t")).unwrap(),
            rid!("explicit")
        );
        assert!(ResourceRouter::validate_to_pattern(&rid!("*", "t")).is_err());
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
            let target = source.replace_prefix(1, &rid!("archive")).unwrap();
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
