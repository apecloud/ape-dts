use std::collections::HashMap;

use anyhow::{bail, ensure, Context, Result};

use crate::{
    config::config_enums::DbType,
    resource::{ResourceKind, RID, WILDCARD},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceRouter {
    forward_map: HashMap<RID, RID>,
    reverse_map: HashMap<RID, RID>,
    /// Original source RID -> dispatch destination.
    /// Priority: `db.orders` > `db.*` > `*.*`; only trailing whole-segment wildcards.
    /// `Some`, even empty, keeps payloads in the source layout; `None` uses the target layout.
    to_map: Option<HashMap<RID, RID>>,
}

impl ResourceRouter {
    pub fn new(
        rules: impl IntoIterator<Item = (RID, RID)>,
        to_map: Option<HashMap<RID, RID>>,
    ) -> Result<Self> {
        let mut forward_map = HashMap::new();
        for (source, target) in rules {
            if let Some(previous) = forward_map.insert(source.clone(), target.clone()) {
                ensure!(
                    previous == target,
                    "conflicting routes for {source}: {previous} and {target}"
                );
            }
        }
        let mut reverse_map = HashMap::new();
        for (source, target) in &forward_map {
            reverse_map
                .entry(target)
                .and_modify(|source| *source = None)
                .or_insert(Some(source));
        }
        let reverse_map = reverse_map
            .into_iter()
            .filter_map(|(target, source)| source.map(|source| (target.clone(), source.clone())))
            .collect();
        Ok(Self {
            forward_map,
            reverse_map,
            to_map,
        })
    }

    pub fn has_to_map(&self) -> bool {
        self.to_map.is_some()
    }

    pub fn mappings(&self) -> &HashMap<RID, RID> {
        &self.forward_map
    }

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

    /// Reverse the longest target prefix; unmatched names stay unchanged.
    pub fn reverse(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        target: &RID,
    ) -> Result<RID> {
        Self::find_reverse(routers, src_db_type, target)?
            .with_context(|| format!("conflicting reverse routes for {target}"))
    }

    pub fn reverse_for_monitor(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        target: &RID,
    ) -> Result<RID> {
        Ok(Self::find_reverse(routers, src_db_type, target)?.unwrap_or_else(|| target.clone()))
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

    fn find_reverse(
        routers: &HashMap<ResourceKind, Self>,
        src_db_type: &DbType,
        target: &RID,
    ) -> Result<Option<RID>> {
        for depth in (1..=target.len()).rev() {
            let prefix = target.prefix(depth);
            let mut matched = false;
            let mut source = None;
            for router in routers.values() {
                let Some(candidate) = router.reverse_route(&prefix) else {
                    if router.forward_map.len() != router.reverse_map.len()
                        && router.forward_map.values().any(|target| target == &prefix)
                    {
                        return Ok(None);
                    }
                    continue;
                };
                matched = true;
                let candidate = target.replace_prefix(depth, candidate);
                if Self::forward(routers, src_db_type, &candidate)? != *target {
                    continue;
                }
                if source
                    .as_ref()
                    .is_some_and(|previous| previous != &candidate)
                {
                    return Ok(None);
                }
                source = Some(candidate);
            }
            if matched {
                return source
                    .map(Some)
                    .with_context(|| format!("unmapped reverse route for {target}"));
            }
        }
        ensure!(
            Self::forward(routers, src_db_type, target)? == *target,
            "unmapped reverse route for {target}"
        );
        Ok(Some(target.clone()))
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
        let table = ResourceRouter::new(
            [
                (rid!("db", "orders"), rid!("cold", "history")),
                (rid!("db", "events"), rid!("cold", "events")),
            ],
            Some(source_map.clone()),
        )
        .unwrap();
        let db = ResourceRouter::new([(rid!("db"), rid!("archive"))], None).unwrap();
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
            ResourceRouter::new([], Some(HashMap::new())).unwrap(),
        )]);
        assert!(ResourceRouter::to(&routers, &DbType::Mysql, &id).is_err());
        let table = ResourceRouter::new(
            [(rid!("a", "other"), rid!("x", "t"))],
            Some(HashMap::from([
                (rid!("a", "other"), rid!("explicit")),
                (rid!("a", "*"), rid!("inherited")),
                (rid!("*", "*"), rid!("shared")),
            ])),
        )
        .unwrap();
        let db = ResourceRouter::new([(rid!("a"), rid!("x"))], None).unwrap();
        routers.insert(ResourceKind::Table, table);
        routers.insert(ResourceKind::Database, db);
        assert_eq!(
            ResourceRouter::to(&routers, &DbType::Mysql, &rid!("x", "t")).unwrap(),
            rid!("explicit")
        );
    }

    #[test]
    fn duplicate_sources_allow_only_the_same_target() {
        let source = rid!("inventory", "orders");
        let target = rid!("archive", "orders");
        for duplicate in [target.clone(), rid!("other", "orders")] {
            let router = ResourceRouter::new(
                [
                    (source.clone(), target.clone()),
                    (source.clone(), duplicate.clone()),
                ],
                None,
            );
            if duplicate == target {
                let router = router.unwrap();
                assert_eq!(router.route(&source), Some(&target));
                assert_eq!(router.reverse_route(&target), Some(&source));
            } else {
                assert!(router.is_err());
            }
        }
    }

    #[test]
    fn reverse_map_keeps_only_unique_targets() {
        for sources in [
            [rid!("a"), rid!("b"), rid!("c")],
            [rid!("c"), rid!("b"), rid!("a")],
            [rid!("a", "t"), rid!("b", "t"), rid!("c", "t")],
        ] {
            let target = sources[0].replace_prefix(1, &rid!("merged"));
            let kind = ResourceKind::from_rid(&DbType::Mysql, &sources[0]).unwrap();
            for count in 1..=sources.len() {
                let rules = sources[..count]
                    .iter()
                    .map(|source| (source.clone(), target.clone()));
                let router = ResourceRouter::new(rules, None).unwrap();
                assert_eq!(
                    router.reverse_route(&target),
                    (count == 1).then_some(&sources[0])
                );
                for source in &sources[..count] {
                    assert_eq!(router.route(source), Some(&target));
                }
                if count > 1 {
                    let routers = HashMap::from([(kind, router)]);
                    for target in [target.clone(), target.child("suffix")] {
                        assert!(ResourceRouter::reverse(&routers, &DbType::Mysql, &target).is_err());
                        assert_eq!(
                            ResourceRouter::reverse_for_monitor(&routers, &DbType::Mysql, &target)
                                .unwrap(),
                            target
                        );
                    }
                }
            }
        }
    }
}
