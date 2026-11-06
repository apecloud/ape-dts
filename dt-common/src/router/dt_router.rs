use std::collections::HashMap;

use anyhow::{ensure, Result};

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
        mut to_maps: HashMap<ResourceKind, HashMap<RID, RID>>,
    ) -> Result<Self> {
        let mut grouped_rules = HashMap::<_, Vec<_>>::new();
        for &kind in to_maps.keys() {
            grouped_rules.entry(kind).or_default();
        }
        for (src, dst) in rules {
            if let Some(kind) = ResourceKind::from_rid(&src_db_type, &src) {
                grouped_rules.entry(kind).or_default().push((src, dst));
            }
        }
        let resource_routers = grouped_rules
            .into_iter()
            .map(|(kind, rules)| {
                let resource = ResourceRouter::new(rules, to_maps.remove(&kind))?;
                if kind == ResourceKind::Column {
                    for (source, target) in resource.mappings() {
                        ensure!(
                            resource.reverse_route(target) == Some(source),
                            "column route collision: {target}"
                        );
                    }
                }
                Ok((kind, resource))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            resource_routers,
            src_db_type,
            dst_db_type,
        })
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

    pub fn route_db_type(&self) -> &DbType {
        ResourceRouter::route_db_type(&self.resource_routers, &self.src_db_type, &self.dst_db_type)
    }

    pub fn forward<T: From<RID>>(&self, source: impl Into<RID>) -> Result<T> {
        let source = source.into();
        ResourceRouter::forward(&self.resource_routers, &self.src_db_type, &source).map(Into::into)
    }

    pub fn reverse<T: From<RID>>(&self, target: impl Into<RID>) -> Result<T> {
        let target = target.into();
        ResourceRouter::reverse(&self.resource_routers, &self.src_db_type, &target).map(Into::into)
    }

    /// Reverse payload routing before matching source-based dispatch rules.
    pub fn to<T: From<RID>>(&self, id: impl Into<RID>) -> Result<T> {
        let id = id.into();
        ResourceRouter::to(&self.resource_routers, &self.src_db_type, &id).map(Into::into)
    }

    pub fn has_resource_router(&self, kind: ResourceKind) -> bool {
        self.resource_routers.contains_key(&kind)
    }

    /// Only monitoring may fall back to the routed target when its source is ambiguous.
    pub fn reverse_for_monitor<T: From<RID>>(&self, target: impl Into<RID>) -> Result<T> {
        ResourceRouter::reverse_for_monitor(
            &self.resource_routers,
            &self.src_db_type,
            &target.into(),
        )
        .map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rid;

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
    fn reverse_routing_checks_overrides_and_swaps() {
        let a = rid!("a", "t");
        let target = rid!("x", "t");
        let router = DtRouter::new(
            DbType::Mysql,
            DbType::Mysql,
            [(rid!("a"), rid!("x")), (a.clone(), rid!("y", "u"))],
        )
        .unwrap();
        assert!(router
            .reverse::<RID>(&target)
            .unwrap_err()
            .to_string()
            .contains("unmapped"));
        assert_eq!(router.reverse::<RID>(rid!("y", "u")).unwrap(), a);
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
        }
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

    #[test]
    fn monitoring_keeps_merged_targets_and_honors_more_specific_routes() {
        for (src_type, dst_type, a, b, target) in [
            (
                DbType::Mysql,
                DbType::Pg,
                rid!("a"),
                rid!("b"),
                rid!("merged"),
            ),
            (
                DbType::Pg,
                DbType::Mssql,
                rid!("a"),
                rid!("b"),
                rid!("db", "merged"),
            ),
            (
                DbType::Mssql,
                DbType::Pg,
                rid!("db", "a"),
                rid!("db", "b"),
                rid!("merged"),
            ),
        ] {
            let explicit = a.child("orders");
            let target_table = target.child("orders");
            for (rules, unique_table) in [
                (
                    vec![(a.clone(), target.clone()), (b.clone(), target.clone())],
                    false,
                ),
                (
                    vec![
                        (a.clone(), target.clone()),
                        (b.clone(), target.clone()),
                        (explicit.clone(), target_table.clone()),
                    ],
                    true,
                ),
                (
                    vec![
                        (a.clone(), target.clone()),
                        (explicit.clone(), target_table.clone()),
                        (b.child("orders"), target_table.clone()),
                    ],
                    false,
                ),
            ] {
                let router = DtRouter::new(src_type.clone(), dst_type.clone(), rules).unwrap();
                for target in [target_table.clone(), target_table.child("id")] {
                    let expected = if unique_table {
                        target.replace_prefix(target_table.len(), &explicit)
                    } else {
                        target.clone()
                    };
                    assert_eq!(
                        router.reverse_for_monitor::<RID>(&target).unwrap(),
                        expected
                    );
                    if unique_table {
                        assert_eq!(router.reverse::<RID>(&target).unwrap(), expected);
                    } else {
                        assert!(router.reverse::<RID>(&target).is_err());
                    }
                }
            }
        }
    }
}
