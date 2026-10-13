use anyhow::{anyhow, bail, ensure, Result};
use strum::{AsRefStr, EnumString};

use super::RID;
use crate::config::config_enums::DbType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum ResourceKind {
    #[strum(to_string = "db", serialize = "database")]
    Database,
    Schema,
    Table,
    Column,
    Collection,
    Topic,
}

const MYSQL_RESOURCE_LEVELS: &[ResourceKind] = &[
    ResourceKind::Database,
    ResourceKind::Table,
    ResourceKind::Column,
];
const PG_RESOURCE_LEVELS: &[ResourceKind] = &[
    ResourceKind::Schema,
    ResourceKind::Table,
    ResourceKind::Column,
];
const MSSQL_RESOURCE_LEVELS: &[ResourceKind] = &[
    ResourceKind::Database,
    ResourceKind::Schema,
    ResourceKind::Table,
    ResourceKind::Column,
];
const MONGO_RESOURCE_LEVELS: &[ResourceKind] = &[ResourceKind::Database, ResourceKind::Collection];
const REDIS_RESOURCE_LEVELS: &[ResourceKind] = &[ResourceKind::Database];
const KAFKA_RESOURCE_LEVELS: &[ResourceKind] = &[ResourceKind::Topic];

pub fn resource_levels(db_type: &DbType) -> &'static [ResourceKind] {
    match db_type {
        DbType::Mysql | DbType::Tidb | DbType::ClickHouse | DbType::StarRocks | DbType::Doris => {
            MYSQL_RESOURCE_LEVELS
        }
        DbType::Pg => PG_RESOURCE_LEVELS,
        DbType::Mssql => MSSQL_RESOURCE_LEVELS,
        DbType::Mongo => MONGO_RESOURCE_LEVELS,
        DbType::Redis => REDIS_RESOURCE_LEVELS,
        DbType::Kafka => KAFKA_RESOURCE_LEVELS,
    }
}

/// Zero-based index in the engine's resource hierarchy.
pub fn resource_depth(db_type: &DbType, kind: ResourceKind) -> Option<usize> {
    resource_levels(db_type)
        .iter()
        .position(|&level| level == kind)
}

pub fn resource_kind(db_type: &DbType, id: &RID) -> Result<ResourceKind> {
    ensure!(!id.is_empty(), "empty {db_type} resource: {id}");
    let depth = id.len() - 1;
    resource_levels(db_type)
        .get(depth)
        .copied()
        .ok_or_else(|| anyhow!("invalid {db_type} resource depth {depth}: {id}"))
}

pub fn validate_rid(db_type: &DbType, kind: ResourceKind, id: &RID) -> Result<()> {
    let Some(depth) = resource_depth(db_type, kind) else {
        bail!("{db_type} does not have a {kind:?} resource");
    };
    let segments = depth + 1;
    ensure!(
        id.len() == segments,
        "{db_type} {kind:?} requires {segments} segments: {id}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rid;

    #[test]
    fn resource_arrays_cover_every_engine_and_validate_depth() {
        use DbType::*;
        use ResourceKind::*;
        for (engine, levels) in [
            (Mysql, vec![Database, Table, Column]),
            (Tidb, vec![Database, Table, Column]),
            (ClickHouse, vec![Database, Table, Column]),
            (StarRocks, vec![Database, Table, Column]),
            (Doris, vec![Database, Table, Column]),
            (Pg, vec![Schema, Table, Column]),
            (Mssql, vec![Database, Schema, Table, Column]),
            (Mongo, vec![Database, Collection]),
            (Redis, vec![Database]),
            (Kafka, vec![Topic]),
        ] {
            assert_eq!(resource_levels(&engine), levels);
            for (index, kind) in levels.iter().enumerate() {
                assert_eq!(resource_depth(&engine, *kind), Some(index));
                let id = RID::new(vec!["name"; index + 1]);
                assert_eq!(resource_kind(&engine, &id).unwrap(), *kind);
                validate_rid(&engine, *kind, &id).unwrap();
                assert!(validate_rid(&engine, *kind, &id.child("extra")).is_err());
            }
            assert!(resource_kind(&engine, &RID::new(vec!["name"; levels.len() + 1])).is_err());
            assert!(resource_kind(&engine, &RID::new(Vec::<String>::new())).is_err());
            assert!(resource_kind(&engine, &rid!("")).is_err());
        }
        assert_eq!(resource_depth(&Pg, Database), None);
        assert!(validate_rid(&Pg, Database, &rid!("db")).is_err());
        assert_eq!(Database.as_ref(), "db");
    }
}
