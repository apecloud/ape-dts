use anyhow::{bail, ensure, Result};

use super::RID;
use crate::config::config_enums::DbType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    Database,
    Schema,
    Table,
    Column,
    Collection,
    Topic,
}

pub fn resource_levels(db_type: &DbType) -> &'static [ResourceKind] {
    use ResourceKind::*;
    match db_type {
        DbType::Mysql | DbType::Tidb | DbType::ClickHouse | DbType::StarRocks | DbType::Doris => {
            &[Database, Table, Column]
        }
        DbType::Pg => &[Schema, Table, Column],
        DbType::Mssql => &[Database, Schema, Table, Column],
        DbType::Mongo => &[Database, Collection],
        DbType::Redis => &[Database],
        DbType::Kafka => &[Topic],
    }
}

pub fn resource_depth(db_type: &DbType, kind: ResourceKind) -> Option<usize> {
    resource_levels(db_type)
        .iter()
        .position(|&level| level == kind)
        .map(|i| i + 1)
}

pub fn resource_kind(db_type: &DbType, id: &RID) -> Result<ResourceKind> {
    resource_levels(db_type)
        .get(id.len() - 1)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("invalid {db_type} resource depth {}: {id}", id.len()))
}

pub fn validate_rid(db_type: &DbType, kind: ResourceKind, id: &RID) -> Result<()> {
    let Some(depth) = resource_depth(db_type, kind) else {
        bail!("{db_type} does not have a {kind:?} resource");
    };
    ensure!(
        id.len() == depth,
        "{db_type} {kind:?} requires {depth} segments: {id}"
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
                let id = RID::new(vec!["name"; index + 1]).unwrap();
                assert_eq!(resource_kind(&engine, &id).unwrap(), *kind);
                validate_rid(&engine, *kind, &id).unwrap();
                assert!(validate_rid(&engine, *kind, &id.child("extra").unwrap()).is_err());
            }
            assert!(
                resource_kind(&engine, &RID::new(vec!["name"; levels.len() + 1]).unwrap()).is_err()
            );
        }
        assert!(validate_rid(&Pg, Database, &rid!("db")).is_err());
    }
}
