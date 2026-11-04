use anyhow::{bail, ensure, Context, Result};
use strum::{AsRefStr, EnumString};

use super::RID;
use crate::config::{config_enums::DbType, filter_config::SOURCE};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum ResourceKind {
    #[strum(to_string = "db", serialize = "database")]
    Database,
    Schema,
    #[strum(to_string = "tb", serialize = "table")]
    Table,
    #[strum(to_string = "col", serialize = "column")]
    Column,
    Collection,
    Topic,
}

const MYSQL_RESOURCE_LEVELS: &[ResourceKind] = &[
    ResourceKind::Database,
    ResourceKind::Table,
    ResourceKind::Column,
];
// TODO: ResourceKind::Database for PG
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

impl ResourceKind {
    pub fn levels(db_type: &DbType) -> &'static [Self] {
        match db_type {
            DbType::Mysql
            | DbType::Tidb
            | DbType::ClickHouse
            | DbType::StarRocks
            | DbType::Doris => MYSQL_RESOURCE_LEVELS,
            DbType::Pg => PG_RESOURCE_LEVELS,
            DbType::Mssql => MSSQL_RESOURCE_LEVELS,
            DbType::Mongo => MONGO_RESOURCE_LEVELS,
            DbType::Redis => REDIS_RESOURCE_LEVELS,
            DbType::Kafka => KAFKA_RESOURCE_LEVELS,
        }
    }

    pub fn from_rid(db_type: &DbType, id: &RID) -> Option<Self> {
        id.len()
            .checked_sub(1)
            .and_then(|depth| Self::levels(db_type).get(depth))
            .copied()
    }

    pub fn validate_rid(self, db_type: &DbType, id: &RID) -> Result<()> {
        let Some(depth) = self.depth(db_type) else {
            bail!("{db_type} does not have a {self:?} resource");
        };
        let segments = depth + 1;
        ensure!(
            id.len() == segments,
            "{db_type} {self:?} requires {segments} segments: {id}"
        );
        Ok(())
    }

    pub fn from_section(name: &str, db_type: &DbType) -> Result<Option<Self>> {
        let section = format!("{SOURCE}.{name}");
        let kind = name
            .parse::<Self>()
            .with_context(|| format!("unknown resource section [{section}]"))?;
        if kind.depth(db_type).is_none() {
            return Ok(None);
        }
        Ok(Some(kind))
    }

    pub fn depth(self, db_type: &DbType) -> Option<usize> {
        Self::levels(db_type)
            .iter()
            .position(|&level| level == self)
    }

    pub fn is_cacheable(self) -> bool {
        matches!(self, ResourceKind::Table | ResourceKind::Collection)
    }

    pub fn child(self, db_type: &DbType) -> Option<Self> {
        self.depth(db_type)
            .and_then(|depth| Self::levels(db_type).get(depth + 1))
            .copied()
    }

    pub fn object_kind(db_type: &DbType) -> Option<Self> {
        Self::levels(db_type)
            .iter()
            .rev()
            .copied()
            .find(|kind| *kind != Self::Column)
    }

    pub fn precheck_path(db_type: &DbType, id: &RID) -> Result<RID> {
        Self::from_rid(db_type, id)
            .with_context(|| format!("invalid resource path for {db_type}: {id}"))?;
        let depth = Self::object_kind(db_type)
            .and_then(|kind| kind.depth(db_type))
            .with_context(|| format!("no record resource for {db_type}"))?;
        let mut len = id.len().min(depth + 1);
        // A complete wildcard suffix denotes the enclosing exact precheck scope.
        while len > 1 && id.segment(len - 1) == super::WILDCARD {
            len -= 1;
        }
        Ok(id.prefix(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_hierarchies_define_children_objects_and_cache_policy() {
        for (db, levels, object) in [
            (
                DbType::Mysql,
                vec![
                    ResourceKind::Database,
                    ResourceKind::Table,
                    ResourceKind::Column,
                ],
                ResourceKind::Table,
            ),
            (
                DbType::Pg,
                vec![
                    ResourceKind::Schema,
                    ResourceKind::Table,
                    ResourceKind::Column,
                ],
                ResourceKind::Table,
            ),
            (
                DbType::Mssql,
                vec![
                    ResourceKind::Database,
                    ResourceKind::Schema,
                    ResourceKind::Table,
                    ResourceKind::Column,
                ],
                ResourceKind::Table,
            ),
            (
                DbType::Mongo,
                vec![ResourceKind::Database, ResourceKind::Collection],
                ResourceKind::Collection,
            ),
            (
                DbType::Redis,
                vec![ResourceKind::Database],
                ResourceKind::Database,
            ),
            (
                DbType::Kafka,
                vec![ResourceKind::Topic],
                ResourceKind::Topic,
            ),
        ] {
            assert_eq!(ResourceKind::levels(&db), levels);
            assert_eq!(ResourceKind::object_kind(&db), Some(object));
            for (index, &kind) in levels.iter().enumerate() {
                assert_eq!(kind.child(&db), levels.get(index + 1).copied());
                let id = RID::new(vec!["name"; index + 1]);
                let path = ResourceKind::precheck_path(&db, &id).unwrap();
                assert_eq!(path.len(), (index + 1).min(object.depth(&db).unwrap() + 1));
            }
        }
        assert!(ResourceKind::Table.is_cacheable());
        assert!(ResourceKind::Collection.is_cacheable());
        assert!(!ResourceKind::Column.is_cacheable());
    }
}
