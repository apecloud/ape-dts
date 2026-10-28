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

    /// Zero-based index in the engine's resource hierarchy.
    pub fn depth(self, db_type: &DbType) -> Option<usize> {
        Self::levels(db_type)
            .iter()
            .position(|&level| level == self)
    }
}
