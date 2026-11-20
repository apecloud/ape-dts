use mongodb::bson::doc;

use super::{
    mongo_create_collection_statement::check_definition,
    struct_statement::{StructKey, StructKeyType},
};
use crate::{
    config::config_enums::DbType,
    filter::DtFilter,
    meta::{
        mongo::mongo_shard::MongoShardCollection,
        struct_meta::structure::structure_type::StructureType,
    },
};

#[derive(Debug, Clone)]
pub struct MongoShardKeyStatement {
    pub shard_collection: MongoShardCollection,
}

impl MongoShardKeyStatement {
    pub fn to_sqls(&self, filter: &DtFilter) -> anyhow::Result<Vec<(StructKey, String)>> {
        if filter.filter_structure(&StructureType::ShardKey) {
            return Ok(Vec::new());
        }
        let (db, collection) = self
            .shard_collection
            .ns
            .split_once('.')
            .filter(|(db, collection)| !db.is_empty() && !collection.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid MongoDB shard namespace: {}",
                    self.shard_collection.ns
                )
            })?;
        anyhow::ensure!(
            !self.shard_collection.key.is_empty(),
            "empty MongoDB shard key: {}",
            self.shard_collection.ns
        );
        Ok(vec![(
            StructKey::new(DbType::Mongo, StructKeyType::ShardKey, [db, collection]),
            check_definition(
                doc! { "key": self.shard_collection.key.clone(), "unique": self.shard_collection.unique },
            )?,
        )])
    }

    pub fn route(
        &mut self,
        src_db: &str,
        src_collection: &str,
        dst_db: &str,
        dst_collection: &str,
    ) {
        let src_ns = format!("{}.{}", src_db, src_collection);
        let dst_ns = format!("{}.{}", dst_db, dst_collection);
        if self.shard_collection.ns == src_ns {
            self.shard_collection.ns = dst_ns;
        }
    }
}
