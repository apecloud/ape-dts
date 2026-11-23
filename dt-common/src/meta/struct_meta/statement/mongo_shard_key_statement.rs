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
        let shard_collection = &self.shard_collection;
        anyhow::ensure!(
            !shard_collection.key.is_empty(),
            "empty MongoDB shard key: {}",
            shard_collection.get_ns()
        );
        Ok(vec![(
            StructKey::new(
                DbType::Mongo,
                StructKeyType::ShardKey,
                [
                    &shard_collection.database_name,
                    &shard_collection.collection_name,
                ],
            ),
            check_definition(
                doc! { "key": shard_collection.key.clone(), "unique": shard_collection.unique },
            )?,
        )])
    }

    pub fn route(&mut self, dst_db: String, dst_collection: String) {
        self.shard_collection.database_name = dst_db;
        self.shard_collection.collection_name = dst_collection;
    }
}
