use crate::meta::mongo::mongo_shard::MongoShardCollection;

#[derive(Debug, Clone)]
pub struct MongoShardKeyStatement {
    pub shard_collection: MongoShardCollection,
}

impl MongoShardKeyStatement {
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
