use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

#[derive(
    Debug, Clone, PartialEq, Display, EnumString, IntoStaticStr, Serialize, Deserialize, Eq,
)]
pub enum DdlType {
    #[strum(serialize = "create_database")]
    CreateDatabase,
    #[strum(serialize = "drop_database")]
    DropDatabase,
    #[strum(serialize = "create_schema")]
    CreateSchema,
    #[strum(serialize = "drop_schema")]
    DropSchema,
    #[strum(serialize = "create_table")]
    CreateTable,
    #[strum(serialize = "drop_table")]
    DropTable,
    #[strum(serialize = "truncate_table")]
    TruncateTable,
    #[strum(serialize = "rename_table")]
    RenameTable,
    #[strum(serialize = "alter_database")]
    AlterDatabase,
    #[strum(serialize = "alter_schema")]
    AlterSchema,
    #[strum(serialize = "alter_table")]
    AlterTable,
    #[strum(serialize = "create_index")]
    CreateIndex,
    #[strum(serialize = "drop_index")]
    DropIndex,
    // MongoDB DDLs
    #[strum(serialize = "mongo_create_collection")]
    MongoCreateCollection,
    #[strum(serialize = "mongo_drop_collection")]
    MongoDropCollection,
    #[strum(serialize = "mongo_rename_collection")]
    MongoRenameCollection,
    #[strum(serialize = "mongo_drop_database")]
    MongoDropDatabase,
    #[strum(serialize = "mongo_create_index")]
    MongoCreateIndex,
    #[strum(serialize = "mongo_drop_index")]
    MongoDropIndex,
    #[strum(serialize = "mongo_coll_mod")]
    MongoCollMod,
    #[strum(serialize = "mongo_shard_collection")]
    MongoShardCollection,
    #[strum(serialize = "mongo_reshard_collection")]
    MongoReshardCollection,
    #[strum(serialize = "mongo_refine_collection_shard_key")]
    MongoRefineCollectionShardKey,

    #[strum(serialize = "unknown")]
    Unknown,
}

impl Default for DdlType {
    fn default() -> Self {
        Self::Unknown
    }
}
