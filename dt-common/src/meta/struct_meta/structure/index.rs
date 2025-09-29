use strum::{Display, EnumString};

#[derive(Debug, Clone, Default)]
pub struct Index {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub index_name: String,
    pub index_kind: IndexKind,
    pub index_type: IndexType,
    pub comment: String,
    pub table_space: String,
    pub definition: String,
    pub columns: Vec<IndexColumn>,
}

#[derive(Debug, Clone, PartialEq, Display, EnumString, Default)]
pub enum IndexKind {
    #[strum(serialize = "UNIQUE")]
    Unique,
    #[strum(serialize = "FULLTEXT")]
    FullText,
    #[strum(serialize = "SPATIAL")]
    Spatial,
    #[default]
    #[strum(serialize = "")]
    Unknown,
}

/// for mysql, IndexType corresponds to INDEX_TYPE in information_schema.statistics
/// for posgres, this is ignored
#[derive(Debug, Clone, PartialEq, Display, EnumString, Default)]
pub enum IndexType {
    #[strum(serialize = "FULLTEXT")]
    FullText,
    #[strum(serialize = "SPATIAL")]
    Spatial,
    #[strum(serialize = "BTREE")]
    Btree,
    // HASH is NOT supported in Storage engine, refer: https://dev.mysql.com/doc/refman/8.0/en/create-index.html
    #[strum(serialize = "HASH")]
    Hash,
    #[default]
    #[strum(serialize = "")]
    Unknown,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexColumn {
    pub column_name: String,
    pub seq_in_index: u32,
    // For example, in MySQL, indexes on BLOB and TEXT fields must specify a size, which is recorded in prefix_length
    pub prefix_length: Option<u64>,
}
