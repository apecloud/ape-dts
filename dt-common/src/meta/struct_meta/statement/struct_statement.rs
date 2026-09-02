use super::{
    mongo_create_collection_statement::MongoCreateCollectionStatement,
    mongo_shard_key_statement::MongoShardKeyStatement,
    mssql_create_database_statement::MssqlCreateDatabaseStatement,
    mssql_create_schema_statement::MssqlCreateSchemaStatement,
    mssql_create_table_statement::MssqlCreateTableStatement,
    mysql_create_database_statement::MysqlCreateDatabaseStatement,
    mysql_create_table_statement::MysqlCreateTableStatement,
    pg_create_rbac_statement::PgCreateRbacStatement,
    pg_create_schema_statement::PgCreateSchemaStatement,
    pg_create_table_statement::PgCreateTableStatement,
};
use crate::{
    meta::struct_meta::statement::{
        pg_create_udf_statement::PgCreateUdfStatement,
        pg_create_udt_statement::PgCreateUdtStatement,
    },
    rdb_filter::RdbFilter,
};

#[derive(Debug, Clone, Default)]
pub enum StructStatement {
    MssqlCreateDatabase(MssqlCreateDatabaseStatement),
    MssqlCreateSchema(MssqlCreateSchemaStatement),
    MssqlCreateTable(MssqlCreateTableStatement),
    MysqlCreateDatabase(MysqlCreateDatabaseStatement),
    PgCreateSchema(PgCreateSchemaStatement),
    MysqlCreateTable(MysqlCreateTableStatement),
    MongoCreateCollection(MongoCreateCollectionStatement),
    MongoShardKey(MongoShardKeyStatement),
    PgCreateTable(PgCreateTableStatement),
    PgCreateRbac(PgCreateRbacStatement),
    PgCreateUdf(PgCreateUdfStatement),
    PgCreateUdt(PgCreateUdtStatement),
    #[default]
    Unknown,
}

impl StructStatement {
    pub fn statement_path(&self) -> (String, String, String) {
        match self {
            Self::MssqlCreateDatabase(statement) => (
                statement.database_name.clone(),
                String::new(),
                String::new(),
            ),
            Self::MssqlCreateSchema(statement) => (
                statement.database_name.clone(),
                statement.schema_name.clone(),
                String::new(),
            ),
            Self::MssqlCreateTable(statement) => (
                statement.table.database_name.clone(),
                statement.table.schema_name.clone(),
                statement.table.table_name.clone(),
            ),
            Self::MysqlCreateDatabase(statement) => (
                String::new(),
                statement.database.name.clone(),
                String::new(),
            ),
            Self::MysqlCreateTable(statement) => (
                String::new(),
                statement.table.database_name.clone(),
                statement.table.table_name.clone(),
            ),
            Self::PgCreateSchema(statement) => {
                (String::new(), statement.schema.name.clone(), String::new())
            }
            Self::PgCreateTable(statement) => (
                String::new(),
                statement.table.schema_name.clone(),
                statement.table.table_name.clone(),
            ),
            Self::PgCreateUdf(statement) => (
                String::new(),
                statement.udf.schema_name.clone(),
                String::new(),
            ),
            Self::PgCreateUdt(statement) => (
                String::new(),
                statement.udt.schema_name.clone(),
                String::new(),
            ),
            _ => (String::new(), String::new(), String::new()),
        }
    }

    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        match self {
            Self::MssqlCreateDatabase(s) => s.to_sqls(filter),
            Self::MssqlCreateSchema(s) => s.to_sqls(filter),
            Self::MssqlCreateTable(s) => s.to_sqls(filter),
            Self::MysqlCreateDatabase(s) => s.to_sqls(filter),
            Self::PgCreateSchema(s) => s.to_sqls(filter),
            Self::MysqlCreateTable(s) => s.to_sqls(filter),
            Self::MongoCreateCollection(_) => Ok(vec![]),
            Self::MongoShardKey(_) => Ok(vec![]),
            Self::PgCreateTable(s) => s.to_sqls(filter),
            Self::PgCreateRbac(s) => s.to_sqls(filter),
            Self::PgCreateUdf(s) => s.to_sqls(filter),
            Self::PgCreateUdt(s) => s.to_sqls(filter),
            _ => Ok(vec![]),
        }
    }
}
