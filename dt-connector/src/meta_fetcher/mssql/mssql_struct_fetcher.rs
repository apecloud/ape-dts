use std::collections::HashSet;

use dt_common::{
    meta::{
        mssql::mssql_connection_pool::MssqlConnectionPool,
        struct_meta::statement::{
            mssql_create_schema_statement::MssqlCreateSchemaStatement,
            mssql_create_table_statement::MssqlCreateTableStatement,
        },
    },
    rdb_filter::RdbFilter,
};

pub struct MssqlStructFetcher {
    pub connection_pool: MssqlConnectionPool,
    pub schemas: HashSet<String>,
    pub filter: Option<RdbFilter>,
}

impl MssqlStructFetcher {
    pub async fn get_create_schema_statements(
        &mut self,
        _schema: &str,
    ) -> anyhow::Result<Vec<MssqlCreateSchemaStatement>> {
        todo!("fetch MSSQL schema metadata")
    }

    pub async fn get_create_table_statements(
        &mut self,
        _schema: &str,
        _tb: &str,
    ) -> anyhow::Result<Vec<MssqlCreateTableStatement>> {
        todo!("fetch MSSQL table, column, constraint, index, and comment metadata")
    }
}
