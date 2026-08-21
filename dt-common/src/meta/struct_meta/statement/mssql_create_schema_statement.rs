use crate::{
    config::config_enums::DbType,
    meta::struct_meta::structure::{schema::Schema, structure_type::StructureType},
    rdb_filter::RdbFilter,
    utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone)]
pub struct MssqlCreateSchemaStatement {
    pub database_name: String,
    pub schema: Schema,
}

impl MssqlCreateSchemaStatement {
    pub fn route(&mut self, dst_db: &str, dst_schema: &str) {
        self.database_name = dst_db.to_string();
        self.schema.name = dst_schema.to_string();
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        if filter.filter_structure(&StructureType::Database) {
            return Ok(Vec::new());
        }

        let schema = SqlUtil::escape_by_db_type(&self.schema.name, &DbType::Mssql);
        let schema_literal = self.schema.name.replace('\'', "''");
        let create_schema = format!(
            "IF SCHEMA_ID(N'{schema_literal}') IS NULL EXEC(N'CREATE SCHEMA {}')",
            schema.replace('\'', "''")
        );
        let sql = if self.database_name.is_empty() {
            create_schema
        } else {
            let database = SqlUtil::escape_by_db_type(&self.database_name, &DbType::Mssql);
            format!(
                "EXEC {database}.sys.sp_executesql N'{}'",
                create_schema.replace('\'', "''")
            )
        };
        Ok(vec![(
            format!("schema.{}.{}", self.database_name, self.schema.name),
            sql,
        )])
    }
}
