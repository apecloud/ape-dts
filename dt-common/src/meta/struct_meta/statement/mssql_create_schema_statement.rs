use crate::{
    config::config_enums::DbType,
    meta::struct_meta::structure::{schema::Schema, structure_type::StructureType},
    rdb_filter::RdbFilter,
    utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone)]
pub struct MssqlCreateSchemaStatement {
    pub schema: Schema,
}

impl MssqlCreateSchemaStatement {
    pub fn route(&mut self, dst_schema: &str) {
        self.schema.name = dst_schema.to_string();
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        if filter.filter_structure(&StructureType::Database) {
            return Ok(Vec::new());
        }

        let schema = SqlUtil::escape_by_db_type(&self.schema.name, &DbType::Mssql);
        let schema_literal = self.schema.name.replace('\'', "''");
        Ok(vec![(
            format!("schema.{}", self.schema.name),
            format!(
                "IF SCHEMA_ID(N'{schema_literal}') IS NULL EXEC(N'CREATE SCHEMA {}')",
                schema.replace('\'', "''")
            ),
        )])
    }
}
