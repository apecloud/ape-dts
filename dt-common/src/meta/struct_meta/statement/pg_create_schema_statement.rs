use crate::meta::struct_meta::structure::{schema::Schema, structure_type::StructureType};
use crate::rdb_filter::RdbFilter;

#[derive(Debug, Clone)]
pub struct PgCreateSchemaStatement {
    pub schema: Schema,
}

impl PgCreateSchemaStatement {
    pub fn route(&mut self, dst_schema: &str) {
        self.schema.name = dst_schema.to_string();
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();
        if filter.filter_structure(&StructureType::Database) {
            return Ok(sqls);
        }

        let key = format!("schema.{}", self.schema.name);
        let sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, self.schema.name);
        sqls.push((key, sql));
        Ok(sqls)
    }
}
