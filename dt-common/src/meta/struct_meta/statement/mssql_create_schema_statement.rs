use crate::{meta::struct_meta::structure::schema::Schema, rdb_filter::RdbFilter};

#[derive(Debug, Clone)]
pub struct MssqlCreateSchemaStatement {
    pub schema: Schema,
}

impl MssqlCreateSchemaStatement {
    pub fn route(&mut self, dst_schema: &str) {
        self.schema.name = dst_schema.to_string();
    }

    pub fn to_sqls(&self, _filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        todo!("generate MSSQL CREATE SCHEMA statements")
    }
}
