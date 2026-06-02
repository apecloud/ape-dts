use crate::meta::struct_meta::structure::user_defined::PgUdf;
use crate::rdb_filter::RdbFilter;

use crate::meta::struct_meta::structure::structure_type::StructureType;

#[derive(Debug, Clone)]
pub struct PgCreateUdfStatement {
    pub udf: PgUdf,
}

impl PgCreateUdfStatement {
    pub fn route(&mut self, _dst_schema: &str) {
        todo!("support route");
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();
        if filter.filter_structure(&StructureType::Udf) {
            return Ok(sqls);
        }

        let sql = self.udf.create_statement.to_string();
        let key = format!(
            "udf.{}.{}({})",
            self.udf.schema_name, self.udf.function_name, self.udf.identity_arguments
        );
        sqls.push((key, sql));
        Ok(sqls)
    }
}
