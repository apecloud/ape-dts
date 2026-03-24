use crate::meta::struct_meta::structure::user_defined::PgUdt;
use crate::rdb_filter::RdbFilter;

use crate::meta::struct_meta::structure::structure_type::StructureType;

#[derive(Debug, Clone)]
pub struct PgCreateUdtStatement {
    pub udt: PgUdt,
}

impl PgCreateUdtStatement {
    pub fn route(&mut self, _dst_schema: &str) {
        todo!("support route");
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();
        if filter.filter_structure(&StructureType::Udt) {
            return Ok(sqls);
        }

        let sql = self.udt.create_statement.to_string();
        sqls.push((String::new(), sql));
        Ok(sqls)
    }
}
