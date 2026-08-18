use crate::meta::struct_meta::structure::{database::Database, structure_type::StructureType};
use crate::rdb_filter::RdbFilter;

#[derive(Debug, Clone)]
pub struct MysqlCreateDatabaseStatement {
    pub database: Database,
}

impl MysqlCreateDatabaseStatement {
    pub fn route(&mut self, dst_db: &str) {
        self.database.name = dst_db.to_string();
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();
        if filter.filter_structure(&StructureType::Database) {
            return Ok(sqls);
        }

        let mut sql = format!(r#"CREATE DATABASE IF NOT EXISTS `{}`"#, self.database.name);
        if !self.database.default_character_set_name.is_empty() {
            sql = format!(
                "{} DEFAULT CHARACTER SET {}",
                sql, self.database.default_character_set_name
            )
        }
        if !self.database.default_collation_name.is_empty() {
            sql = format!(
                "{} DEFAULT COLLATE {}",
                sql, self.database.default_collation_name
            )
        }

        let key = format!("database.{}", self.database.name.clone());
        sqls.push((key, sql));
        Ok(sqls)
    }
}
