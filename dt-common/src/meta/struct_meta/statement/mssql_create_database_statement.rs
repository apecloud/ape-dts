use super::{
    mssql_comment_statement::MssqlComment,
    struct_statement::{StructKey, StructKeyType},
};
use crate::{
    config::config_enums::DbType, meta::struct_meta::structure::structure_type::StructureType,
    rdb_filter::RdbFilter, utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone)]
pub struct MssqlCreateDatabaseStatement {
    pub database_name: String,
    pub collation_name: String,
    pub comments: Vec<MssqlComment>,
}

impl MssqlCreateDatabaseStatement {
    pub fn route(&mut self, dst_db: &str) {
        self.database_name = dst_db.to_string();
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(StructKey, String)>> {
        if filter.filter_structure(&StructureType::Database) {
            return Ok(Vec::new());
        }

        let database = SqlUtil::escape_by_db_type(&self.database_name, &DbType::Mssql);
        let database_literal = self.database_name.replace('\'', "''");
        let mut create_database = format!("CREATE DATABASE {database}");
        if !self.collation_name.is_empty() {
            create_database.push_str(&format!(" COLLATE {}", self.collation_name));
        }
        let create_database = create_database.replace('\'', "''");
        let mut sqls = vec![(
            StructKey::new(
                DbType::Mssql,
                StructKeyType::Database,
                [self.database_name.as_str()],
            ),
            format!("IF DB_ID(N'{database_literal}') IS NULL EXEC(N'{create_database}')"),
        )];
        for comment in &self.comments {
            if let Some(sql) = comment.to_sql(&self.database_name, "", "", filter) {
                sqls.push(sql);
            }
        }
        Ok(sqls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::filter_config::FilterConfig;

    #[test]
    fn database_sql_preserves_escaping_and_collation() {
        let filter = RdbFilter::from_config(
            &FilterConfig {
                do_structures: "database".into(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();
        for (name, collation, expected) in [
            ("db]with'quote", "", "IF DB_ID(N'db]with''quote') IS NULL EXEC(N'CREATE DATABASE [db]]with''quote]')"),
            ("test_db", "Latin1_General_100_BIN2", "IF DB_ID(N'test_db') IS NULL EXEC(N'CREATE DATABASE [test_db] COLLATE Latin1_General_100_BIN2')"),
        ] {
            let statement = MssqlCreateDatabaseStatement {
                database_name: name.into(),
                collation_name: collation.into(),
                comments: Vec::new(),
            };
            assert_eq!(statement.to_sqls(&filter).unwrap(), vec![(
                StructKey::new(DbType::Mssql, StructKeyType::Database, [name]),
                expected.to_string(),
            )], "{name}");
        }
    }
}
