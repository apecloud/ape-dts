use super::mssql_comment_statement::MssqlComment;
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

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
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
            format!("database.{}", self.database_name),
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
    use super::MssqlCreateDatabaseStatement;
    use crate::{
        config::{config_enums::DbType, filter_config::FilterConfig},
        rdb_filter::RdbFilter,
    };

    #[test]
    fn to_sqls_escapes_database_name() {
        let statement = MssqlCreateDatabaseStatement {
            database_name: "db]with'quote".to_string(),
            collation_name: String::new(),
            comments: Vec::new(),
        };
        let filter = RdbFilter::from_config(
            &FilterConfig {
                do_structures: "database".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();

        assert_eq!(
            statement.to_sqls(&filter).unwrap(),
            vec![(
                "database.db]with'quote".to_string(),
                "IF DB_ID(N'db]with''quote') IS NULL EXEC(N'CREATE DATABASE [db]]with''quote]')"
                    .to_string(),
            )]
        );
    }

    #[test]
    fn to_sqls_preserves_database_collation() {
        let statement = MssqlCreateDatabaseStatement {
            database_name: "test_db".to_string(),
            collation_name: "Latin1_General_100_BIN2".to_string(),
            comments: Vec::new(),
        };
        let filter = RdbFilter::from_config(
            &FilterConfig {
                do_structures: "database".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();

        assert_eq!(
            statement.to_sqls(&filter).unwrap(),
            vec![(
                "database.test_db".to_string(),
                "IF DB_ID(N'test_db') IS NULL EXEC(N'CREATE DATABASE [test_db] COLLATE Latin1_General_100_BIN2')".to_string(),
            )]
        );
    }
}
