use crate::{
    config::config_enums::DbType,
    meta::struct_meta::structure::{database::Database, structure_type::StructureType},
    rdb_filter::RdbFilter,
    utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone)]
pub struct MssqlCreateDatabaseStatement {
    pub database: Database,
}

impl MssqlCreateDatabaseStatement {
    pub fn route(&mut self, dst_db: &str) {
        self.database.name = dst_db.to_string();
    }

    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        if filter.filter_structure(&StructureType::Database) {
            return Ok(Vec::new());
        }

        let database = SqlUtil::escape_by_db_type(&self.database.name, &DbType::Mssql);
        let database_literal = self.database.name.replace('\'', "''");
        let create_database = format!("CREATE DATABASE {}", database).replace('\'', "''");
        Ok(vec![(
            format!("database.{}", self.database.name),
            format!("IF DB_ID(N'{database_literal}') IS NULL EXEC(N'{create_database}')"),
        )])
    }
}

#[cfg(test)]
mod tests {
    use super::MssqlCreateDatabaseStatement;
    use crate::{
        config::{config_enums::DbType, filter_config::FilterConfig},
        meta::struct_meta::structure::database::Database,
        rdb_filter::RdbFilter,
    };

    #[test]
    fn to_sqls_escapes_database_name() {
        let statement = MssqlCreateDatabaseStatement {
            database: Database {
                name: "db]with'quote".to_string(),
                ..Default::default()
            },
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
}
