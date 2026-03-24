use crate::config::config_enums::DbType;

pub struct SystemDb {}

impl SystemDb {
    const MYSQL: [&str; 4] = ["information_schema", "mysql", "performance_schema", "sys"];
    const POSTGRES: [&str; 2] = ["pg_catalog", "information_schema"];
    const MONGO: [&str; 3] = ["admin", "config", "local"];

    pub fn is_system_db(db: &str, db_type: &DbType) -> bool {
        match db_type {
            DbType::Mysql => Self::MYSQL.contains(&db),
            DbType::Pg => Self::POSTGRES.contains(&db),
            DbType::Mongo => Self::MONGO.contains(&db),
            _ => false,
        }
    }

    pub fn get_system_dbs(db_type: &DbType) -> Option<Vec<&str>> {
        match db_type {
            DbType::Mysql => Some(Self::MYSQL.to_vec()),
            DbType::Pg => Some(Self::POSTGRES.to_vec()),
            DbType::Mongo => Some(Self::MONGO.to_vec()),
            _ => None,
        }
    }
}
