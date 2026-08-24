use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{ddl_statement::DdlStatement, ddl_type::DdlType};
use crate::config::config_enums::DbType;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DdlData {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default_db: String,
    pub default_schema: String,
    pub query: String,
    pub ddl_type: DdlType,
    pub db_type: DbType,
    pub statement: DdlStatement,
}

impl std::fmt::Display for DdlData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl DdlData {
    pub fn to_sql(&self) -> String {
        self.statement.to_sql_with_default_info(
            &self.db_type,
            &self.default_db,
            &self.default_schema,
        )
    }

    pub fn fill_default_info(&mut self, default_db: &str, default_schema: &str, query: &str) {
        if self.default_db.is_empty() {
            self.default_db = default_db.to_string();
        }
        if self.default_schema.is_empty() {
            self.default_schema = default_schema.to_string();
        }
        self.query = query.to_string();
    }

    pub fn get_schema_tb(&self) -> (String, String) {
        if !matches!(self.db_type, DbType::Mssql) {
            let (mut schema, tb) = self.statement.get_schema_tb();
            if schema.is_empty() {
                schema = self.default_schema.clone();
            }
            return (schema, tb);
        }

        let (db, schema, tb) = self.get_db_schema_tb();
        match self.ddl_type {
            DdlType::CreateDatabase | DdlType::DropDatabase | DdlType::AlterDatabase => {
                (db, String::new())
            }
            DdlType::CreateSchema | DdlType::DropSchema | DdlType::AlterSchema => {
                (schema, String::new())
            }
            _ => (schema, tb),
        }
    }

    pub fn get_db_schema_tb(&self) -> (String, String, String) {
        if !matches!(self.db_type, DbType::Mssql) {
            let (schema, tb) = self.get_schema_tb();
            return (self.default_db.clone(), schema, tb);
        }

        let (mut db, mut schema, tb) = self.statement.get_db_schema_tb(&self.db_type);
        match self.ddl_type {
            DdlType::CreateDatabase | DdlType::DropDatabase | DdlType::AlterDatabase => {
                return (db, String::new(), String::new());
            }
            DdlType::CreateSchema | DdlType::DropSchema | DdlType::AlterSchema => {
                if db.is_empty() {
                    db = self.default_db.clone();
                }
                return (db, schema, String::new());
            }
            _ => {}
        }

        if db.is_empty() {
            db = self.default_db.clone();
        }
        if schema.is_empty() {
            schema = self.default_schema.clone();
        }
        (db, schema, tb)
    }

    pub fn get_rename_to_schema_tb(&self) -> (String, String) {
        if !matches!(self.db_type, DbType::Mssql) {
            let (mut schema, tb) = self.statement.get_rename_to_schema_tb();
            if schema.is_empty() {
                schema = self.default_schema.clone();
            }
            return (schema, tb);
        }

        let (_, schema, tb) = self.get_rename_to_db_schema_tb();
        (schema, tb)
    }

    pub fn get_rename_to_db_schema_tb(&self) -> (String, String, String) {
        if !matches!(self.db_type, DbType::Mssql) {
            let (schema, tb) = self.get_rename_to_schema_tb();
            return (self.default_db.clone(), schema, tb);
        }

        let (mut db, mut schema, tb) = self.statement.get_rename_to_db_schema_tb(&self.db_type);
        if tb.is_empty() {
            return (String::new(), String::new(), String::new());
        }

        let (src_db, src_schema, _) = self.get_db_schema_tb();
        if db.is_empty() {
            db = src_db;
        }
        if schema.is_empty() {
            schema = src_schema;
        }
        (db, schema, tb)
    }

    pub fn route(&mut self, dst_db: String, dst_schema: String, dst_tb: String) {
        self.statement.route_db_schema_tb(
            &self.db_type,
            dst_db.clone(),
            dst_schema.clone(),
            dst_tb,
        );
        self.default_db = dst_db;
        self.default_schema = dst_schema;
    }

    #[allow(clippy::too_many_arguments)]
    pub fn route_rename(
        &mut self,
        dst_db: String,
        dst_schema: String,
        dst_tb: String,
        dst_new_db: String,
        dst_new_schema: String,
        dst_new_tb: String,
    ) {
        self.statement.route_rename_db_schema_tb(
            &self.db_type,
            dst_db.clone(),
            dst_schema.clone(),
            dst_tb,
            dst_new_db,
            dst_new_schema,
            dst_new_tb,
        );
        self.default_db = dst_db;
        self.default_schema = dst_schema;
    }

    pub fn split_to_multi(self) -> Vec<DdlData> {
        let mut res = Vec::new();
        for statement in self.statement.split_to_multi() {
            res.push(Self {
                default_db: self.default_db.clone(),
                default_schema: self.default_schema.clone(),
                query: self.query.clone(),
                ddl_type: self.ddl_type.clone(),
                db_type: self.db_type.clone(),
                statement,
            });
        }
        res
    }

    pub fn get_data_size(&self) -> u64 {
        self.to_sql().len() as u64
    }

    pub fn get_malloc_size(&self) -> u64 {
        let mut size: u64 = 0;

        size += self.default_db.len() as u64;
        size += self.default_schema.len() as u64;
        size += self.query.len() as u64;
        size += std::mem::size_of::<DdlType>() as u64;
        size += std::mem::size_of::<DbType>() as u64;
        size += self.statement.get_malloc_size();

        size
    }
}
