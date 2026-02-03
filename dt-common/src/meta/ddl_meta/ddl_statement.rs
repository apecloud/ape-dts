use serde::{Deserialize, Serialize};

use crate::{config::config_enums::DbType, utils::sql_util::SqlUtil};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum DdlStatement {
    CreateDatabase(CreateDatabaseStatement),
    DropDatabase(DropDatabaseStatement),
    AlterDatabase(AlterDatabaseStatement),

    CreateSchema(CreateSchemaStatement),
    DropSchema(DropSchemaStatement),
    AlterSchema(AlterSchemaStatement),

    MysqlCreateTable(MysqlCreateTableStatement),
    MysqlAlterTable(MysqlAlterTableStatement),
    MysqlAlterTableRename(MysqlAlterTableRenameStatement),
    MysqlTruncateTable(MysqlTruncateTableStatement),
    MysqlCreateIndex(MysqlCreateIndexStatement),
    MysqlDropIndex(MysqlDropIndexStatement),

    PgCreateTable(PgCreateTableStatement),
    PgAlterTable(PgAlterTableStatement),
    PgAlterTableRename(PgAlterTableRenameStatement),
    PgAlterTableSetSchema(PgAlterTableSetSchemaStatement),
    PgTruncateTable(PgTruncateTableStatement),
    PgCreateIndex(PgCreateIndexStatement),

    DropMultiTable(DropMultiTableStatement),
    RenameMultiTable(RenameMultiTableStatement),
    PgDropMultiIndex(PgDropMultiIndexStatement),

    DropTable(DropTableStatement),
    RenameTable(RenameTableStatement),
    PgDropIndex(PgDropIndexStatement),

    #[default]
    Unknown,
}

impl DdlStatement {
    pub fn split_to_multi(self) -> Vec<DdlStatement> {
        let mut res = Vec::new();
        match self {
            DdlStatement::DropMultiTable(s) => {
                for (schema, tb) in s.schema_tbs.iter() {
                    let statement = DropTableStatement {
                        schema: schema.clone(),
                        tb: tb.clone(),
                        if_exists: s.if_exists,
                        unparsed: s.unparsed.clone(),
                    };
                    res.push(DdlStatement::DropTable(statement));
                }
            }

            DdlStatement::RenameMultiTable(s) => {
                for (i, (schema, tb)) in s.schema_tbs.iter().enumerate() {
                    let (new_schema, new_tb) = &s.new_schema_tbs[i];
                    let statement = RenameTableStatement {
                        schema: schema.clone(),
                        tb: tb.clone(),
                        new_schema: new_schema.clone(),
                        new_tb: new_tb.clone(),
                        unparsed: s.unparsed.clone(),
                    };
                    res.push(DdlStatement::RenameTable(statement));
                }
            }

            DdlStatement::PgDropMultiIndex(s) => {
                for index_name in s.index_names.iter() {
                    let statement = PgDropIndexStatement {
                        index_name: index_name.clone(),
                        if_exists: s.if_exists,
                        is_concurrently: s.is_concurrently,
                        unparsed: s.unparsed.clone(),
                    };
                    res.push(DdlStatement::PgDropIndex(statement));
                }
            }

            _ => res.push(self),
        }
        res
    }

    pub fn get_schema_tb(&self) -> (String, String) {
        match self {
            DdlStatement::CreateDatabase(s) => (s.db.clone(), String::new()),
            DdlStatement::DropDatabase(s) => (s.db.clone(), String::new()),
            DdlStatement::AlterDatabase(s) => (s.db.clone(), String::new()),

            DdlStatement::CreateSchema(s) => (s.schema.clone(), String::new()),
            DdlStatement::DropSchema(s) => (s.schema.clone(), String::new()),
            DdlStatement::AlterSchema(s) => (s.schema.clone(), String::new()),

            DdlStatement::MysqlCreateTable(s) => (s.db.clone(), s.tb.clone()),
            DdlStatement::MysqlAlterTable(s) => (s.db.clone(), s.tb.clone()),
            DdlStatement::MysqlTruncateTable(s) => (s.db.clone(), s.tb.clone()),
            DdlStatement::MysqlCreateIndex(s) => (s.db.clone(), s.tb.clone()),
            DdlStatement::MysqlDropIndex(s) => (s.db.clone(), s.tb.clone()),

            DdlStatement::PgCreateTable(s) => (s.schema.clone(), s.tb.clone()),
            DdlStatement::PgAlterTable(s) => (s.schema.clone(), s.tb.clone()),
            DdlStatement::PgTruncateTable(s) => (s.schema.clone(), s.tb.clone()),
            DdlStatement::PgCreateIndex(s) => (s.schema.clone(), s.tb.clone()),

            DdlStatement::DropTable(s) => (s.schema.clone(), s.tb.clone()),

            DdlStatement::RenameTable(s) => (s.schema.clone(), s.tb.clone()),
            DdlStatement::MysqlAlterTableRename(s) => (s.db.clone(), s.tb.clone()),
            DdlStatement::PgAlterTableRename(s) => (s.schema.clone(), s.tb.clone()),
            DdlStatement::PgAlterTableSetSchema(s) => (s.schema.clone(), s.tb.clone()),

            DdlStatement::PgDropIndex(_)
            | DdlStatement::PgDropMultiIndex(_)
            | DdlStatement::DropMultiTable(_)
            | DdlStatement::RenameMultiTable(_)
            | DdlStatement::Unknown => (String::new(), String::new()),
        }
    }

    pub fn get_rename_to_schema_tb(&self) -> (String, String) {
        match self {
            DdlStatement::RenameTable(s) => (s.new_schema.clone(), s.new_tb.clone()),
            DdlStatement::MysqlAlterTableRename(s) => (s.new_db.clone(), s.new_tb.clone()),
            DdlStatement::PgAlterTableRename(s) => (s.new_schema.clone(), s.new_tb.clone()),
            _ => (String::new(), String::new()),
        }
    }

    pub fn route_rename_table(
        &mut self,
        dst_schema: String,
        dst_tb: String,
        dst_new_schema: String,
        dst_new_tb: String,
    ) {
        match self {
            DdlStatement::MysqlAlterTableRename(s) => {
                if !s.db.is_empty() {
                    s.db = dst_schema;
                }
                if !s.new_db.is_empty() {
                    s.new_db = dst_new_schema;
                }
                s.tb = dst_tb;
                s.new_tb = dst_new_tb;
            }

            DdlStatement::PgAlterTableRename(s) => {
                if !s.schema.is_empty() {
                    s.schema = dst_schema;
                }
                if !s.new_schema.is_empty() {
                    s.new_schema = dst_new_schema;
                }
                s.tb = dst_tb;
                s.new_tb = dst_new_tb;
            }

            DdlStatement::RenameTable(s) => {
                if !s.schema.is_empty() {
                    s.schema = dst_schema;
                }
                if !s.new_schema.is_empty() {
                    s.new_schema = dst_new_schema;
                }
                s.tb = dst_tb;
                s.new_tb = dst_new_tb;
            }

            _ => {}
        }
    }

    pub fn route(&mut self, dst_schema: String, dst_tb: String) {
        match self {
            DdlStatement::CreateDatabase(s) => {
                s.db = dst_schema;
            }
            DdlStatement::DropDatabase(s) => {
                s.db = dst_schema;
            }
            DdlStatement::AlterDatabase(s) => {
                s.db = dst_schema;
            }

            DdlStatement::CreateSchema(s) => {
                s.schema = dst_schema;
            }
            DdlStatement::DropSchema(s) => {
                s.schema = dst_schema;
            }
            DdlStatement::AlterSchema(s) => {
                s.schema = dst_schema;
            }

            DdlStatement::MysqlCreateTable(s) => {
                if !s.db.is_empty() {
                    s.db = dst_schema;
                }
                s.tb = dst_tb;
            }
            DdlStatement::MysqlAlterTable(s) => {
                if !s.db.is_empty() {
                    s.db = dst_schema;
                }
                s.tb = dst_tb;
            }
            DdlStatement::MysqlTruncateTable(s) => {
                if !s.db.is_empty() {
                    s.db = dst_schema;
                }
                s.tb = dst_tb;
            }
            DdlStatement::MysqlCreateIndex(s) => {
                if !s.db.is_empty() {
                    s.db = dst_schema;
                }
                s.tb = dst_tb;
            }
            DdlStatement::MysqlDropIndex(s) => {
                if !s.db.is_empty() {
                    s.db = dst_schema;
                }
                s.tb = dst_tb;
            }

            DdlStatement::PgCreateTable(s) => {
                if !s.schema.is_empty() {
                    s.schema = dst_schema;
                }
                s.tb = dst_tb;
            }
            DdlStatement::PgAlterTable(s) => {
                if !s.schema.is_empty() {
                    s.schema = dst_schema;
                }
                s.tb = dst_tb;
            }
            DdlStatement::PgTruncateTable(s) => {
                if !s.schema.is_empty() {
                    s.schema = dst_schema;
                }
                s.tb = dst_tb;
            }
            DdlStatement::PgCreateIndex(s) => {
                if !s.schema.is_empty() {
                    s.schema = dst_schema;
                }
                s.tb = dst_tb;
            }

            DdlStatement::DropTable(s) => {
                if !s.schema.is_empty() {
                    s.schema = dst_schema;
                }
                s.tb = dst_tb;
            }

            // not supported
            DdlStatement::RenameTable(_)
            | DdlStatement::MysqlAlterTableRename(_)
            | DdlStatement::PgAlterTableRename(_)
            | DdlStatement::PgAlterTableSetSchema(_)
            | DdlStatement::PgDropIndex(_)
            | DdlStatement::PgDropMultiIndex(_)
            | DdlStatement::DropMultiTable(_)
            | DdlStatement::RenameMultiTable(_)
            | DdlStatement::Unknown => {}
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct CreateDatabaseStatement {
    pub db: String,
    pub if_not_exists: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DropDatabaseStatement {
    pub db: String,
    pub if_exists: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AlterDatabaseStatement {
    pub db: String,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct CreateSchemaStatement {
    pub schema: String,
    pub if_not_exists: bool,
    pub authorization: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DropSchemaStatement {
    pub schema: String,
    pub if_exists: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AlterSchemaStatement {
    pub schema: String,
    pub new_schema: Option<String>,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MysqlCreateTableStatement {
    pub db: String,
    pub tb: String,
    pub if_not_exists: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PgCreateTableStatement {
    pub schema: String,
    pub tb: String,
    pub temporary: Option<String>,
    pub unlogged: Option<String>,
    pub if_not_exists: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DropMultiTableStatement {
    pub schema_tbs: Vec<(String, String)>,
    pub if_exists: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DropTableStatement {
    pub schema: String,
    pub tb: String,
    pub if_exists: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MysqlAlterTableStatement {
    pub db: String,
    pub tb: String,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MysqlAlterTableRenameStatement {
    pub db: String,
    pub tb: String,
    pub new_db: String,
    pub new_tb: String,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PgAlterTableStatement {
    pub schema: String,
    pub tb: String,
    pub if_exists: bool,
    pub is_only: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PgAlterTableRenameStatement {
    pub schema: String,
    pub tb: String,
    pub new_schema: String,
    pub new_tb: String,
    pub if_exists: bool,
    pub is_only: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PgAlterTableSetSchemaStatement {
    pub schema: String,
    pub tb: String,
    pub new_schema: String,
    pub new_tb: String,
    pub if_exists: bool,
    pub is_only: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MysqlTruncateTableStatement {
    pub db: String,
    pub tb: String,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PgTruncateTableStatement {
    pub schema: String,
    pub tb: String,
    pub is_only: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RenameMultiTableStatement {
    pub schema_tbs: Vec<(String, String)>,
    pub new_schema_tbs: Vec<(String, String)>,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RenameTableStatement {
    pub schema: String,
    pub tb: String,
    pub new_schema: String,
    pub new_tb: String,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MysqlCreateIndexStatement {
    pub db: String,
    pub tb: String,
    pub index_name: String,
    pub index_kind: Option<String>,
    pub index_type: Option<String>,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PgCreateIndexStatement {
    pub schema: String,
    pub tb: String,
    pub index_name: Option<String>,
    pub is_unique: bool,
    pub is_concurrently: bool,
    pub if_not_exists: bool,
    pub is_only: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MysqlDropIndexStatement {
    pub db: String,
    pub tb: String,
    pub index_name: String,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PgDropMultiIndexStatement {
    pub index_names: Vec<String>,
    pub if_exists: bool,
    pub is_concurrently: bool,
    pub unparsed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PgDropIndexStatement {
    pub index_name: String,
    pub if_exists: bool,
    pub is_concurrently: bool,
    pub unparsed: String,
}

impl DdlStatement {
    pub fn to_sql(&self, db_type: &DbType) -> String {
        match self {
            DdlStatement::CreateDatabase(s) => {
                let mut sql = "CREATE DATABASE".to_string();
                if s.if_not_exists {
                    sql = format!("{} IF NOT EXISTS", sql);
                }
                sql = append_identifier(&sql, &s.db, true, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::DropDatabase(s) => {
                let mut sql = "DROP DATABASE".to_string();
                if s.if_exists {
                    sql = format!("{} IF EXISTS", sql);
                }
                sql = append_identifier(&sql, &s.db, true, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::AlterDatabase(s) => {
                let mut sql = "ALTER DATABASE".to_string();
                sql = append_identifier(&sql, &s.db, true, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::CreateSchema(s) => {
                let mut sql = "CREATE SCHEMA".to_string();
                if s.if_not_exists {
                    sql = format!("{} IF NOT EXISTS", sql);
                }
                if s.authorization {
                    sql = format!("{} AUTHORIZATION", sql);
                }
                sql = append_identifier(&sql, &s.schema, true, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::DropSchema(s) => {
                let mut sql = "DROP SCHEMA".to_string();
                if s.if_exists {
                    sql = format!("{} IF EXISTS", sql);
                }
                sql = append_identifier(&sql, &s.schema, true, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::AlterSchema(s) => {
                let mut sql = "ALTER SCHEMA".to_string();
                sql = append_identifier(&sql, &s.schema, true, db_type);
                if let Some(new_schema) = &s.new_schema {
                    sql = format!("{} RENAME TO", sql);
                    append_identifier(&sql, new_schema, true, db_type)
                } else {
                    append_unparsed(sql, &s.unparsed)
                }
            }

            DdlStatement::MysqlCreateTable(s) => {
                let mut sql = "CREATE TABLE".to_string();
                if s.if_not_exists {
                    sql = format!("{} IF NOT EXISTS", sql);
                }
                sql = append_tb(&sql, &s.db, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::PgCreateTable(s) => {
                let mut sql = "CREATE".to_string();
                sql = append_opt_str(&sql, &s.temporary);
                sql = append_opt_str(&sql, &s.unlogged);
                sql = format!("{} TABLE", sql);
                if s.if_not_exists {
                    sql = format!("{} IF NOT EXISTS", sql);
                }
                sql = append_tb(&sql, &s.schema, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::DropMultiTable(s) => s.to_sql(db_type),

            DdlStatement::DropTable(s) => {
                let multi_s = DropMultiTableStatement {
                    if_exists: s.if_exists,
                    schema_tbs: vec![(s.schema.clone(), s.tb.clone())],
                    unparsed: s.unparsed.clone(),
                };
                multi_s.to_sql(db_type)
            }

            DdlStatement::MysqlTruncateTable(s) => {
                let mut sql = "TRUNCATE TABLE".to_string();
                sql = append_tb(&sql, &s.db, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::PgTruncateTable(s) => {
                let mut sql = "TRUNCATE TABLE".to_string();
                if s.is_only {
                    sql = format!("{} ONLY", sql);
                }
                sql = append_tb(&sql, &s.schema, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::MysqlAlterTable(s) => {
                let mut sql = "ALTER TABLE".to_string();
                sql = append_tb(&sql, &s.db, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::MysqlAlterTableRename(s) => {
                let mut sql = "ALTER TABLE".to_string();
                sql = append_tb(&sql, &s.db, &s.tb, db_type);
                sql = format!("{} RENAME TO", sql);
                sql = append_tb(&sql, &s.new_db, &s.new_tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::PgAlterTable(s) => {
                let mut sql = "ALTER TABLE".to_string();
                if s.if_exists {
                    sql = format!("{} IF EXISTS", sql);
                }
                if s.is_only {
                    sql = format!("{} ONLY", sql);
                }
                sql = append_tb(&sql, &s.schema, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::PgAlterTableRename(s) => {
                let mut sql = "ALTER TABLE".to_string();
                if s.if_exists {
                    sql = format!("{} IF EXISTS", sql);
                }
                if s.is_only {
                    sql = format!("{} ONLY", sql);
                }
                sql = append_tb(&sql, &s.schema, &s.tb, db_type);
                sql = format!("{} RENAME TO", sql);
                sql = append_tb(&sql, &s.new_schema, &s.new_tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::PgAlterTableSetSchema(s) => {
                let mut sql = "ALTER TABLE".to_string();
                if s.if_exists {
                    sql = format!("{} IF EXISTS", sql);
                }
                if s.is_only {
                    sql = format!("{} ONLY", sql);
                }
                sql = append_tb(&sql, &s.schema, &s.tb, db_type);
                sql = format!("{} SET SCHEMA", sql);
                sql = append_identifier(&sql, &s.new_schema, true, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::RenameMultiTable(s) => s.to_sql(db_type),

            DdlStatement::RenameTable(s) => {
                let multi_s = RenameMultiTableStatement {
                    schema_tbs: vec![(s.schema.clone(), s.tb.clone())],
                    new_schema_tbs: vec![(s.new_schema.clone(), s.new_tb.clone())],
                    unparsed: s.unparsed.clone(),
                };
                multi_s.to_sql(db_type)
            }

            DdlStatement::MysqlCreateIndex(s) => {
                let mut sql = "CREATE".to_string();
                if let Some(index_kind) = &s.index_kind {
                    sql = format!("{} {}", sql, index_kind.to_uppercase());
                }
                sql = format!("{} INDEX", sql);
                sql = append_identifier(&sql, &s.index_name, true, db_type);
                if let Some(index_type) = &s.index_type {
                    sql = format!("{} USING {}", sql, index_type.to_uppercase());
                }
                sql = format!("{} ON", sql);
                sql = append_tb(&sql, &s.db, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::PgCreateIndex(s) => {
                let mut sql = "CREATE".to_string();
                if s.is_unique {
                    sql = format!("{} UNIQUE", sql);
                }
                sql = format!("{} INDEX", sql);
                if s.is_concurrently {
                    sql = format!("{} CONCURRENTLY", sql);
                }
                if s.if_not_exists {
                    sql = format!("{} IF NOT EXISTS", sql);
                }
                if let Some(index_name) = &s.index_name {
                    sql = append_identifier(&sql, index_name, true, db_type);
                }
                sql = format!("{} ON", sql);
                if s.is_only {
                    sql = format!("{} ONLY", sql);
                }
                sql = append_tb(&sql, &s.schema, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::MysqlDropIndex(s) => {
                let mut sql = "DROP INDEX".to_string();
                sql = append_identifier(&sql, &s.index_name, true, db_type);
                sql = format!("{} ON", sql);
                sql = append_tb(&sql, &s.db, &s.tb, db_type);
                append_unparsed(sql, &s.unparsed)
            }

            DdlStatement::PgDropMultiIndex(s) => s.to_sql(db_type),

            DdlStatement::PgDropIndex(s) => {
                let multi_s = PgDropMultiIndexStatement {
                    if_exists: s.if_exists,
                    is_concurrently: s.is_concurrently,
                    unparsed: s.unparsed.clone(),
                    index_names: vec![s.index_name.clone()],
                };
                multi_s.to_sql(db_type)
            }

            _ => String::new(),
        }
    }

    pub fn get_malloc_size(&self) -> u64 {
        let mut size = 0;
        match &self {
            DdlStatement::CreateDatabase(create_database_statement) => {
                size += create_database_statement.db.len() as u64;
                size += create_database_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::DropDatabase(drop_database_statement) => {
                size += drop_database_statement.db.len() as u64;
                size += drop_database_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::AlterDatabase(alter_database_statement) => {
                size += alter_database_statement.db.len() as u64;
                size += alter_database_statement.unparsed.len() as u64;
            }
            DdlStatement::CreateSchema(create_schema_statement) => {
                size += create_schema_statement.schema.len() as u64;
                size += create_schema_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::DropSchema(drop_schema_statement) => {
                size += drop_schema_statement.schema.len() as u64;
                size += drop_schema_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::AlterSchema(alter_schema_statement) => {
                size += alter_schema_statement.schema.len() as u64;
                size += alter_schema_statement.unparsed.len() as u64;
                size += std::mem::size_of::<Option<String>>() as u64;
                size += alter_schema_statement
                    .new_schema
                    .as_ref()
                    .map_or(0, |s| s.len() as u64);
            }
            DdlStatement::MysqlCreateTable(mysql_create_table_statement) => {
                size += mysql_create_table_statement.db.len() as u64;
                size += mysql_create_table_statement.tb.len() as u64;
                size += mysql_create_table_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::MysqlAlterTable(mysql_alter_table_statement) => {
                size += mysql_alter_table_statement.db.len() as u64;
                size += mysql_alter_table_statement.tb.len() as u64;
                size += mysql_alter_table_statement.unparsed.len() as u64;
            }
            DdlStatement::MysqlAlterTableRename(mysql_alter_table_rename_statement) => {
                size += mysql_alter_table_rename_statement.db.len() as u64;
                size += mysql_alter_table_rename_statement.tb.len() as u64;
                size += mysql_alter_table_rename_statement.new_db.len() as u64;
                size += mysql_alter_table_rename_statement.new_tb.len() as u64;
                size += mysql_alter_table_rename_statement.unparsed.len() as u64;
            }
            DdlStatement::MysqlTruncateTable(mysql_truncate_table_statement) => {
                size += mysql_truncate_table_statement.db.len() as u64;
                size += mysql_truncate_table_statement.tb.len() as u64;
                size += mysql_truncate_table_statement.unparsed.len() as u64;
            }
            DdlStatement::PgCreateTable(pg_create_table_statement) => {
                size += pg_create_table_statement.schema.len() as u64;
                size += pg_create_table_statement.tb.len() as u64;
                size += pg_create_table_statement.unparsed.len() as u64;
                size += std::mem::size_of::<Option<String>>() as u64 * 2;
                size += pg_create_table_statement
                    .temporary
                    .as_ref()
                    .map_or(0, |s| s.len() as u64);
                size += pg_create_table_statement
                    .unlogged
                    .as_ref()
                    .map_or(0, |s| s.len() as u64);
                size += 1;
            }
            DdlStatement::PgAlterTable(pg_alter_table_statement) => {
                size += pg_alter_table_statement.schema.len() as u64;
                size += pg_alter_table_statement.tb.len() as u64;
                size += pg_alter_table_statement.unparsed.len() as u64;
                size += 2;
            }
            DdlStatement::PgAlterTableRename(pg_alter_table_rename_statement) => {
                size += pg_alter_table_rename_statement.schema.len() as u64;
                size += pg_alter_table_rename_statement.tb.len() as u64;
                size += pg_alter_table_rename_statement.new_schema.len() as u64;
                size += pg_alter_table_rename_statement.new_tb.len() as u64;
                size += pg_alter_table_rename_statement.unparsed.len() as u64;
                size += 2;
            }
            DdlStatement::PgAlterTableSetSchema(pg_alter_table_set_schema_statement) => {
                size += pg_alter_table_set_schema_statement.schema.len() as u64;
                size += pg_alter_table_set_schema_statement.tb.len() as u64;
                size += pg_alter_table_set_schema_statement.new_schema.len() as u64;
                size += pg_alter_table_set_schema_statement.new_tb.len() as u64;
                size += pg_alter_table_set_schema_statement.unparsed.len() as u64;
                size += 2;
            }
            DdlStatement::PgTruncateTable(pg_truncate_table_statement) => {
                size += pg_truncate_table_statement.schema.len() as u64;
                size += pg_truncate_table_statement.tb.len() as u64;
                size += pg_truncate_table_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::PgCreateIndex(pg_create_index_statement) => {
                size += pg_create_index_statement.schema.len() as u64;
                size += pg_create_index_statement.tb.len() as u64;
                size += pg_create_index_statement.unparsed.len() as u64;
                size += std::mem::size_of::<Option<String>>() as u64;
                size += pg_create_index_statement
                    .index_name
                    .as_ref()
                    .map_or(0, |s| s.len() as u64);
                size += 4;
            }
            DdlStatement::PgDropIndex(pg_drop_index_statement) => {
                size += pg_drop_index_statement.index_name.len() as u64;
                size += pg_drop_index_statement.unparsed.len() as u64;
                size += 2;
            }
            DdlStatement::PgDropMultiIndex(pg_drop_multi_index_statement) => {
                size += std::mem::size_of::<Vec<String>>() as u64;
                size += pg_drop_multi_index_statement
                    .index_names
                    .iter()
                    .map(|s| s.len() as u64)
                    .sum::<u64>();
                size += pg_drop_multi_index_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::DropMultiTable(drop_multi_table_statement) => {
                size += std::mem::size_of::<Vec<(String, String)>>() as u64;
                size += drop_multi_table_statement
                    .schema_tbs
                    .iter()
                    .map(|(s, t)| s.len() as u64 + t.len() as u64)
                    .sum::<u64>();
                size += drop_multi_table_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::RenameMultiTable(rename_multi_table_statement) => {
                size += std::mem::size_of::<Vec<(String, String)>>() as u64 * 2;
                size += rename_multi_table_statement
                    .schema_tbs
                    .iter()
                    .map(|(s1, s2)| s1.len() as u64 + s2.len() as u64)
                    .sum::<u64>();
                size += rename_multi_table_statement
                    .new_schema_tbs
                    .iter()
                    .map(|(s1, s2)| s1.len() as u64 + s2.len() as u64)
                    .sum::<u64>();
                size += rename_multi_table_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::DropTable(drop_table_statement) => {
                size += drop_table_statement.schema.len() as u64;
                size += drop_table_statement.tb.len() as u64;
                size += drop_table_statement.unparsed.len() as u64;
                size += 1;
            }
            DdlStatement::RenameTable(rename_table_statement) => {
                size += rename_table_statement.schema.len() as u64;
                size += rename_table_statement.tb.len() as u64;
                size += rename_table_statement.new_schema.len() as u64;
                size += rename_table_statement.new_tb.len() as u64;
                size += rename_table_statement.unparsed.len() as u64;
            }
            DdlStatement::MysqlCreateIndex(mysql_create_index_statement) => {
                size += mysql_create_index_statement.db.len() as u64;
                size += mysql_create_index_statement.tb.len() as u64;
                size += mysql_create_index_statement.index_name.len() as u64;
                size += mysql_create_index_statement.unparsed.len() as u64;
                size += std::mem::size_of::<Option<String>>() as u64 * 2;
                size += mysql_create_index_statement
                    .index_kind
                    .as_ref()
                    .map_or(0, |s| s.len() as u64);
                size += mysql_create_index_statement
                    .index_type
                    .as_ref()
                    .map_or(0, |s| s.len() as u64);
            }
            DdlStatement::MysqlDropIndex(mysql_drop_index_statement) => {
                size += mysql_drop_index_statement.db.len() as u64;
                size += mysql_drop_index_statement.tb.len() as u64;
                size += mysql_drop_index_statement.index_name.len() as u64;
                size += mysql_drop_index_statement.unparsed.len() as u64;
            }
            DdlStatement::Unknown => {}
        }
        size
    }
}

impl DropMultiTableStatement {
    pub fn to_sql(&self, db_type: &DbType) -> String {
        let mut sql = "DROP TABLE".to_string();
        if self.if_exists {
            sql = format!("{} IF EXISTS", sql);
        }

        for (schema, tb) in self.schema_tbs.iter() {
            sql = append_tb(&sql, schema, tb, db_type);
        }
        append_unparsed(sql, &self.unparsed)
    }
}

impl RenameMultiTableStatement {
    pub fn to_sql(&self, db_type: &DbType) -> String {
        let mut sql = "RENAME TABLE".to_string();
        for (i, (schema, tb)) in self.schema_tbs.iter().enumerate() {
            let (new_schema, new_tb) = &self.new_schema_tbs[i];
            sql = append_tb(&sql, schema, tb, db_type);
            sql = format!("{} TO", sql);
            sql = append_tb(&sql, new_schema, new_tb, db_type);
            if i < self.schema_tbs.len() - 1 {
                sql = format!("{},", sql);
            }
        }
        sql
    }
}

impl PgDropMultiIndexStatement {
    pub fn to_sql(&self, db_type: &DbType) -> String {
        let mut sql = "DROP INDEX".to_string();
        if self.is_concurrently {
            sql = format!("{} CONCURRENTLY", sql);
        }
        if self.if_exists {
            sql = format!("{} IF EXISTS", sql);
        }
        for (i, name) in self.index_names.iter().enumerate() {
            sql = append_identifier(&sql, name, true, db_type);
            if i < self.index_names.len() - 1 {
                sql = format!("{},", sql);
            }
        }
        append_unparsed(sql, &self.unparsed)
    }
}

fn append_tb(sql: &str, schema: &str, tb: &str, db_type: &DbType) -> String {
    let tb = escape_identifier(tb, db_type);
    if schema.is_empty() {
        format!("{} {}", sql, tb)
    } else {
        let schema = escape_identifier(schema, db_type);
        format!("{} {}.{}", sql, schema, tb)
    }
}

fn append_opt_str(sql: &str, opt_str: &Option<String>) -> String {
    if let Some(s) = opt_str {
        format!("{} {}", sql, s)
    } else {
        sql.to_string()
    }
}

fn append_identifier(
    sql: &str,
    identifier: &str,
    with_white_space: bool,
    db_type: &DbType,
) -> String {
    let identifier = escape_identifier(identifier, db_type);
    if with_white_space {
        format!("{} {}", sql, identifier)
    } else {
        format!("{}{}", sql, identifier)
    }
}

fn append_unparsed(sql: String, unparsed: &str) -> String {
    if !unparsed.is_empty() {
        return format!("{} {}", sql, unparsed);
    }
    sql
}

fn escape_identifier(identifier: &str, db_type: &DbType) -> String {
    SqlUtil::escape_by_db_type(identifier, db_type)
}
