use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{ddl_statement::DdlStatement, ddl_type::DdlType};
use crate::{config::config_enums::DbType, resource::RID};

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
        self.statement.to_sql(&self.db_type)
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

    /// Namespace DDL omits the table: use `into_left()` for fixed levels; tuples right-align.
    pub fn get_id<T: From<RID>>(&self) -> T {
        let (mut schema, tb) = self.statement.get_schema_tb();
        if schema.is_empty() {
            schema = self.default_schema.clone();
        }
        RID::new([self.default_db.clone(), schema, tb]).into()
    }

    pub fn get_rename_id<T: From<RID>>(&self) -> T {
        let (mut schema, tb) = self.statement.get_rename_to_schema_tb();
        if schema.is_empty() {
            schema = self.default_schema.clone();
        }
        RID::new([self.default_db.clone(), schema, tb]).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        meta::ddl_meta::ddl_statement::{
            CreateDatabaseStatement, CreateSchemaStatement, DropMultiTableStatement,
            PgDropIndexStatement, RenameMultiTableStatement, RenameTableStatement,
        },
        rid,
    };

    #[test]
    fn generic_ids_resolve_defaults_and_keep_explicit_namespaces() {
        for (db, schema, new_schema, expected_schema, expected_new_schema) in [
            ("", "", "", "inventory", "inventory"),
            ("", "sales.eu", "archive", "sales.eu", "archive"),
            ("market", "", "archive", "inventory", "archive"),
            ("market", "sales", "", "sales", "inventory"),
        ] {
            let ddl = DdlData {
                default_db: db.into(),
                default_schema: "inventory".into(),
                statement: DdlStatement::RenameTable(RenameTableStatement {
                    schema: schema.into(),
                    tb: "orders".into(),
                    new_schema: new_schema.into(),
                    new_tb: "events".into(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            assert_eq!(ddl.get_id::<RID>(), rid!(db, expected_schema, "orders"));
            assert_eq!(
                ddl.get_rename_id::<RID>(),
                rid!(db, expected_new_schema, "events")
            );
        }
    }

    #[test]
    fn namespace_ids_keep_their_resource_depth() {
        for (db, statement, expected, name) in [
            (
                "",
                DdlStatement::CreateDatabase(CreateDatabaseStatement {
                    db: "inventory".into(),
                    ..Default::default()
                }),
                rid!("inventory"),
                "inventory",
            ),
            (
                "",
                DdlStatement::CreateSchema(CreateSchemaStatement {
                    schema: "inventory".into(),
                    ..Default::default()
                }),
                rid!("inventory"),
                "inventory",
            ),
            (
                "market",
                DdlStatement::CreateSchema(CreateSchemaStatement {
                    schema: "inventory".into(),
                    ..Default::default()
                }),
                rid!("market", "inventory"),
                "inventory",
            ),
            (
                "",
                DdlStatement::DropMultiTable(DropMultiTableStatement {
                    schema_tbs: vec![(String::new(), "orders".into())],
                    ..Default::default()
                }),
                rid!("fallback"),
                "fallback",
            ),
            (
                "",
                DdlStatement::RenameMultiTable(RenameMultiTableStatement {
                    schema_tbs: vec![(String::new(), "orders".into())],
                    new_schema_tbs: vec![(String::new(), "events".into())],
                    ..Default::default()
                }),
                rid!("fallback"),
                "fallback",
            ),
            (
                "",
                DdlStatement::PgDropIndex(PgDropIndexStatement {
                    index_name: "orders_idx".into(),
                    ..Default::default()
                }),
                rid!("fallback"),
                "fallback",
            ),
        ] {
            let ddl = DdlData {
                default_db: db.into(),
                default_schema: "fallback".into(),
                statement,
                ..Default::default()
            };
            assert_eq!(ddl.get_id::<RID>(), expected);
            assert_eq!(ddl.get_id::<String>(), name);
        }
        assert!(DdlData::default().get_id::<RID>().is_empty());
        assert!(DdlData::default().get_rename_id::<RID>().is_empty());
    }
}
