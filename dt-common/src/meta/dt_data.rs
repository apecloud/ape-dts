use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{ddl_meta::ddl_data::DdlData, row_data::RowData, struct_meta::struct_data::StructData};
use crate::{
    meta::{
        dcl_meta::dcl_data::DclData,
        position::Position,
        redis::redis_entry::RedisEntry,
        row_type::RowSqlType,
        struct_meta::{
            statement::struct_statement::StructStatement, structure::constraint::ConstraintType,
        },
    },
    queue::dependency_queue::{DependencyKey, StructDependencyKey, StructObjectType},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DtItem {
    pub dt_data: DtData,
    pub position: Position,
    pub data_origin_node: String,
}

impl DtItem {
    pub fn is_ddl(&self) -> bool {
        self.dt_data.is_ddl()
    }

    pub fn is_dcl(&self) -> bool {
        self.dt_data.is_dcl()
    }

    pub fn get_row_sql_type(&self) -> RowSqlType {
        match &self.dt_data {
            DtData::Ddl { .. } => RowSqlType::DDL,
            DtData::Dcl { .. } => RowSqlType::DCL,
            _ => RowSqlType::DML,
        }
    }

    pub fn get_data_size(&self) -> u64 {
        self.dt_data.get_data_size()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DtData {
    Struct {
        struct_data: StructData,
    },
    Ddl {
        ddl_data: DdlData,
    },
    Dcl {
        dcl_data: DclData,
    },
    Dml {
        row_data: RowData,
    },
    Begin {},
    Commit {
        xid: String,
    },
    Heartbeat {},
    #[serde(skip)]
    Redis {
        entry: RedisEntry,
    },
}

impl DtData {
    pub fn is_begin(&self) -> bool {
        matches!(self, DtData::Begin { .. })
    }

    pub fn is_commit(&self) -> bool {
        matches!(self, DtData::Commit { .. })
    }

    pub fn is_ddl(&self) -> bool {
        matches!(self, DtData::Ddl { .. })
    }

    pub fn is_dcl(&self) -> bool {
        matches!(self, DtData::Dcl { .. })
    }

    pub fn get_data_size(&self) -> u64 {
        match &self {
            DtData::Dml { row_data } => row_data.data_size as u64,
            DtData::Dcl { dcl_data } => dcl_data.get_malloc_size(),
            DtData::Ddl { ddl_data } => ddl_data.get_malloc_size(),
            DtData::Redis { entry } => entry.get_data_malloc_size() as u64,
            // ignore other item types
            _ => 0,
        }
    }

    pub fn get_data_count(&self) -> usize {
        match &self {
            DtData::Begin {} | DtData::Commit { .. } | DtData::Heartbeat {} => 0,
            _ => 1,
        }
    }

    pub fn object_key(&self) -> anyhow::Result<DependencyKey> {
        let DtData::Struct { struct_data } = self else {
            return Ok(DependencyKey::None);
        };

        use crate::meta::struct_meta::statement::struct_statement::StructStatement;
        let key = match &struct_data.statement {
            StructStatement::MysqlCreateDatabase(statement) => {
                struct_key(StructObjectType::Schema, "", &statement.database.name)
            }
            StructStatement::PgCreateSchema(statement) => {
                struct_key(StructObjectType::Schema, "", &statement.schema.name)
            }
            StructStatement::MysqlCreateTable(statement) => struct_key(
                StructObjectType::Table,
                &statement.table.database_name,
                &statement.table.table_name,
            ),
            StructStatement::PgCreateTable(statement) => struct_key(
                StructObjectType::Table,
                &statement.table.schema_name,
                &statement.table.table_name,
            ),
            StructStatement::MongoCreateCollection(statement) => struct_key(
                StructObjectType::Collection,
                &statement.database_name,
                &statement.collection_name,
            ),
            StructStatement::MongoShardKey(statement) => {
                let (database, collection) = split_namespace(&statement.shard_collection.ns);
                struct_key(StructObjectType::ShardKey, database, collection)
            }
            StructStatement::PgCreateUdf(statement) => struct_key(
                StructObjectType::Udf,
                &statement.udf.schema_name,
                &format!(
                    "{}({})",
                    statement.udf.function_name, statement.udf.identity_arguments
                ),
            ),
            StructStatement::PgCreateUdt(statement) => struct_key(
                StructObjectType::Udt,
                &statement.udt.schema_name,
                &statement.udt.typ_name,
            ),
            StructStatement::PgCreateRbac(_) => struct_key(StructObjectType::Rbac, "", "global"),
            StructStatement::Unknown => DependencyKey::None,
        };
        Ok(key)
    }

    pub fn parent_object_keys(&self) -> Result<Vec<DependencyKey>> {
        // TODO: more data types should support
        let DtData::Struct { struct_data } = self else {
            return Ok(Vec::new());
        };

        let mut keys = Vec::new();
        match &struct_data.statement {
            StructStatement::MysqlCreateTable(statement) => {
                keys.push(struct_key(
                    StructObjectType::Schema,
                    "",
                    &statement.table.database_name,
                ));
                keys.extend(
                    statement
                        .constraints
                        .iter()
                        .filter(|constraint| {
                            constraint.constraint_type == ConstraintType::Foreign
                                && !constraint.referenced_table_name.is_empty()
                        })
                        .map(|constraint| {
                            struct_key(
                                StructObjectType::Table,
                                &constraint.referenced_database_name,
                                &constraint.referenced_table_name,
                            )
                        }),
                );
            }
            StructStatement::PgCreateTable(statement) => {
                keys.push(struct_key(
                    StructObjectType::Schema,
                    "",
                    &statement.table.schema_name,
                ));
                keys.extend(
                    statement
                        .constraints
                        .iter()
                        .filter(|constraint| {
                            constraint.constraint_type == ConstraintType::Foreign
                                && !constraint.referenced_table_name.is_empty()
                        })
                        .map(|constraint| {
                            struct_key(
                                StructObjectType::Table,
                                &constraint.referenced_schema_name,
                                &constraint.referenced_table_name,
                            )
                        }),
                );
            }
            StructStatement::MongoShardKey(statement) => {
                let (database, collection) = split_namespace(&statement.shard_collection.ns);
                keys.push(struct_key(
                    StructObjectType::Collection,
                    database,
                    collection,
                ));
            }
            _ => {}
        }
        Ok(keys)
    }
}

fn struct_key(object_type: StructObjectType, schema: &str, name: &str) -> DependencyKey {
    DependencyKey::Struct(StructDependencyKey::Name {
        object_type,
        schema: Arc::from(schema),
        name: Arc::from(name),
    })
}

fn split_namespace(namespace: &str) -> (&str, &str) {
    namespace.split_once('.').unwrap_or(("", namespace))
}

impl std::fmt::Display for DtData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}
