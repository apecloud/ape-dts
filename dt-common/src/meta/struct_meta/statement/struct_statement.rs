use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
};

use strum::Display as StrumDisplay;

use super::{
    mongo_create_collection_statement::MongoCreateCollectionStatement,
    mongo_shard_key_statement::MongoShardKeyStatement,
    mssql_create_database_statement::MssqlCreateDatabaseStatement,
    mssql_create_schema_statement::MssqlCreateSchemaStatement,
    mssql_create_table_statement::MssqlCreateTableStatement,
    mysql_create_database_statement::MysqlCreateDatabaseStatement,
    mysql_create_table_statement::MysqlCreateTableStatement,
    pg_create_rbac_statement::PgCreateRbacStatement,
    pg_create_schema_statement::PgCreateSchemaStatement,
    pg_create_table_statement::PgCreateTableStatement,
};
use crate::{
    meta::struct_meta::statement::{
        pg_create_udf_statement::PgCreateUdfStatement,
        pg_create_udt_statement::PgCreateUdtStatement,
    },
    rdb_filter::RdbFilter,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, StrumDisplay)]
pub enum StructKeyType {
    #[strum(serialize = "database")]
    Database,
    #[strum(serialize = "schema")]
    Schema,
    #[strum(serialize = "table")]
    Table,
    #[strum(serialize = "index")]
    Index,
    #[strum(serialize = "constraint")]
    Constraint,
    #[strum(serialize = "sequence")]
    Sequence,
    #[strum(serialize = "sequence_owner")]
    SequenceOwner,
    #[strum(serialize = "column_comment")]
    ColumnComment,
    #[strum(serialize = "table_comment")]
    TableComment,
    #[strum(serialize = "database_comment")]
    DatabaseComment,
    #[strum(serialize = "schema_comment")]
    SchemaComment,
    #[strum(serialize = "sequence_comment")]
    SequenceComment,
    #[strum(serialize = "constraint_comment")]
    ConstraintComment,
    #[strum(serialize = "index_comment")]
    IndexComment,
    #[strum(serialize = "udt")]
    Udt,
    #[strum(serialize = "udf")]
    Udf,
    #[strum(serialize = "rbac.role")]
    RbacRole,
    #[strum(serialize = "rbac.role_config")]
    RbacRoleConfig,
    #[strum(serialize = "rbac.member")]
    RbacMember,
    #[strum(serialize = "rbac.privilege.schema")]
    RbacPrivilegeSchema,
    #[strum(serialize = "rbac.privilege.table")]
    RbacPrivilegeTable,
    #[strum(serialize = "rbac.privilege.column")]
    RbacPrivilegeColumn,
    #[strum(serialize = "rbac.privilege.sequence")]
    RbacPrivilegeSequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructKey {
    key_type: StructKeyType,
    database: Option<String>,
    segments: Vec<String>,
}

impl StructKey {
    pub fn new<I, S>(key_type: StructKeyType, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            key_type,
            database: None,
            segments: segments.into_iter().map(Into::into).collect(),
        }
    }

    pub fn with_database(mut self, database: &str) -> Self {
        self.database = Some(database.to_string());
        self
    }

    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn schema(&self) -> &str {
        self.key_type
            .schema_index()
            .and_then(|index| self.segments.get(index))
            .map(String::as_str)
            .unwrap_or_default()
    }

    pub fn table(&self) -> &str {
        self.key_type
            .table_index()
            .and_then(|index| self.segments.get(index))
            .map(String::as_str)
            .unwrap_or_default()
    }

    pub fn with_location(&self, schema: &str, table: &str) -> Self {
        let mut key = self.clone();
        if let Some(segment) = self
            .key_type
            .schema_index()
            .and_then(|i| key.segments.get_mut(i))
        {
            *segment = schema.to_string();
        }
        if let Some(segment) = self
            .key_type
            .table_index()
            .and_then(|i| key.segments.get_mut(i))
        {
            *segment = table.to_string();
        }
        key
    }

    pub fn is_table_scoped(&self) -> bool {
        self.key_type.is_table_scoped()
    }

    pub fn is_sequence(&self) -> bool {
        matches!(
            self.key_type,
            StructKeyType::Sequence | StructKeyType::SequenceComment
        )
    }
}

impl Display for StructKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key_type)?;
        if let Some(database) = &self.database {
            write!(f, ".{database}")?;
        }
        for segment in &self.segments {
            write!(f, ".{segment}")?;
        }
        Ok(())
    }
}

impl Ord for StructKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_string()
            .cmp(&other.to_string())
            .then_with(|| self.key_type.cmp(&other.key_type))
            .then_with(|| self.database.cmp(&other.database))
            .then_with(|| self.segments.cmp(&other.segments))
    }
}

impl PartialOrd for StructKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl StructKeyType {
    pub fn schema_index(self) -> Option<usize> {
        match self {
            Self::Database | Self::Schema | Self::SchemaComment => Some(0),
            Self::DatabaseComment => None,
            Self::Table
            | Self::Index
            | Self::Constraint
            | Self::Sequence
            | Self::SequenceOwner
            | Self::ColumnComment
            | Self::TableComment
            | Self::SequenceComment
            | Self::ConstraintComment
            | Self::IndexComment
            | Self::Udt
            | Self::Udf => Some(0),
            Self::RbacPrivilegeSchema
            | Self::RbacPrivilegeTable
            | Self::RbacPrivilegeColumn
            | Self::RbacPrivilegeSequence => Some(0),
            Self::RbacRole | Self::RbacRoleConfig | Self::RbacMember => None,
        }
    }

    pub fn table_index(self) -> Option<usize> {
        match self {
            Self::Table
            | Self::Index
            | Self::Constraint
            | Self::Sequence
            | Self::SequenceOwner
            | Self::ColumnComment
            | Self::TableComment
            | Self::ConstraintComment
            | Self::IndexComment
            | Self::SequenceComment => Some(1),
            Self::RbacPrivilegeTable | Self::RbacPrivilegeColumn | Self::RbacPrivilegeSequence => {
                Some(1)
            }
            _ => None,
        }
    }

    pub fn is_table_scoped(self) -> bool {
        matches!(
            self,
            Self::Table
                | Self::Index
                | Self::Constraint
                | Self::SequenceOwner
                | Self::ColumnComment
                | Self::TableComment
                | Self::ConstraintComment
                | Self::IndexComment
                | Self::RbacPrivilegeTable
                | Self::RbacPrivilegeColumn
                | Self::RbacPrivilegeSequence
        )
    }
}

#[derive(Debug, Clone, Default)]
pub enum StructStatement {
    MssqlCreateDatabase(MssqlCreateDatabaseStatement),
    MssqlCreateSchema(MssqlCreateSchemaStatement),
    MssqlCreateTable(MssqlCreateTableStatement),
    MysqlCreateDatabase(MysqlCreateDatabaseStatement),
    PgCreateSchema(PgCreateSchemaStatement),
    MysqlCreateTable(MysqlCreateTableStatement),
    MongoCreateCollection(MongoCreateCollectionStatement),
    MongoShardKey(MongoShardKeyStatement),
    PgCreateTable(PgCreateTableStatement),
    PgCreateRbac(PgCreateRbacStatement),
    PgCreateUdf(PgCreateUdfStatement),
    PgCreateUdt(PgCreateUdtStatement),
    #[default]
    Unknown,
}

impl StructStatement {
    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(StructKey, String)>> {
        match self {
            Self::MssqlCreateDatabase(s) => s.to_sqls(filter),
            Self::MssqlCreateSchema(s) => s.to_sqls(filter),
            Self::MssqlCreateTable(s) => s.to_sqls(filter),
            Self::MysqlCreateDatabase(s) => s.to_sqls(filter),
            Self::PgCreateSchema(s) => s.to_sqls(filter),
            Self::MysqlCreateTable(s) => s.to_sqls(filter),
            Self::MongoCreateCollection(_) => Ok(vec![]),
            Self::MongoShardKey(_) => Ok(vec![]),
            Self::PgCreateTable(s) => s.to_sqls(filter),
            Self::PgCreateRbac(s) => s.to_sqls(filter),
            Self::PgCreateUdf(s) => s.to_sqls(filter),
            Self::PgCreateUdt(s) => s.to_sqls(filter),
            _ => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn mssql_keys_preserve_database_and_identifier_boundaries() {
        let keys = [
            StructKey::new(StructKeyType::Table, ["c", "d"]).with_database("a.b"),
            StructKey::new(StructKeyType::Table, ["b.c", "d"]).with_database("a"),
            StructKey::new(StructKeyType::Table, ["b", "c.d"]).with_database("a"),
        ];
        for key in &keys {
            assert_eq!(key.to_string(), "table.a.b.c.d");
        }
        assert_eq!(BTreeSet::from(keys).len(), 3);

        let key = StructKey::new(StructKeyType::Table, ["dbo", "users"]);
        assert_ne!(key.clone().with_database("db1"), key.with_database("db2"));
    }

    #[test]
    fn struct_key_keeps_identifier_boundaries_and_legacy_display() {
        let key = StructKey::new(
            StructKeyType::RbacPrivilegeColumn,
            ["a.b", "t.1", "SELECT", "user.name", "NO"],
        );

        assert_eq!(key.schema(), "a.b");
        assert_eq!(key.table(), "t.1");
        assert_eq!(
            key.to_string(),
            "rbac.privilege.column.a.b.t.1.SELECT.user.name.NO"
        );

        let sequence = StructKey::new(StructKeyType::Sequence, ["public", "seq"]);
        let table = StructKey::new(StructKeyType::Table, ["public", "tb"]);
        assert!(sequence < table);
    }
}
