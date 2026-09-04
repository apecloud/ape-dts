use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
};

use strum::Display as StrumDisplay;

use super::{
    mongo_create_collection_statement::MongoCreateCollectionStatement,
    mongo_shard_key_statement::MongoShardKeyStatement,
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
            segments: segments.into_iter().map(Into::into).collect(),
        }
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
        if let Some(index) = self.key_type.schema_index() {
            key.segments[index] = schema.to_string();
        }
        if let Some(index) = self.key_type.table_index() {
            key.segments[index] = table.to_string();
        }
        key
    }

    pub fn is_table_scoped(&self) -> bool {
        self.key_type.is_table_scoped()
    }

    pub fn is_sequence(&self) -> bool {
        self.key_type == StructKeyType::Sequence
    }

    pub fn parent_table_key(&self) -> Option<Self> {
        if !matches!(
            self.key_type,
            StructKeyType::Index
                | StructKeyType::Constraint
                | StructKeyType::SequenceOwner
                | StructKeyType::ColumnComment
                | StructKeyType::TableComment
        ) {
            return None;
        }

        Some(Self::new(
            StructKeyType::Table,
            [self.schema(), self.table()],
        ))
    }
}

impl Display for StructKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key_type)?;
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
            Self::Database | Self::Schema => Some(0),
            Self::Table
            | Self::Index
            | Self::Constraint
            | Self::Sequence
            | Self::SequenceOwner
            | Self::ColumnComment
            | Self::TableComment
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
            | Self::TableComment => Some(1),
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
                | Self::RbacPrivilegeTable
                | Self::RbacPrivilegeColumn
                | Self::RbacPrivilegeSequence
        )
    }
}

#[derive(Debug, Clone, Default)]
pub enum StructStatement {
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
    use super::*;

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

        let index = StructKey::new(StructKeyType::Index, ["a.b", "t.1", "idx"]);
        assert_eq!(
            index.parent_table_key(),
            Some(StructKey::new(StructKeyType::Table, ["a.b", "t.1"]))
        );
        assert!(table.parent_table_key().is_none());
    }
}
