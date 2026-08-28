use super::{
    mongo_create_collection_statement::MongoCreateCollectionStatement,
    mongo_shard_key_statement::MongoShardKeyStatement,
    mysql_create_database_statement::MysqlCreateDatabaseStatement,
    mysql_create_table_statement::MysqlCreateTableStatement,
    pg_create_rbac_statement::PgCreateRbacStatement,
    pg_create_schema_statement::PgCreateSchemaStatement,
    pg_create_table_statement::PgCreateTableStatement,
};
use strum::{Display, EnumIter, IntoEnumIterator, IntoStaticStr};

use crate::{
    meta::struct_meta::statement::{
        pg_create_udf_statement::PgCreateUdfStatement,
        pg_create_udt_statement::PgCreateUdtStatement,
    },
    rdb_filter::RdbFilter,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, EnumIter, IntoStaticStr)]
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

impl StructKeyType {
    pub fn from_key(key: &str) -> Option<Self> {
        Self::iter().find(|key_type| {
            let prefix: &'static str = key_type.into();
            key.strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('.'))
        })
    }

    pub fn schema_index(self) -> Option<usize> {
        match self {
            Self::Database | Self::Schema => Some(1),
            Self::Table
            | Self::Index
            | Self::Constraint
            | Self::Sequence
            | Self::SequenceOwner
            | Self::ColumnComment
            | Self::TableComment
            | Self::Udt
            | Self::Udf => Some(1),
            Self::RbacPrivilegeSchema
            | Self::RbacPrivilegeTable
            | Self::RbacPrivilegeColumn
            | Self::RbacPrivilegeSequence => Some(3),
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
            | Self::TableComment => Some(2),
            Self::RbacPrivilegeTable | Self::RbacPrivilegeColumn | Self::RbacPrivilegeSequence => {
                Some(4)
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
    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
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
    fn struct_key_type_renders_and_parses_key_prefixes() {
        for key_type in StructKeyType::iter() {
            let prefix: &'static str = key_type.into();
            assert_eq!(
                StructKeyType::from_key(&format!("{prefix}.value")),
                Some(key_type)
            );
        }

        assert_eq!(
            StructKeyType::from_key("rbac.privilege.column.public.t1.SELECT.user.NO"),
            Some(StructKeyType::RbacPrivilegeColumn)
        );
        assert_eq!(StructKeyType::from_key("unknown.value"), None);
    }
}
