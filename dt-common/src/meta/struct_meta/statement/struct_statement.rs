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
    config::config_enums::DbType,
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
    db_type: DbType,
    segments: Vec<String>,
}

impl StructKey {
    pub fn new<I, S>(db_type: DbType, key_type: StructKeyType, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            key_type,
            db_type,
            segments: segments.into_iter().map(Into::into).collect(),
        }
    }

    pub fn database(&self) -> Option<&str> {
        self.segment(self.key_type.database_index(&self.db_type))
    }

    pub fn schema(&self) -> &str {
        self.segment(self.key_type.schema_index(&self.db_type))
            .unwrap_or_default()
    }

    pub fn table(&self) -> &str {
        self.segment(self.key_type.table_index(&self.db_type))
            .unwrap_or_default()
    }

    fn segment(&self, index: Option<usize>) -> Option<&str> {
        index
            .and_then(|index| self.segments.get(index))
            .map(String::as_str)
    }

    pub fn with_location(&self, db: &str, schema: &str, table: &str) -> Self {
        let mut key = self.clone();
        for (index, value) in [
            (self.key_type.database_index(&self.db_type), db),
            (self.key_type.schema_index(&self.db_type), schema),
            (self.key_type.table_index(&self.db_type), table),
        ] {
            if let Some(segment) = index.and_then(|index| key.segments.get_mut(index)) {
                *segment = value.to_string();
            }
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
            .then_with(|| {
                self.db_type
                    .diagnostic_name()
                    .cmp(other.db_type.diagnostic_name())
            })
            .then_with(|| self.segments.cmp(&other.segments))
    }
}

impl PartialOrd for StructKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl StructKeyType {
    fn database_index(self, db_type: &DbType) -> Option<usize> {
        match (db_type, self) {
            (_, Self::RbacRole | Self::RbacRoleConfig | Self::RbacMember) => None,
            (DbType::Mssql, _) => Some(0),
            _ => None,
        }
    }

    fn schema_index(self, db_type: &DbType) -> Option<usize> {
        match self {
            Self::Database if matches!(db_type, DbType::Mssql) => None,
            Self::DatabaseComment | Self::RbacRole | Self::RbacRoleConfig | Self::RbacMember => {
                None
            }
            _ => Some(usize::from(matches!(db_type, DbType::Mssql))),
        }
    }

    fn table_index(self, db_type: &DbType) -> Option<usize> {
        if self.is_table_scoped() || matches!(self, Self::Sequence | Self::SequenceComment) {
            self.schema_index(db_type).map(|index| index + 1)
        } else {
            None
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
    fn key_identity_includes_database_type_and_identifier_boundaries() {
        for keys in [
            vec![
                StructKey::new(DbType::Mssql, StructKeyType::Table, ["a.b", "c", "d"]),
                StructKey::new(DbType::Mssql, StructKeyType::Table, ["a", "b.c", "d"]),
                StructKey::new(DbType::Mssql, StructKeyType::Table, ["a", "b", "c.d"]),
            ],
            vec![
                StructKey::new(DbType::Mysql, StructKeyType::Table, ["s", "t"]),
                StructKey::new(DbType::Pg, StructKeyType::Table, ["s", "t"]),
            ],
        ] {
            assert!(keys
                .iter()
                .all(|key| key.to_string() == keys[0].to_string()));
            let count = keys.len();
            assert_eq!(BTreeSet::from_iter(keys).len(), count);
        }
    }

    #[test]
    fn locations_and_routing_follow_database_layout() {
        let cases = [
            (
                DbType::Mysql,
                StructKeyType::Database,
                vec!["s"],
                (None, "s", ""),
                "database.target_schema",
            ),
            (
                DbType::Mysql,
                StructKeyType::Table,
                vec!["s", "t"],
                (None, "s", "t"),
                "table.target_schema.target_table",
            ),
            (
                DbType::Pg,
                StructKeyType::Schema,
                vec!["s"],
                (None, "s", ""),
                "schema.target_schema",
            ),
            (
                DbType::Pg,
                StructKeyType::RbacRole,
                vec!["role.with.dot"],
                (None, "", ""),
                "rbac.role.role.with.dot",
            ),
            (
                DbType::Pg,
                StructKeyType::Index,
                vec!["s", "t", "index.with.dot"],
                (None, "s", "t"),
                "index.target_schema.target_table.index.with.dot",
            ),
            (
                DbType::Mssql,
                StructKeyType::Database,
                vec!["db"],
                (Some("db"), "", ""),
                "database.target_db",
            ),
            (
                DbType::Mssql,
                StructKeyType::DatabaseComment,
                vec!["db"],
                (Some("db"), "", ""),
                "database_comment.target_db",
            ),
            (
                DbType::Mssql,
                StructKeyType::Schema,
                vec!["db", "s"],
                (Some("db"), "s", ""),
                "schema.target_db.target_schema",
            ),
            (
                DbType::Mssql,
                StructKeyType::Index,
                vec!["db", "s", "t", "index.with.dot"],
                (Some("db"), "s", "t"),
                "index.target_db.target_schema.target_table.index.with.dot",
            ),
        ];
        for (db_type, key_type, segments, location, routed_display) in cases {
            let key = StructKey::new(db_type, key_type, segments);
            assert_eq!((key.database(), key.schema(), key.table()), location);
            let routed = key.with_location("target_db", "target_schema", "target_table");
            assert_eq!(routed.to_string(), routed_display);
            assert_eq!((key.database(), key.schema(), key.table()), location);
        }
    }

    #[test]
    fn struct_key_keeps_identifier_boundaries_and_legacy_display() {
        let key = StructKey::new(
            DbType::Pg,
            StructKeyType::RbacPrivilegeColumn,
            ["a.b", "t.1", "SELECT", "user.name", "NO"],
        );

        assert_eq!(key.schema(), "a.b");
        assert_eq!(key.table(), "t.1");
        assert_eq!(
            key.to_string(),
            "rbac.privilege.column.a.b.t.1.SELECT.user.name.NO"
        );

        let sequence = StructKey::new(DbType::Pg, StructKeyType::Sequence, ["public", "seq"]);
        let table = StructKey::new(DbType::Pg, StructKeyType::Table, ["public", "tb"]);
        assert!(sequence < table);
    }
}
