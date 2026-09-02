use crate::{
    config::config_enums::DbType, meta::struct_meta::structure::structure_type::StructureType,
    rdb_filter::RdbFilter, utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MssqlComment {
    Database {
        comment: String,
    },
    Schema {
        comment: String,
    },
    Table {
        comment: String,
    },
    Sequence {
        comment: String,
    },
    Column {
        column_name: String,
        comment: String,
    },
    Constraint {
        constraint_name: String,
        is_key: bool,
        comment: String,
    },
    Index {
        index_name: String,
        is_constraint: bool,
        comment: String,
    },
}

impl MssqlComment {
    pub fn to_sql(
        &self,
        database_name: &str,
        schema_name: &str,
        object_name: &str,
        filter: &RdbFilter,
    ) -> Option<(String, String)> {
        if filter.filter_structure(&StructureType::Comment) || !self.owner_enabled(filter) {
            return None;
        }

        let (key, comment, level1, level2) = match self {
            Self::Database { comment } => (
                format!("database_comment.{database_name}"),
                comment,
                None,
                None,
            ),
            Self::Schema { comment } => (
                format!("schema_comment.{database_name}.{schema_name}"),
                comment,
                None,
                None,
            ),
            Self::Table { comment } => (
                format!("table_comment.{database_name}.{schema_name}.{object_name}"),
                comment,
                Some(("TABLE", object_name)),
                None,
            ),
            Self::Sequence { comment } => (
                format!("sequence_comment.{database_name}.{schema_name}.{object_name}"),
                comment,
                Some(("SEQUENCE", object_name)),
                None,
            ),
            Self::Column {
                column_name,
                comment,
            } => (
                format!(
                    "column_comment.{database_name}.{schema_name}.{object_name}.{column_name}"
                ),
                comment,
                Some(("TABLE", object_name)),
                Some(("COLUMN", column_name.as_str())),
            ),
            Self::Constraint {
                constraint_name,
                comment,
                ..
            } => (
                format!(
                    "constraint_comment.{database_name}.{schema_name}.{object_name}.{constraint_name}"
                ),
                comment,
                Some(("TABLE", object_name)),
                Some(("CONSTRAINT", constraint_name.as_str())),
            ),
            Self::Index {
                index_name,
                comment,
                ..
            } => (
                format!(
                    "index_comment.{database_name}.{schema_name}.{object_name}.{index_name}"
                ),
                comment,
                Some(("TABLE", object_name)),
                Some(("INDEX", index_name.as_str())),
            ),
        };

        let database = if database_name.is_empty() {
            String::new()
        } else {
            format!("{}.", Self::quote(database_name))
        };
        let mut sql = format!(
            "EXEC {database}sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{}'",
            Self::literal(comment)
        );
        if !matches!(self, Self::Database { .. }) {
            sql.push_str(&format!(
                ", @level0type=N'SCHEMA', @level0name=N'{}'",
                Self::literal(schema_name)
            ));
        }
        if let Some((level_type, level_name)) = level1 {
            sql.push_str(&format!(
                ", @level1type=N'{level_type}', @level1name=N'{}'",
                Self::literal(level_name)
            ));
        }
        if let Some((level_type, level_name)) = level2 {
            sql.push_str(&format!(
                ", @level2type=N'{level_type}', @level2name=N'{}'",
                Self::literal(level_name)
            ));
        }
        Some((key, sql))
    }

    fn owner_enabled(&self, filter: &RdbFilter) -> bool {
        let structure_type = match self {
            Self::Database { .. } | Self::Schema { .. } => StructureType::Database,
            Self::Table { .. } | Self::Column { .. } => StructureType::Table,
            Self::Sequence { .. } => StructureType::Sequence,
            Self::Constraint { is_key, .. } => {
                if *is_key {
                    StructureType::Table
                } else {
                    StructureType::Constraint
                }
            }
            Self::Index { is_constraint, .. } => {
                if *is_constraint {
                    StructureType::Table
                } else {
                    StructureType::Index
                }
            }
        };
        !filter.filter_structure(&structure_type)
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }

    fn literal(value: &str) -> String {
        value.replace('\'', "''")
    }
}
