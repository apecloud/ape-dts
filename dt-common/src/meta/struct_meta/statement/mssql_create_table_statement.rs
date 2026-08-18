use crate::{
    config::config_enums::DbType,
    error::DtError,
    meta::struct_meta::structure::{
        comment::{Comment, CommentType},
        constraint::{Constraint, ConstraintType},
        index::Index,
        structure_type::StructureType,
        table::Table,
    },
    rdb_filter::RdbFilter,
    utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlIdentityColumn {
    pub column_name: String,
    pub seed_value: String,
    pub increment_value: String,
    pub is_not_for_replication: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlComputedColumn {
    pub column_name: String,
    pub definition: String,
    pub is_persisted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDefaultConstraint {
    pub constraint_name: String,
    pub column_name: String,
    pub definition: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlIndexColumn {
    pub column_name: String,
    pub key_ordinal: u32,
    pub is_descending_key: bool,
    pub is_included_column: bool,
}

#[derive(Debug, Clone)]
pub struct MssqlIndex {
    pub index: Index,
    pub index_type_desc: String,
    pub is_primary_key: bool,
    pub is_unique_constraint: bool,
    pub filter_definition: Option<String>,
    pub columns: Vec<MssqlIndexColumn>,
}

#[derive(Debug, Clone)]
pub struct MssqlCreateTableStatement {
    pub table: Table,
    pub identity_columns: Vec<MssqlIdentityColumn>,
    pub computed_columns: Vec<MssqlComputedColumn>,
    pub default_constraints: Vec<MssqlDefaultConstraint>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<MssqlIndex>,
    pub comments: Vec<Comment>,
}

impl MssqlCreateTableStatement {
    pub fn route(&mut self, dst_schema: &str, dst_tb: &str) {
        self.table.schema_name = dst_schema.to_string();
        self.table.table_name = dst_tb.to_string();

        for constraint in &mut self.constraints {
            constraint.schema_name = dst_schema.to_string();
            constraint.table_name = dst_tb.to_string();
        }

        for index in &mut self.indexes {
            index.index.schema_name = dst_schema.to_string();
            index.index.table_name = dst_tb.to_string();
        }

        for comment in &mut self.comments {
            comment.schema_name = dst_schema.to_string();
            comment.table_name = dst_tb.to_string();
        }
    }

    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();
        let table_enabled = !filter.filter_structure(&StructureType::Table);

        if table_enabled {
            sqls.push((
                format!("table.{}.{}", self.table.schema_name, self.table.table_name),
                self.table_to_sql()?,
            ));
        }

        for constraint in &self.constraints {
            let is_key = matches!(
                constraint.constraint_type,
                ConstraintType::Primary | ConstraintType::Unique
            );
            if (is_key && !table_enabled)
                || (!is_key && filter.filter_structure(&StructureType::Constraint))
            {
                continue;
            }
            sqls.push((
                format!(
                    "constraint.{}.{}.{}",
                    constraint.schema_name, constraint.table_name, constraint.constraint_name
                ),
                Self::constraint_to_sql(constraint),
            ));
        }

        if !filter.filter_structure(&StructureType::Index) {
            for index in &self.indexes {
                if index.is_primary_key || index.is_unique_constraint {
                    continue;
                }
                sqls.push((
                    format!(
                        "index.{}.{}.{}",
                        index.index.schema_name, index.index.table_name, index.index.index_name
                    ),
                    Self::index_to_sql(index)?,
                ));
            }
        }

        if table_enabled && !filter.filter_structure(&StructureType::Comment) {
            for comment in &self.comments {
                let key = match &comment.comment_type {
                    CommentType::Table => format!(
                        "table_comment.{}.{}",
                        comment.schema_name, comment.table_name
                    ),
                    CommentType::Column => format!(
                        "column_comment.{}.{}.{}",
                        comment.schema_name, comment.table_name, comment.column_name
                    ),
                };
                sqls.push((key, Self::comment_to_sql(comment)));
            }
        }

        Ok(sqls)
    }

    fn table_to_sql(&mut self) -> anyhow::Result<String> {
        self.table
            .columns
            .sort_by_key(|column| column.ordinal_position);
        let mut column_sqls = Vec::with_capacity(self.table.columns.len());
        for column in &self.table.columns {
            let column_name = Self::quote(&column.column_name);
            if let Some(computed) = self
                .computed_columns
                .iter()
                .find(|computed| computed.column_name == column.column_name)
            {
                let persisted = if computed.is_persisted {
                    " PERSISTED"
                } else {
                    ""
                };
                column_sqls.push(format!(
                    "{column_name} AS {}{persisted}",
                    computed.definition
                ));
                continue;
            }

            let mut sql = format!("{column_name} {}", column.column_type);
            if !column.collation_name.is_empty() {
                sql.push_str(&format!(" COLLATE {}", column.collation_name));
            }
            if let Some(identity) = self
                .identity_columns
                .iter()
                .find(|identity| identity.column_name == column.column_name)
            {
                sql.push_str(&format!(
                    " IDENTITY({}, {})",
                    identity.seed_value, identity.increment_value
                ));
                if identity.is_not_for_replication {
                    sql.push_str(" NOT FOR REPLICATION");
                }
            }
            if let Some(default) = self
                .default_constraints
                .iter()
                .find(|default| default.column_name == column.column_name)
            {
                sql.push_str(&format!(
                    " CONSTRAINT {} DEFAULT {}",
                    Self::quote(&default.constraint_name),
                    default.definition
                ));
            }
            sql.push_str(if column.is_nullable {
                " NULL"
            } else {
                " NOT NULL"
            });
            column_sqls.push(sql);
        }

        if column_sqls.is_empty() {
            return Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL table {}.{} has no columns",
                self.table.schema_name, self.table.table_name
            ))
            .into());
        }

        Ok(format!(
            "CREATE TABLE {}.{} ({})",
            Self::quote(&self.table.schema_name),
            Self::quote(&self.table.table_name),
            column_sqls.join(", ")
        ))
    }

    fn constraint_to_sql(constraint: &Constraint) -> String {
        format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT {} {}",
            Self::quote(&constraint.schema_name),
            Self::quote(&constraint.table_name),
            Self::quote(&constraint.constraint_name),
            constraint.definition
        )
    }

    fn index_to_sql(index: &MssqlIndex) -> anyhow::Result<String> {
        if !matches!(index.index_type_desc.as_str(), "CLUSTERED" | "NONCLUSTERED") {
            return Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL index {} uses unsupported type {}",
                index.index.index_name, index.index_type_desc
            ))
            .into());
        }

        let mut key_columns = index
            .columns
            .iter()
            .filter(|column| !column.is_included_column && column.key_ordinal > 0)
            .collect::<Vec<_>>();
        key_columns.sort_by_key(|column| column.key_ordinal);
        if key_columns.is_empty() {
            return Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL index {} has no key columns",
                index.index.index_name
            ))
            .into());
        }
        let key_columns = key_columns
            .iter()
            .map(|column| {
                format!(
                    "{} {}",
                    Self::quote(&column.column_name),
                    if column.is_descending_key {
                        "DESC"
                    } else {
                        "ASC"
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let unique = if index.index.index_kind
            == crate::meta::struct_meta::structure::index::IndexKind::Unique
        {
            "UNIQUE "
        } else {
            ""
        };
        let mut sql = format!(
            "CREATE {unique}{} INDEX {} ON {}.{} ({key_columns})",
            index.index_type_desc,
            Self::quote(&index.index.index_name),
            Self::quote(&index.index.schema_name),
            Self::quote(&index.index.table_name)
        );

        let included_columns = index
            .columns
            .iter()
            .filter(|column| column.is_included_column)
            .collect::<Vec<_>>();
        if !included_columns.is_empty() {
            sql.push_str(&format!(
                " INCLUDE ({})",
                included_columns
                    .iter()
                    .map(|column| Self::quote(&column.column_name))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(filter) = &index.filter_definition {
            sql.push_str(&format!(" WHERE {filter}"));
        }
        Ok(sql)
    }

    fn comment_to_sql(comment: &Comment) -> String {
        let value = comment.comment.replace('\'', "''");
        let schema = comment.schema_name.replace('\'', "''");
        let table = comment.table_name.replace('\'', "''");
        let mut sql = format!(
            "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{value}', \
             @level0type=N'SCHEMA', @level0name=N'{schema}', \
             @level1type=N'TABLE', @level1name=N'{table}'"
        );
        if matches!(comment.comment_type, CommentType::Column) {
            let column = comment.column_name.replace('\'', "''");
            sql.push_str(&format!(", @level2type=N'COLUMN', @level2name=N'{column}'"));
        }
        sql
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::filter_config::FilterConfig, meta::struct_meta::structure::column::Column,
    };

    #[test]
    fn comment_sqls_use_summary_compatible_keys() {
        let filter = RdbFilter::from_config(
            &FilterConfig {
                do_schemas: "*".to_string(),
                do_structures: "*".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();
        let mut statement = MssqlCreateTableStatement {
            table: Table {
                schema_name: "dbo".to_string(),
                table_name: "users".to_string(),
                columns: vec![Column {
                    column_name: "id".to_string(),
                    ordinal_position: 1,
                    column_type: "INT".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            identity_columns: Vec::new(),
            computed_columns: Vec::new(),
            default_constraints: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
            comments: vec![
                Comment {
                    comment_type: CommentType::Table,
                    database_name: String::new(),
                    schema_name: "dbo".to_string(),
                    table_name: "users".to_string(),
                    column_name: String::new(),
                    comment: "users table".to_string(),
                },
                Comment {
                    comment_type: CommentType::Column,
                    database_name: String::new(),
                    schema_name: "dbo".to_string(),
                    table_name: "users".to_string(),
                    column_name: "id".to_string(),
                    comment: "primary identifier".to_string(),
                },
            ],
        };

        let keys = statement
            .to_sqls(&filter)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        assert!(keys.contains(&"table_comment.dbo.users".to_string()));
        assert!(keys.contains(&"column_comment.dbo.users.id".to_string()));
    }
}
