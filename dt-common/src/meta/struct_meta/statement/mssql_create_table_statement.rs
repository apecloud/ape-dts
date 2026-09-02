use super::mssql_comment_statement::MssqlComment;
use crate::{
    config::config_enums::DbType, error::DtError,
    meta::struct_meta::structure::structure_type::StructureType, rdb_filter::RdbFilter,
    utils::sql_util::SqlUtil,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlIdentity {
    pub seed_value: String,
    pub increment_value: String,
    pub is_not_for_replication: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MssqlColumnDefinition {
    Regular {
        column_type: String,
        collation_name: String,
        is_nullable: bool,
        identity: Option<MssqlIdentity>,
    },
    Computed {
        definition: String,
        is_persisted: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlColumn {
    pub column_name: String,
    pub ordinal_position: u32,
    pub definition: MssqlColumnDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlKeyColumn {
    pub column_name: String,
    pub is_descending_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MssqlKeyConstraintType {
    PrimaryKey,
    Unique,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MssqlConstraintKind {
    Default {
        column_name: String,
        definition: String,
    },
    Key {
        constraint_type: MssqlKeyConstraintType,
        index_type_desc: String,
        columns: Vec<MssqlKeyColumn>,
    },
    Check {
        definition: String,
        is_not_for_replication: bool,
        is_disabled: bool,
        is_not_trusted: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlConstraint {
    pub constraint_name: String,
    pub kind: MssqlConstraintKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlIndexColumn {
    pub column_name: String,
    pub index_column_id: u32,
    pub key_ordinal: u32,
    pub is_descending_key: bool,
    pub is_included_column: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlIndex {
    pub index_name: String,
    pub index_id: u32,
    pub index_type: u8,
    pub index_type_desc: String,
    pub is_unique: bool,
    pub is_disabled: bool,
    pub filter_definition: Option<String>,
    pub xml_primary_index_name: Option<String>,
    pub xml_secondary_type_desc: Option<String>,
    pub hash_bucket_count: Option<u64>,
    pub columns: Vec<MssqlIndexColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlTable {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub is_memory_optimized: bool,
    pub durability_desc: String,
    pub columns: Vec<MssqlColumn>,
    pub constraints: Vec<MssqlConstraint>,
    pub indexes: Vec<MssqlIndex>,
    pub comments: Vec<MssqlComment>,
}

#[derive(Debug, Clone)]
pub struct MssqlCreateTableStatement {
    pub table: MssqlTable,
}

impl MssqlCreateTableStatement {
    pub fn route(&mut self, dst_db: &str, dst_schema: &str, dst_tb: &str) {
        self.table.database_name = dst_db.to_string();
        self.table.schema_name = dst_schema.to_string();
        self.table.table_name = dst_tb.to_string();
    }

    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();
        let table_enabled = !filter.filter_structure(&StructureType::Table);

        if table_enabled {
            sqls.push((
                format!(
                    "table.{}.{}.{}",
                    self.table.database_name, self.table.schema_name, self.table.table_name
                ),
                self.table_to_sql()?,
            ));
        }

        for constraint in &self.table.constraints {
            let is_key = matches!(&constraint.kind, MssqlConstraintKind::Key { .. });
            if (is_key && !table_enabled)
                || (!is_key && filter.filter_structure(&StructureType::Constraint))
            {
                continue;
            }
            sqls.push((
                format!(
                    "constraint.{}.{}.{}.{}",
                    self.table.database_name,
                    self.table.schema_name,
                    self.table.table_name,
                    constraint.constraint_name
                ),
                self.constraint_to_sql(constraint)?,
            ));
        }
        if !filter.filter_structure(&StructureType::Constraint) {
            for constraint in &self.table.constraints {
                let Some((state, sql)) = self.check_constraint_state_to_sql(constraint) else {
                    continue;
                };
                sqls.push((
                    format!(
                        "constraint.{}.{}.{}.{}.{}",
                        self.table.database_name,
                        self.table.schema_name,
                        self.table.table_name,
                        constraint.constraint_name,
                        state
                    ),
                    sql,
                ));
            }
        }

        if !filter.filter_structure(&StructureType::Index) {
            self.table.indexes.sort_by_key(|index| index.index_id);
            for index in &self.table.indexes {
                sqls.push((
                    format!(
                        "index.{}.{}.{}.{}",
                        self.table.database_name,
                        self.table.schema_name,
                        self.table.table_name,
                        index.index_name
                    ),
                    self.index_to_sql(index)?,
                ));
            }
            for index in self.table.indexes.iter().filter(|index| index.is_disabled) {
                sqls.push((
                    format!(
                        "index.{}.{}.{}.{}.disable",
                        self.table.database_name,
                        self.table.schema_name,
                        self.table.table_name,
                        index.index_name
                    ),
                    self.index_disable_sql(index)?,
                ));
            }
        }

        for comment in &self.table.comments {
            if let Some(sql) = comment.to_sql(
                &self.table.database_name,
                &self.table.schema_name,
                &self.table.table_name,
                filter,
            ) {
                sqls.push(sql);
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
            match &column.definition {
                MssqlColumnDefinition::Computed {
                    definition,
                    is_persisted,
                } => {
                    let persisted = if *is_persisted { " PERSISTED" } else { "" };
                    column_sqls.push(format!("{column_name} AS {definition}{persisted}"));
                }
                MssqlColumnDefinition::Regular {
                    column_type,
                    collation_name,
                    is_nullable,
                    identity,
                } => {
                    let mut sql = format!("{column_name} {column_type}");
                    if !collation_name.is_empty() {
                        sql.push_str(&format!(" COLLATE {collation_name}"));
                    }
                    if let Some(identity) = identity {
                        sql.push_str(&format!(
                            " IDENTITY({}, {})",
                            identity.seed_value, identity.increment_value
                        ));
                        if identity.is_not_for_replication {
                            sql.push_str(" NOT FOR REPLICATION");
                        }
                    }
                    sql.push_str(if *is_nullable { " NULL" } else { " NOT NULL" });
                    column_sqls.push(sql);
                }
            }
        }

        if column_sqls.is_empty() {
            return Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL table {} has no columns",
                self.qualified_table_name()
            ))
            .into());
        }

        let mut sql = format!(
            "CREATE TABLE {} ({})",
            self.qualified_table_name(),
            column_sqls.join(", ")
        );
        if self.table.is_memory_optimized {
            if !matches!(
                self.table.durability_desc.as_str(),
                "SCHEMA_AND_DATA" | "SCHEMA_ONLY"
            ) {
                return Err(DtError::UnsupportedTableStructure(format!(
                    "MSSQL memory-optimized table {} uses unsupported durability {}",
                    self.qualified_table_name(),
                    self.table.durability_desc,
                ))
                .into());
            }
            sql.push_str(&format!(
                " WITH (MEMORY_OPTIMIZED = ON, DURABILITY = {})",
                self.table.durability_desc
            ));
        }
        Ok(sql)
    }

    fn constraint_to_sql(&self, constraint: &MssqlConstraint) -> anyhow::Result<String> {
        let definition = match &constraint.kind {
            MssqlConstraintKind::Default {
                column_name,
                definition,
            } => {
                let column = self
                    .table
                    .columns
                    .iter()
                    .find(|column| column.column_name == *column_name)
                    .ok_or_else(|| {
                        DtError::UnsupportedTableStructure(format!(
                            "MSSQL default constraint {} references missing column {} on table {}",
                            Self::quote(&constraint.constraint_name),
                            Self::quote(column_name),
                            self.qualified_table_name(),
                        ))
                    })?;
                if matches!(column.definition, MssqlColumnDefinition::Computed { .. }) {
                    return Err(DtError::UnsupportedTableStructure(format!(
                        "MSSQL computed column {} on table {} has a default constraint",
                        Self::quote(column_name),
                        self.qualified_table_name(),
                    ))
                    .into());
                }
                format!("DEFAULT {definition} FOR {}", Self::quote(column_name))
            }
            MssqlConstraintKind::Key {
                constraint_type,
                index_type_desc,
                columns,
            } => {
                if !matches!(index_type_desc.as_str(), "CLUSTERED" | "NONCLUSTERED") {
                    return Err(DtError::UnsupportedTableStructure(format!(
                        "MSSQL constraint {} uses unsupported index type {}",
                        constraint.constraint_name, index_type_desc
                    ))
                    .into());
                }
                if columns.is_empty() {
                    return Err(DtError::UnsupportedTableStructure(format!(
                        "MSSQL constraint {} has no key columns",
                        constraint.constraint_name
                    ))
                    .into());
                }
                let constraint_type = match constraint_type {
                    MssqlKeyConstraintType::PrimaryKey => "PRIMARY KEY",
                    MssqlKeyConstraintType::Unique => "UNIQUE",
                };
                let columns = columns
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
                format!("{constraint_type} {index_type_desc} ({columns})")
            }
            MssqlConstraintKind::Check {
                definition,
                is_not_for_replication,
                ..
            } => format!(
                "CHECK {}{}",
                if *is_not_for_replication {
                    "NOT FOR REPLICATION "
                } else {
                    ""
                },
                definition
            ),
        };
        let with_nocheck = if matches!(
            &constraint.kind,
            MssqlConstraintKind::Check {
                is_not_trusted: true,
                ..
            }
        ) {
            " WITH NOCHECK"
        } else {
            ""
        };
        Ok(format!(
            "ALTER TABLE {}{with_nocheck} ADD CONSTRAINT {} {}",
            self.qualified_table_name(),
            Self::quote(&constraint.constraint_name),
            definition
        ))
    }

    fn check_constraint_state_to_sql(
        &self,
        constraint: &MssqlConstraint,
    ) -> Option<(&'static str, String)> {
        let MssqlConstraintKind::Check {
            is_disabled,
            is_not_trusted,
            ..
        } = &constraint.kind
        else {
            return None;
        };
        let (state, action) = if *is_disabled {
            ("disable", "NOCHECK CONSTRAINT")
        } else if *is_not_trusted {
            ("untrust", "WITH NOCHECK CHECK CONSTRAINT")
        } else {
            return None;
        };
        Some((
            state,
            format!(
                "ALTER TABLE {} {action} {}",
                self.qualified_table_name(),
                Self::quote(&constraint.constraint_name)
            ),
        ))
    }

    fn index_to_sql(&self, index: &MssqlIndex) -> anyhow::Result<String> {
        match index.index_type {
            1 | 2 => self.rowstore_index_to_sql(index),
            3 => self.xml_index_to_sql(index),
            4 => self.single_column_index_to_sql(index, "CREATE SPATIAL INDEX"),
            5 => Ok(format!(
                "CREATE CLUSTERED COLUMNSTORE INDEX {} ON {}",
                Self::quote(&index.index_name),
                self.qualified_table_name()
            )),
            6 => {
                let columns = self.index_columns_by_position(index)?;
                let mut sql = format!(
                    "CREATE NONCLUSTERED COLUMNSTORE INDEX {} ON {} ({columns})",
                    Self::quote(&index.index_name),
                    self.qualified_table_name()
                );
                if let Some(filter) = &index.filter_definition {
                    sql.push_str(&format!(" WHERE {filter}"));
                }
                Ok(sql)
            }
            7 => {
                let columns = self.index_columns_by_position(index)?;
                let bucket_count = index.hash_bucket_count.ok_or_else(|| {
                    DtError::UnsupportedTableStructure(format!(
                        "MSSQL hash index {} has no bucket count",
                        Self::quote(&index.index_name),
                    ))
                })?;
                Ok(format!(
                    "ALTER TABLE {} ADD INDEX {} HASH ({columns}) WITH (BUCKET_COUNT = {bucket_count})",
                    self.qualified_table_name(),
                    Self::quote(&index.index_name),
                ))
            }
            9 => self.single_column_index_to_sql(index, "CREATE JSON INDEX"),
            _ => Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL index {} uses unknown type {} ({})",
                index.index_name, index.index_type, index.index_type_desc
            ))
            .into()),
        }
    }

    fn rowstore_index_to_sql(&self, index: &MssqlIndex) -> anyhow::Result<String> {
        let mut key_columns = index
            .columns
            .iter()
            .filter(|column| !column.is_included_column && column.key_ordinal > 0)
            .collect::<Vec<_>>();
        key_columns.sort_by_key(|column| column.key_ordinal);
        if key_columns.is_empty() {
            return Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL index {} has no key columns",
                index.index_name
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

        let unique = if index.is_unique { "UNIQUE " } else { "" };
        let mut sql = format!(
            "CREATE {unique}{} INDEX {} ON {} ({key_columns})",
            index.index_type_desc,
            Self::quote(&index.index_name),
            self.qualified_table_name()
        );

        let mut included_columns = index
            .columns
            .iter()
            .filter(|column| column.is_included_column)
            .collect::<Vec<_>>();
        included_columns.sort_by_key(|column| column.index_column_id);
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

    fn xml_index_to_sql(&self, index: &MssqlIndex) -> anyhow::Result<String> {
        let column = self.single_index_column(index)?;
        if let Some(primary_index_name) = &index.xml_primary_index_name {
            let secondary_type = index.xml_secondary_type_desc.as_deref().ok_or_else(|| {
                DtError::UnsupportedTableStructure(format!(
                    "MSSQL secondary XML index {} has no secondary type",
                    Self::quote(&index.index_name),
                ))
            })?;
            if !matches!(secondary_type, "PATH" | "VALUE" | "PROPERTY") {
                return Err(DtError::UnsupportedTableStructure(format!(
                    "MSSQL XML index {} uses unsupported secondary type {secondary_type}",
                    Self::quote(&index.index_name),
                ))
                .into());
            }
            Ok(format!(
                "CREATE XML INDEX {} ON {} ({}) USING XML INDEX {} FOR {secondary_type}",
                Self::quote(&index.index_name),
                self.qualified_table_name(),
                Self::quote(&column.column_name),
                Self::quote(primary_index_name),
            ))
        } else {
            Ok(format!(
                "CREATE PRIMARY XML INDEX {} ON {} ({})",
                Self::quote(&index.index_name),
                self.qualified_table_name(),
                Self::quote(&column.column_name),
            ))
        }
    }

    fn single_column_index_to_sql(
        &self,
        index: &MssqlIndex,
        prefix: &str,
    ) -> anyhow::Result<String> {
        let column = self.single_index_column(index)?;
        Ok(format!(
            "{prefix} {} ON {} ({})",
            Self::quote(&index.index_name),
            self.qualified_table_name(),
            Self::quote(&column.column_name),
        ))
    }

    fn single_index_column<'a>(
        &self,
        index: &'a MssqlIndex,
    ) -> anyhow::Result<&'a MssqlIndexColumn> {
        if index.columns.len() != 1 {
            return Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL index {} of type {} has {} columns, expected one",
                Self::quote(&index.index_name),
                index.index_type_desc,
                index.columns.len(),
            ))
            .into());
        }
        Ok(&index.columns[0])
    }

    fn index_columns_by_position(&self, index: &MssqlIndex) -> anyhow::Result<String> {
        let mut columns = index.columns.iter().collect::<Vec<_>>();
        columns.sort_by_key(|column| column.index_column_id);
        if columns.is_empty() {
            return Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL index {} has no columns",
                Self::quote(&index.index_name),
            ))
            .into());
        }
        Ok(columns
            .into_iter()
            .map(|column| Self::quote(&column.column_name))
            .collect::<Vec<_>>()
            .join(", "))
    }

    fn index_disable_sql(&self, index: &MssqlIndex) -> anyhow::Result<String> {
        if index.index_type == 7 {
            return Err(DtError::UnsupportedTableStructure(format!(
                "MSSQL memory-optimized hash index {} cannot be disabled",
                Self::quote(&index.index_name),
            ))
            .into());
        }
        Ok(format!(
            "ALTER INDEX {} ON {} DISABLE",
            Self::quote(&index.index_name),
            self.qualified_table_name()
        ))
    }

    fn quote(identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }

    fn qualified_table(db: &str, schema: &str, tb: &str) -> String {
        SqlUtil::render_rdb_table(&DbType::Mssql, db, schema, tb)
    }

    fn qualified_table_name(&self) -> String {
        Self::qualified_table(
            &self.table.database_name,
            &self.table.schema_name,
            &self.table.table_name,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::filter_config::FilterConfig;

    #[test]
    fn index_types_not_executed_by_e2e_generate_sql() {
        let filter = RdbFilter::from_config(
            &FilterConfig {
                do_schemas: "*".to_string(),
                do_structures: "index".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();
        let column = |name: &str| MssqlIndexColumn {
            column_name: name.to_string(),
            index_column_id: 1,
            key_ordinal: 0,
            is_descending_key: false,
            is_included_column: false,
        };
        let index = |index_name: &str,
                     index_id: u32,
                     index_type: u8,
                     index_type_desc: &str,
                     columns: Vec<MssqlIndexColumn>| MssqlIndex {
            index_name: index_name.to_string(),
            index_id,
            index_type,
            index_type_desc: index_type_desc.to_string(),
            is_unique: false,
            is_disabled: false,
            filter_definition: None,
            xml_primary_index_name: None,
            xml_secondary_type_desc: None,
            hash_bucket_count: None,
            columns,
        };

        let mut hash = index("ix_hash", 1, 7, "NONCLUSTERED HASH", vec![column("id")]);
        hash.hash_bucket_count = Some(1024);

        let cases = vec![
            (
                hash,
                "ALTER TABLE [test_db].[dbo].[users] ADD INDEX [ix_hash] HASH ([id]) WITH (BUCKET_COUNT = 1024)",
            ),
            (
                index(
                    "ix_json",
                    2,
                    9,
                    "JSON",
                    vec![column("json_value")],
                ),
                "CREATE JSON INDEX [ix_json] ON [test_db].[dbo].[users] ([json_value])",
            ),
        ];

        for (index, expected_sql) in cases {
            let mut statement = index_test_statement(vec![index]);
            let sqls = statement.to_sqls(&filter).unwrap();
            assert_eq!(sqls.len(), 1);
            assert_eq!(sqls[0].1, expected_sql);
        }
    }

    fn index_test_statement(indexes: Vec<MssqlIndex>) -> MssqlCreateTableStatement {
        MssqlCreateTableStatement {
            table: MssqlTable {
                database_name: "test_db".to_string(),
                schema_name: "dbo".to_string(),
                table_name: "users".to_string(),
                is_memory_optimized: false,
                durability_desc: "SCHEMA_AND_DATA".to_string(),
                columns: Vec::new(),
                constraints: Vec::new(),
                indexes,
                comments: Vec::new(),
            },
        }
    }
}
