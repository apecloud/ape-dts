use std::collections::{BTreeMap, HashSet};

use anyhow::bail;
use dt_common::{
    config::config_enums::DbType,
    error::{DtError, DtResultExt, ErrorCode},
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        mssql::mssql_connection_pool::MssqlConnectionPool,
        struct_meta::{
            statement::{
                mssql_create_schema_statement::MssqlCreateSchemaStatement,
                mssql_create_table_statement::{
                    MssqlComputedColumn, MssqlCreateTableStatement, MssqlDefaultConstraint,
                    MssqlIdentityColumn, MssqlIndex, MssqlIndexColumn,
                },
            },
            structure::{
                column::Column,
                comment::{Comment, CommentType},
                constraint::{Constraint, ConstraintType},
                index::{Index, IndexKind, IndexType},
                schema::Schema,
                table::Table,
            },
        },
    },
    rdb_filter::RdbFilter,
    utils::sql_util::SqlUtil,
};

const SCHEMAS_SQL: &str = r#"
SELECT s.name AS schema_name
FROM sys.schemas AS s
ORDER BY s.name
"#;

const TABLE_COLUMNS_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    c.name AS column_name,
    CONVERT(bigint, c.column_id) AS ordinal_position,
    ty.name AS type_name,
    CONVERT(bigint, c.max_length) AS max_length,
    CONVERT(bigint, c.precision) AS numeric_precision,
    CONVERT(bigint, c.scale) AS numeric_scale,
    c.is_nullable,
    c.collation_name,
    dc.name AS default_constraint_name,
    dc.definition AS default_definition,
    CONVERT(nvarchar(100), ic.seed_value) AS identity_seed,
    CONVERT(nvarchar(100), ic.increment_value) AS identity_increment,
    CONVERT(bit, COALESCE(ic.is_not_for_replication, 0)) AS identity_not_for_replication,
    cc.definition AS computed_definition,
    CONVERT(bit, COALESCE(cc.is_persisted, 0)) AS computed_persisted
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.columns AS c ON c.object_id = t.object_id
JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
LEFT JOIN sys.default_constraints AS dc
  ON dc.parent_object_id = c.object_id
 AND dc.parent_column_id = c.column_id
LEFT JOIN sys.identity_columns AS ic
  ON ic.object_id = c.object_id
 AND ic.column_id = c.column_id
LEFT JOIN sys.computed_columns AS cc
  ON cc.object_id = c.object_id
 AND cc.column_id = c.column_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name, c.column_id
"#;

const KEY_CONSTRAINTS_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    kc.name AS constraint_name,
    CASE kc.type WHEN 'PK' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type,
    i.type_desc AS index_type_desc,
    c.name AS column_name,
    CONVERT(bigint, ic.key_ordinal) AS key_ordinal,
    ic.is_descending_key
FROM sys.key_constraints AS kc
JOIN sys.tables AS t ON t.object_id = kc.parent_object_id
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.indexes AS i
  ON i.object_id = kc.parent_object_id
 AND i.index_id = kc.unique_index_id
JOIN sys.index_columns AS ic
  ON ic.object_id = i.object_id
 AND ic.index_id = i.index_id
JOIN sys.columns AS c
  ON c.object_id = ic.object_id
 AND c.column_id = ic.column_id
WHERE t.is_ms_shipped = 0
  AND ic.is_included_column = 0
  AND ic.key_ordinal > 0
ORDER BY s.name, t.name, kc.name, ic.key_ordinal
"#;

const CHECK_CONSTRAINTS_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    cc.name AS constraint_name,
    cc.definition,
    cc.is_not_for_replication
FROM sys.check_constraints AS cc
JOIN sys.tables AS t ON t.object_id = cc.parent_object_id
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name, cc.name
"#;

const INDEXES_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc AS index_type_desc,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint,
    i.filter_definition,
    c.name AS column_name,
    CONVERT(bigint, ic.key_ordinal) AS key_ordinal,
    ic.is_descending_key,
    ic.is_included_column,
    CONVERT(bigint, ic.index_column_id) AS index_column_id
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.indexes AS i ON i.object_id = t.object_id
JOIN sys.index_columns AS ic
  ON ic.object_id = i.object_id
 AND ic.index_id = i.index_id
JOIN sys.columns AS c
  ON c.object_id = ic.object_id
 AND c.column_id = ic.column_id
WHERE t.is_ms_shipped = 0
  AND i.index_id > 0
  AND i.is_hypothetical = 0
  AND i.is_disabled = 0
  AND i.type IN (1, 2)
ORDER BY s.name, t.name, i.index_id, ic.index_column_id
"#;

const COMMENTS_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    CONVERT(bigint, ep.minor_id) AS minor_id,
    c.name AS column_name,
    CONVERT(nvarchar(max), ep.value) AS comment
FROM sys.extended_properties AS ep
JOIN sys.tables AS t
  ON ep.class = 1
 AND ep.major_id = t.object_id
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
LEFT JOIN sys.columns AS c
  ON c.object_id = t.object_id
 AND c.column_id = ep.minor_id
WHERE t.is_ms_shipped = 0
  AND ep.name = N'MS_Description'
ORDER BY s.name, t.name, ep.minor_id
"#;

type TableKey = (String, String);
type KeyConstraintDetails = (String, String, Vec<(i64, String, bool)>);
type IndexDetails = (
    String,
    bool,
    bool,
    bool,
    Option<String>,
    Vec<(i64, MssqlIndexColumn)>,
);

pub struct MssqlStructFetcher {
    pub connection_pool: MssqlConnectionPool,
    pub schemas: HashSet<String>,
    pub filter: Option<RdbFilter>,
}

impl MssqlStructFetcher {
    pub async fn get_create_schema_statements(
        &mut self,
        schema: &str,
    ) -> anyhow::Result<Vec<MssqlCreateSchemaStatement>> {
        if !schema.is_empty() && !self.schemas.contains(schema) {
            return Ok(Vec::new());
        }

        let targets = if schema.is_empty() {
            self.schemas.clone()
        } else {
            HashSet::from([schema.to_string()])
        };
        if targets.is_empty() {
            return Ok(Vec::new());
        }

        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(SCHEMAS_SQL, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;

        let mut found = HashSet::new();
        for row in rows {
            let name = MssqlColValueConvertor::from_query_required_string(&row, "schema_name")
                .code(ErrorCode::MetadataReadFailed)?;
            if targets.contains(&name) && !self.filter_schema(&name) {
                found.insert(name);
            }
        }

        let mut missing = targets
            .iter()
            .filter(|name| !found.contains(*name) && !self.filter_schema(name))
            .cloned()
            .collect::<Vec<_>>();
        missing.sort();
        if !missing.is_empty() {
            bail!(DtError::DatabaseObjectNotFound(
                DbType::Mssql,
                format!("schemas: {} not found", missing.join(",")),
            ));
        }

        let mut found = found.into_iter().collect::<Vec<_>>();
        found.sort();
        Ok(found
            .into_iter()
            .map(|name| MssqlCreateSchemaStatement {
                schema: Schema { name },
            })
            .collect())
    }

    pub async fn get_create_table_statements(
        &mut self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Vec<MssqlCreateTableStatement>> {
        let mut statements = self.get_tables(schema, table).await?;
        if statements.is_empty() {
            return Ok(Vec::new());
        }

        self.attach_key_constraints(schema, table, &mut statements)
            .await?;
        self.attach_check_constraints(schema, table, &mut statements)
            .await?;
        self.attach_indexes(schema, table, &mut statements).await?;
        self.attach_comments(schema, table, &mut statements).await?;
        Ok(statements.into_values().collect())
    }

    async fn get_tables(
        &self,
        requested_schema: &str,
        requested_table: &str,
    ) -> anyhow::Result<BTreeMap<TableKey, MssqlCreateTableStatement>> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(TABLE_COLUMNS_SQL, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;

        let mut results = BTreeMap::new();
        for row in rows {
            let schema_name =
                MssqlColValueConvertor::from_query_required_string(&row, "schema_name")
                    .code(ErrorCode::MetadataReadFailed)?;
            let table_name = MssqlColValueConvertor::from_query_required_string(&row, "table_name")
                .code(ErrorCode::MetadataReadFailed)?;
            if !self.include_table(&schema_name, &table_name, requested_schema, requested_table) {
                continue;
            }

            let column_name =
                MssqlColValueConvertor::from_query_required_string(&row, "column_name")
                    .code(ErrorCode::MetadataReadFailed)?;
            let type_name = MssqlColValueConvertor::from_query_required_string(&row, "type_name")
                .code(ErrorCode::MetadataReadFailed)?;
            let max_length = MssqlColValueConvertor::from_query_required_i64(&row, "max_length")
                .code(ErrorCode::MetadataReadFailed)?;
            let precision =
                MssqlColValueConvertor::from_query_required_i64(&row, "numeric_precision")
                    .code(ErrorCode::MetadataReadFailed)?;
            let scale = MssqlColValueConvertor::from_query_required_i64(&row, "numeric_scale")
                .code(ErrorCode::MetadataReadFailed)?;
            let computed_definition =
                MssqlColValueConvertor::from_query_optional_string(&row, "computed_definition")
                    .code(ErrorCode::MetadataReadFailed)?;

            let statement = results
                .entry((schema_name.clone(), table_name.clone()))
                .or_insert_with(|| MssqlCreateTableStatement {
                    table: Table {
                        schema_name: schema_name.clone(),
                        table_name: table_name.clone(),
                        ..Default::default()
                    },
                    identity_columns: Vec::new(),
                    computed_columns: Vec::new(),
                    default_constraints: Vec::new(),
                    constraints: Vec::new(),
                    indexes: Vec::new(),
                    comments: Vec::new(),
                });

            statement.table.columns.push(Column {
                column_name: column_name.clone(),
                ordinal_position: u32::try_from(
                    MssqlColValueConvertor::from_query_required_i64(&row, "ordinal_position")
                        .code(ErrorCode::MetadataReadFailed)?,
                )?,
                is_nullable: MssqlColValueConvertor::from_query_required_bool(&row, "is_nullable")
                    .code(ErrorCode::MetadataReadFailed)?,
                column_type: Self::format_column_type(&type_name, max_length, precision, scale),
                collation_name: MssqlColValueConvertor::from_query_optional_string(
                    &row,
                    "collation_name",
                )
                .code(ErrorCode::MetadataReadFailed)?
                .unwrap_or_default(),
                ..Default::default()
            });

            if let Some(definition) = computed_definition {
                statement.computed_columns.push(MssqlComputedColumn {
                    column_name: column_name.clone(),
                    definition,
                    is_persisted: MssqlColValueConvertor::from_query_required_bool(
                        &row,
                        "computed_persisted",
                    )
                    .code(ErrorCode::MetadataReadFailed)?,
                });
            }
            if let Some(seed_value) =
                MssqlColValueConvertor::from_query_optional_string(&row, "identity_seed")
                    .code(ErrorCode::MetadataReadFailed)?
            {
                statement.identity_columns.push(MssqlIdentityColumn {
                    column_name: column_name.clone(),
                    seed_value,
                    increment_value: MssqlColValueConvertor::from_query_required_string(
                        &row,
                        "identity_increment",
                    )
                    .code(ErrorCode::MetadataReadFailed)?,
                    is_not_for_replication: MssqlColValueConvertor::from_query_required_bool(
                        &row,
                        "identity_not_for_replication",
                    )
                    .code(ErrorCode::MetadataReadFailed)?,
                });
            }
            if let Some(constraint_name) =
                MssqlColValueConvertor::from_query_optional_string(&row, "default_constraint_name")
                    .code(ErrorCode::MetadataReadFailed)?
            {
                statement.default_constraints.push(MssqlDefaultConstraint {
                    constraint_name,
                    column_name,
                    definition: MssqlColValueConvertor::from_query_required_string(
                        &row,
                        "default_definition",
                    )
                    .code(ErrorCode::MetadataReadFailed)?,
                });
            }
        }
        Ok(results)
    }

    async fn attach_key_constraints(
        &self,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(KEY_CONSTRAINTS_SQL, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        let mut grouped: BTreeMap<(String, String, String), KeyConstraintDetails> = BTreeMap::new();
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(&schema, &table, requested_schema, requested_table) {
                continue;
            }
            let name = Self::required_string(&row, "constraint_name")?;
            let constraint_type = Self::required_string(&row, "constraint_type")?;
            let index_type = Self::required_string(&row, "index_type_desc")?;
            let entry = grouped
                .entry((schema, table, name))
                .or_insert_with(|| (constraint_type, index_type, Vec::new()));
            entry.2.push((
                MssqlColValueConvertor::from_query_required_i64(&row, "key_ordinal")
                    .code(ErrorCode::MetadataReadFailed)?,
                Self::required_string(&row, "column_name")?,
                MssqlColValueConvertor::from_query_required_bool(&row, "is_descending_key")
                    .code(ErrorCode::MetadataReadFailed)?,
            ));
        }

        for ((schema, table, name), (constraint_type, index_type, mut columns)) in grouped {
            columns.sort_by_key(|column| column.0);
            let columns = columns
                .into_iter()
                .map(|(_, name, descending)| {
                    format!(
                        "{} {}",
                        SqlUtil::escape_by_db_type(&name, &DbType::Mssql),
                        if descending { "DESC" } else { "ASC" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            if let Some(statement) = statements.get_mut(&(schema.clone(), table.clone())) {
                statement.constraints.push(Constraint {
                    database_name: String::new(),
                    schema_name: schema,
                    table_name: table,
                    constraint_name: name,
                    constraint_type: ConstraintType::from_str(&constraint_type, DbType::Mssql),
                    definition: format!("{constraint_type} {index_type} ({columns})"),
                });
            }
        }
        Ok(())
    }

    async fn attach_check_constraints(
        &self,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(CHECK_CONSTRAINTS_SQL, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(&schema, &table, requested_schema, requested_table) {
                continue;
            }
            if let Some(statement) = statements.get_mut(&(schema.clone(), table.clone())) {
                let not_for_replication = MssqlColValueConvertor::from_query_required_bool(
                    &row,
                    "is_not_for_replication",
                )
                .code(ErrorCode::MetadataReadFailed)?;
                statement.constraints.push(Constraint {
                    database_name: String::new(),
                    schema_name: schema,
                    table_name: table,
                    constraint_name: Self::required_string(&row, "constraint_name")?,
                    constraint_type: ConstraintType::Check,
                    definition: format!(
                        "CHECK {}{}",
                        if not_for_replication {
                            "NOT FOR REPLICATION "
                        } else {
                            ""
                        },
                        Self::required_string(&row, "definition")?
                    ),
                });
            }
        }
        Ok(())
    }

    async fn attach_indexes(
        &self,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(INDEXES_SQL, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        let mut grouped: BTreeMap<(String, String, String), IndexDetails> = BTreeMap::new();
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(&schema, &table, requested_schema, requested_table) {
                continue;
            }
            let name = Self::required_string(&row, "index_name")?;
            let index_type = Self::required_string(&row, "index_type_desc")?;
            let unique = MssqlColValueConvertor::from_query_required_bool(&row, "is_unique")
                .code(ErrorCode::MetadataReadFailed)?;
            let primary = MssqlColValueConvertor::from_query_required_bool(&row, "is_primary_key")
                .code(ErrorCode::MetadataReadFailed)?;
            let unique_constraint =
                MssqlColValueConvertor::from_query_required_bool(&row, "is_unique_constraint")
                    .code(ErrorCode::MetadataReadFailed)?;
            let filter =
                MssqlColValueConvertor::from_query_optional_string(&row, "filter_definition")
                    .code(ErrorCode::MetadataReadFailed)?;
            let entry = grouped.entry((schema, table, name)).or_insert_with(|| {
                (
                    index_type,
                    unique,
                    primary,
                    unique_constraint,
                    filter,
                    Vec::new(),
                )
            });
            entry.5.push((
                MssqlColValueConvertor::from_query_required_i64(&row, "index_column_id")
                    .code(ErrorCode::MetadataReadFailed)?,
                MssqlIndexColumn {
                    column_name: Self::required_string(&row, "column_name")?,
                    key_ordinal: u32::try_from(
                        MssqlColValueConvertor::from_query_required_i64(&row, "key_ordinal")
                            .code(ErrorCode::MetadataReadFailed)?,
                    )?,
                    is_descending_key: MssqlColValueConvertor::from_query_required_bool(
                        &row,
                        "is_descending_key",
                    )
                    .code(ErrorCode::MetadataReadFailed)?,
                    is_included_column: MssqlColValueConvertor::from_query_required_bool(
                        &row,
                        "is_included_column",
                    )
                    .code(ErrorCode::MetadataReadFailed)?,
                },
            ));
        }

        for (
            (schema, table, name),
            (index_type, unique, primary, unique_constraint, filter, mut columns),
        ) in grouped
        {
            columns.sort_by_key(|column| column.0);
            if let Some(statement) = statements.get_mut(&(schema.clone(), table.clone())) {
                statement.indexes.push(MssqlIndex {
                    index: Index {
                        schema_name: schema,
                        table_name: table,
                        index_name: name,
                        index_kind: if unique {
                            IndexKind::Unique
                        } else {
                            IndexKind::Unknown
                        },
                        index_type: IndexType::Btree,
                        ..Default::default()
                    },
                    index_type_desc: index_type,
                    is_primary_key: primary,
                    is_unique_constraint: unique_constraint,
                    filter_definition: filter,
                    columns: columns.into_iter().map(|column| column.1).collect(),
                });
            }
        }
        Ok(())
    }

    async fn attach_comments(
        &self,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(COMMENTS_SQL, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(&schema, &table, requested_schema, requested_table) {
                continue;
            }
            if let Some(statement) = statements.get_mut(&(schema.clone(), table.clone())) {
                let minor_id = MssqlColValueConvertor::from_query_required_i64(&row, "minor_id")
                    .code(ErrorCode::MetadataReadFailed)?;
                let column_name =
                    MssqlColValueConvertor::from_query_optional_string(&row, "column_name")
                        .code(ErrorCode::MetadataReadFailed)?
                        .unwrap_or_default();
                statement.comments.push(Comment {
                    comment_type: if minor_id == 0 {
                        CommentType::Table
                    } else {
                        CommentType::Column
                    },
                    database_name: String::new(),
                    schema_name: schema,
                    table_name: table,
                    column_name,
                    comment: Self::required_string(&row, "comment")?,
                });
            }
        }
        Ok(())
    }

    fn include_table(
        &self,
        schema: &str,
        table: &str,
        requested_schema: &str,
        requested_table: &str,
    ) -> bool {
        self.schemas.contains(schema)
            && (requested_schema.is_empty() || requested_schema == schema)
            && (requested_table.is_empty() || requested_table == table)
            && self
                .filter
                .as_ref()
                .map(|filter| !filter.filter_tb(schema, table))
                .unwrap_or(true)
    }

    fn filter_schema(&self, schema: &str) -> bool {
        self.filter
            .as_ref()
            .map(|filter| filter.filter_schema(schema))
            .unwrap_or(false)
    }

    fn required_string(row: &tiberius::Row, column: &str) -> anyhow::Result<String> {
        MssqlColValueConvertor::from_query_required_string(row, column)
            .code(ErrorCode::MetadataReadFailed)
    }

    fn format_column_type(type_name: &str, max_length: i64, precision: i64, scale: i64) -> String {
        let type_name = type_name.to_uppercase();
        match type_name.as_str() {
            "CHAR" | "VARCHAR" | "BINARY" | "VARBINARY" => {
                let length = if max_length == -1 {
                    "MAX".to_string()
                } else {
                    max_length.to_string()
                };
                format!("{type_name}({length})")
            }
            "NCHAR" | "NVARCHAR" => {
                let length = if max_length == -1 {
                    "MAX".to_string()
                } else {
                    (max_length / 2).to_string()
                };
                format!("{type_name}({length})")
            }
            "DECIMAL" | "NUMERIC" => format!("{type_name}({precision}, {scale})"),
            "TIME" | "DATETIME2" | "DATETIMEOFFSET" => format!("{type_name}({scale})"),
            "FLOAT" => format!("FLOAT({precision})"),
            _ => type_name,
        }
    }
}
