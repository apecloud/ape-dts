use std::collections::BTreeMap;

use dt_common::{
    error::{DtResultExt, ErrorCode},
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        mssql::mssql_connection_pool::MssqlConnectionPool,
    },
};
use tiberius::{Query, Row};

const COLUMNS_SQL: &str = r#"
SELECT
    CONVERT(nvarchar(max), c.column_id) AS ordinal_position,
    c.name AS column_name,
    ty.name AS type_name,
    CONVERT(nvarchar(max), c.max_length) AS max_length,
    CONVERT(nvarchar(max), c.precision) AS numeric_precision,
    CONVERT(nvarchar(max), c.scale) AS numeric_scale,
    CONVERT(nvarchar(max), c.is_nullable) AS is_nullable,
    c.collation_name,
    dc.name AS default_constraint_name,
    dc.definition AS default_definition,
    CONVERT(nvarchar(max), ic.seed_value) AS identity_seed,
    CONVERT(nvarchar(max), ic.increment_value) AS identity_increment,
    CONVERT(nvarchar(max), ic.is_not_for_replication) AS identity_not_for_replication,
    cc.definition AS computed_definition,
    CONVERT(nvarchar(max), cc.is_persisted) AS computed_persisted
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
WHERE s.name = @P1
  AND t.name = @P2
  AND t.is_ms_shipped = 0
ORDER BY c.column_id
"#;

const CONSTRAINTS_SQL: &str = r#"
SELECT
    kc.name AS constraint_name,
    CONVERT(nvarchar(max), kc.type) AS constraint_type,
    i.type_desc AS index_type_desc,
    c.name AS column_name,
    CONVERT(nvarchar(max), ic.key_ordinal) AS key_ordinal,
    CONVERT(nvarchar(max), ic.is_descending_key) AS is_descending_key,
    CONVERT(nvarchar(max), NULL) AS definition,
    CONVERT(nvarchar(max), NULL) AS is_not_for_replication
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
WHERE s.name = @P1
  AND t.name = @P2
  AND ic.is_included_column = 0
  AND ic.key_ordinal > 0
UNION ALL
SELECT
    cc.name AS constraint_name,
    N'C' AS constraint_type,
    CONVERT(nvarchar(max), NULL) AS index_type_desc,
    CONVERT(nvarchar(max), NULL) AS column_name,
    CONVERT(nvarchar(max), NULL) AS key_ordinal,
    CONVERT(nvarchar(max), NULL) AS is_descending_key,
    cc.definition,
    CONVERT(nvarchar(max), cc.is_not_for_replication) AS is_not_for_replication
FROM sys.check_constraints AS cc
JOIN sys.tables AS t ON t.object_id = cc.parent_object_id
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = @P1
  AND t.name = @P2
ORDER BY constraint_name, key_ordinal
"#;

const INDEXES_SQL: &str = r#"
SELECT
    i.name AS index_name,
    i.type_desc AS index_type_desc,
    CONVERT(nvarchar(max), i.is_unique) AS is_unique,
    CONVERT(nvarchar(max), i.is_primary_key) AS is_primary_key,
    CONVERT(nvarchar(max), i.is_unique_constraint) AS is_unique_constraint,
    i.filter_definition,
    c.name AS column_name,
    CONVERT(nvarchar(max), ic.key_ordinal) AS key_ordinal,
    CONVERT(nvarchar(max), ic.is_descending_key) AS is_descending_key,
    CONVERT(nvarchar(max), ic.is_included_column) AS is_included_column,
    CONVERT(nvarchar(max), ic.index_column_id) AS index_column_id
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.indexes AS i ON i.object_id = t.object_id
JOIN sys.index_columns AS ic
  ON ic.object_id = i.object_id
 AND ic.index_id = i.index_id
JOIN sys.columns AS c
  ON c.object_id = ic.object_id
 AND c.column_id = ic.column_id
WHERE s.name = @P1
  AND t.name = @P2
  AND i.index_id > 0
  AND i.is_hypothetical = 0
  AND i.is_disabled = 0
  AND i.type IN (1, 2)
ORDER BY i.name, ic.index_column_id
"#;

const COMMENTS_SQL: &str = r#"
SELECT
    CONVERT(nvarchar(max), ep.minor_id) AS minor_id,
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
WHERE s.name = @P1
  AND t.name = @P2
  AND ep.name = N'MS_Description'
ORDER BY ep.minor_id
"#;

const VIEW_EXISTS_SQL: &str = r#"
SELECT CONVERT(nvarchar(max), COUNT_BIG(*)) AS object_count
FROM sys.views AS v
JOIN sys.schemas AS s ON s.schema_id = v.schema_id
WHERE s.name = @P1
  AND v.name = @P2
"#;

#[derive(Debug, PartialEq, Eq)]
pub struct MssqlCheckTableInfo {
    pub columns: Vec<BTreeMap<String, String>>,
    pub constraints: Vec<BTreeMap<String, String>>,
    pub indexes: Vec<BTreeMap<String, String>>,
    pub comments: Vec<BTreeMap<String, String>>,
}

pub struct MssqlStructCheckFetcher {
    pub connection_pool: MssqlConnectionPool,
}

impl MssqlStructCheckFetcher {
    pub async fn view_exists(&self, schema: &str, view: &str) -> anyhow::Result<bool> {
        let rows = self
            .fetch_rows(VIEW_EXISTS_SQL, schema, view, &["object_count"])
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.get("object_count"))
            .is_some_and(|count| count != "0"))
    }

    pub async fn fetch_table(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<MssqlCheckTableInfo> {
        let columns = self
            .fetch_rows(
                COLUMNS_SQL,
                schema,
                table,
                &[
                    "ordinal_position",
                    "column_name",
                    "type_name",
                    "max_length",
                    "numeric_precision",
                    "numeric_scale",
                    "is_nullable",
                    "collation_name",
                    "default_constraint_name",
                    "default_definition",
                    "identity_seed",
                    "identity_increment",
                    "identity_not_for_replication",
                    "computed_definition",
                    "computed_persisted",
                ],
            )
            .await?;
        if columns.is_empty() {
            anyhow::bail!("MSSQL table {schema}.{table} was not found");
        }

        Ok(MssqlCheckTableInfo {
            columns,
            constraints: self
                .fetch_rows(
                    CONSTRAINTS_SQL,
                    schema,
                    table,
                    &[
                        "constraint_name",
                        "constraint_type",
                        "index_type_desc",
                        "column_name",
                        "key_ordinal",
                        "is_descending_key",
                        "definition",
                        "is_not_for_replication",
                    ],
                )
                .await?,
            indexes: self
                .fetch_rows(
                    INDEXES_SQL,
                    schema,
                    table,
                    &[
                        "index_name",
                        "index_type_desc",
                        "is_unique",
                        "is_primary_key",
                        "is_unique_constraint",
                        "filter_definition",
                        "column_name",
                        "key_ordinal",
                        "is_descending_key",
                        "is_included_column",
                        "index_column_id",
                    ],
                )
                .await?,
            comments: self
                .fetch_rows(
                    COMMENTS_SQL,
                    schema,
                    table,
                    &["minor_id", "column_name", "comment"],
                )
                .await?,
        })
    }

    async fn fetch_rows(
        &self,
        sql: &str,
        schema: &str,
        table: &str,
        columns: &[&str],
    ) -> anyhow::Result<Vec<BTreeMap<String, String>>> {
        let mut query = Query::new(sql);
        query.bind(schema);
        query.bind(table);
        let mut connection = self.connection_pool.get().await?;
        let rows = query
            .query(connection.client_mut())
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        rows.iter()
            .map(|row| Self::parse_row(row, columns))
            .collect()
    }

    fn parse_row(row: &Row, columns: &[&str]) -> anyhow::Result<BTreeMap<String, String>> {
        columns
            .iter()
            .map(|column| {
                let value = MssqlColValueConvertor::from_query_optional_string(row, column)?
                    .unwrap_or_default();
                Ok(((*column).to_string(), value))
            })
            .collect()
    }
}
