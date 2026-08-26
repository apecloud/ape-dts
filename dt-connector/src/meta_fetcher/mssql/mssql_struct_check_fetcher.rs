use std::collections::BTreeMap;

use dt_common::{
    config::config_enums::DbType,
    error::{DtResultExt, ErrorCode},
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        mssql::mssql_connection_pool::MssqlConnectionPool,
    },
    utils::sql_util::SqlUtil,
};
use tiberius::{Query, Row};

const DATABASE_SQL: &str = r#"
SELECT
    d.collation_name,
    (
        SELECT CONVERT(nvarchar(max), ep.value)
        FROM {catalog}sys.extended_properties AS ep
        WHERE ep.class = 0
          AND ep.major_id = 0
          AND ep.minor_id = 0
          AND ep.name = N'MS_Description'
    ) AS comment
FROM sys.databases AS d
WHERE d.name = @P1
"#;

const SCHEMA_SQL: &str = r#"
SELECT CONVERT(nvarchar(max), ep.value) AS comment
FROM {catalog}sys.schemas AS s
LEFT JOIN {catalog}sys.extended_properties AS ep
  ON ep.class = 3
 AND ep.major_id = s.schema_id
 AND ep.minor_id = 0
 AND ep.name = N'MS_Description'
WHERE s.name = @P1
"#;

const SEQUENCES_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    seq.name AS sequence_name,
    ty.name AS type_name,
    CONVERT(nvarchar(max), seq.precision) AS numeric_precision,
    CONVERT(nvarchar(max), seq.scale) AS numeric_scale,
    CONVERT(nvarchar(max), seq.start_value) AS start_value,
    CONVERT(nvarchar(max), seq.increment) AS increment,
    CONVERT(nvarchar(max), seq.minimum_value) AS minimum_value,
    CONVERT(nvarchar(max), seq.maximum_value) AS maximum_value,
    CONVERT(nvarchar(max), seq.is_cycling) AS is_cycling,
    CONVERT(nvarchar(max), seq.is_cached) AS is_cached,
    CONVERT(nvarchar(max), seq.cache_size) AS cache_size,
    CONVERT(nvarchar(max), ep.value) AS comment
FROM {catalog}sys.sequences AS seq
JOIN {catalog}sys.schemas AS s ON s.schema_id = seq.schema_id
JOIN {catalog}sys.types AS ty
  ON ty.user_type_id = seq.system_type_id
 AND ty.is_user_defined = 0
LEFT JOIN {catalog}sys.extended_properties AS ep
  ON ep.class = 1
 AND ep.major_id = seq.object_id
 AND ep.minor_id = 0
 AND ep.name = N'MS_Description'
ORDER BY s.name, seq.name
"#;

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
FROM {catalog}sys.tables AS t
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
JOIN {catalog}sys.columns AS c ON c.object_id = t.object_id
JOIN {catalog}sys.types AS ty ON ty.user_type_id = c.user_type_id
LEFT JOIN {catalog}sys.default_constraints AS dc
  ON dc.parent_object_id = c.object_id
 AND dc.parent_column_id = c.column_id
LEFT JOIN {catalog}sys.identity_columns AS ic
  ON ic.object_id = c.object_id
 AND ic.column_id = c.column_id
LEFT JOIN {catalog}sys.computed_columns AS cc
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
    CONVERT(nvarchar(max), NULL) AS is_not_for_replication,
    CONVERT(nvarchar(max), NULL) AS is_disabled,
    CONVERT(nvarchar(max), NULL) AS is_not_trusted
FROM {catalog}sys.key_constraints AS kc
JOIN {catalog}sys.tables AS t ON t.object_id = kc.parent_object_id
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
JOIN {catalog}sys.indexes AS i
  ON i.object_id = kc.parent_object_id
 AND i.index_id = kc.unique_index_id
JOIN {catalog}sys.index_columns AS ic
  ON ic.object_id = i.object_id
 AND ic.index_id = i.index_id
JOIN {catalog}sys.columns AS c
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
    CONVERT(nvarchar(max), cc.is_not_for_replication) AS is_not_for_replication,
    CONVERT(nvarchar(max), cc.is_disabled) AS is_disabled,
    CONVERT(nvarchar(max), cc.is_not_trusted) AS is_not_trusted
FROM {catalog}sys.check_constraints AS cc
JOIN {catalog}sys.tables AS t ON t.object_id = cc.parent_object_id
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = @P1
  AND t.name = @P2
ORDER BY constraint_name, key_ordinal
"#;

const INDEXES_SQL: &str = r#"
SELECT
    i.name AS index_name,
    CONVERT(nvarchar(max), i.type) AS index_type,
    i.type_desc AS index_type_desc,
    CONVERT(nvarchar(max), i.is_unique) AS is_unique,
    CONVERT(nvarchar(max), i.is_disabled) AS is_disabled,
    CONVERT(nvarchar(max), i.is_primary_key) AS is_primary_key,
    CONVERT(nvarchar(max), i.is_unique_constraint) AS is_unique_constraint,
    i.filter_definition,
    c.name AS column_name,
    CONVERT(nvarchar(max), ic.key_ordinal) AS key_ordinal,
    CONVERT(nvarchar(max), ic.is_descending_key) AS is_descending_key,
    CONVERT(nvarchar(max), ic.is_included_column) AS is_included_column,
    CONVERT(nvarchar(max), ic.index_column_id) AS index_column_id,
    pxi.name AS xml_primary_index_name,
    xi.secondary_type_desc AS xml_secondary_type_desc,
    CONVERT(nvarchar(max), hi.bucket_count) AS hash_bucket_count
FROM {catalog}sys.tables AS t
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
JOIN {catalog}sys.indexes AS i ON i.object_id = t.object_id
JOIN {catalog}sys.index_columns AS ic
  ON ic.object_id = i.object_id
 AND ic.index_id = i.index_id
JOIN {catalog}sys.columns AS c
  ON c.object_id = ic.object_id
 AND c.column_id = ic.column_id
LEFT JOIN {catalog}sys.xml_indexes AS xi
  ON xi.object_id = i.object_id
 AND xi.index_id = i.index_id
LEFT JOIN {catalog}sys.indexes AS pxi
  ON pxi.object_id = xi.object_id
 AND pxi.index_id = xi.using_xml_index_id
LEFT JOIN {catalog}sys.hash_indexes AS hi
  ON hi.object_id = i.object_id
 AND hi.index_id = i.index_id
WHERE s.name = @P1
  AND t.name = @P2
  AND i.type > 0
  AND i.is_hypothetical = 0
ORDER BY i.name, ic.index_column_id
"#;

const COMMENTS_SQL: &str = r#"
SELECT
    CASE WHEN ep.minor_id = 0 THEN N'TABLE' ELSE N'COLUMN' END AS comment_type,
    c.name AS object_name,
    CONVERT(nvarchar(max), ep.value) AS comment
FROM {catalog}sys.extended_properties AS ep
JOIN {catalog}sys.tables AS t
  ON ep.class = 1
 AND ep.major_id = t.object_id
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
LEFT JOIN {catalog}sys.columns AS c
  ON c.object_id = t.object_id
 AND c.column_id = ep.minor_id
WHERE s.name = @P1
  AND t.name = @P2
  AND ep.name = N'MS_Description'
UNION ALL
SELECT
    N'CONSTRAINT' AS comment_type,
    o.name AS object_name,
    CONVERT(nvarchar(max), ep.value) AS comment
FROM {catalog}sys.extended_properties AS ep
JOIN {catalog}sys.objects AS o
  ON ep.class = 1
 AND ep.major_id = o.object_id
 AND ep.minor_id = 0
JOIN {catalog}sys.tables AS t ON t.object_id = o.parent_object_id
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
WHERE o.type IN (N'C', N'D', N'PK', N'UQ')
  AND s.name = @P1
  AND t.name = @P2
  AND ep.name = N'MS_Description'
UNION ALL
SELECT
    N'INDEX' AS comment_type,
    i.name AS object_name,
    CONVERT(nvarchar(max), ep.value) AS comment
FROM {catalog}sys.extended_properties AS ep
JOIN {catalog}sys.tables AS t
  ON ep.class = 7
 AND ep.major_id = t.object_id
JOIN {catalog}sys.indexes AS i
  ON i.object_id = ep.major_id
 AND i.index_id = ep.minor_id
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = @P1
  AND t.name = @P2
  AND ep.name = N'MS_Description'
ORDER BY comment_type, object_name
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
    pub async fn fetch_database(&self, db: &str) -> anyhow::Result<BTreeMap<String, String>> {
        let mut query = Query::new(Self::catalog_sql(DATABASE_SQL, db));
        query.bind(db);
        let mut connection = self.connection_pool.get().await?;
        let rows = query
            .query(connection.client_mut())
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        let Some(row) = rows.first() else {
            anyhow::bail!("MSSQL database {db} was not found");
        };
        Self::parse_row(row, &["collation_name", "comment"])
    }

    pub async fn fetch_schema(
        &self,
        db: &str,
        schema: &str,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        let mut query = Query::new(Self::catalog_sql(SCHEMA_SQL, db));
        query.bind(schema);
        let mut connection = self.connection_pool.get().await?;
        let rows = query
            .query(connection.client_mut())
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        let Some(row) = rows.first() else {
            anyhow::bail!("MSSQL schema {db}.{schema} was not found");
        };
        Self::parse_row(row, &["comment"])
    }

    pub async fn fetch_sequences(&self, db: &str) -> anyhow::Result<Vec<BTreeMap<String, String>>> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&Self::catalog_sql(SEQUENCES_SQL, db), &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        rows.iter()
            .map(|row| {
                Self::parse_row(
                    row,
                    &[
                        "schema_name",
                        "sequence_name",
                        "type_name",
                        "numeric_precision",
                        "numeric_scale",
                        "start_value",
                        "increment",
                        "minimum_value",
                        "maximum_value",
                        "is_cycling",
                        "is_cached",
                        "cache_size",
                        "comment",
                    ],
                )
            })
            .collect()
    }

    pub async fn fetch_table(
        &self,
        db: &str,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<MssqlCheckTableInfo> {
        let columns = self
            .fetch_rows(
                COLUMNS_SQL,
                db,
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
            anyhow::bail!("MSSQL table {db}.{schema}.{table} was not found");
        }

        Ok(MssqlCheckTableInfo {
            columns,
            constraints: self
                .fetch_rows(
                    CONSTRAINTS_SQL,
                    db,
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
                        "is_disabled",
                        "is_not_trusted",
                    ],
                )
                .await?,
            indexes: self
                .fetch_rows(
                    INDEXES_SQL,
                    db,
                    schema,
                    table,
                    &[
                        "index_name",
                        "index_type",
                        "index_type_desc",
                        "is_unique",
                        "is_disabled",
                        "is_primary_key",
                        "is_unique_constraint",
                        "filter_definition",
                        "column_name",
                        "key_ordinal",
                        "is_descending_key",
                        "is_included_column",
                        "index_column_id",
                        "xml_primary_index_name",
                        "xml_secondary_type_desc",
                        "hash_bucket_count",
                    ],
                )
                .await?,
            comments: self
                .fetch_rows(
                    COMMENTS_SQL,
                    db,
                    schema,
                    table,
                    &["comment_type", "object_name", "comment"],
                )
                .await?,
        })
    }

    async fn fetch_rows(
        &self,
        sql: &str,
        db: &str,
        schema: &str,
        table: &str,
        columns: &[&str],
    ) -> anyhow::Result<Vec<BTreeMap<String, String>>> {
        let mut query = Query::new(Self::catalog_sql(sql, db));
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

    fn catalog_sql(template: &str, db: &str) -> String {
        let catalog = if db.is_empty() {
            String::new()
        } else {
            format!("{}.", SqlUtil::escape_by_db_type(db, &DbType::Mssql))
        };
        template.replace("{catalog}", &catalog)
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
