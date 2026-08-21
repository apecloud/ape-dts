use std::collections::{HashMap, HashSet};

use tiberius::Query;

use crate::{
    config::config_enums::DbType,
    error::{DtError, DtErrorContextExt, DtResultExt, ErrorCode, ErrorObject},
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        ddl_meta::ddl_data::DdlData,
        rdb_meta_manager::{RdbMetaManager, RDB_PRIMARY_KEY_FLAG},
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
    },
    utils::sql_util::SqlUtil,
};

use super::{
    mssql_col_type::{parse_mssql_col_type_with_length, MssqlColType},
    mssql_connection_pool::MssqlConnectionPool,
    mssql_tb_meta::MssqlTbMeta,
};

const DATABASES_SQL: &str = r#"
SELECT name AS database_name
FROM sys.databases
WHERE state_desc = 'ONLINE'
  AND HAS_DBACCESS(name) = 1
ORDER BY name
"#;

const TABLE_COLUMNS_SQL: &str = r#"
SELECT
    c.name AS column_name,
    user_type.name AS user_type_name,
    COALESCE(system_type.name, user_type.name) AS system_type_name,
    c.max_length,
    c.is_nullable,
    c.is_identity,
    c.is_computed,
    c.generated_always_type
FROM {catalog}sys.tables AS t
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
JOIN {catalog}sys.columns AS c ON c.object_id = t.object_id
JOIN {catalog}sys.types AS user_type ON user_type.user_type_id = c.user_type_id
LEFT JOIN {catalog}sys.types AS system_type
  ON system_type.system_type_id = c.system_type_id
 AND system_type.user_type_id = system_type.system_type_id
WHERE s.name = @P1
  AND t.name = @P2
  AND t.is_ms_shipped = 0
ORDER BY c.column_id
"#;

const TABLE_KEYS_SQL: &str = r#"
SELECT
    i.name AS index_name,
    i.is_primary_key,
    c.name AS column_name
FROM {catalog}sys.tables AS t
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
JOIN {catalog}sys.indexes AS i ON i.object_id = t.object_id
JOIN {catalog}sys.index_columns AS ic
  ON ic.object_id = i.object_id
 AND ic.index_id = i.index_id
JOIN {catalog}sys.columns AS c
  ON c.object_id = ic.object_id
 AND c.column_id = ic.column_id
WHERE s.name = @P1
  AND t.name = @P2
  AND t.is_ms_shipped = 0
  AND i.is_unique = 1
  AND i.is_disabled = 0
  AND i.is_hypothetical = 0
  AND i.has_filter = 0
  AND ic.is_included_column = 0
  AND ic.key_ordinal > 0
ORDER BY i.index_id, ic.key_ordinal
"#;

const SCHEMAS_SQL: &str = r#"
SELECT DISTINCT s.name AS schema_name
FROM {catalog}sys.schemas AS s
JOIN {catalog}sys.tables AS t ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name
"#;

const TABLES_SQL: &str = r#"
SELECT t.name AS table_name
FROM {catalog}sys.tables AS t
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = @P1
  AND t.is_ms_shipped = 0
ORDER BY t.name
"#;

const SCHEMA_TABLES_SQL: &str = r#"
SELECT s.name AS schema_name, t.name AS table_name
FROM {catalog}sys.tables AS t
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name
"#;

struct ParsedColumns {
    cols: Vec<String>,
    col_origin_type_map: HashMap<String, String>,
    col_type_map: HashMap<String, MssqlColType>,
    nullable_cols: HashSet<String>,
    identity_col: Option<String>,
    computed_cols: HashSet<String>,
    generated_always_type_map: HashMap<String, u8>,
    rowversion_cols: HashSet<String>,
}

#[derive(Clone)]
pub struct MssqlMetaManager {
    pub connection_pool: MssqlConnectionPool,
    cache: HashMap<(String, String, String), MssqlTbMeta>,
}

impl MssqlMetaManager {
    pub async fn new(connection_pool: MssqlConnectionPool) -> anyhow::Result<Self> {
        Ok(Self {
            connection_pool,
            cache: HashMap::new(),
        })
    }

    pub async fn get_tb_meta<'a>(
        &'a mut self,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<&'a MssqlTbMeta> {
        let cache_key = (db.to_string(), schema.to_string(), tb.to_string());
        if !self.cache.contains_key(&cache_key) {
            let ParsedColumns {
                cols,
                col_origin_type_map,
                col_type_map,
                nullable_cols,
                identity_col,
                computed_cols,
                generated_always_type_map,
                rowversion_cols,
            } = self.parse_cols(db, schema, tb).await?;
            if cols.is_empty() {
                return Err(Self::table_not_found(db, schema, tb));
            }

            let key_map = self.parse_keys(db, schema, tb).await?;
            let (order_cols, partition_col, id_cols) =
                RdbMetaManager::parse_rdb_cols(&key_map, &cols, &nullable_cols)?;
            self.cache.insert(
                cache_key.clone(),
                MssqlTbMeta {
                    basic: RdbTbMeta {
                        db: db.to_string(),
                        schema: schema.to_string(),
                        tb: tb.to_string(),
                        cols,
                        nullable_cols,
                        col_origin_type_map,
                        key_map,
                        order_cols,
                        partition_col,
                        id_cols,
                        foreign_keys: vec![],
                        ref_by_foreign_keys: vec![],
                    },
                    col_type_map,
                    identity_col,
                    computed_cols,
                    generated_always_type_map,
                    rowversion_cols,
                },
            );
        }

        self.cache
            .get(&cache_key)
            .ok_or_else(|| Self::table_not_found(db, schema, tb))
    }

    pub async fn get_tb_meta_by_row_data<'a>(
        &'a mut self,
        row_data: &RowData,
    ) -> anyhow::Result<&'a MssqlTbMeta> {
        self.get_tb_meta(&row_data.db, &row_data.schema, &row_data.tb)
            .await
    }

    pub async fn list_databases(&self) -> anyhow::Result<Vec<String>> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(DATABASES_SQL, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;

        rows.iter()
            .map(|row| {
                MssqlColValueConvertor::from_query_required_string(row, "database_name")
                    .code(ErrorCode::MetadataReadFailed)
            })
            .collect()
    }

    pub async fn list_schemas(&self, db: &str) -> anyhow::Result<Vec<String>> {
        let sql = Self::catalog_sql(SCHEMAS_SQL, db);
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&sql, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;

        rows.iter()
            .map(|row| {
                MssqlColValueConvertor::from_query_required_string(row, "schema_name")
                    .code(ErrorCode::MetadataReadFailed)
            })
            .collect()
    }

    pub async fn list_tables(&self, db: &str, schema: &str) -> anyhow::Result<Vec<String>> {
        let mut query = Query::new(Self::catalog_sql(TABLES_SQL, db));
        query.bind(schema);
        let mut connection = self.connection_pool.get().await?;
        let rows = query
            .query(connection.client_mut())
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;

        rows.iter()
            .map(|row| {
                MssqlColValueConvertor::from_query_required_string(row, "table_name")
                    .code(ErrorCode::MetadataReadFailed)
            })
            .collect()
    }

    pub async fn list_schema_tables(&self, db: &str) -> anyhow::Result<Vec<(String, String)>> {
        let sql = Self::catalog_sql(SCHEMA_TABLES_SQL, db);
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&sql, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;

        rows.iter()
            .map(|row| {
                Ok((
                    MssqlColValueConvertor::from_query_required_string(row, "schema_name")?,
                    MssqlColValueConvertor::from_query_required_string(row, "table_name")?,
                ))
            })
            .collect()
    }

    pub fn invalidate_cache(&mut self, db: &str, schema: &str, tb: &str) {
        if db.is_empty() && schema.is_empty() {
            self.cache.clear();
        } else if schema.is_empty() {
            self.cache.retain(|(cached_db, _, _), _| cached_db != db);
        } else if tb.is_empty() {
            self.cache.retain(|(cached_db, cached_schema, _), _| {
                cached_db != db || cached_schema != schema
            });
        } else {
            self.cache
                .remove(&(db.to_string(), schema.to_string(), tb.to_string()));
        }
    }

    pub fn invalidate_cache_for_table(&mut self, db: &str, schema: &str, tb: &str) {
        if !schema.is_empty() && !tb.is_empty() {
            self.invalidate_cache(db, schema, tb);
        }
    }

    pub fn invalidate_cache_by_ddl_data(&mut self, ddl_data: &DdlData) {
        let (db, schema, tb) = ddl_data.get_db_schema_tb();
        self.invalidate_cache(&db, &schema, &tb);
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.connection_pool.close().await
    }

    async fn parse_cols(&self, db: &str, schema: &str, tb: &str) -> anyhow::Result<ParsedColumns> {
        let mut query = Query::new(Self::catalog_sql(TABLE_COLUMNS_SQL, db));
        query.bind(schema);
        query.bind(tb);
        let mut connection = self.connection_pool.get().await?;
        let rows = query
            .query(connection.client_mut())
            .await
            .code(ErrorCode::MetadataReadFailed)
            .object(Self::table_object(schema, tb))?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)
            .object(Self::table_object(schema, tb))?;

        let mut cols = Vec::with_capacity(rows.len());
        let mut col_origin_type_map = HashMap::with_capacity(rows.len());
        let mut col_type_map = HashMap::with_capacity(rows.len());
        let mut nullable_cols = HashSet::new();
        let mut identity_col = None;
        let mut computed_cols = HashSet::new();
        let mut generated_always_type_map = HashMap::with_capacity(rows.len());
        let mut rowversion_cols = HashSet::new();
        for row in rows {
            let col = MssqlColValueConvertor::from_query_required_string(&row, "column_name")
                .code(ErrorCode::MetadataReadFailed)?;
            let user_type_name =
                MssqlColValueConvertor::from_query_required_string(&row, "user_type_name")
                    .code(ErrorCode::MetadataReadFailed)?;
            let system_type_name =
                MssqlColValueConvertor::from_query_required_string(&row, "system_type_name")
                    .code(ErrorCode::MetadataReadFailed)?;
            let max_length = MssqlColValueConvertor::from_query_required_i16(&row, "max_length")
                .code(ErrorCode::MetadataReadFailed)?;
            let is_nullable = MssqlColValueConvertor::from_query_required_bool(&row, "is_nullable")
                .code(ErrorCode::MetadataReadFailed)?;
            let is_identity = MssqlColValueConvertor::from_query_required_bool(&row, "is_identity")
                .code(ErrorCode::MetadataReadFailed)?;
            let is_computed = MssqlColValueConvertor::from_query_required_bool(&row, "is_computed")
                .code(ErrorCode::MetadataReadFailed)?;
            let generated_always_type =
                MssqlColValueConvertor::from_query_required_u8(&row, "generated_always_type")
                    .code(ErrorCode::MetadataReadFailed)?;
            let col_type = parse_mssql_col_type_with_length(&system_type_name, max_length)
                .map_err(|error| {
                    DtError::DatabaseUnsupportedTableStructure(
                        DbType::Mssql,
                        format!(
                            "column {schema}.{tb}.{col} uses unsupported type {user_type_name} \
                             (system type {system_type_name}): {error}"
                        ),
                    )
                    .message("An MSSQL source column type is not supported")
                    .hint("Exclude or convert the reported source column before retrying the task.")
                    .object(ErrorObject {
                        schema: Some(schema.to_string()),
                        table: Some(tb.to_string()),
                        column: Some(col.clone()),
                        ..Default::default()
                    })
                })?;

            cols.push(col.clone());
            col_origin_type_map.insert(col.clone(), user_type_name);
            col_type_map.insert(col.clone(), col_type);
            if is_nullable {
                nullable_cols.insert(col.clone());
            }
            if is_identity {
                identity_col = Some(col.clone());
            }
            if is_computed {
                computed_cols.insert(col.clone());
            }
            if matches!(
                system_type_name.to_ascii_lowercase().as_str(),
                "rowversion" | "timestamp"
            ) {
                rowversion_cols.insert(col.clone());
            }
            generated_always_type_map.insert(col, generated_always_type);
        }

        Ok(ParsedColumns {
            cols,
            col_origin_type_map,
            col_type_map,
            nullable_cols,
            identity_col,
            computed_cols,
            generated_always_type_map,
            rowversion_cols,
        })
    }

    async fn parse_keys(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let mut query = Query::new(Self::catalog_sql(TABLE_KEYS_SQL, db));
        query.bind(schema);
        query.bind(tb);
        let mut connection = self.connection_pool.get().await?;
        let rows = query
            .query(connection.client_mut())
            .await
            .code(ErrorCode::MetadataReadFailed)
            .object(Self::table_object(schema, tb))?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)
            .object(Self::table_object(schema, tb))?;

        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        for row in rows {
            let index_name = MssqlColValueConvertor::from_query_required_string(&row, "index_name")
                .code(ErrorCode::MetadataReadFailed)?;
            let is_primary_key =
                MssqlColValueConvertor::from_query_required_bool(&row, "is_primary_key")
                    .code(ErrorCode::MetadataReadFailed)?;
            let col = MssqlColValueConvertor::from_query_required_string(&row, "column_name")
                .code(ErrorCode::MetadataReadFailed)?;
            let key_name = if is_primary_key {
                RDB_PRIMARY_KEY_FLAG.to_string()
            } else {
                index_name
            };
            key_map.entry(key_name).or_default().push(col);
        }
        Ok(key_map)
    }

    fn catalog_sql(template: &str, db: &str) -> String {
        let catalog = if db.is_empty() {
            String::new()
        } else {
            format!("{}.", SqlUtil::escape_by_db_type(db, &DbType::Mssql))
        };
        template.replace("{catalog}", &catalog)
    }

    fn table_not_found(db: &str, schema: &str, tb: &str) -> anyhow::Error {
        let table_name = if db.is_empty() {
            format!("{schema}.{tb}")
        } else {
            format!("{db}.{schema}.{tb}")
        };
        DtError::DatabaseObjectNotFound(
            DbType::Mssql,
            format!("source table {table_name} was not found or is not readable"),
        )
        .message("The MSSQL source table definition could not be loaded")
        .hint("Verify that the table exists and that the source account can read its metadata.")
        .object(Self::table_object(schema, tb))
    }

    fn table_object(schema: &str, tb: &str) -> ErrorObject {
        ErrorObject {
            schema: Some(schema.to_string()),
            table: Some(tb.to_string()),
            ..Default::default()
        }
    }
}
