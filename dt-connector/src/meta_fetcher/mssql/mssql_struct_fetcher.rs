use std::collections::{BTreeMap, BTreeSet};

use anyhow::bail;
use dt_common::{
    config::config_enums::DbType,
    error::{DtError, DtResultExt, ErrorCode},
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        mssql::mssql_connection_pool::MssqlConnectionPool,
        struct_meta::statement::{
            mssql_create_database_statement::MssqlCreateDatabaseStatement,
            mssql_create_schema_statement::MssqlCreateSchemaStatement,
            mssql_create_table_statement::{
                MssqlColumn, MssqlColumnDefinition, MssqlComment, MssqlConstraint,
                MssqlConstraintKind, MssqlCreateTableStatement, MssqlIdentity, MssqlIndex,
                MssqlIndexColumn, MssqlKeyColumn, MssqlKeyConstraintType, MssqlTable,
            },
        },
    },
    utils::sql_util::SqlUtil,
};
use tiberius::Query;

use crate::rdb_struct_filter::RdbStructFilter;

const DATABASE_SQL: &str = r#"
SELECT name AS database_name, collation_name
FROM sys.databases
WHERE state_desc = 'ONLINE'
  AND name = @P1
"#;

const SCHEMAS_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name
FROM {catalog}sys.schemas AS s
JOIN {catalog}sys.tables AS t ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name
"#;

const TABLE_COLUMNS_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    t.is_memory_optimized,
    t.durability_desc,
    c.name AS column_name,
    CONVERT(bigint, c.column_id) AS ordinal_position,
    ty.name AS type_name,
    CONVERT(bigint, c.max_length) AS max_length,
    CONVERT(bigint, c.precision) AS numeric_precision,
    CONVERT(bigint, c.scale) AS numeric_scale,
    c.is_nullable,
    c.collation_name,
    CONVERT(nvarchar(100), ic.seed_value) AS identity_seed,
    CONVERT(nvarchar(100), ic.increment_value) AS identity_increment,
    CONVERT(bit, COALESCE(ic.is_not_for_replication, 0)) AS identity_not_for_replication,
    cc.definition AS computed_definition,
    CONVERT(bit, COALESCE(cc.is_persisted, 0)) AS computed_persisted
FROM {catalog}sys.tables AS t
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
JOIN {catalog}sys.columns AS c ON c.object_id = t.object_id
JOIN {catalog}sys.types AS ty ON ty.user_type_id = c.user_type_id
LEFT JOIN {catalog}sys.identity_columns AS ic
  ON ic.object_id = c.object_id
 AND ic.column_id = c.column_id
LEFT JOIN {catalog}sys.computed_columns AS cc
  ON cc.object_id = c.object_id
 AND cc.column_id = c.column_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name, c.column_id
"#;

const DEFAULT_CONSTRAINTS_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    dc.name AS constraint_name,
    c.name AS column_name,
    dc.definition
FROM {catalog}sys.default_constraints AS dc
JOIN {catalog}sys.tables AS t ON t.object_id = dc.parent_object_id
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
JOIN {catalog}sys.columns AS c
  ON c.object_id = dc.parent_object_id
 AND c.column_id = dc.parent_column_id
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
    cc.is_not_for_replication,
    cc.is_disabled,
    cc.is_not_trusted
FROM {catalog}sys.check_constraints AS cc
JOIN {catalog}sys.tables AS t ON t.object_id = cc.parent_object_id
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name, cc.name
"#;

const INDEXES_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    CONVERT(bigint, i.index_id) AS index_id,
    i.type AS index_type,
    i.type_desc AS index_type_desc,
    i.is_unique,
    i.is_disabled,
    i.filter_definition,
    pxi.name AS xml_primary_index_name,
    xi.secondary_type_desc AS xml_secondary_type_desc,
    CONVERT(nvarchar(100), hi.bucket_count) AS hash_bucket_count,
    c.name AS column_name,
    CONVERT(bigint, ic.key_ordinal) AS key_ordinal,
    ic.is_descending_key,
    ic.is_included_column,
    CONVERT(bigint, ic.index_column_id) AS index_column_id
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
WHERE t.is_ms_shipped = 0
  AND i.type > 0
  AND i.is_hypothetical = 0
  AND i.is_primary_key = 0
  AND i.is_unique_constraint = 0
ORDER BY s.name, t.name, i.index_id, ic.index_column_id
"#;

const COMMENTS_SQL: &str = r#"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    CONVERT(bigint, ep.minor_id) AS minor_id,
    c.name AS column_name,
    CONVERT(nvarchar(max), ep.value) AS comment
FROM {catalog}sys.extended_properties AS ep
JOIN {catalog}sys.tables AS t
  ON ep.class = 1
 AND ep.major_id = t.object_id
JOIN {catalog}sys.schemas AS s ON s.schema_id = t.schema_id
LEFT JOIN {catalog}sys.columns AS c
  ON c.object_id = t.object_id
 AND c.column_id = ep.minor_id
WHERE t.is_ms_shipped = 0
  AND ep.name = N'MS_Description'
ORDER BY s.name, t.name, ep.minor_id
"#;

type TableKey = (String, String, String);
type KeyConstraintDetails = (String, String, Vec<(i64, String, bool)>);
type IndexDetails = (
    u32,
    u8,
    String,
    bool,
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<u64>,
    Vec<MssqlIndexColumn>,
);

pub struct MssqlStructFetcher {
    pub connection_pool: MssqlConnectionPool,
    pub db: String,
    pub filter: RdbStructFilter,
    pub allow_missing_database: bool,
}

impl MssqlStructFetcher {
    pub async fn get_create_database_statements(
        &mut self,
    ) -> anyhow::Result<Vec<MssqlCreateDatabaseStatement>> {
        Ok(self
            .get_database()
            .await?
            .into_iter()
            .map(
                |(database_name, collation_name)| MssqlCreateDatabaseStatement {
                    database_name,
                    collation_name,
                },
            )
            .collect())
    }

    pub async fn get_create_schema_statements(
        &mut self,
        requested_schema: &str,
    ) -> anyhow::Result<Vec<MssqlCreateSchemaStatement>> {
        let Some((database_name, _)) = self.get_database().await? else {
            return Ok(Vec::new());
        };
        let mut statements = Vec::new();
        let sql = Self::catalog_sql(SCHEMAS_SQL, &database_name);
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&sql, &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;

        let mut schemas = BTreeSet::new();
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if (!requested_schema.is_empty() && requested_schema != schema)
                || self
                    .filter
                    .filter_tb_with_db(&database_name, &schema, &table)
            {
                continue;
            }
            schemas.insert(schema);
        }
        for name in schemas {
            statements.push(MssqlCreateSchemaStatement {
                database_name: database_name.clone(),
                schema_name: name,
            });
        }
        Ok(statements)
    }

    pub async fn get_create_table_statements(
        &mut self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Vec<MssqlCreateTableStatement>> {
        let Some((db, _)) = self.get_database().await? else {
            return Ok(Vec::new());
        };
        let mut statements = self.get_tables(&db, schema, table).await?;
        if statements.is_empty() {
            return Ok(Vec::new());
        }

        self.attach_default_constraints(&db, schema, table, &mut statements)
            .await?;
        self.attach_key_constraints(&db, schema, table, &mut statements)
            .await?;
        self.attach_check_constraints(&db, schema, table, &mut statements)
            .await?;
        self.attach_indexes(&db, schema, table, &mut statements)
            .await?;
        self.attach_comments(&db, schema, table, &mut statements)
            .await?;
        Ok(statements.into_values().collect())
    }

    async fn get_tables(
        &self,
        db: &str,
        requested_schema: &str,
        requested_table: &str,
    ) -> anyhow::Result<BTreeMap<TableKey, MssqlCreateTableStatement>> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&Self::catalog_sql(TABLE_COLUMNS_SQL, db), &[])
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
            if !self.include_table(
                db,
                &schema_name,
                &table_name,
                requested_schema,
                requested_table,
            ) {
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
            let is_memory_optimized =
                MssqlColValueConvertor::from_query_required_bool(&row, "is_memory_optimized")
                    .code(ErrorCode::MetadataReadFailed)?;
            let durability_desc = Self::required_string(&row, "durability_desc")?;
            let identity = if let Some(seed_value) =
                MssqlColValueConvertor::from_query_optional_string(&row, "identity_seed")
                    .code(ErrorCode::MetadataReadFailed)?
            {
                Some(MssqlIdentity {
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
                })
            } else {
                None
            };
            let column_definition = if let Some(definition) = computed_definition {
                MssqlColumnDefinition::Computed {
                    definition,
                    is_persisted: MssqlColValueConvertor::from_query_required_bool(
                        &row,
                        "computed_persisted",
                    )
                    .code(ErrorCode::MetadataReadFailed)?,
                }
            } else {
                MssqlColumnDefinition::Regular {
                    column_type: Self::format_column_type(&type_name, max_length, precision, scale),
                    collation_name: MssqlColValueConvertor::from_query_optional_string(
                        &row,
                        "collation_name",
                    )
                    .code(ErrorCode::MetadataReadFailed)?
                    .unwrap_or_default(),
                    is_nullable: MssqlColValueConvertor::from_query_required_bool(
                        &row,
                        "is_nullable",
                    )
                    .code(ErrorCode::MetadataReadFailed)?,
                    identity,
                }
            };

            let statement = results
                .entry((db.to_string(), schema_name.clone(), table_name.clone()))
                .or_insert_with(|| MssqlCreateTableStatement {
                    table: MssqlTable {
                        database_name: db.to_string(),
                        schema_name: schema_name.clone(),
                        table_name: table_name.clone(),
                        is_memory_optimized,
                        durability_desc,
                        columns: Vec::new(),
                        constraints: Vec::new(),
                        indexes: Vec::new(),
                        comments: Vec::new(),
                    },
                });

            statement.table.columns.push(MssqlColumn {
                column_name,
                ordinal_position: u32::try_from(
                    MssqlColValueConvertor::from_query_required_i64(&row, "ordinal_position")
                        .code(ErrorCode::MetadataReadFailed)?,
                )?,
                definition: column_definition,
            });
        }
        Ok(results)
    }

    async fn attach_default_constraints(
        &self,
        db: &str,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&Self::catalog_sql(DEFAULT_CONSTRAINTS_SQL, db), &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(db, &schema, &table, requested_schema, requested_table) {
                continue;
            }
            if let Some(statement) =
                statements.get_mut(&(db.to_string(), schema.clone(), table.clone()))
            {
                statement.table.constraints.push(MssqlConstraint {
                    constraint_name: Self::required_string(&row, "constraint_name")?,
                    kind: MssqlConstraintKind::Default {
                        column_name: Self::required_string(&row, "column_name")?,
                        definition: Self::required_string(&row, "definition")?,
                    },
                });
            }
        }
        Ok(())
    }

    async fn attach_key_constraints(
        &self,
        db: &str,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&Self::catalog_sql(KEY_CONSTRAINTS_SQL, db), &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        let mut grouped: BTreeMap<(String, String, String), KeyConstraintDetails> = BTreeMap::new();
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(db, &schema, &table, requested_schema, requested_table) {
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
            let constraint_type = match constraint_type.as_str() {
                "PRIMARY KEY" => MssqlKeyConstraintType::PrimaryKey,
                "UNIQUE" => MssqlKeyConstraintType::Unique,
                _ => {
                    return Err(DtError::UnsupportedTableStructure(format!(
                        "MSSQL constraint {name} uses unsupported type {constraint_type}"
                    ))
                    .into())
                }
            };
            let columns = columns
                .into_iter()
                .map(|(_, column_name, is_descending_key)| MssqlKeyColumn {
                    column_name,
                    is_descending_key,
                })
                .collect();
            if let Some(statement) =
                statements.get_mut(&(db.to_string(), schema.clone(), table.clone()))
            {
                statement.table.constraints.push(MssqlConstraint {
                    constraint_name: name,
                    kind: MssqlConstraintKind::Key {
                        constraint_type,
                        index_type_desc: index_type,
                        columns,
                    },
                });
            }
        }
        Ok(())
    }

    async fn attach_check_constraints(
        &self,
        db: &str,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&Self::catalog_sql(CHECK_CONSTRAINTS_SQL, db), &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(db, &schema, &table, requested_schema, requested_table) {
                continue;
            }
            if let Some(statement) =
                statements.get_mut(&(db.to_string(), schema.clone(), table.clone()))
            {
                let not_for_replication = MssqlColValueConvertor::from_query_required_bool(
                    &row,
                    "is_not_for_replication",
                )
                .code(ErrorCode::MetadataReadFailed)?;
                statement.table.constraints.push(MssqlConstraint {
                    constraint_name: Self::required_string(&row, "constraint_name")?,
                    kind: MssqlConstraintKind::Check {
                        definition: Self::required_string(&row, "definition")?,
                        is_not_for_replication: not_for_replication,
                        is_disabled: MssqlColValueConvertor::from_query_required_bool(
                            &row,
                            "is_disabled",
                        )
                        .code(ErrorCode::MetadataReadFailed)?,
                        is_not_trusted: MssqlColValueConvertor::from_query_required_bool(
                            &row,
                            "is_not_trusted",
                        )
                        .code(ErrorCode::MetadataReadFailed)?,
                    },
                });
            }
        }
        Ok(())
    }

    async fn attach_indexes(
        &self,
        db: &str,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&Self::catalog_sql(INDEXES_SQL, db), &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        let mut grouped: BTreeMap<(String, String, String), IndexDetails> = BTreeMap::new();
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(db, &schema, &table, requested_schema, requested_table) {
                continue;
            }
            let name = Self::required_string(&row, "index_name")?;
            let index_id = u32::try_from(
                MssqlColValueConvertor::from_query_required_i64(&row, "index_id")
                    .code(ErrorCode::MetadataReadFailed)?,
            )?;
            let index_type = MssqlColValueConvertor::from_query_required_u8(&row, "index_type")
                .code(ErrorCode::MetadataReadFailed)?;
            let index_type_desc = Self::required_string(&row, "index_type_desc")?;
            let unique = MssqlColValueConvertor::from_query_required_bool(&row, "is_unique")
                .code(ErrorCode::MetadataReadFailed)?;
            let disabled = MssqlColValueConvertor::from_query_required_bool(&row, "is_disabled")
                .code(ErrorCode::MetadataReadFailed)?;
            let filter =
                MssqlColValueConvertor::from_query_optional_string(&row, "filter_definition")
                    .code(ErrorCode::MetadataReadFailed)?;
            let xml_primary_index_name =
                MssqlColValueConvertor::from_query_optional_string(&row, "xml_primary_index_name")
                    .code(ErrorCode::MetadataReadFailed)?;
            let xml_secondary_type_desc =
                MssqlColValueConvertor::from_query_optional_string(&row, "xml_secondary_type_desc")
                    .code(ErrorCode::MetadataReadFailed)?;
            let hash_bucket_count =
                MssqlColValueConvertor::from_query_optional_string(&row, "hash_bucket_count")
                    .code(ErrorCode::MetadataReadFailed)?
                    .map(|value| value.parse::<u64>())
                    .transpose()?;
            let entry = grouped.entry((schema, table, name)).or_insert_with(|| {
                (
                    index_id,
                    index_type,
                    index_type_desc,
                    unique,
                    disabled,
                    filter,
                    xml_primary_index_name,
                    xml_secondary_type_desc,
                    hash_bucket_count,
                    Vec::new(),
                )
            });
            entry.9.push(MssqlIndexColumn {
                column_name: Self::required_string(&row, "column_name")?,
                index_column_id: u32::try_from(
                    MssqlColValueConvertor::from_query_required_i64(&row, "index_column_id")
                        .code(ErrorCode::MetadataReadFailed)?,
                )?,
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
            });
        }

        for (
            (schema, table, name),
            (
                index_id,
                index_type,
                index_type_desc,
                unique,
                disabled,
                filter,
                xml_primary_index_name,
                xml_secondary_type_desc,
                hash_bucket_count,
                mut columns,
            ),
        ) in grouped
        {
            columns.sort_by_key(|column| column.index_column_id);
            if let Some(statement) =
                statements.get_mut(&(db.to_string(), schema.clone(), table.clone()))
            {
                statement.table.indexes.push(MssqlIndex {
                    index_name: name,
                    index_id,
                    index_type,
                    index_type_desc,
                    is_unique: unique,
                    is_disabled: disabled,
                    filter_definition: filter,
                    xml_primary_index_name,
                    xml_secondary_type_desc,
                    hash_bucket_count,
                    columns,
                });
            }
        }
        Ok(())
    }

    async fn attach_comments(
        &self,
        db: &str,
        requested_schema: &str,
        requested_table: &str,
        statements: &mut BTreeMap<TableKey, MssqlCreateTableStatement>,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&Self::catalog_sql(COMMENTS_SQL, db), &[])
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        for row in rows {
            let schema = Self::required_string(&row, "schema_name")?;
            let table = Self::required_string(&row, "table_name")?;
            if !self.include_table(db, &schema, &table, requested_schema, requested_table) {
                continue;
            }
            if let Some(statement) =
                statements.get_mut(&(db.to_string(), schema.clone(), table.clone()))
            {
                let minor_id = MssqlColValueConvertor::from_query_required_i64(&row, "minor_id")
                    .code(ErrorCode::MetadataReadFailed)?;
                let column_name =
                    MssqlColValueConvertor::from_query_optional_string(&row, "column_name")
                        .code(ErrorCode::MetadataReadFailed)?
                        .unwrap_or_default();
                statement.table.comments.push(if minor_id == 0 {
                    MssqlComment::Table {
                        comment: Self::required_string(&row, "comment")?,
                    }
                } else {
                    MssqlComment::Column {
                        column_name,
                        comment: Self::required_string(&row, "comment")?,
                    }
                });
            }
        }
        Ok(())
    }

    fn include_table(
        &self,
        db: &str,
        schema: &str,
        table: &str,
        requested_schema: &str,
        requested_table: &str,
    ) -> bool {
        (requested_schema.is_empty() || requested_schema == schema)
            && (requested_table.is_empty() || requested_table == table)
            && !self.filter.filter_tb_with_db(db, schema, table)
    }

    async fn get_database(&self) -> anyhow::Result<Option<(String, String)>> {
        if self.db.is_empty() {
            bail!(DtError::invalid_config(
                "MSSQL struct fetcher requires a database"
            ));
        }
        if self.filter.filter_schema(&self.db) {
            return Ok(None);
        }

        let mut query = Query::new(DATABASE_SQL);
        query.bind(&self.db);
        let mut connection = self.connection_pool.get().await?;
        let rows = query
            .query(connection.client_mut())
            .await
            .code(ErrorCode::MetadataReadFailed)?
            .into_first_result()
            .await
            .code(ErrorCode::MetadataReadFailed)?;
        let Some(row) = rows.first() else {
            if self.allow_missing_database {
                return Ok(None);
            }
            bail!(DtError::DatabaseObjectNotFound(
                DbType::Mssql,
                format!("database: {} not found", self.db),
            ));
        };

        Ok(Some((
            Self::required_string(row, "database_name")?,
            MssqlColValueConvertor::from_query_optional_string(row, "collation_name")
                .code(ErrorCode::MetadataReadFailed)?
                .unwrap_or_default(),
        )))
    }

    fn catalog_sql(template: &str, db: &str) -> String {
        let catalog = if db.is_empty() {
            String::new()
        } else {
            format!("{}.", SqlUtil::escape_by_db_type(db, &DbType::Mssql))
        };
        template.replace("{catalog}", &catalog)
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
