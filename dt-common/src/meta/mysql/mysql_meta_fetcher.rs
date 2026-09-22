use std::collections::{HashMap, HashSet};

use anyhow::{bail, Ok};
use futures::TryStreamExt;
use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

use super::{
    mysql_col_type::MysqlColType, mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta,
};
use crate::{
    config::config_enums::DbType,
    error::{DtError, DtErrorContextExt, DtOptionExt, DtResultExt, ErrorObject},
    meta::{
        ddl_meta::ddl_data::DdlData,
        foreign_key::ForeignKey,
        rdb_meta_manager::RdbMetaManager,
        rdb_meta_manager::RDB_PRIMARY_KEY,
        rdb_tb_meta::{RdbTbMeta, SortDirection},
        row_data::RowData,
    },
    utils::sql_util::SqlUtil,
};

#[derive(Clone)]
pub struct MysqlMetaFetcher {
    pub conn_pool: Pool<MySql>,
    pub cache: HashMap<String, MysqlTbMeta>,
    pub version: String,
    pub db_type: DbType,
}

const COLUMN_NAME: &str = "COLUMN_NAME";
const COLUMN_TYPE: &str = "COLUMN_TYPE";
const DATA_TYPE: &str = "DATA_TYPE";
const CHARACTER_MAXIMUM_LENGTH: &str = "CHARACTER_MAXIMUM_LENGTH";
const CHARACTER_SET_NAME: &str = "CHARACTER_SET_NAME";
const NUMERIC_PRECISION: &str = "NUMERIC_PRECISION";
const NUMERIC_SCALE: &str = "NUMERIC_SCALE";
const IS_NULLABLE: &str = "IS_NULLABLE";

impl MysqlMetaFetcher {
    pub async fn new(conn_pool: Pool<MySql>) -> anyhow::Result<Self> {
        Self::new_mysql_compatible(conn_pool, DbType::Mysql).await
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub async fn new_mysql_compatible(
        conn_pool: Pool<MySql>,
        db_type: DbType,
    ) -> anyhow::Result<Self> {
        let mut me = Self {
            conn_pool,
            cache: HashMap::new(),
            version: String::new(),
            db_type,
        };
        me.init_version().await?;
        Ok(me)
    }

    pub fn invalidate_cache(&mut self, schema: &str, tb: &str) {
        if !schema.is_empty() && !tb.is_empty() {
            let full_name = format!("{}.{}", schema, tb);
            self.cache.remove(&full_name);
        } else {
            // clear all cache is always safe
            self.cache.clear();
        }
    }

    pub fn invalidate_cache_by_ddl_data(&mut self, ddl_data: &DdlData) {
        let (schema, tb) = ddl_data.get_schema_tb();
        self.invalidate_cache(&schema, &tb);
    }

    pub async fn get_tb_meta_by_row_data<'a>(
        &'a mut self,
        row_data: &RowData,
    ) -> anyhow::Result<&'a MysqlTbMeta> {
        self.get_tb_meta(&row_data.schema, &row_data.tb).await
    }

    pub async fn get_tb_meta<'a>(
        &'a mut self,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<&'a MysqlTbMeta> {
        let full_name = format!("{}.{}", schema, tb);
        if !self.cache.contains_key(&full_name) {
            let (cols, col_origin_type_map, col_type_map, nullable_cols) =
                Self::parse_cols(&self.conn_pool, &self.db_type, schema, tb).await?;
            let (key_map, key_col_attrs) = Self::parse_keys(&self.conn_pool, schema, tb).await?;
            // disable get_foreign_keys since we don't support foreign key check,
            // also querying them is very slow, which may cause terrible performance issue if there were many tables in a CDC task.
            // let (foreign_keys, ref_by_foreign_keys) =
            //     Self::get_foreign_keys(&self.conn_pool, &self.db_type, schema, tb).await?;

            let basic = RdbTbMeta {
                schema: schema.to_string(),
                tb: tb.to_string(),
                cols,
                nullable_cols,
                col_origin_type_map,
                key_map,
                ..Default::default()
            };
            let mut tb_meta = MysqlTbMeta {
                basic,
                col_type_map,
            };
            let key_scores = MysqlMetaManager::get_key_scores(&tb_meta)?;
            RdbMetaManager::set_order_cols(&mut tb_meta.basic, &key_scores, key_col_attrs)?;
            self.cache.insert(full_name.clone(), tb_meta);
        }
        self.cache
            .get(&full_name)
            .or_dt_error(DtError::DatabaseObjectNotFound(self.db_type.clone(), format!(
                "Ape-DTS could not find the previously loaded definition for source table {full_name}"
            )))
            .message("The source table definition could not be loaded")
            .hint(
                "Verify that the source table still exists and is readable, then restart the task.",
            )
            .object(ErrorObject {
                schema: Some(schema.to_string()),
                table: Some(tb.to_string()),
                ..Default::default()
            })
    }

    async fn parse_cols(
        conn_pool: &Pool<MySql>,
        db_type: &DbType,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<(
        Vec<String>,
        HashMap<String, String>,
        HashMap<String, MysqlColType>,
        HashSet<String>,
    )> {
        let mut cols = Vec::new();
        let mut col_origin_type_map = HashMap::new();
        let mut col_type_map = HashMap::new();
        let mut nullable_cols = HashSet::new();

        let sql = if matches!(db_type, DbType::Mysql) {
            "SELECT * FROM information_schema.columns
             WHERE table_schema = ? AND table_name = ? ORDER BY ORDINAL_POSITION"
                .to_string()
        } else {
            format!(
                "SELECT * FROM information_schema.columns 
                WHERE table_schema = '{}' AND table_name = '{}' ORDER BY ORDINAL_POSITION",
                schema, tb
            )
        };

        let mut rows = if matches!(db_type, DbType::Mysql) {
            sqlx::query(&sql).bind(schema).bind(tb).fetch(conn_pool)
        } else {
            // for starrocks
            sqlx::raw_sql(&sql).fetch(conn_pool)
        };

        while let Some(row) = rows.try_next().await? {
            let col = SqlUtil::try_get_mysql_string(&row, COLUMN_NAME)?;
            // Column and index names are not case sensitive on any platform, nor are column aliases.
            cols.push(col.clone());
            let (origin_type, col_type) = Self::get_col_type(&row).await?;
            col_origin_type_map.insert(col.clone(), origin_type);
            col_type_map.insert(col.clone(), col_type);

            let is_nullable =
                SqlUtil::try_get_mysql_string(&row, IS_NULLABLE)?.to_lowercase() == "yes";
            if is_nullable {
                nullable_cols.insert(col);
            }
        }

        if cols.is_empty() {
            bail! {DtError::DatabaseObjectNotFound(db_type.clone(), format!(
                "failed to get table metadata for: `{}`.`{}`",
                schema, tb
            ))
            .object(ErrorObject {
                schema: Some(schema.to_string()),
                table: Some(tb.to_string()),
                ..Default::default()
            }) }
        }
        Ok((cols, col_origin_type_map, col_type_map, nullable_cols))
    }

    async fn get_col_type(row: &MySqlRow) -> anyhow::Result<(String, MysqlColType)> {
        let column_type = SqlUtil::try_get_mysql_string(row, COLUMN_TYPE)?;
        let data_type = SqlUtil::try_get_mysql_string(row, DATA_TYPE)?;
        let is_nullable = SqlUtil::try_get_mysql_string(row, IS_NULLABLE)?.to_lowercase() == "yes";

        let parse_precision = || {
            let precision = if column_type.contains('(') {
                // "datetime(6)", "timestamp(6)"
                column_type
                    .split('(')
                    .nth(1)
                    .and_then(|s| s.trim_end_matches(')').parse().ok())
                    .unwrap_or(0)
            } else {
                0
            };
            precision
        };

        let unsigned = column_type.to_lowercase().contains("unsigned");
        let col_type = match data_type.as_str() {
            "tinyint" => MysqlColType::TinyInt { unsigned },
            "smallint" => MysqlColType::SmallInt { unsigned },
            "bigint" => MysqlColType::BigInt { unsigned },
            "mediumint" => MysqlColType::MediumInt { unsigned },
            "int" => MysqlColType::Int { unsigned },

            "varbinary" => MysqlColType::VarBinary {
                length: Self::get_u64_col(row, CHARACTER_MAXIMUM_LENGTH) as u16,
            },
            "binary" => MysqlColType::Binary {
                length: Self::get_u64_col(row, CHARACTER_MAXIMUM_LENGTH) as u8,
            },

            "varchar" | "char" | "tinytext" | "mediumtext" | "longtext" | "text" => {
                let length = Self::get_u64_col(row, CHARACTER_MAXIMUM_LENGTH);
                let mut charset = String::new();
                if let Some(value) =
                    SqlUtil::try_get_mysql_optional_string(row, CHARACTER_SET_NAME)?
                {
                    charset = value;
                }
                match data_type.as_str() {
                    "char" => MysqlColType::Char { length, charset },
                    "varchar" => MysqlColType::Varchar { length, charset },
                    "tinytext" => MysqlColType::TinyText { length, charset },
                    "mediumtext" => MysqlColType::MediumText { length, charset },
                    "longtext" => MysqlColType::LongText { length, charset },
                    "text" => MysqlColType::Text { length, charset },
                    _ => MysqlColType::Unknown,
                }
            }

            // as a client of mysql, sqlx's client timezone is UTC by default,
            // so no matter what timezone of src/dst server is,
            // src server will convert the timestamp field into UTC for sqx,
            // and then sqx will write it into dst server by UTC,
            // and then dst server will convert the received UTC timestamp into its own timezone.
            "timestamp" => MysqlColType::Timestamp {
                precision: parse_precision(),
                timezone_offset: 0,
                is_nullable,
            },

            "tinyblob" => MysqlColType::TinyBlob,
            "mediumblob" => MysqlColType::MediumBlob,
            "longblob" => MysqlColType::LongBlob,
            "blob" => MysqlColType::Blob,

            "float" => MysqlColType::Float,
            "double" => MysqlColType::Double,

            "decimal" => MysqlColType::Decimal {
                precision: Self::get_u64_col(row, NUMERIC_PRECISION) as u32,
                scale: Self::get_u64_col(row, NUMERIC_SCALE) as u32,
            },

            "enum" => {
                // enum('x-small','small','medium','large','x-large')
                let enum_str = column_type
                    .trim_start_matches("enum(")
                    .trim_end_matches(')');
                let enum_str_items: Vec<String> = enum_str
                    .split(',')
                    .map(|i| {
                        i.trim_start_matches('\'')
                            .trim_end_matches('\'')
                            .to_string()
                    })
                    .collect();
                MysqlColType::Enum {
                    items: enum_str_items,
                }
            }

            "set" => {
                // set('a','b','c','d','e')
                let set_str = column_type.trim_start_matches("set(").trim_end_matches(')');
                let set_str_items: Vec<String> = set_str
                    .split(',')
                    .map(|i| {
                        i.trim_start_matches('\'')
                            .trim_end_matches('\'')
                            .to_string()
                    })
                    .collect();
                let mut items = HashMap::new();
                let mut key = 1;
                for str in set_str_items {
                    items.insert(key, str);
                    key <<= 1;
                }
                MysqlColType::Set { items }
            }

            "datetime" => MysqlColType::DateTime {
                precision: parse_precision(),
                is_nullable,
            },

            "date" => MysqlColType::Date { is_nullable },
            "time" => MysqlColType::Time {
                precision: parse_precision(),
            },
            "year" => MysqlColType::Year,
            "bit" => MysqlColType::Bit,
            "geometry" => MysqlColType::Geometry,
            "point" => MysqlColType::Point,
            "linestring" => MysqlColType::LineString,
            "polygon" => MysqlColType::Polygon,
            "multipoint" => MysqlColType::MultiPoint,
            "multilinestring" => MysqlColType::MultiLineString,
            "multipolygon" => MysqlColType::MultiPolygon,
            "geometrycollection" | "geomcollection" => MysqlColType::GeometryCollection,
            "json" => MysqlColType::Json,
            _ => MysqlColType::Unknown,
        };

        Ok((data_type.to_string(), col_type))
    }

    fn get_u64_col(row: &MySqlRow, col: &str) -> u64 {
        // use let length: u64 = row.try_get_unchecked(CHARACTER_MAXIMUM_LENGTH);
        // instead of let length: u64 = row.try_get(CHARACTER_MAXIMUM_LENGTH)?;
        // since
        // in mysql 5.*, CHARACTER_MAXIMUM_LENGTH: bigint(21) unsigned
        // in mysql 8.*, CHARACTER_MAXIMUM_LENGTH: bigint
        row.try_get_unchecked::<u64, &str>(col).unwrap_or_default()
    }

    // Example (MySQL 8.0; exercised by snapshot/order_key_test):
    // CREATE TABLE order_key_src.parse_keys_example (
    //     id int NOT NULL, value int NOT NULL,
    //     PRIMARY KEY some_pk_name (id DESC, value ASC),
    //     UNIQUE KEY some_uk_name (value DESC)
    // );
    // CREATE UNIQUE INDEX uk_example ON order_key_src.parse_keys_example (value ASC, id DESC);
    // CREATE INDEX non_unique_key ON order_key_src.parse_keys_example (id);
    // SHOW INDEXES FROM order_key_src.parse_keys_example;
    // Relevant fields (other SHOW INDEXES fields omitted):
    // Non_unique | Key_name       | Seq_in_index | Column_name | Collation
    // 0          | PRIMARY        | 1            | id          | D
    // 0          | PRIMARY        | 2            | value       | A
    // 0          | some_uk_name   | 1            | value       | D
    // 0          | uk_example     | 1            | value       | A
    // 0          | uk_example     | 2            | id          | D
    // 1          | non_unique_key | 1            | id          | A
    // MySQL reports PRIMARY even when the primary key was declared with a name.
    // key_map = {RDB_PRIMARY_KEY: [id, value], some_uk_name: [value], uk_example: [value, id]}
    // key_col_attrs = {RDB_PRIMARY_KEY: {id: Desc, value: Asc},
    //                  some_uk_name: {value: Desc}, uk_example: {value: Asc, id: Desc}}
    // Seq_in_index preserves composite-key order; non-unique indexes are excluded.
    // A functional index has a NULL Column_name for its expression: exclude the
    // entire index, including any ordinary columns in the same index.
    async fn parse_keys(
        conn_pool: &Pool<MySql>,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<(
        HashMap<String, Vec<String>>,
        HashMap<String, HashMap<String, SortDirection>>,
    )> {
        let mut keys: HashMap<String, Vec<(u64, String, SortDirection)>> = HashMap::new();
        let mut invalid_keys = HashSet::new();
        let sql = format!(
            "SHOW INDEXES FROM {}.{}",
            SqlUtil::escape_by_db_type(schema, &DbType::Mysql),
            SqlUtil::escape_by_db_type(tb, &DbType::Mysql)
        );
        let mut rows = sqlx::raw_sql(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await? {
            let non_unique: i8 = row.try_get("Non_unique")?;
            if non_unique == 1 {
                continue;
            }
            let mut key = SqlUtil::try_get_mysql_string(&row, "Key_name")?;
            if key == "PRIMARY" {
                key = RDB_PRIMARY_KEY.to_string();
            }
            // Reject the whole functional index, not just its expression columns.
            let Some(col) = SqlUtil::try_get_mysql_optional_string(&row, "Column_name")? else {
                invalid_keys.insert(key);
                continue;
            };
            let ordinal: u64 = row.try_get_unchecked("Seq_in_index")?;
            let direction =
                match SqlUtil::try_get_mysql_optional_string(&row, "Collation")?.as_deref() {
                    Some("D") => SortDirection::Desc,
                    _ => SortDirection::Asc,
                };
            keys.entry(key).or_default().push((ordinal, col, direction));
        }
        let mut key_map = HashMap::new();
        let mut key_col_attrs = HashMap::new();
        for (key, mut cols) in keys {
            if invalid_keys.contains(&key) {
                continue;
            }
            cols.sort_by_key(|(ordinal, _, _)| *ordinal);
            key_map.insert(
                key.clone(),
                cols.iter().map(|(_, col, _)| col.clone()).collect(),
            );
            key_col_attrs.insert(
                key,
                cols.into_iter()
                    .map(|(_, col, direction)| (col, direction))
                    .collect(),
            );
        }
        Ok((key_map, key_col_attrs))
    }

    #[allow(dead_code)]
    async fn get_foreign_keys(
        conn_pool: &Pool<MySql>,
        db_type: &DbType,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<(Vec<ForeignKey>, Vec<ForeignKey>)> {
        let mut foreign_keys = Vec::new();
        let mut ref_by_foreign_keys = Vec::new();
        if !matches!(db_type, DbType::Mysql) {
            return Ok((foreign_keys, ref_by_foreign_keys));
        }

        // this will be a very slow query if NOT set "SET GLOBAL innodb_stats_on_metadata = OFF;"
        // https://www.percona.com/blog/innodb_stats_on_metadata-slow-queries-information_schema/
        let sql = format!(
            "SELECT
                kcu.CONSTRAINT_SCHEMA,
                kcu.TABLE_NAME,
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_SCHEMA,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA
            WHERE
                tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
                AND (
                  (kcu.CONSTRAINT_SCHEMA = '{}' AND kcu.TABLE_NAME = '{}')
                    OR 
                  (kcu.REFERENCED_TABLE_SCHEMA = '{}' and kcu.REFERENCED_TABLE_NAME = '{}')
                )
            ",
            schema, tb, schema, tb
        );

        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await? {
            let my_schema = SqlUtil::try_get_mysql_string(&row, "CONSTRAINT_SCHEMA")?;
            let my_tb = SqlUtil::try_get_mysql_string(&row, "TABLE_NAME")?;
            let my_col = SqlUtil::try_get_mysql_string(&row, "COLUMN_NAME")?;
            let ref_schema = SqlUtil::try_get_mysql_string(&row, "REFERENCED_TABLE_SCHEMA")?;
            let ref_tb = SqlUtil::try_get_mysql_string(&row, "REFERENCED_TABLE_NAME")?;
            let ref_col = SqlUtil::try_get_mysql_string(&row, "REFERENCED_COLUMN_NAME")?;
            let key = ForeignKey {
                db: String::new(),
                schema: my_schema,
                tb: my_tb,
                col: my_col,
                ref_db: String::new(),
                ref_schema,
                ref_tb,
                ref_col,
            };

            if key.schema == schema && key.tb == tb {
                foreign_keys.push(key.clone());
            }
            if key.ref_schema == schema && key.ref_tb == tb {
                ref_by_foreign_keys.push(key)
            }
        }
        Ok((foreign_keys, ref_by_foreign_keys))
    }

    async fn init_version(&mut self) -> anyhow::Result<()> {
        let sql = "SELECT VERSION()";
        let mut rows = sqlx::raw_sql(sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            let version = SqlUtil::try_get_mysql_string(&row, 0)?;
            self.version = version.trim().into();
            return Ok(());
        }
        bail! {DtError::UnsupportedDatabaseVersion(
            self.db_type.clone(),
            "failed to initialize the database version".to_string(),
        )}
    }
}
