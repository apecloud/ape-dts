use std::collections::HashMap;

use crate::{config::config_enums::DbType, error::Error, meta::ddl_meta::ddl_data::DdlData};
use anyhow::{bail, Ok};
use futures::TryStreamExt;

use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

use crate::meta::{
    foreign_key::ForeignKey, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
};

use super::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta};

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
        self.conn_pool.close().await;
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
            let (cols, col_origin_type_map, col_type_map) =
                Self::parse_cols(&self.conn_pool, &self.db_type, schema, tb).await?;
            let key_map = Self::parse_keys(&self.conn_pool, schema, tb).await?;
            let (order_col, partition_col, id_cols) =
                RdbMetaManager::parse_rdb_cols(&key_map, &cols)?;
            // disable get_foreign_keys since we don't support foreign key check,
            // also querying them is very slow, which may cause terrible performance issue if there were many tables in a CDC task.
            let (foreign_keys, ref_by_foreign_keys) = (vec![], vec![]);
            // let (foreign_keys, ref_by_foreign_keys) =
            //     Self::get_foreign_keys(&self.conn_pool, &self.db_type, schema, tb).await?;

            let basic = RdbTbMeta {
                schema: schema.to_string(),
                tb: tb.to_string(),
                cols,
                col_origin_type_map,
                key_map,
                order_col,
                partition_col,
                id_cols,
                foreign_keys,
                ref_by_foreign_keys,
            };
            let tb_meta = MysqlTbMeta {
                basic,
                col_type_map,
            };
            self.cache.insert(full_name.clone(), tb_meta);
        }
        Ok(self.cache.get(&full_name).unwrap())
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
    )> {
        let mut cols = Vec::new();
        let mut col_origin_type_map = HashMap::new();
        let mut col_type_map = HashMap::new();

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
            sqlx::query(&sql).disable_arguments().fetch(conn_pool)
        };

        while let Some(row) = rows.try_next().await? {
            let col: String = row.try_get(COLUMN_NAME)?;
            // Column and index names are not case sensitive on any platform, nor are column aliases.
            cols.push(col.clone());
            let (origin_type, col_type) = Self::get_col_type(&row).await?;
            col_origin_type_map.insert(col.clone(), origin_type);
            col_type_map.insert(col, col_type);
        }

        if cols.is_empty() {
            bail! {Error::MetadataError(format!(
                "failed to get table metadata for: `{}`.`{}`",
                schema, tb
            )) }
        }
        Ok((cols, col_origin_type_map, col_type_map))
    }

    async fn get_col_type(row: &MySqlRow) -> anyhow::Result<(String, MysqlColType)> {
        let column_type: String = row.try_get(COLUMN_TYPE)?;
        let data_type: String = row.try_get(DATA_TYPE)?;
        let is_nullable = row.try_get::<String, _>(IS_NULLABLE)?.to_lowercase() == "yes";

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
                let unchecked: Option<Vec<u8>> = row.get_unchecked(CHARACTER_SET_NAME);
                if unchecked.is_some() {
                    charset = row.try_get(CHARACTER_SET_NAME)?;
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
                let column_type: String = row.try_get(COLUMN_TYPE).unwrap();
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
                let column_type: String = row.try_get(COLUMN_TYPE).unwrap();
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
            "json" => MysqlColType::Json,

            // TODO
            // "geometry": "geometrycollection": "linestring": "multilinestring":
            // "multipoint": "multipolygon": "polygon": "point"
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

    async fn parse_keys(
        conn_pool: &Pool<MySql>,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        let sql = format!("SHOW INDEXES FROM `{}`.`{}`", schema, tb);
        let mut rows = sqlx::query(&sql).disable_arguments().fetch(conn_pool);
        while let Some(row) = rows.try_next().await? {
            let non_unique: i8 = row.try_get("Non_unique")?;
            if non_unique == 1 {
                continue;
            }

            // the key name for primary key is always "PRIMARY" even if created with a name
            // create table test_db_1.a(id int, value int,
            //      primary key some_pk_name(id, value),
            //      unique key some_uk_name(value));
            // mysql> SHOW INDEXES FROM test_db_1.a;
            // +-------+------------+--------------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
            // | Table | Non_unique | Key_name     | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
            // +-------+------------+--------------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
            // | a     |          0 | PRIMARY      |            1 | id          | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
            // | a     |          0 | PRIMARY      |            2 | value       | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
            // | a     |          0 | some_uk_name |            1 | value       | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
            // +-------+------------+--------------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
            let key_name: String = row.try_get("Key_name")?;
            let col_name: String = row.try_get("Column_name")?;
            if let Some(key_cols) = key_map.get_mut(&key_name) {
                key_cols.push(col_name);
            } else {
                key_map.insert(key_name, vec![col_name]);
            }
        }
        Ok(key_map)
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
        if matches!(db_type, DbType::StarRocks | DbType::Doris) {
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
            let my_schema: String = row.try_get("CONSTRAINT_SCHEMA")?;
            let my_tb: String = row.try_get("TABLE_NAME")?;
            let my_col: String = row.try_get("COLUMN_NAME")?;
            let ref_schema: String = row.try_get("REFERENCED_TABLE_SCHEMA")?;
            let ref_tb: String = row.try_get("REFERENCED_TABLE_NAME")?;
            let ref_col: String = row.try_get("REFERENCED_COLUMN_NAME")?;
            let key = ForeignKey {
                schema: my_schema,
                tb: my_tb,
                col: my_col,
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
        let mut rows = sqlx::query(sql).disable_arguments().fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            let version: String = row.get_unchecked(0);
            self.version = version.trim().into();
            return Ok(());
        }
        bail! {Error::MetadataError("failed to init mysql version".into())}
    }
}
