use std::collections::HashMap;

use sqlx::{MySql, Pool};

use super::{
    mysql_dbengine_meta_center::MysqlDbEngineMetaCenter, mysql_meta_fetcher::MysqlMetaFetcher,
    mysql_tb_meta::MysqlTbMeta,
};
use crate::meta::{mysql::mysql_col_type::MysqlColType, row_data::RowData};
use crate::{config::config_enums::DbType, meta::ddl_meta::ddl_data::DdlData};

#[derive(Clone)]
pub struct MysqlMetaManager {
    pub meta_center: Option<MysqlDbEngineMetaCenter>,
    pub meta_fetcher: MysqlMetaFetcher,
}

impl MysqlMetaManager {
    pub async fn new(conn_pool: Pool<MySql>) -> anyhow::Result<Self> {
        Self::new_mysql_compatible(conn_pool, DbType::Mysql).await
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if let Some(meta_center) = &self.meta_center {
            meta_center.meta_fetcher.close().await?;
        }
        self.meta_fetcher.close().await
    }

    pub async fn new_mysql_compatible(
        conn_pool: Pool<MySql>,
        db_type: DbType,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            meta_center: None,
            meta_fetcher: MysqlMetaFetcher::new_mysql_compatible(conn_pool, db_type).await?,
        })
    }

    pub fn invalidate_cache(&mut self, schema: &str, tb: &str) {
        if let Some(meta_center) = &mut self.meta_center {
            meta_center.meta_fetcher.invalidate_cache(schema, tb);
        }
        self.meta_fetcher.invalidate_cache(schema, tb)
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
        if let Some(meta_center) = &mut self.meta_center {
            if let Ok(tb_meta) = meta_center.meta_fetcher.get_tb_meta(schema, tb).await {
                return Ok(tb_meta);
            }
        }
        self.meta_fetcher.get_tb_meta(schema, tb).await
    }

    pub fn to_simple_mysql_col_type(&self, col_type_str: &str) -> MysqlColType {
        match col_type_str {
            "tinyint" => MysqlColType::TinyInt { unsigned: false },
            "smallint" => MysqlColType::SmallInt { unsigned: false },
            "bigint" => MysqlColType::BigInt { unsigned: false },
            "mediumint" => MysqlColType::MediumInt { unsigned: false },
            "int" => MysqlColType::Int { unsigned: false },

            "varbinary" => MysqlColType::VarBinary { length: 0u16 },
            "binary" => MysqlColType::Binary { length: 0u8 },

            "char" => MysqlColType::Char {
                length: 0u64,
                charset: String::new(),
            },
            "varchar" => MysqlColType::Varchar {
                length: 0u64,
                charset: String::new(),
            },
            "tinytext" => MysqlColType::TinyText {
                length: 0u64,
                charset: String::new(),
            },
            "mediumtext" => MysqlColType::MediumText {
                length: 0u64,
                charset: String::new(),
            },
            "longtext" => MysqlColType::LongText {
                length: 0u64,
                charset: String::new(),
            },
            "text" => MysqlColType::Text {
                length: 0u64,
                charset: String::new(),
            },

            // as a client of mysql, sqlx's client timezone is UTC by default,
            // so no matter what timezone of src/dst server is,
            // src server will convert the timestamp field into UTC for sqx,
            // and then sqx will write it into dst server by UTC,
            // and then dst server will convert the received UTC timestamp into its own timezone.
            "timestamp" => MysqlColType::Timestamp {
                precision: 0u32,
                timezone_offset: 0,
                is_nullable: false,
            },

            "tinyblob" => MysqlColType::TinyBlob,
            "mediumblob" => MysqlColType::MediumBlob,
            "longblob" => MysqlColType::LongBlob,
            "blob" => MysqlColType::Blob,

            "float" => MysqlColType::Float,
            "double" => MysqlColType::Double,

            "decimal" => MysqlColType::Decimal {
                precision: 0u32,
                scale: 0u32,
            },

            "enum" => MysqlColType::Enum { items: Vec::new() },

            "set" => MysqlColType::Set {
                items: HashMap::new(),
            },

            "datetime" => MysqlColType::DateTime {
                precision: 0u32,
                is_nullable: false,
            },

            "date" => MysqlColType::Date { is_nullable: false },
            "time" => MysqlColType::Time { precision: 0u32 },
            "year" => MysqlColType::Year,
            "bit" => MysqlColType::Bit,
            "json" => MysqlColType::Json,

            // TODO
            // "geometry": "geometrycollection": "linestring": "multilinestring":
            // "multipoint": "multipolygon": "polygon": "point"
            _ => MysqlColType::Unknown,
        }
    }
}
