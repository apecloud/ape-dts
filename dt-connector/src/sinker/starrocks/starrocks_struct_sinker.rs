use std::cmp;

use crate::{close_conn_pool, rdb_router::RdbRouter, Sinker};

use anyhow::bail;
use dt_common::{
    config::config_enums::{ConflictPolicyEnum, DbType},
    log_error, log_info,
    meta::{
        mysql::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta},
        pg::{pg_tb_meta::PgTbMeta, pg_value_type::PgValueType},
        rdb_meta_manager::RdbMetaManager,
        rdb_tb_meta::RdbTbMeta,
        struct_meta::{
            statement::struct_statement::StructStatement,
            struct_data::StructData,
            structure::{column::Column, table::Table},
        },
    },
    rdb_filter::RdbFilter,
};

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};

const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const SIGN_COL_TYPE: &str = "BOOLEAN";
const TIMESTAMP_COL_NAME: &str = "_ape_dts_timestamp";
const TIMESTAMP_COL_TYPE: &str = "BIGINT";

#[derive(Clone)]
pub struct StarrocksStructSinker {
    pub db_type: DbType,
    pub conn_pool: Pool<MySql>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: RdbRouter,
    pub extractor_meta_manager: RdbMetaManager,
    pub backend_count: i32,
}

#[async_trait]
impl Sinker for StarrocksStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        if self.backend_count == 0 {
            self.backend_count = self.get_backend_count().await?;
        }

        let reverse_router = self.router.reverse();
        for i in data {
            match i.statement {
                StructStatement::MysqlCreateDatabase(statement) => {
                    let sql = format!(
                        "CREATE DATABASE IF NOT EXISTS `{}`",
                        statement.database.name
                    );
                    self.execute_sql(&sql).await?;
                }

                StructStatement::MysqlCreateTable(statement) => {
                    let (schema, tb) = reverse_router
                        .get_tb_map(&statement.table.database_name, &statement.table.table_name);
                    if let Some(meta_manager) =
                        self.extractor_meta_manager.mysql_meta_manager.as_mut()
                    {
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?.to_owned();
                        let sql =
                            self.get_create_table_sql(&statement.table, Some(&tb_meta), None)?;
                        self.execute_sql(&sql).await?;
                    }
                }

                StructStatement::PgCreateSchema(statement) => {
                    let sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", statement.schema.name);
                    self.execute_sql(&sql).await?;
                }

                StructStatement::PgCreateTable(statement) => {
                    let (schema, tb) = reverse_router
                        .get_tb_map(&statement.table.schema_name, &statement.table.table_name);
                    if let Some(meta_manager) = self.extractor_meta_manager.pg_meta_manager.as_mut()
                    {
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?.to_owned();
                        let sql =
                            self.get_create_table_sql(&statement.table, None, Some(&tb_meta))?;
                        self.execute_sql(&sql).await?;
                    }
                }

                _ => {}
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.extractor_meta_manager.close().await?;
        return close_conn_pool!(self);
    }
}

impl StarrocksStructSinker {
    fn get_create_table_sql(
        &self,
        table: &Table,
        mysql_tb_meta: Option<&MysqlTbMeta>,
        pg_tb_meta: Option<&PgTbMeta>,
    ) -> anyhow::Result<String> {
        let rdb_tb_meta = if let Some(tb_meta) = pg_tb_meta {
            &tb_meta.basic
        } else {
            &mysql_tb_meta.as_ref().unwrap().basic
        };

        let mut dst_cols = vec![];

        // In StarRocks:
        // Key columns must be the first few columns of the schema and the order
        // of the key columns must be consistent with the order of the schema.
        for col in rdb_tb_meta.id_cols.iter() {
            for column in table.columns.iter() {
                if column.column_name == *col {
                    dst_cols.push(self.get_dst_col(
                        column,
                        mysql_tb_meta,
                        pg_tb_meta,
                        rdb_tb_meta,
                    )?);
                }
            }
        }

        for column in table.columns.iter() {
            if !rdb_tb_meta.id_cols.contains(&column.column_name) {
                dst_cols.push(self.get_dst_col(column, mysql_tb_meta, pg_tb_meta, rdb_tb_meta)?);
            }
        }

        // sign and timestamp cols
        if self.db_type == DbType::StarRocks {
            dst_cols.push(format!("`{}` {}", SIGN_COL_NAME, SIGN_COL_TYPE));
            dst_cols.push(format!("`{}` {}", TIMESTAMP_COL_NAME, TIMESTAMP_COL_TYPE));
        }

        let schema = if mysql_tb_meta.is_some() {
            &table.database_name
        } else {
            &table.schema_name
        };
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({})",
            schema,
            table.table_name,
            dst_cols.join(", "),
        );

        if !rdb_tb_meta.id_cols.is_empty() {
            let primary_keys = rdb_tb_meta
                .id_cols
                .iter()
                .map(|i| format!("`{}`", i))
                .collect::<Vec<String>>()
                .join(",");

            if self.db_type == DbType::Doris {
                sql = format!("{} UNIQUE KEY ({})", sql, primary_keys);
            } else {
                sql = format!("{} PRIMARY KEY ({})", sql, primary_keys);
            }

            if !table.table_comment.is_empty() {
                sql = format!("{} COMMENT '{}'", sql, table.table_comment);
            }
            sql = format!("{} DISTRIBUTED BY HASH(`{}`)", sql, rdb_tb_meta.id_cols[0]);
        }

        if self.backend_count < 3 {
            sql = format!(r#"{} PROPERTIES ("replication_num" = "1")"#, sql);
        }
        Ok(sql)
    }

    fn get_dst_col(
        &self,
        column: &Column,
        mysql_tb_meta: Option<&MysqlTbMeta>,
        pg_tb_meta: Option<&PgTbMeta>,
        rdb_tb_meta: &RdbTbMeta,
    ) -> anyhow::Result<String> {
        let col = &column.column_name;
        let dst_col_type = if let Some(tb_meta) = mysql_tb_meta {
            self.get_dst_col_type_from_mysql(col, tb_meta)
        } else {
            self.get_dst_col_type_from_pg(col, pg_tb_meta.unwrap())
        }?;

        // The delete operation in Doris (-H "merge_type: delete") is implemented by inserting a record marked for deletion,
        // which means it validates the values of all fields (even if -H "strict_mode: false" is set).
        // Consider a scenario:
        //      Source: A Postgres table: tb_1 (id int not null, f_1 int not null, primary key(id))
        //      Target: A Doris table: tb_1 (id int not null, f_1 int not null) unique key(id)
        // When a DELETE event is generated by Postgres CDC, and if Postgres is not set with REPLICA IDENTITY = FULL,
        // the event data retrieved from the WAL (Write-Ahead Log) for the DELETE operation would be:
        //      {"id": 1, "value": null}.
        // At this point, attempting to delete the data using the following stream_load method will fail
        // because the value of f_1 field (null) is invalid:
        //      curl -X PUT "http://127.0.0.1:8042/api/public/tb_1/_stream_load" -H "Expect: 100-continue" -H "format: json"  -H "strip_outer_array: true" -H "timezone: UTC" -H "merge_type: delete" -u "root:" -d '[{"id": 1, value: null}]'
        let mut is_nullable = pg_tb_meta.is_some() && !rdb_tb_meta.id_cols.contains(col);
        is_nullable |= column.is_nullable;
        let mut dst_col = if is_nullable {
            format!("`{}` {}", col, dst_col_type)
        } else {
            format!("`{}` {} NOT NULL", col, dst_col_type)
        };

        if !column.column_comment.is_empty() {
            dst_col = format!("{} COMMENT '{}'", dst_col, column.column_comment);
        }

        Ok(dst_col)
    }

    fn get_dst_col_type_from_mysql(
        &self,
        col: &str,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<String> {
        let mysql_col_type = tb_meta.get_col_type(col)?;
        let dst_col = match mysql_col_type {
            MysqlColType::TinyInt { unsigned: false } => "TINYINT",
            MysqlColType::TinyInt { unsigned: true } => "SMALLINT",
            MysqlColType::SmallInt { unsigned: false } => "SMALLINT",
            MysqlColType::SmallInt { unsigned: true } => "INT",
            MysqlColType::MediumInt { unsigned: false } => "INT",
            MysqlColType::MediumInt { unsigned: true } => "BIGINT",
            MysqlColType::Int { unsigned: false } => "INT",
            MysqlColType::Int { unsigned: true } => "BIGINT",
            MysqlColType::BigInt { unsigned: false } => "BIGINT",
            MysqlColType::BigInt { unsigned: true } => "LARGEINT",

            MysqlColType::Float => "FLOAT",
            MysqlColType::Double => "DOUBLE",
            MysqlColType::Decimal { precision, scale } => {
                // https://docs.starrocks.io/docs/sql-reference/data-types/numeric/DECIMAL/
                if *precision <= 38 {
                    format!("DECIMAL({},{})", precision, scale).leak()
                } else {
                    "STRING"
                }
            }

            MysqlColType::Time { .. } => "VARCHAR(255)",
            MysqlColType::Year => "INT",

            // https://github.com/apache/doris-flink-connector/blob/master/flink-doris-connector/src/main/java/org/apache/doris/flink/tools/cdc/mysql/MysqlType.java
            MysqlColType::Date { .. } => "DATE",
            MysqlColType::DateTime { precision, .. }
            | MysqlColType::Timestamp { precision, .. } => {
                if self.db_type == DbType::StarRocks {
                    "DATETIME"
                } else {
                    format!("DATETIME({})", precision).leak()
                }
            }

            // In MySQL, CHAR(30) can hold up to 30 characters
            // In Doris/Starrocks, CHAR(30) can hold up to 30 bytes
            MysqlColType::Char { length: v, .. } => {
                format!("CHAR({})", cmp::min(v * 4, 255)).leak()
            }
            MysqlColType::Varchar { length: v, .. } => {
                format!("VARCHAR({})", cmp::min(v * 4, 65533)).leak()
            }

            MysqlColType::TinyText { .. }
            | MysqlColType::MediumText { .. }
            | MysqlColType::Text { .. }
            | MysqlColType::LongText { .. } => "STRING",

            MysqlColType::Binary { .. }
            | MysqlColType::VarBinary { .. }
            | MysqlColType::TinyBlob
            | MysqlColType::MediumBlob
            | MysqlColType::Blob
            | MysqlColType::LongBlob => {
                if self.db_type == DbType::StarRocks {
                    "VARBINARY"
                } else {
                    "STRING"
                }
            }

            MysqlColType::Bit => "BIGINT",
            MysqlColType::Set { items: _ } => "VARCHAR(255)",
            MysqlColType::Enum { items: _ } => "VARCHAR(255)",
            MysqlColType::Json => "JSON",
            MysqlColType::Unknown => "STRING",
        };
        Ok(dst_col.to_string())
    }

    fn get_dst_col_type_from_pg(&self, col: &str, tb_meta: &PgTbMeta) -> anyhow::Result<String> {
        let pg_col_type = tb_meta.get_col_type(col)?;
        let dst_col = match pg_col_type.value_type {
            // boolean == tinyint(1)
            PgValueType::Boolean => "BOOLEAN",
            PgValueType::Int16 => "SMALLINT",
            PgValueType::Int32 => "INT",
            PgValueType::Int64 => "BIGINT",
            PgValueType::Float32 => "FLOAT",
            PgValueType::Float64 => "DOUBLE",

            // TODO, set precision / scale according to source
            PgValueType::Numeric => "DECIMAL(38,9)",
            PgValueType::Char => "CHAR",
            PgValueType::String => "STRING",
            PgValueType::JSON => "JSON",

            PgValueType::Time | PgValueType::TimeTZ | PgValueType::Interval => "VARCHAR(255)",
            PgValueType::Timestamp | PgValueType::TimestampTZ => {
                if self.db_type == DbType::StarRocks {
                    "DATETIME"
                } else {
                    "DATETIME(6)"
                }
            }
            PgValueType::Date => "DATE",

            PgValueType::Bytes => {
                if self.db_type == DbType::StarRocks {
                    "VARBINARY"
                } else {
                    "STRING"
                }
            }
            _ => "STRING",
        };
        Ok(dst_col.to_string())
    }

    async fn get_backend_count(&self) -> anyhow::Result<i32> {
        let sql = "SHOW BACKENDS";
        let mut count = 0;
        let mut rows = sqlx::query(sql).fetch(&self.conn_pool);
        while (rows.try_next().await?).is_some() {
            count += 1;
        }
        Ok(count)
    }

    async fn execute_sql(&self, sql: &str) -> anyhow::Result<()> {
        log_info!("ddl begin: {}", sql);
        let query = sqlx::query(sql);
        match query.execute(&self.conn_pool).await {
            Ok(_) => {
                log_info!("ddl succeed");
            }

            Err(error) => {
                log_error!("ddl failed, error: {}", error);
                match self.conflict_policy {
                    ConflictPolicyEnum::Interrupt => bail! {error},
                    ConflictPolicyEnum::Ignore => {}
                }
            }
        }
        Ok(())
    }
}
