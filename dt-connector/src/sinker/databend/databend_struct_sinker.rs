use crate::{rdb_router::RdbRouter, Sinker};
use anyhow::bail;
use databend_driver::Client;
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    log_error, log_info,
    meta::{
        mysql::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta},
        pg::{pg_tb_meta::PgTbMeta, pg_value_type::PgValueType},
        rdb_meta_manager::RdbMetaManager,
        struct_meta::{
            statement::struct_statement::StructStatement,
            struct_data::StructData,
            structure::{column::Column, table::Table},
        },
    },
    rdb_filter::RdbFilter,
};

use async_trait::async_trait;

// 定义用于软删除和时间戳的列名和类型
const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const SIGN_COL_TYPE: &str = "Int8";
const TIMESTAMP_COL_NAME: &str = "_ape_dts_timestamp";
const TIMESTAMP_COL_TYPE: &str = "Int64";

// DatabendStructSinker结构体定义
#[derive(Clone)]
pub struct DatabendStructSinker {
    pub client: Client,                              // Databend客户端连接
    pub conflict_policy: ConflictPolicyEnum,         // 冲突处理策略
    pub engine: String,                              // 表引擎类型
    pub filter: RdbFilter,                           // 数据过滤器
    pub router: RdbRouter,                           // 路由管理器
    pub extractor_meta_manager: RdbMetaManager,      // 元数据管理器
}

#[async_trait]
impl Sinker for DatabendStructSinker {
    // 实现结构化数据写入方法
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let reverse_router = self.router.reverse();
        for i in data {
            match i.statement {
                // 处理MySQL数据库创建语句
                StructStatement::MysqlCreateDatabase(statement) => {
                    let sql = format!(
                        "CREATE DATABASE IF NOT EXISTS `{}`",
                        statement.database.name
                    );
                    self.execute_sql(&sql).await?;
                }

                // 处理MySQL表创建语句
                StructStatement::MysqlCreateTable(statement) => {
                    let (schema, tb) = reverse_router
                        .get_tb_map(&statement.table.database_name, &statement.table.table_name);
                    if let Some(meta_manager) =
                        self.extractor_meta_manager.mysql_meta_manager.as_mut()
                    {
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?;
                        let sql =
                            Self::get_create_table_sql(&statement.table, Some(tb_meta), None)?;
                        self.execute_sql(&sql).await?;
                    }
                }

                // 处理PostgreSQL schema创建语句
                StructStatement::PgCreateSchema(statement) => {
                    let sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", statement.schema.name);
                    self.execute_sql(&sql).await?;
                }

                // 处理PostgreSQL表创建语句
                StructStatement::PgCreateTable(statement) => {
                    let (schema, tb) = reverse_router
                        .get_tb_map(&statement.table.schema_name, &statement.table.table_name);
                    if let Some(meta_manager) = self.extractor_meta_manager.pg_meta_manager.as_mut()
                    {
                        let tb_meta = meta_manager.get_tb_meta(schema, tb).await?.to_owned();
                        let sql =
                            Self::get_create_table_sql(&statement.table, None, Some(&tb_meta))?;
                        self.execute_sql(&sql).await?;
                    }
                }

                _ => {}
            }
        }

        Ok(())
    }
}

impl DatabendStructSinker {
    // 生成建表SQL语句
    fn get_create_table_sql(
        table: &Table,
        mysql_tb_meta: Option<&MysqlTbMeta>,
        pg_tb_meta: Option<&PgTbMeta>,
    ) -> anyhow::Result<String> {
        let rdb_tb_meta = if let Some(tb_meta) = pg_tb_meta {
            &tb_meta.basic
        } else {
            &mysql_tb_meta.as_ref().unwrap().basic
        };

        // 构建列定义
        let mut dst_cols = vec![];
        for column in table.columns.iter() {
            dst_cols.push(Self::get_dst_col(column, mysql_tb_meta, pg_tb_meta)?);
        }

        // 添加软删除标记和时间戳列
        dst_cols.push(format!("`{}` {}", SIGN_COL_NAME, SIGN_COL_TYPE));
        dst_cols.push(format!("`{}` {}", TIMESTAMP_COL_NAME, TIMESTAMP_COL_TYPE));

        let schema = if mysql_tb_meta.is_some() {
            &table.database_name
        } else {
            &table.schema_name
        };

        // 构建建表SQL，使用ReplacingMergeTree引擎
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}) ENGINE = ReplacingMergeTree(`{}`)",
            schema,
            table.table_name,
            dst_cols.join(", "),
            TIMESTAMP_COL_NAME
        );

        // 添加主键和排序键
        if !rdb_tb_meta.id_cols.is_empty() {
            let order_by = rdb_tb_meta
                .id_cols
                .iter()
                .map(|i| format!("`{}`", i))
                .collect::<Vec<String>>()
                .join(",");
            sql = format!("{} PRIMARY KEY ({}) ORDER BY ({})", sql, order_by, order_by);
        }
        Ok(sql)
    }

    // 获取目标列定义
    fn get_dst_col(
        column: &Column,
        mysql_tb_meta: Option<&MysqlTbMeta>,
        pg_tb_meta: Option<&PgTbMeta>,
    ) -> anyhow::Result<String> {
        let col = &column.column_name;
        let dst_col_type = if let Some(tb_meta) = mysql_tb_meta {
            Self::get_dst_col_type_from_mysql(col, tb_meta)
        } else {
            Self::get_dst_col_type_from_pg(col, pg_tb_meta.unwrap())
        }?;

        // 处理可空类型
        let mut dst_col = if column.is_nullable && !dst_col_type.starts_with("Array") {
            format!("`{}` Nullable({})", col, dst_col_type)
        } else {
            format!("`{}` {}", col, dst_col_type)
        };

        // 添加列注释
        if !column.column_comment.is_empty() {
            dst_col = format!("{} COMMENT='{}'", dst_col, column.column_comment);
        }

        Ok(dst_col)
    }

    // MySQL数据类型转换为Databend数据类型
    fn get_dst_col_type_from_mysql(col: &str, tb_meta: &MysqlTbMeta) -> anyhow::Result<String> {
        let mysql_col_type = tb_meta.get_col_type(col)?;
        let dst_col = match mysql_col_type {
            MysqlColType::TinyInt { unsigned: false } => "Int8",
            MysqlColType::TinyInt { unsigned: true } => "UInt8",
            MysqlColType::SmallInt { unsigned: false } => "Int16",
            MysqlColType::SmallInt { unsigned: true } => "UInt16",
            MysqlColType::MediumInt { unsigned: false } => "Int32",
            MysqlColType::MediumInt { unsigned: true } => "UInt32",
            MysqlColType::Int { unsigned: false } => "Int32",
            MysqlColType::Int { unsigned: true } => "UInt32",
            MysqlColType::BigInt { unsigned: false } => "Int64",
            MysqlColType::BigInt { unsigned: true } => "UInt64",

            MysqlColType::Float => "Float32",
            MysqlColType::Double => "Float64",
            MysqlColType::Decimal { precision, scale } => {
                format!("Decimal({},{})", precision, scale).leak()
            }

            MysqlColType::Time { .. } => "String",
            MysqlColType::Date { .. } => "Date32",
            MysqlColType::DateTime { .. } => "DateTime64(6)",
            MysqlColType::Timestamp { .. } => "DateTime64(6)",
            MysqlColType::Year => "Int32",

            MysqlColType::Char { .. }
            | MysqlColType::Varchar { .. }
            | MysqlColType::TinyText { .. }
            | MysqlColType::MediumText { .. }
            | MysqlColType::Text { .. }
            | MysqlColType::LongText { .. } => "String",

            MysqlColType::Binary { length: _ } => "String",
            MysqlColType::VarBinary { length: _ } => "String",
            MysqlColType::TinyBlob
            | MysqlColType::MediumBlob
            | MysqlColType::Blob
            | MysqlColType::LongBlob => "String",

            MysqlColType::Bit => "UInt64",
            MysqlColType::Set { items: _ } => "String",
            MysqlColType::Enum { items: _ } => "String",
            MysqlColType::Json => "String",
            MysqlColType::Unknown => "String",
        };
        Ok(dst_col.to_string())
    }

    // PostgreSQL数据类型转换为Databend数据类型
    fn get_dst_col_type_from_pg(col: &str, tb_meta: &PgTbMeta) -> anyhow::Result<String> {
        let pg_col_type = tb_meta.get_col_type(col)?;
        let dst_col = match pg_col_type.value_type {
            PgValueType::Boolean => "Bool",
            PgValueType::Int16 => "Int16",
            PgValueType::Int32 => "Int32",
            PgValueType::Int64 => "Int64",
            PgValueType::Float32 => "Float32",
            PgValueType::Float64 => "Float64",
            PgValueType::Numeric => "Decimal128(9)",
            PgValueType::Char => "FixedString(1)",
            PgValueType::String => "String",
            PgValueType::JSON => "String",
            PgValueType::Timestamp => "DateTime64(6)",
            PgValueType::TimestampTZ => "DateTime64(6)",
            PgValueType::Date => "Date32",
            PgValueType::Bytes => "String",
            PgValueType::Struct => "String",
            PgValueType::UUID => "UUID",
            _ => "String",
        };
        Ok(dst_col.to_string())
    }

    // 执行SQL语句
    async fn execute_sql(&self, sql: &str) -> anyhow::Result<()> {
        log_info!("ddl begin: {}", sql);
        match self.client.get_conn().await?.exec(sql, ()).await {
            Ok(_) => {
                log_info!("ddl succeed");
            }

            Err(error) => {
                log_error!("ddl failed, error: {}", error);
                match self.conflict_policy {
                    ConflictPolicyEnum::Interrupt => bail!(anyhow::Error::msg(error.to_string())),
                    ConflictPolicyEnum::Ignore => {}
                }
            }
        }
        Ok(())
    }
}
