use std::collections::HashMap;

use async_trait::async_trait;
use log::debug;

use crate::{
    error::Error,
    meta::{
        mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
        row_data::RowData, row_type::RowType,
    },
    traits::traits::Partitioner,
};

pub struct RdbPartitioner {
    mysql_meta_manager: Option<MysqlMetaManager>,
    pg_meta_manager: Option<PgMetaManager>,
}

#[async_trait]
impl Partitioner for RdbPartitioner {
    async fn partition(
        &mut self,
        data: Vec<RowData>,
        partition_count: usize,
    ) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_datas = Vec::new();
        for _ in 0..partition_count {
            sub_datas.push(Vec::new());
        }

        for row_data in data {
            let partition = self.get_partition_index(&row_data, partition_count).await?;
            sub_datas[partition].push(row_data);
        }

        Ok(sub_datas)
    }

    async fn can_be_partitioned<'a>(&mut self, row_data: &'a RowData) -> Result<bool, Error> {
        if row_data.row_type != RowType::Update {
            return Ok(true);
        }

        let (_, key_map) = self.get_tb_meta_info(&row_data.db, &row_data.tb).await?;
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();
        // check if any col value of pk & uk has changed, if changed
        for key_cols in key_map.values() {
            for col in key_cols {
                let col_value_before = before.get(col);
                let col_value_after = after.get(col);
                if col_value_before != col_value_after {
                    debug!(
                        "{}.{}.{} changed from {} to {}",
                        &row_data.db,
                        &row_data.tb,
                        col,
                        col_value_before.unwrap().to_string(),
                        col_value_after.unwrap().to_string()
                    );
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

impl RdbPartitioner {
    pub fn new_for_mysql(meta_manager: MysqlMetaManager) -> RdbPartitioner {
        Self {
            mysql_meta_manager: Some(meta_manager),
            pg_meta_manager: Option::None,
        }
    }

    pub fn new_for_pg(meta_manager: PgMetaManager) -> RdbPartitioner {
        Self {
            mysql_meta_manager: Option::None,
            pg_meta_manager: Some(meta_manager),
        }
    }

    async fn get_partition_index(
        &mut self,
        row_data: &RowData,
        slice_count: usize,
    ) -> Result<usize, Error> {
        if slice_count <= 1 {
            return Ok(0);
        }

        let col_values = match row_data.row_type {
            RowType::Insert => row_data.after.as_ref().unwrap(),
            _ => row_data.before.as_ref().unwrap(),
        };

        let (partition_col, _) = self.get_tb_meta_info(&row_data.db, &row_data.tb).await?;
        if let Some(partition_col_value) = col_values.get(&partition_col) {
            Ok(partition_col_value.hash_code() as usize % slice_count)
        } else {
            Ok(0)
        }
    }

    async fn get_tb_meta_info(
        &mut self,
        schema: &str,
        tb: &str,
    ) -> Result<(String, HashMap<String, Vec<String>>), Error> {
        if let Some(mysql_meta_manager) = self.mysql_meta_manager.as_mut() {
            let tb_meta = mysql_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok((tb_meta.partition_col, tb_meta.key_map));
        }

        if let Some(pg_meta_manager) = self.pg_meta_manager.as_mut() {
            let tb_meta = pg_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok((tb_meta.partition_col, tb_meta.key_map));
        }

        Err(Error::Unexpected {
            error: "no available meta_manager in partitioner".to_string(),
        })
    }
}