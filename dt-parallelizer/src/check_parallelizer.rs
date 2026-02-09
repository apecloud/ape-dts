use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use super::{base_parallelizer::BaseParallelizer, snapshot_parallelizer::SnapshotParallelizer};
use crate::{DataSize, Merger, Parallelizer};
use dt_common::meta::{
    col_value::ColValue, dt_data::DtItem, dt_queue::DtQueue, row_data::RowData, row_type::RowType,
    struct_meta::struct_data::StructData,
};
use dt_connector::{checker::CheckerHandle, Sinker};

/// A deleted-row identifier: maps column name to its stringified value.
/// Uses only the columns present in the DELETE event's `before` image
/// (typically just primary key columns), so it can match INSERT rows
/// that contain a superset of columns.
type DeleteId = HashMap<String, Option<String>>;

pub struct CheckParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: Box<dyn Merger + Send + Sync>,
    pub parallel_size: usize,
    pub checker: Option<CheckerHandle>,
    /// Tracks id columns of deleted rows across sink_dml calls.
    /// INSERT rows matching these ids are excluded from checker verification
    /// to avoid false misses caused by cross-batch race conditions.
    deleted_ids: Vec<(String, String, DeleteId)>,
}

impl CheckParallelizer {
    pub fn new(
        base_parallelizer: BaseParallelizer,
        merger: Box<dyn Merger + Send + Sync>,
        parallel_size: usize,
        checker: Option<CheckerHandle>,
    ) -> Self {
        Self {
            base_parallelizer,
            merger,
            parallel_size,
            checker,
            deleted_ids: Vec::new(),
        }
    }

    /// Extract id column values from a DELETE row's `before` image.
    fn delete_id(row: &RowData) -> Option<DeleteId> {
        let before = row.before.as_ref()?;
        if before.is_empty() {
            return None;
        }
        let id: DeleteId = before
            .iter()
            .map(|(k, v)| (k.clone(), v.to_option_string()))
            .collect();
        Some(id)
    }

    /// Check whether a non-DELETE row matches any recorded delete id.
    /// Compares only the columns present in the delete id (typically PK columns)
    /// against the row's `after` values.
    fn is_deleted(&self, row: &RowData) -> bool {
        if self.deleted_ids.is_empty() {
            return false;
        }
        let after = match row.after.as_ref() {
            Some(a) if !a.is_empty() => a,
            _ => return false,
        };
        self.deleted_ids.iter().any(|(schema, tb, del_id)| {
            if schema != &row.schema || tb != &row.tb {
                return false;
            }
            del_id.iter().all(|(col, del_val)| {
                let row_val = after.get(col).map(ColValue::to_option_string);
                matches!(row_val, Some(ref v) if v == del_val)
            })
        })
    }

    fn record_deletes(&mut self, data: &[Arc<RowData>]) {
        for row in data {
            if row.row_type == RowType::Delete {
                if let Some(id) = Self::delete_id(row) {
                    self.deleted_ids
                        .push((row.schema.clone(), row.tb.clone(), id));
                }
            }
        }
    }
}

#[async_trait]
impl Parallelizer for CheckParallelizer {
    fn get_name(&self) -> String {
        "CheckParallelizer".to_string()
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(checker) = &mut self.checker {
            checker.close().await?;
        }
        self.merger.close().await
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<Arc<RowData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let mut data_size = DataSize::default();
        let mut check_data: Vec<Arc<RowData>> = Vec::new();

        // Record DELETE keys from raw data before merging.
        // These accumulate across sink_dml calls so that INSERT rows
        // whose counterpart DELETE arrives in a later batch are also filtered.
        if self.checker.is_some() {
            self.record_deletes(&data);
        }

        let mut merged_data_items = self.merger.merge(data).await?;
        for tb_merged_data in merged_data_items.drain(..) {
            // delete first, then insert (same order as MergeParallelizer)
            let delete_data = tb_merged_data.delete_rows;
            data_size
                .add_count(delete_data.len() as u64)
                .add_bytes(delete_data.iter().map(|v| v.get_data_size()).sum());
            let delete_sub_data_items =
                SnapshotParallelizer::partition(delete_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(delete_sub_data_items, sinkers, self.parallel_size, false)
                .await?;

            let batch_data = tb_merged_data.insert_rows;
            if self.checker.is_some() {
                for row in batch_data.iter() {
                    if !self.is_deleted(row) {
                        check_data.push(row.clone());
                    }
                }
            }
            data_size
                .add_count(batch_data.len() as u64)
                .add_bytes(batch_data.iter().map(|v| v.get_data_size()).sum());
            let batch_sub_data_items =
                SnapshotParallelizer::partition(batch_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(batch_sub_data_items, sinkers, self.parallel_size, true)
                .await?;

            let serial_data = tb_merged_data.unmerged_rows;
            if self.checker.is_some() {
                for row in serial_data.iter() {
                    if row.row_type != RowType::Delete && !self.is_deleted(row) {
                        check_data.push(row.clone());
                    }
                }
            }
            data_size
                .add_count(serial_data.len() as u64)
                .add_bytes(serial_data.iter().map(|v| v.get_data_size()).sum());
            let serial_sub_data_items =
                SnapshotParallelizer::partition(serial_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(serial_sub_data_items, sinkers, self.parallel_size, false)
                .await?;
        }

        if let Some(checker) = &self.checker {
            if !check_data.is_empty() {
                if let Err(err) = checker.check_rows(check_data).await {
                    log::warn!("checker sidecar failed: {}", err);
                }
            }
        }

        Ok(data_size)
    }

    async fn sink_struct(
        &mut self,
        data: Vec<StructData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<DataSize> {
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: 0,
        };
        let data_for_check = if self.checker.is_some() {
            Some(data.clone())
        } else {
            None
        };
        sinkers[0].lock().await.sink_struct(data).await?;
        if let (Some(checker), Some(data_for_check)) = (&self.checker, data_for_check) {
            if let Err(err) = checker.check_struct(data_for_check).await {
                log::warn!("checker sidecar failed: {}", err);
            }
        }
        Ok(data_size)
    }
}
