use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};
use tokio::sync::{mpsc, Mutex};

use dt_common::meta::{pg::pg_meta_manager::PgMetaManager, row_data::RowData};
use dt_common::{log_error, log_info, log_warn};

use crate::checker::base_checker::{
    has_null_key, CheckProcessor, Checker, CheckerCommon, CheckerTbMeta,
};
use crate::checker::{CheckerHandle, CheckerMode};
use crate::rdb_query_builder::RdbQueryBuilder;

struct PgChecker {
    conn_pool: Pool<Postgres>,
    meta_manager: PgMetaManager,
}

#[async_trait]
impl Checker for PgChecker {
    async fn get_tb_meta(&mut self, row: &Arc<RowData>) -> anyhow::Result<CheckerTbMeta> {
        Ok(CheckerTbMeta::Pg(
            self.meta_manager
                .get_tb_meta_by_row_data(row)
                .await?
                .clone(),
        ))
    }

    async fn fetch_batch(
        &self,
        tb_meta: &CheckerTbMeta,
        data: &[&Arc<RowData>],
    ) -> anyhow::Result<Vec<RowData>> {
        let pg_meta = tb_meta.pg()?;
        let qb = RdbQueryBuilder::new_for_pg(pg_meta, None);

        let mut res = Vec::with_capacity(data.len());
        let mut batch_rows = Vec::with_capacity(data.len());

        for &row in data {
            if has_null_key(row, &pg_meta.basic.id_cols) {
                continue;
            }
            batch_rows.push(row);
        }

        if batch_rows.is_empty() {
            return Ok(res);
        }

        let query_info = qb.get_batch_select_query(&batch_rows, 0, batch_rows.len())?;
        let query = qb.create_pg_query(&query_info)?;
        let mut rows = query.fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await? {
            res.push(RowData::from_pg_row(&row, pg_meta, &None));
        }

        Ok(res)
    }
}

pub struct PgCheckerHandle {
    processor: Option<Mutex<CheckProcessor<PgChecker>>>,
    sender: Option<mpsc::Sender<Vec<Arc<RowData>>>>,
    mode: CheckerMode,
    dropped_batches: AtomicU64,
}

impl PgCheckerHandle {
    pub fn new(
        conn_pool: Pool<Postgres>,
        meta_manager: PgMetaManager,
        common: CheckerCommon,
        mode: CheckerMode,
    ) -> Self {
        let backend = PgChecker {
            conn_pool,
            meta_manager,
        };
        let processor = CheckProcessor::new(backend, common);
        let (sender, processor) = match mode {
            CheckerMode::Sync => (None, Some(Mutex::new(processor))),
            CheckerMode::AsyncBlocking { buffer_size } | CheckerMode::AsyncDrop { buffer_size } => {
                let (tx, mut rx) = mpsc::channel(buffer_size.max(1));
                tokio::spawn(async move {
                    log_info!("Checker [PgChecker] background worker started.");
                    let mut processor = processor;
                    while let Some(batch) = rx.recv().await {
                        if let Err(err) = processor.sink_dml(batch, true).await {
                            log_error!("Checker [PgChecker] batch failed: {}", err);
                        }
                    }
                    if let Err(err) = processor.close().await {
                        log_error!("Checker [PgChecker] close failed: {}", err);
                    }
                    log_info!("Checker [PgChecker] background worker stopped.");
                });
                (Some(tx), None)
            }
        };

        Self {
            processor,
            sender,
            mode,
            dropped_batches: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl CheckerHandle for PgCheckerHandle {
    async fn check(&self, data: Vec<Arc<RowData>>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        match self.mode {
            CheckerMode::Sync => {
                let processor = self
                    .processor
                    .as_ref()
                    .ok_or_else(|| anyhow!("Checker processor missing"))?;
                let mut guard = processor.lock().await;
                guard.sink_dml(data, true).await
            }
            CheckerMode::AsyncBlocking { .. } => {
                if let Some(tx) = &self.sender {
                    tx.send(data)
                        .await
                        .map_err(|_| anyhow!("Checker worker closed"))?;
                }
                Ok(())
            }
            CheckerMode::AsyncDrop { .. } => {
                if let Some(tx) = &self.sender {
                    match tx.try_send(data) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            let dropped = self.dropped_batches.fetch_add(1, Ordering::Relaxed) + 1;
                            if dropped % 1000 == 0 {
                                log_warn!("Checker queue full, dropped {} batches.", dropped);
                            }
                        }
                        Err(_) => return Err(anyhow!("Checker worker closed")),
                    }
                }
                Ok(())
            }
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.sender.take();
        if matches!(self.mode, CheckerMode::Sync) {
            if let Some(processor) = &self.processor {
                let mut guard = processor.lock().await;
                guard.close().await?;
            }
        }
        Ok(())
    }
}
