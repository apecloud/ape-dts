use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use tokio::sync::Mutex;
use zookeeper_client::{Client as ZkClient, CreateMode, Acls, Stat};

use dt_common::{
    log_error, log_info, log_warn,
    meta::{
        dt_data::DtData,
        position::Position,
        syncer::Syncer,
        zk::{zk_entry::ZkEntry, zk_event_type::ZkEventType, zk_stat::ZkStat},
    },
    zk_filter::ZkFilter,
};

use crate::extractor::base_extractor::{BaseExtractor, ExtractState};
use crate::extractor::resumer::recovery::Recovery;
use crate::Extractor;

pub struct ZkExtractor {
    pub url: String,
    pub watch_paths: Vec<String>,
    pub scan_interval_secs: u64,
    pub include_ephemeral: bool,
    pub heartbeat_interval_secs: u64,
    pub filter: ZkFilter,
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub syncer: Arc<Mutex<Syncer>>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

impl ZkExtractor {
    fn stat_to_zk_stat(stat: &Stat) -> ZkStat {
        ZkStat {
            version: stat.version,
            cversion: stat.cversion,
            aversion: stat.aversion,
            mtime: stat.mtime,
            ctime: stat.ctime,
            czxid: stat.czxid,
            mzxid: stat.mzxid,
            ephemeral_owner: stat.ephemeral_owner,
        }
    }

    fn is_ephemeral(stat: &Stat) -> bool {
        stat.ephemeral_owner != 0
    }

    async fn recover_position(&mut self) -> anyhow::Result<(HashMap<String, (i32, i64)>, i64)> {
        if let Some(recovery) = &self.recovery {
            if let Some(position) = recovery.get_cdc_resume_position().await {
                if let Position::Zk {
                    path_versions,
                    high_water_zxid,
                    last_scan_timestamp,
                    total_paths,
                } = &position
                {
                    log_info!(
                        "ZkExtractor recovery: {} paths, high_water_zxid={}, last_scan={}",
                        total_paths, high_water_zxid, last_scan_timestamp
                    );
                    self.base_extractor
                        .push_dt_data(&mut self.extract_state, DtData::Heartbeat {}, position.clone())
                        .await?;
                    return Ok((path_versions.clone(), *high_water_zxid));
                }
            }
        }
        Ok((HashMap::new(), 0))
    }

    async fn full_scan(
        &mut self,
        client: &ZkClient,
        path_versions: &mut HashMap<String, (i32, i64)>,
        high_water_zxid: &mut i64,
        source_id: &str,
    ) -> anyhow::Result<()> {
        log_info!("ZkExtractor starting full scan");
        let old_versions = path_versions.clone();
        for watch_path in &self.watch_paths.clone() {
            if self.base_extractor.shut_down.load(Ordering::Relaxed) {
                break;
            }
            self.scan_node_recursive(
                client, watch_path, &old_versions, path_versions, high_water_zxid, source_id,
            )
            .await?;
        }
        log_info!(
            "ZkExtractor full scan complete: {} paths, high_water_zxid={}",
            path_versions.len(), high_water_zxid
        );
        Ok(())
    }

    async fn scan_node_recursive(
        &mut self,
        client: &ZkClient,
        path: &str,
        old_versions: &HashMap<String, (i32, i64)>,
        current_versions: &mut HashMap<String, (i32, i64)>,
        high_water_zxid: &mut i64,
        source_id: &str,
    ) -> anyhow::Result<()> {
        if self.base_extractor.shut_down.load(Ordering::Relaxed) {
            return Ok(());
        }

        if self.filter.filter_path(path) {
            return Ok(());
        }

        let (data, stat) = match client.get_data(path).await {
            Ok(r) => r,
            Err(e) => {
                log_warn!("ZkExtractor: get_data failed for {}: {}", path, e);
                return Ok(());
            }
        };

        let ephemeral = Self::is_ephemeral(&stat);
        if self.filter.filter_ephemeral(ephemeral) {
            return Ok(());
        }

        let (already_synced, is_update) = match old_versions.get(path) {
            Some((v, _)) => (*v >= stat.version, true),
            None => (false, false),
        };

        if !already_synced {
            let event_type = if is_update {
                ZkEventType::Updated
            } else {
                ZkEventType::Created
            };
            let entry = ZkEntry {
                path: path.to_string(),
                data: Some(data),
                stat: Self::stat_to_zk_stat(&stat),
                ephemeral,
                event_type,
                source_id: source_id.to_string(),
            };
            let position = self.build_position(current_versions, *high_water_zxid);
            self.extract_state
                .push_dt_data(&self.base_extractor, DtData::Zk { entry }, position)
                .await?;
        }

        current_versions.insert(path.to_string(), (stat.version, stat.mzxid));
        if stat.mzxid > *high_water_zxid {
            *high_water_zxid = stat.mzxid;
        }

        let children = match client.list_children(path).await {
            Ok(c) => c,
            Err(e) => {
                log_warn!("ZkExtractor: list_children failed for {}: {}", path, e);
                return Ok(());
            }
        };

        for child in children {
            let child_path = if path == "/" {
                format!("/{}", child)
            } else {
                format!("{}/{}", path, child)
            };
            Box::pin(self.scan_node_recursive(
                client, &child_path, old_versions, current_versions, high_water_zxid, source_id,
            ))
            .await?;
        }

        Ok(())
    }

    async fn incremental_rescan(
        &mut self,
        client: &ZkClient,
        path_versions: &mut HashMap<String, (i32, i64)>,
        high_water_zxid: &mut i64,
        source_id: &str,
    ) -> anyhow::Result<()> {
        log_info!("ZkExtractor starting incremental rescan");
        let old_versions = path_versions.clone();

        let mut current_versions: HashMap<String, (i32, i64)> = HashMap::new();
        let mut current_zxid = *high_water_zxid;

        for watch_path in &self.watch_paths.clone() {
            if self.base_extractor.shut_down.load(Ordering::Relaxed) {
                break;
            }
            self.scan_node_recursive(
                client, watch_path, &old_versions, &mut current_versions, &mut current_zxid,
                source_id,
            )
            .await?;
        }

        // Detect deletes: paths in old_versions but missing from current scan
        for (path, _) in &old_versions {
            if current_versions.contains_key(path) {
                continue;
            }
            if self.filter.filter_path(path) {
                continue;
            }
            let entry = ZkEntry {
                path: path.clone(),
                data: None,
                stat: ZkStat::default(),
                ephemeral: false,
                event_type: ZkEventType::Deleted,
                source_id: source_id.to_string(),
            };
            let position = self.build_position(&current_versions, current_zxid);
            self.extract_state
                .push_dt_data(&self.base_extractor, DtData::Zk { entry }, position)
                .await?;
        }

        *path_versions = current_versions;
        *high_water_zxid = current_zxid;

        log_info!(
            "ZkExtractor incremental rescan complete: {} paths",
            path_versions.len()
        );
        Ok(())
    }

    fn build_position(
        &self,
        path_versions: &HashMap<String, (i32, i64)>,
        high_water_zxid: i64,
    ) -> Position {
        Position::Zk {
            path_versions: path_versions.clone(),
            last_scan_timestamp: chrono::Utc::now().timestamp_millis(),
            high_water_zxid,
            total_paths: path_versions.len(),
        }
    }

    fn start_heartbeat(
        &self,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
        url: String,
        heartbeat_interval_secs: u64,
        source_id: String,
    ) {
        if heartbeat_interval_secs == 0 {
            return;
        }
        tokio::spawn(async move {
            let hb_client = match ZkClient::connect(&url).await {
                Ok(c) => c,
                Err(e) => {
                    log_error!("ZkExtractor heartbeat: connect failed: {}", e);
                    return;
                }
            };

            let marker_path = "/__ape_dts_heartbeat";
            let _ = hb_client
                .create(marker_path, b"", &CreateMode::Persistent.with_acls(Acls::anyone_all()))
                .await;

            loop {
                if shut_down.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(heartbeat_interval_secs)).await;
                if shut_down.load(Ordering::Relaxed) {
                    break;
                }

                let syncer_guard = syncer.lock().await;
                let hb_data = serde_json::json!({
                    "source_id": source_id,
                    "received_position": syncer_guard.received_position.to_string(),
                    "committed_position": syncer_guard.committed_position.to_string(),
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                });
                drop(syncer_guard);

                if let Err(e) = hb_client.set_data(marker_path, hb_data.to_string().as_bytes(), None).await {
                    log_warn!("ZkExtractor heartbeat write failed: {}", e);
                }
            }
        });
    }
}

#[async_trait]
impl Extractor for ZkExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        let source_id = self
            .extract_state
            .data_marker
            .as_ref()
            .map(|dm| dm.src_node.clone())
            .unwrap_or_else(|| format!("ape_dts_{}", self.url));

        let (mut path_versions, mut high_water_zxid) = self.recover_position().await?;

        let client = ZkClient::connect(&self.url).await?;
        log_info!("ZkExtractor connected to {}", self.url);

        self.start_heartbeat(
            self.base_extractor.shut_down.clone(),
            self.syncer.clone(),
            self.url.clone(),
            self.heartbeat_interval_secs,
            source_id.clone(),
        );

        self.full_scan(&client, &mut path_versions, &mut high_water_zxid, &source_id)
            .await?;

        while !self.base_extractor.shut_down.load(Ordering::Relaxed) {
            tokio::time::sleep(tokio::time::Duration::from_secs(self.scan_interval_secs)).await;
            if self.base_extractor.shut_down.load(Ordering::Relaxed) {
                break;
            }
            self.incremental_rescan(&client, &mut path_versions, &mut high_water_zxid, &source_id)
                .await?;
        }

        self.base_extractor.wait_task_finish(&mut self.extract_state).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
