use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::bail;
use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};
use zookeeper_client::{
    Acls, AddWatchMode, Client as ZkClient, CreateMode, EventType, SessionState, Stat, WatchedEvent,
};

use dt_common::{
    log_error, log_info, log_warn,
    meta::{
        dt_data::DtData,
        position::Position,
        syncer::Syncer,
        zk::{
            zk_entry::{ZkEntry, ZkOrderOrigin},
            zk_event_type::ZkEventType,
            zk_stat::ZkStat,
        },
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

#[derive(Debug, Clone)]
struct ZkWatchEvent {
    watch_path: String,
    event: WatchedEvent,
}

#[derive(Debug, Clone)]
enum ReconcileScope {
    All,
    Path(String),
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
                        total_paths,
                        high_water_zxid,
                        last_scan_timestamp
                    );
                    self.base_extractor
                        .push_dt_data(
                            &mut self.extract_state,
                            DtData::Heartbeat {},
                            position.clone(),
                        )
                        .await?;
                    return Ok((path_versions.clone(), *high_water_zxid));
                }
            }
        }
        Ok((HashMap::new(), 0))
    }

    async fn reconcile(
        &mut self,
        client: &ZkClient,
        path_versions: &mut HashMap<String, (i32, i64)>,
        high_water_zxid: &mut i64,
        source_id: &str,
        scope: ReconcileScope,
        reason: &str,
    ) -> anyhow::Result<()> {
        let old_versions = path_versions.clone();
        let mut current_versions = match scope {
            ReconcileScope::All => HashMap::new(),
            ReconcileScope::Path(_) => old_versions.clone(),
        };
        let mut current_zxid = *high_water_zxid;
        let roots = self.reconcile_roots(&scope);

        if matches!(scope, ReconcileScope::Path(_)) {
            current_versions.retain(|path, _| !Self::scope_contains_path(&scope, path));
        }

        log_info!(
            "ZkExtractor reconciliation start: reason={}, scope={:?}, roots={:?}",
            reason,
            scope,
            roots
        );

        for root in roots {
            if self.base_extractor.shut_down.load(Ordering::Relaxed) {
                break;
            }
            self.scan_node_recursive(
                client,
                &root,
                &old_versions,
                &mut current_versions,
                &mut current_zxid,
                source_id,
            )
            .await?;
        }

        for (path, (old_version, old_mzxid)) in &old_versions {
            if !Self::scope_contains_path(&scope, path) {
                continue;
            }
            if current_versions.contains_key(path) {
                continue;
            }
            if self.filter.filter_path(path) {
                continue;
            }
            let delete_order_millis = chrono::Utc::now().timestamp_millis();
            let delete_zxid = if *old_mzxid > 0 {
                *old_mzxid
            } else {
                delete_order_millis
            };
            let entry = ZkEntry {
                path: path.clone(),
                data: None,
                stat: ZkStat {
                    version: *old_version,
                    mzxid: delete_zxid,
                    mtime: delete_order_millis,
                    ..Default::default()
                },
                ephemeral: false,
                event_type: ZkEventType::Deleted,
                source_id: source_id.to_string(),
                source_order_millis: delete_order_millis,
                source_zxid: delete_zxid,
                order_origin: ZkOrderOrigin::ReconcileObserved,
            };
            let position = self.build_position(&current_versions, current_zxid);
            self.extract_state
                .push_dt_data(&self.base_extractor, DtData::Zk { entry }, position)
                .await?;
        }

        *path_versions = current_versions;
        *high_water_zxid = current_zxid;

        log_info!(
            "ZkExtractor reconciliation complete: reason={}, {} paths, high_water_zxid={}",
            reason,
            path_versions.len(),
            high_water_zxid
        );
        Ok(())
    }

    fn reconcile_roots(&self, scope: &ReconcileScope) -> Vec<String> {
        match scope {
            ReconcileScope::All => self.watch_paths.clone(),
            ReconcileScope::Path(path) => {
                if self.watch_paths.iter().any(|watch_path| {
                    Self::path_contains(watch_path, path) || Self::path_contains(path, watch_path)
                }) {
                    vec![path.clone()]
                } else {
                    Vec::new()
                }
            }
        }
    }

    fn scope_contains_path(scope: &ReconcileScope, path: &str) -> bool {
        match scope {
            ReconcileScope::All => true,
            ReconcileScope::Path(scope_path) => Self::path_contains(scope_path, path),
        }
    }

    fn path_contains(root: &str, path: &str) -> bool {
        root == "/" || path == root || path.starts_with(&format!("{}/", root.trim_end_matches('/')))
    }

    async fn full_scan(
        &mut self,
        client: &ZkClient,
        path_versions: &mut HashMap<String, (i32, i64)>,
        high_water_zxid: &mut i64,
        source_id: &str,
    ) -> anyhow::Result<()> {
        self.reconcile(
            client,
            path_versions,
            high_water_zxid,
            source_id,
            ReconcileScope::All,
            "initial",
        )
        .await
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
            Some((v, old_mzxid)) => (*v >= stat.version && *old_mzxid >= stat.mzxid, true),
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
                source_order_millis: stat.mtime,
                source_zxid: stat.mzxid,
                order_origin: ZkOrderOrigin::SourceStatMtime,
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
                client,
                &child_path,
                old_versions,
                current_versions,
                high_water_zxid,
                source_id,
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
        self.reconcile(
            client,
            path_versions,
            high_water_zxid,
            source_id,
            ReconcileScope::All,
            "periodic",
        )
        .await
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
                .create(
                    marker_path,
                    b"",
                    &CreateMode::Persistent.with_acls(Acls::anyone_all()),
                )
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

                if let Err(e) = hb_client
                    .set_data(marker_path, hb_data.to_string().as_bytes(), None)
                    .await
                {
                    log_warn!("ZkExtractor heartbeat write failed: {}", e);
                }
            }
        });
    }

    async fn start_watchers(
        &self,
        client: &ZkClient,
    ) -> anyhow::Result<mpsc::UnboundedReceiver<ZkWatchEvent>> {
        if self.watch_paths.is_empty() {
            bail!("ZkExtractor requires at least one watch_path");
        }

        let (tx, rx) = mpsc::unbounded_channel();
        for watch_path in self.watch_paths.clone() {
            let mut watcher = client
                .watch(&watch_path, AddWatchMode::PersistentRecursive)
                .await
                .map_err(|e| anyhow::anyhow!("ZK register watcher {} failed: {}", watch_path, e))?;
            let tx = tx.clone();
            let shut_down = self.base_extractor.shut_down.clone();

            log_info!(
                "ZkExtractor registered persistent recursive watcher on {}",
                watch_path
            );
            tokio::spawn(async move {
                loop {
                    if shut_down.load(Ordering::Relaxed) {
                        break;
                    }

                    let event = watcher.changed().await;
                    let terminal = event.event_type == EventType::Session
                        && event.session_state.is_terminated();
                    if tx
                        .send(ZkWatchEvent {
                            watch_path: watch_path.clone(),
                            event,
                        })
                        .is_err()
                    {
                        break;
                    }
                    if terminal {
                        break;
                    }
                }
            });
        }
        drop(tx);
        Ok(rx)
    }

    async fn handle_watch_event(
        &mut self,
        client: &ZkClient,
        path_versions: &mut HashMap<String, (i32, i64)>,
        high_water_zxid: &mut i64,
        source_id: &str,
        watch_event: ZkWatchEvent,
        session_uncertain: &mut bool,
    ) -> anyhow::Result<()> {
        let event = watch_event.event;
        log_info!(
            "ZkExtractor watcher event: watch_path={}, event_type={}, state={}, path={}, zxid={}",
            watch_event.watch_path,
            event.event_type,
            event.session_state,
            event.path,
            event.zxid
        );

        match event.event_type {
            EventType::Session => match event.session_state {
                SessionState::Disconnected => {
                    *session_uncertain = true;
                    log_warn!(
                        "ZkExtractor watcher disconnected; next connected event will force reconciliation"
                    );
                    Ok(())
                }
                SessionState::SyncConnected | SessionState::ConnectedReadOnly => {
                    if *session_uncertain {
                        *session_uncertain = false;
                        self.reconcile(
                            client,
                            path_versions,
                            high_water_zxid,
                            source_id,
                            ReconcileScope::All,
                            "watch-reconnect",
                        )
                        .await?;
                    }
                    Ok(())
                }
                SessionState::AuthFailed | SessionState::Expired | SessionState::Closed => {
                    bail!(
                        "ZkExtractor watcher terminal session state: {}",
                        event.session_state
                    )
                }
            },
            EventType::NodeCreated
            | EventType::NodeDeleted
            | EventType::NodeDataChanged
            | EventType::NodeChildrenChanged => {
                if event.path.is_empty() {
                    return Ok(());
                }
                if self.filter.filter_path(&event.path) {
                    return Ok(());
                }
                if *session_uncertain {
                    log_warn!(
                        "ZkExtractor treats watcher event {} as hint while session is uncertain",
                        event.path
                    );
                    return Ok(());
                }
                self.reconcile(
                    client,
                    path_versions,
                    high_water_zxid,
                    source_id,
                    ReconcileScope::Path(event.path.clone()),
                    "watch-event",
                )
                .await
            }
        }
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

        let mut watch_rx = self.start_watchers(&client).await?;

        self.full_scan(
            &client,
            &mut path_versions,
            &mut high_water_zxid,
            &source_id,
        )
        .await?;

        let mut reconciliation_timer =
            tokio::time::interval(Duration::from_secs(self.scan_interval_secs.max(1)));
        reconciliation_timer.tick().await;
        let mut session_uncertain = false;
        let mut watch_rx_closed = false;

        while !self.base_extractor.shut_down.load(Ordering::Relaxed) {
            tokio::select! {
                maybe_event = watch_rx.recv(), if !watch_rx_closed => {
                    if let Some(watch_event) = maybe_event {
                        self.handle_watch_event(
                            &client,
                            &mut path_versions,
                            &mut high_water_zxid,
                            &source_id,
                            watch_event,
                            &mut session_uncertain,
                        ).await?;
                    } else {
                        log_warn!("ZkExtractor watcher channel closed; forcing periodic reconciliation only");
                        watch_rx_closed = true;
                    }
                }
                _ = reconciliation_timer.tick() => {
                    if self.base_extractor.shut_down.load(Ordering::Relaxed) {
                        break;
                    }
                    self.incremental_rescan(
                        &client,
                        &mut path_versions,
                        &mut high_water_zxid,
                        &source_id,
                    )
                    .await?;
                }
            }
        }

        self.base_extractor
            .wait_task_finish(&mut self.extract_state)
            .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_contains_matches_exact_and_descendants() {
        assert!(ZkExtractor::path_contains("/", "/app"));
        assert!(ZkExtractor::path_contains("/app", "/app"));
        assert!(ZkExtractor::path_contains("/app", "/app/service"));
        assert!(!ZkExtractor::path_contains("/app", "/application"));
    }

    #[test]
    fn scoped_reconcile_only_matches_path_subtree() {
        let scope = ReconcileScope::Path("/app".to_string());

        assert!(ZkExtractor::scope_contains_path(&scope, "/app"));
        assert!(ZkExtractor::scope_contains_path(&scope, "/app/service"));
        assert!(!ZkExtractor::scope_contains_path(&scope, "/config"));
    }
}
