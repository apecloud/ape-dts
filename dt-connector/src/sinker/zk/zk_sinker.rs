use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use tokio::sync::RwLock;
use zookeeper_client as zk;

use dt_common::config::config_enums::ConflictPolicyEnum;
use dt_common::log_info;
use dt_common::log_warn;
use dt_common::meta::dt_data::{DtData, DtItem};
use dt_common::meta::zk::zk_entry::{ZkEntry, ZkOrderOrigin};
use dt_common::meta::zk::zk_event_type::ZkEventType;

use crate::data_marker::DataMarker;
use crate::sinker::base_sinker::BaseSinker;
use crate::zk_router::ZkRouter;
use crate::Sinker;

pub struct ZkSinker {
    pub url: String,
    pub batch_size: usize,
    pub create_if_not_exists: bool,
    pub sync_ephemeral_as_persistent: bool,
    pub conflict_policy: ConflictPolicyEnum,
    pub router: ZkRouter,
    pub base_sinker: BaseSinker,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub client: Option<zk::Client>,
}

#[async_trait]
impl Sinker for ZkSinker {
    async fn sink_raw(&mut self, data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let client = self.get_or_connect().await?;
        self.serial_sink_raw(&client, data).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.client = None;
        Ok(())
    }
}

impl ZkSinker {
    async fn get_or_connect(&mut self) -> anyhow::Result<zk::Client> {
        if let Some(client) = &self.client {
            return Ok(client.clone());
        }
        let client = zk::Client::connect(&self.url)
            .await
            .map_err(|e| anyhow::anyhow!("ZK connect to {} failed: {}", self.url, e))?;
        log_info!("ZkSinker connected to {}", self.url);
        self.client = Some(client.clone());
        Ok(client)
    }

    async fn serial_sink_raw(
        &mut self,
        client: &zk::Client,
        data: Vec<DtItem>,
    ) -> anyhow::Result<()> {
        let mut data_size = 0u64;
        let mut record_count = 0u64;

        for dt_item in data.iter() {
            if let DtData::Zk { entry } = &dt_item.dt_data {
                data_size += entry.get_data_size();
                let routed_path = self.router.route_path(&entry.path);

                if self.conflict_policy == ConflictPolicyEnum::LastWriteWins
                    && self.should_skip_by_lww(client, &routed_path, entry).await?
                {
                    continue;
                }

                self.apply_zk_event(client, &routed_path, entry).await?;
                record_count += 1;
            }
        }

        self.write_data_marker(client).await?;

        self.base_sinker
            .update_serial_monitor(record_count, data_size)
            .await
    }

    async fn should_skip_by_lww(
        &self,
        client: &zk::Client,
        path: &str,
        entry: &ZkEntry,
    ) -> anyhow::Result<bool> {
        let shadow = shadow_path(path);
        if let Ok((data, _)) = client.get_data(&shadow).await {
            if let Ok(shadow_meta) = serde_json::from_slice::<ZkShadowMeta>(&data) {
                return Ok(Self::should_skip_by_shadow_meta(&shadow_meta, entry));
            }
        }
        Ok(false)
    }

    fn should_skip_by_shadow_meta(shadow: &ZkShadowMeta, entry: &ZkEntry) -> bool {
        let shadow_order = shadow.source_order_millis();
        let entry_order = entry.source_order_millis();

        shadow_order > entry_order
            || (shadow_order == entry_order && shadow.source_id.as_str() > entry.source_id.as_str())
    }

    async fn apply_zk_event(
        &self,
        client: &zk::Client,
        path: &str,
        entry: &ZkEntry,
    ) -> anyhow::Result<()> {
        let data = entry.data.as_deref().unwrap_or(&[]);

        if matches!(
            entry.event_type,
            ZkEventType::Created | ZkEventType::Updated
        ) {
            if let Ok((existing_data, _)) = client.get_data(path).await {
                if existing_data == data {
                    self.write_shadow(client, path, entry).await?;
                    return Ok(());
                }
            }
        }

        match entry.event_type {
            ZkEventType::Created => {
                let options = self.create_options(entry);

                if self.create_if_not_exists {
                    self.ensure_parent_paths(client, path).await?;
                }

                match client.create(path, data, &options).await {
                    Ok(_) => {}
                    Err(ref e) if is_node_exists(e) => {
                        client.set_data(path, data, None).await.map_err(|e| {
                            anyhow::anyhow!("ZK set_data (upsert) {} failed: {}", path, e)
                        })?;
                    }
                    Err(e) => bail!("ZK create {} failed: {}", path, e),
                }
                self.write_shadow(client, path, entry).await?;
                Ok(())
            }

            ZkEventType::Updated => {
                match client.set_data(path, data, None).await {
                    Ok(_) => {}
                    Err(ref e) if is_no_node(e) && self.create_if_not_exists => {
                        self.ensure_parent_paths(client, path).await?;
                        let options = self.create_options(entry);
                        client.create(path, data, &options).await.map_err(|e| {
                            anyhow::anyhow!("ZK create (auto) {} failed: {}", path, e)
                        })?;
                    }
                    Err(e) => bail!("ZK set_data {} failed: {}", path, e),
                }
                self.write_shadow(client, path, entry).await?;
                Ok(())
            }

            ZkEventType::Deleted => {
                match client.delete(path, None).await {
                    Ok(()) => {}
                    Err(ref e) if is_no_node(e) => {}
                    Err(ref e) if is_not_empty(e) => {
                        log_warn!(
                            "ZK delete {} skipped: node has children (may have new data from peer)",
                            path
                        );
                        return Ok(());
                    }
                    Err(e) => bail!("ZK delete {} failed: {}", path, e),
                }
                self.write_tombstone_shadow(client, path, entry).await?;
                Ok(())
            }

            ZkEventType::ChildrenChanged => Ok(()),
        }
    }

    fn create_options(&self, entry: &ZkEntry) -> zk::CreateOptions<'static> {
        let mode = if entry.ephemeral && !self.sync_ephemeral_as_persistent {
            zk::CreateMode::Ephemeral
        } else {
            zk::CreateMode::Persistent
        };
        mode.with_acls(zk::Acls::anyone_all())
    }

    fn persistent_options(&self) -> zk::CreateOptions<'static> {
        zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all())
    }

    async fn ensure_parent_paths(&self, client: &zk::Client, path: &str) -> anyhow::Result<()> {
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.len() <= 1 {
            return Ok(());
        }
        let options = self.persistent_options();
        let mut current = String::new();
        for part in &parts[..parts.len() - 1] {
            current.push('/');
            current.push_str(part);
            match client.create(&current, &[], &options).await {
                Ok(_) => {}
                Err(ref e) if is_node_exists(e) => {}
                Err(e) => bail!("ZK create parent {} failed: {}", current, e),
            }
        }
        Ok(())
    }

    async fn write_shadow(
        &self,
        client: &zk::Client,
        path: &str,
        entry: &ZkEntry,
    ) -> anyhow::Result<()> {
        let shadow = shadow_path(path);
        let shadow_data = Self::shadow_data(entry, false)?;
        match client.set_data(&shadow, &shadow_data, None).await {
            Ok(_) => {}
            Err(ref e) if is_no_node(e) => {
                self.ensure_parent_paths(client, &shadow).await?;
                let options = self.persistent_options();
                match client.create(&shadow, &shadow_data, &options).await {
                    Ok(_) => {}
                    Err(ref e) if is_node_exists(e) => {
                        client.set_data(&shadow, &shadow_data, None).await.ok();
                    }
                    Err(e) => bail!("ZK create shadow {} failed: {}", shadow, e),
                }
            }
            Err(e) => bail!("ZK write shadow {} failed: {}", shadow, e),
        }
        Ok(())
    }

    async fn write_tombstone_shadow(
        &self,
        client: &zk::Client,
        path: &str,
        entry: &ZkEntry,
    ) -> anyhow::Result<()> {
        let shadow = shadow_path(path);
        let shadow_data = Self::shadow_data(entry, true)?;
        match client.set_data(&shadow, &shadow_data, None).await {
            Ok(_) => {}
            Err(ref e) if is_no_node(e) => {
                self.ensure_parent_paths(client, &shadow).await?;
                let options = self.persistent_options();
                match client.create(&shadow, &shadow_data, &options).await {
                    Ok(_) => {}
                    Err(ref e) if is_node_exists(e) => {
                        client.set_data(&shadow, &shadow_data, None).await.ok();
                    }
                    Err(e) => bail!("ZK create tombstone shadow {} failed: {}", shadow, e),
                }
            }
            Err(e) => bail!("ZK write tombstone shadow {} failed: {}", shadow, e),
        }
        Ok(())
    }

    fn shadow_data(entry: &ZkEntry, deleted: bool) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(&ZkShadowMeta::from_entry(
            entry, deleted,
        ))?)
    }

    async fn write_data_marker(&self, client: &zk::Client) -> anyhow::Result<()> {
        if let Some(data_marker) = &self.data_marker {
            let data_marker = data_marker.read().await;
            let marker_data = serde_json::to_vec(&serde_json::json!({
                "source_id": data_marker.src_node
            }))?;

            match client
                .set_data(&data_marker.marker, &marker_data, None)
                .await
            {
                Ok(_) => {}
                Err(ref e) if is_no_node(e) => {
                    self.ensure_parent_paths(client, &data_marker.marker)
                        .await?;
                    let options = self.persistent_options();
                    client
                        .create(&data_marker.marker, &marker_data, &options)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("ZK create marker {} failed: {}", data_marker.marker, e)
                        })?;
                }
                Err(e) => bail!("ZK write marker {} failed: {}", data_marker.marker, e),
            }
        }
        Ok(())
    }
}

const SHADOW_PREFIX: &str = "/__ape_dts_shadow";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct ZkShadowMeta {
    pub source_id: String,
    #[serde(default)]
    pub source_order_millis: i64,
    #[serde(default)]
    pub source_mtime: i64,
    #[serde(default)]
    pub source_zxid: Option<i64>,
    #[serde(default)]
    pub version: i32,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub order_origin: ZkOrderOrigin,
}

impl ZkShadowMeta {
    fn from_entry(entry: &ZkEntry, deleted: bool) -> Self {
        Self {
            source_id: entry.source_id.clone(),
            source_order_millis: entry.source_order_millis(),
            source_mtime: entry.stat.mtime,
            source_zxid: (entry.source_zxid != 0).then_some(entry.source_zxid),
            version: entry.stat.version,
            deleted,
            order_origin: entry.order_origin.clone(),
        }
    }

    fn source_order_millis(&self) -> i64 {
        if self.source_order_millis != 0 {
            self.source_order_millis
        } else {
            self.source_mtime
        }
    }
}

fn shadow_path(path: &str) -> String {
    format!("{}{}", SHADOW_PREFIX, path)
}

fn is_node_exists(e: &zk::Error) -> bool {
    matches!(e, zk::Error::NodeExists)
}

fn is_no_node(e: &zk::Error) -> bool {
    matches!(e, zk::Error::NoNode)
}

fn is_not_empty(e: &zk::Error) -> bool {
    matches!(e, zk::Error::NotEmpty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::meta::zk::zk_stat::ZkStat;

    fn entry(order: i64, source_id: &str, event_type: ZkEventType) -> ZkEntry {
        ZkEntry {
            path: "/app/service".to_string(),
            data: Some(b"value".to_vec()),
            stat: ZkStat {
                version: 1,
                mtime: order,
                mzxid: order,
                ..Default::default()
            },
            ephemeral: false,
            event_type,
            source_id: source_id.to_string(),
            source_order_millis: order,
            source_zxid: order,
            order_origin: ZkOrderOrigin::SourceStatMtime,
        }
    }

    #[test]
    fn newer_tombstone_skips_older_upsert() {
        let old_upsert = entry(100, "node-a", ZkEventType::Updated);
        let tombstone = ZkShadowMeta {
            source_id: "node-b".to_string(),
            source_order_millis: 200,
            source_mtime: 200,
            source_zxid: Some(200),
            version: 1,
            deleted: true,
            order_origin: ZkOrderOrigin::ReconcileObserved,
        };

        assert!(ZkSinker::should_skip_by_shadow_meta(
            &tombstone,
            &old_upsert
        ));
    }

    #[test]
    fn newer_upsert_can_pass_older_tombstone() {
        let new_upsert = entry(300, "node-a", ZkEventType::Updated);
        let tombstone = ZkShadowMeta {
            source_id: "node-b".to_string(),
            source_order_millis: 200,
            source_mtime: 200,
            source_zxid: Some(200),
            version: 1,
            deleted: true,
            order_origin: ZkOrderOrigin::ReconcileObserved,
        };

        assert!(!ZkSinker::should_skip_by_shadow_meta(
            &tombstone,
            &new_upsert
        ));
    }

    #[test]
    fn equal_order_uses_source_id_tie_break() {
        let entry_from_a = entry(100, "node-a", ZkEventType::Updated);
        let shadow_from_b = ZkShadowMeta {
            source_id: "node-b".to_string(),
            source_order_millis: 100,
            source_mtime: 100,
            source_zxid: Some(100),
            version: 1,
            deleted: false,
            order_origin: ZkOrderOrigin::SourceStatMtime,
        };

        assert!(ZkSinker::should_skip_by_shadow_meta(
            &shadow_from_b,
            &entry_from_a
        ));
    }

    #[test]
    fn delete_shadow_data_is_tombstone() {
        let delete = ZkEntry {
            data: None,
            event_type: ZkEventType::Deleted,
            order_origin: ZkOrderOrigin::ReconcileObserved,
            ..entry(400, "node-a", ZkEventType::Deleted)
        };

        let bytes = ZkSinker::shadow_data(&delete, true).unwrap();
        let shadow: ZkShadowMeta = serde_json::from_slice(&bytes).unwrap();

        assert!(shadow.deleted);
        assert_eq!(shadow.source_order_millis, 400);
        assert_eq!(shadow.source_mtime, 400);
        assert_eq!(shadow.source_zxid, Some(400));
        assert_eq!(shadow.order_origin, ZkOrderOrigin::ReconcileObserved);
    }

    #[test]
    fn old_shadow_json_falls_back_to_source_mtime() {
        let old_json = br#"{"source_id":"node-b","source_mtime":200,"version":1}"#;
        let shadow: ZkShadowMeta = serde_json::from_slice(old_json).unwrap();
        let old_upsert = entry(100, "node-a", ZkEventType::Updated);

        assert_eq!(shadow.source_order_millis(), 200);
        assert!(ZkSinker::should_skip_by_shadow_meta(&shadow, &old_upsert));
    }
}
