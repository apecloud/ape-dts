use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use tokio::sync::RwLock;
use zookeeper_client as zk;

use dt_common::config::config_enums::ConflictPolicyEnum;
use dt_common::log_info;
use dt_common::log_warn;
use dt_common::meta::dt_data::{DtData, DtItem};
use dt_common::meta::zk::zk_entry::ZkEntry;
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
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                let source_mtime = val
                    .get("source_mtime")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let source_id = val.get("source_id").and_then(|v| v.as_str()).unwrap_or("");
                if source_mtime > entry.stat.mtime {
                    return Ok(true);
                }
                if source_mtime == entry.stat.mtime && *source_id > *entry.source_id {
                    return Ok(true);
                }
            }
        }
        Ok(false)
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
                self.delete_shadow(client, path).await?;
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
        let shadow_data = serde_json::to_vec(&serde_json::json!({
            "source_id": entry.source_id,
            "source_mtime": entry.stat.mtime,
            "version": entry.stat.version,
        }))?;
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

    async fn delete_shadow(&self, client: &zk::Client, path: &str) -> anyhow::Result<()> {
        let shadow = shadow_path(path);
        match client.delete(&shadow, None).await {
            Ok(()) => {}
            Err(ref e) if is_no_node(e) => {}
            Err(e) => bail!("ZK delete shadow {} failed: {}", shadow, e),
        }
        Ok(())
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
