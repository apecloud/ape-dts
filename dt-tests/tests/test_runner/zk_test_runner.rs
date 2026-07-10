use anyhow::{bail, ensure};
use dt_common::{
    config::{
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    utils::time_util::TimeUtil,
};
use zookeeper_client as zk;

use super::base_test_runner::BaseTestRunner;

const SHADOW_PREFIX: &str = "/__ape_dts_shadow";
const POLL_INTERVAL_MILLIS: u64 = 200;

pub struct ZkTestRunner {
    pub base: BaseTestRunner,
    pub src_url: String,
    pub dst_url: String,
}

impl ZkTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = BaseTestRunner::new(relative_test_dir).await?;
        let config = TaskConfig::new(&base.task_config_file)?;

        let src_url = match &config.extractor {
            ExtractorConfig::Zk { url, .. } => url.clone(),
            _ => bail!(Error::ConfigError("expected Zk extractor config".into())),
        };

        let dst_url = match &config.sinker {
            SinkerConfig::Zk { url, .. } => url.clone(),
            _ => bail!(Error::ConfigError("expected Zk sinker config".into())),
        };

        Ok(Self {
            base,
            src_url,
            dst_url,
        })
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> anyhow::Result<()> {
        let src = zk::Client::connect(&self.src_url)
            .await
            .map_err(|e| anyhow::anyhow!("connect src ZK failed: {}", e))?;
        let dst = zk::Client::connect(&self.dst_url)
            .await
            .map_err(|e| anyhow::anyhow!("connect dst ZK failed: {}", e))?;

        self.prepare_znodes(&src, &dst).await?;

        let task = self.base.spawn_task().await?;
        let test_result = async {
            TimeUtil::sleep_millis(start_millis).await;

            self.execute_live_test_operations(&src).await?;
            Self::wait_for_live_sync(&dst, "/app/svc-gamma", b"gamma-v1", parse_millis).await?;

            src.delete("/app/svc-gamma", None)
                .await
                .map_err(|e| anyhow::anyhow!("delete /app/svc-gamma failed: {}", e))?;
            Self::wait_for_tombstone(&dst, "/app/svc-gamma", parse_millis).await?;

            Self::compare_znodes(&src, &dst, "/app").await?;
            Self::assert_shadow_metadata(&dst, "/app/svc-alpha").await?;
            Self::assert_shadow_metadata(&dst, "/app/svc-beta").await
        }
        .await;

        let abort_result = self.base.abort_task(&task).await;
        match test_result {
            Ok(()) => abort_result,
            Err(error) => {
                let _ = abort_result;
                Err(error)
            }
        }
    }

    async fn prepare_znodes(&self, src: &zk::Client, dst: &zk::Client) -> anyhow::Result<()> {
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());

        Self::ensure_path(src, "/app", &options).await?;
        Self::ensure_path(dst, "/app", &options).await?;

        Self::delete_children(src, "/app").await?;
        Self::delete_children(dst, "/app").await?;

        Self::delete_children(dst, SHADOW_PREFIX).await?;
        Self::delete_node(dst, SHADOW_PREFIX).await?;

        Ok(())
    }

    async fn execute_live_test_operations(&self, src: &zk::Client) -> anyhow::Result<()> {
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());

        src.create("/app/svc-alpha", b"alpha-v1", &options)
            .await
            .map_err(|e| anyhow::anyhow!("create /app/svc-alpha failed: {}", e))?;

        src.create("/app/svc-beta", b"beta-v1", &options)
            .await
            .map_err(|e| anyhow::anyhow!("create /app/svc-beta failed: {}", e))?;

        src.set_data("/app/svc-alpha", b"alpha-v2", None)
            .await
            .map_err(|e| anyhow::anyhow!("update /app/svc-alpha failed: {}", e))?;

        src.create("/app/svc-gamma", b"gamma-v1", &options)
            .await
            .map_err(|e| anyhow::anyhow!("create /app/svc-gamma failed: {}", e))?;

        Ok(())
    }

    async fn wait_for_live_sync(
        client: &zk::Client,
        data_path: &str,
        expected_data: &[u8],
        timeout_millis: u64,
    ) -> anyhow::Result<()> {
        let shadow_path = format!("{}{}", SHADOW_PREFIX, data_path);
        let deadline =
            tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_millis);

        loop {
            let (data_matches, data_observation) = match client.get_data(data_path).await {
                Ok((data, _)) => (
                    data == expected_data,
                    format!("data={:?}", String::from_utf8_lossy(&data)),
                ),
                Err(ref e) if matches!(e, zk::Error::NoNode) => {
                    (false, String::from("data=missing"))
                }
                Err(e) => bail!("get live data {} failed: {}", data_path, e),
            };

            let (shadow_matches, shadow_observation) = match client.get_data(&shadow_path).await {
                Ok((data, _)) => {
                    let json: serde_json::Value = serde_json::from_slice(&data).map_err(|e| {
                        anyhow::anyhow!("shadow {} invalid JSON: {}", shadow_path, e)
                    })?;
                    (Self::live_shadow_matches(&json), format!("shadow={}", json))
                }
                Err(ref e) if matches!(e, zk::Error::NoNode) => {
                    (false, String::from("shadow=missing"))
                }
                Err(e) => bail!("get live shadow {} failed: {}", shadow_path, e),
            };

            if data_matches && shadow_matches {
                return Ok(());
            }
            let last_observation = format!("{}, {}", data_observation, shadow_observation);
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "live sync {} did not converge within {}ms; last observation: {}",
                    data_path,
                    timeout_millis,
                    last_observation
                );
            }
            TimeUtil::sleep_millis(POLL_INTERVAL_MILLIS).await;
        }
    }

    async fn wait_for_tombstone(
        client: &zk::Client,
        data_path: &str,
        timeout_millis: u64,
    ) -> anyhow::Result<()> {
        let shadow_path = format!("{}{}", SHADOW_PREFIX, data_path);
        let deadline =
            tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_millis);

        loop {
            let (data_absent, data_observation) = match client.get_data(data_path).await {
                Ok((data, _)) => (false, format!("data={:?}", String::from_utf8_lossy(&data))),
                Err(ref e) if matches!(e, zk::Error::NoNode) => {
                    (true, String::from("data=missing"))
                }
                Err(e) => bail!("get deleted data {} failed: {}", data_path, e),
            };

            let (shadow_matches, shadow_observation) = match client.get_data(&shadow_path).await {
                Ok((data, _)) => {
                    let json: serde_json::Value = serde_json::from_slice(&data).map_err(|e| {
                        anyhow::anyhow!("tombstone shadow {} invalid JSON: {}", shadow_path, e)
                    })?;
                    (
                        Self::tombstone_shadow_matches(&json),
                        format!("shadow={}", json),
                    )
                }
                Err(ref e) if matches!(e, zk::Error::NoNode) => {
                    (false, String::from("shadow=missing"))
                }
                Err(e) => bail!("get tombstone shadow {} failed: {}", shadow_path, e),
            };

            if data_absent && shadow_matches {
                return Ok(());
            }
            let last_observation = format!("{}, {}", data_observation, shadow_observation);
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "delete sync {} did not converge within {}ms; last observation: {}",
                    data_path,
                    timeout_millis,
                    last_observation
                );
            }
            TimeUtil::sleep_millis(POLL_INTERVAL_MILLIS).await;
        }
    }

    fn live_shadow_matches(json: &serde_json::Value) -> bool {
        json.get("source_id")
            .and_then(|v| v.as_str())
            .is_some_and(|v| !v.is_empty())
            && json
                .get("source_order_millis")
                .and_then(|v| v.as_i64())
                .is_some_and(|v| v > 0)
            && json.get("version").and_then(|v| v.as_i64()).is_some()
            && json.get("deleted").and_then(|v| v.as_bool()) == Some(false)
    }

    fn tombstone_shadow_matches(json: &serde_json::Value) -> bool {
        json.get("source_id")
            .and_then(|v| v.as_str())
            .is_some_and(|v| !v.is_empty())
            && json.get("deleted").and_then(|v| v.as_bool()) == Some(true)
            && json
                .get("source_order_millis")
                .and_then(|v| v.as_i64())
                .is_some_and(|v| v > 0)
            && json
                .get("source_zxid")
                .and_then(|v| v.as_i64())
                .is_some_and(|v| v > 0)
    }

    fn compare_znodes<'a>(
        src: &'a zk::Client,
        dst: &'a zk::Client,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>> {
        Box::pin(async move {
            let src_children = match src.get_children(path).await {
                Ok((children, _)) => children,
                Err(ref e) if matches!(e, zk::Error::NoNode) => vec![],
                Err(e) => bail!("get_children src {} failed: {}", path, e),
            };

            let dst_children = match dst.get_children(path).await {
                Ok((children, _)) => children,
                Err(ref e) if matches!(e, zk::Error::NoNode) => vec![],
                Err(e) => bail!("get_children dst {} failed: {}", path, e),
            };

            let mut src_sorted = src_children.clone();
            src_sorted.sort();
            let mut dst_sorted = dst_children.clone();
            dst_sorted.sort();

            let src_filtered: Vec<&String> = src_sorted
                .iter()
                .filter(|c| !c.starts_with("__ape_dts_"))
                .collect();
            let dst_filtered: Vec<&String> = dst_sorted
                .iter()
                .filter(|c| !c.starts_with("__ape_dts_"))
                .collect();

            ensure!(
                src_filtered == dst_filtered,
                "children mismatch at {}: src={:?}, dst={:?}",
                path,
                src_filtered,
                dst_filtered
            );

            for child in &src_filtered {
                let child_path = format!("{}/{}", path, child);

                let (src_data, _) = src
                    .get_data(&child_path)
                    .await
                    .map_err(|e| anyhow::anyhow!("get_data src {} failed: {}", child_path, e))?;
                let (dst_data, _) = dst
                    .get_data(&child_path)
                    .await
                    .map_err(|e| anyhow::anyhow!("get_data dst {} failed: {}", child_path, e))?;

                ensure!(
                    src_data == dst_data,
                    "data mismatch at {}: src={:?}, dst={:?}",
                    child_path,
                    String::from_utf8_lossy(&src_data),
                    String::from_utf8_lossy(&dst_data)
                );

                Self::compare_znodes(src, dst, &child_path).await?;
            }

            Ok(())
        })
    }

    async fn assert_shadow_metadata(client: &zk::Client, data_path: &str) -> anyhow::Result<()> {
        let shadow_path = format!("{}{}", SHADOW_PREFIX, data_path);
        let (data, _) = client
            .get_data(&shadow_path)
            .await
            .map_err(|e| anyhow::anyhow!("shadow znode {} should exist: {}", shadow_path, e))?;
        let json: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| anyhow::anyhow!("shadow {} invalid JSON: {}", shadow_path, e))?;
        ensure!(
            Self::live_shadow_matches(&json),
            "shadow {} missing required live metadata: {}",
            shadow_path,
            json
        );
        Ok(())
    }

    async fn delete_node(client: &zk::Client, path: &str) -> anyhow::Result<()> {
        match client.delete(path, None).await {
            Ok(()) => Ok(()),
            Err(ref e) if matches!(e, zk::Error::NoNode) => Ok(()),
            Err(e) => bail!("delete_node {} failed: {}", path, e),
        }
    }

    async fn ensure_path(
        client: &zk::Client,
        path: &str,
        options: &zk::CreateOptions<'_>,
    ) -> anyhow::Result<()> {
        match client.create(path, &[], options).await {
            Ok(_) => Ok(()),
            Err(ref e) if matches!(e, zk::Error::NodeExists) => Ok(()),
            Err(e) => bail!("ensure_path {} failed: {}", path, e),
        }
    }

    fn delete_children<'a>(
        client: &'a zk::Client,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>> {
        Box::pin(async move {
            let children = match client.get_children(path).await {
                Ok((children, _)) => children,
                Err(ref e) if matches!(e, zk::Error::NoNode) => return Ok(()),
                Err(e) => bail!("get_children {} failed: {}", path, e),
            };

            for child in children {
                let child_path = format!("{}/{}", path, child);
                Self::delete_children(client, &child_path).await?;
                match client.delete(&child_path, None).await {
                    Ok(()) => {}
                    Err(ref e) if matches!(e, zk::Error::NoNode) => {}
                    Err(e) => bail!("delete {} failed: {}", child_path, e),
                }
            }
            Ok(())
        })
    }
}
