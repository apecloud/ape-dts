use dt_common::utils::time_util::TimeUtil;
use tokio::task::JoinHandle;
use zookeeper_client as zk;

use crate::test_config_util::TestConfigUtil;

use super::zk_test_runner::ZkTestRunner;

const SHADOW_PREFIX: &str = "/__ape_dts_shadow";
const MARKER_PATH: &str = "/__ape_dts_marker";

pub struct ZkCycleTestRunner {}

impl ZkCycleTestRunner {
    pub async fn run_cycle_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let sub_paths = TestConfigUtil::get_absolute_sub_dir(test_dir);
        assert!(
            !sub_paths.is_empty(),
            "cycle test requires at least one sub-directory in {}",
            test_dir
        );

        let mut handlers: Vec<JoinHandle<()>> = vec![];
        let mut runners: Vec<(String, ZkTestRunner)> = vec![];

        for sub_path in &sub_paths {
            let runner = ZkTestRunner::new(format!("{}/{}", test_dir, sub_path.1).as_str())
                .await
                .unwrap();
            runners.push((sub_path.1.clone(), runner));
        }

        let first_runner = &runners[0].1;
        let src = zk::Client::connect(&first_runner.src_url).await.unwrap();
        let dst = zk::Client::connect(&first_runner.dst_url).await.unwrap();

        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        ZkCycleTestRunner::ensure_path(&src, "/app", &options).await;
        ZkCycleTestRunner::ensure_path(&dst, "/app", &options).await;
        ZkCycleTestRunner::delete_children(&src, "/app").await;
        ZkCycleTestRunner::delete_children(&dst, "/app").await;

        ZkCycleTestRunner::delete_children(&src, SHADOW_PREFIX).await;
        ZkCycleTestRunner::delete_node(&src, SHADOW_PREFIX).await;
        ZkCycleTestRunner::delete_children(&dst, SHADOW_PREFIX).await;
        ZkCycleTestRunner::delete_node(&dst, SHADOW_PREFIX).await;
        ZkCycleTestRunner::delete_node(&src, MARKER_PATH).await;
        ZkCycleTestRunner::delete_node(&dst, MARKER_PATH).await;

        for (_, runner) in &runners {
            handlers.push(runner.base.spawn_task().await.unwrap());
            TimeUtil::sleep_millis(start_millis).await;
        }

        src.create("/app/from-node1", b"hello-from-1", &options)
            .await
            .unwrap();
        dst.create("/app/from-node2", b"hello-from-2", &options)
            .await
            .unwrap();

        TimeUtil::sleep_millis(parse_millis).await;

        // data sync assertions
        let (src_data_1, _) = src.get_data("/app/from-node1").await.unwrap();
        let (dst_data_1, _) = dst.get_data("/app/from-node1").await.unwrap();
        assert_eq!(src_data_1, dst_data_1, "from-node1 data mismatch");

        let (src_data_2, _) = src.get_data("/app/from-node2").await.unwrap();
        let (dst_data_2, _) = dst.get_data("/app/from-node2").await.unwrap();
        assert_eq!(src_data_2, dst_data_2, "from-node2 data mismatch");

        // shadow metadata assertions — verify shadow znodes exist on both sides
        Self::assert_shadow_exists(&dst, "/app/from-node1").await;
        Self::assert_shadow_exists(&src, "/app/from-node2").await;

        // marker assertions — verify data markers written on both sides
        Self::assert_marker_exists(&src).await;
        Self::assert_marker_exists(&dst).await;

        // anti-loop stability — sleep again and verify data hasn't changed (no bounce-back loop)
        let snapshot_src_1 = src.get_data("/app/from-node1").await.unwrap();
        let snapshot_dst_2 = dst.get_data("/app/from-node2").await.unwrap();
        TimeUtil::sleep_millis(parse_millis / 2).await;
        let check_src_1 = src.get_data("/app/from-node1").await.unwrap();
        let check_dst_2 = dst.get_data("/app/from-node2").await.unwrap();
        assert_eq!(
            snapshot_src_1.1.version, check_src_1.1.version,
            "anti-loop: /app/from-node1 on src was modified again (version changed {} -> {})",
            snapshot_src_1.1.version, check_src_1.1.version
        );
        assert_eq!(
            snapshot_dst_2.1.version, check_dst_2.1.version,
            "anti-loop: /app/from-node2 on dst was modified again (version changed {} -> {})",
            snapshot_dst_2.1.version, check_dst_2.1.version
        );

        for handler in handlers {
            handler.abort();
            while !handler.is_finished() {
                TimeUtil::sleep_millis(1).await;
            }
        }
    }

    async fn assert_shadow_exists(client: &zk::Client, data_path: &str) {
        let shadow_path = format!("{}{}", SHADOW_PREFIX, data_path);
        let (data, _) = client
            .get_data(&shadow_path)
            .await
            .unwrap_or_else(|e| panic!("shadow znode {} should exist: {}", shadow_path, e));
        let json: serde_json::Value = serde_json::from_slice(&data)
            .unwrap_or_else(|e| panic!("shadow {} invalid JSON: {}", shadow_path, e));
        assert!(
            json.get("source_id")
                .and_then(|v| v.as_str())
                .map_or(false, |v| !v.is_empty()),
            "shadow {} missing or empty source_id",
            shadow_path
        );
        assert!(
            json.get("source_order_millis")
                .and_then(|v| v.as_i64())
                .map_or(false, |v| v > 0),
            "shadow {} missing or zero source_order_millis",
            shadow_path
        );
        assert!(
            json.get("version").and_then(|v| v.as_i64()).is_some(),
            "shadow {} missing or invalid version",
            shadow_path
        );
        assert_eq!(
            json.get("deleted").and_then(|v| v.as_bool()),
            Some(false),
            "shadow {} should have deleted=false",
            shadow_path
        );
    }

    async fn assert_marker_exists(client: &zk::Client) {
        let (data, _) = client
            .get_data(MARKER_PATH)
            .await
            .unwrap_or_else(|e| panic!("marker {} should exist: {}", MARKER_PATH, e));
        let json: serde_json::Value = serde_json::from_slice(&data)
            .unwrap_or_else(|e| panic!("marker {} invalid JSON: {}", MARKER_PATH, e));
        assert!(
            json.get("source_id")
                .and_then(|v| v.as_str())
                .map_or(false, |v| !v.is_empty()),
            "marker {} missing or empty source_id",
            MARKER_PATH
        );
    }

    async fn delete_node(client: &zk::Client, path: &str) {
        match client.delete(path, None).await {
            Ok(()) => {}
            Err(ref e) if matches!(e, zk::Error::NoNode) => {}
            Err(e) => panic!("delete_node {} failed: {}", path, e),
        }
    }

    async fn ensure_path(client: &zk::Client, path: &str, options: &zk::CreateOptions<'_>) {
        match client.create(path, &[], options).await {
            Ok(_) => {}
            Err(ref e) if matches!(e, zk::Error::NodeExists) => {}
            Err(e) => panic!("ensure_path {} failed: {}", path, e),
        }
    }

    fn delete_children<'a>(
        client: &'a zk::Client,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let children = match client.get_children(path).await {
                Ok((children, _)) => children,
                Err(ref e) if matches!(e, zk::Error::NoNode) => return,
                Err(e) => panic!("get_children {} failed: {}", path, e),
            };
            for child in children {
                let child_path = format!("{}/{}", path, child);
                Self::delete_children(client, &child_path).await;
                match client.delete(&child_path, None).await {
                    Ok(()) => {}
                    Err(ref e) if matches!(e, zk::Error::NoNode) => {}
                    Err(e) => panic!("delete {} failed: {}", child_path, e),
                }
            }
        })
    }
}
