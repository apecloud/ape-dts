use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use anyhow::anyhow;

use super::task_util::TaskUtil;
use dt_common::{
    config::{config_enums::ParallelType, sinker_config::SinkerConfig, task_config::TaskConfig},
    meta::redis::command::key_parser::KeyParser,
    monitor::monitor::Monitor,
    utils::redis_util::RedisUtil,
};
use dt_parallelizer::{
    base_parallelizer::BaseParallelizer, foxlake_parallelizer::FoxlakeParallelizer,
    merge_parallelizer::MergeParallelizer, mongo_merger::MongoMerger,
    partition_parallelizer::PartitionParallelizer, rdb_merger::RdbMerger,
    rdb_partitioner::RdbPartitioner, redis_parallelizer::RedisParallelizer,
    serial_parallelizer::SerialParallelizer, snapshot_parallelizer::SnapshotParallelizer,
    table_parallelizer::TableParallelizer, Merger, Parallelizer,
};

pub struct ParallelizerUtil {}

impl ParallelizerUtil {
    pub async fn create_parallelizer(
        config: &TaskConfig,
        monitor: Arc<Monitor>,
    ) -> anyhow::Result<Box<dyn Parallelizer + Send + Sync>> {
        let parallel_size = config.parallelizer.parallel_size;
        let parallel_type = &config.parallelizer.parallel_type;
        let base_parallelizer = BaseParallelizer {
            popped_data: VecDeque::new(),
            monitor,
        };

        let parallelizer: Box<dyn Parallelizer + Send + Sync> = match parallel_type {
            ParallelType::Snapshot => Box::new(SnapshotParallelizer {
                base_parallelizer,
                parallel_size,
            }),

            ParallelType::RdbPartition => {
                let partitioner = Self::create_rdb_partitioner(config).await?;
                Box::new(PartitionParallelizer {
                    base_parallelizer,
                    partitioner,
                    parallel_size,
                })
            }

            ParallelType::RdbMerge => {
                Self::create_rdb_merge_parallelizer(config, base_parallelizer, parallel_size)
                    .await?
            }

            ParallelType::Serial => Box::new(SerialParallelizer { base_parallelizer }),

            ParallelType::Table => Box::new(TableParallelizer {
                base_parallelizer,
                parallel_size,
            }),

            ParallelType::Mongo => {
                Self::create_mongo_parallelizer(config, base_parallelizer, parallel_size).await?
            }

            ParallelType::Redis => {
                let mut slot_node_map = HashMap::new();
                if let SinkerConfig::Redis { is_cluster, .. } = config.sinker {
                    let mut conn = RedisUtil::create_redis_conn(
                        &config.sinker_basic.url,
                        &config.sinker_basic.connection_auth,
                    )
                    .await?;
                    if is_cluster {
                        let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)?;
                        slot_node_map = RedisUtil::get_slot_address_map(&nodes);
                    }
                }
                Box::new(RedisParallelizer {
                    base_parallelizer,
                    parallel_size,
                    slot_node_map,
                    key_parser: KeyParser::new(),
                    node_sinker_index_map: HashMap::new(),
                })
            }

            ParallelType::Foxlake => {
                let snapshot_parallelizer = SnapshotParallelizer {
                    base_parallelizer,
                    parallel_size,
                };
                Box::new(FoxlakeParallelizer {
                    task_config: config.clone(),
                    snapshot_parallelizer,
                })
            }
        };
        Ok(parallelizer)
    }

    async fn create_rdb_merger(
        config: &TaskConfig,
    ) -> anyhow::Result<Box<dyn Merger + Send + Sync>> {
        let rdb_meta_manager = TaskUtil::create_rdb_meta_manager(config)
            .await?
            .ok_or_else(|| anyhow!("failed to create RDB meta manager for merger target"))?;

        let rdb_merger = RdbMerger { rdb_meta_manager };
        Ok(Box::new(rdb_merger))
    }

    async fn create_mongo_merger() -> anyhow::Result<Box<dyn Merger + Send + Sync>> {
        let mongo_merger = MongoMerger {};
        Ok(Box::new(mongo_merger))
    }

    async fn create_rdb_partitioner(config: &TaskConfig) -> anyhow::Result<RdbPartitioner> {
        let meta_manager = TaskUtil::create_rdb_meta_manager(config)
            .await?
            .ok_or_else(|| anyhow!("failed to create RDB meta manager for partitioner target"))?;
        Ok(RdbPartitioner { meta_manager })
    }

    async fn create_rdb_merge_parallelizer(
        config: &TaskConfig,
        base_parallelizer: BaseParallelizer,
        parallel_size: usize,
    ) -> anyhow::Result<Box<dyn Parallelizer + Send + Sync>> {
        let merger = Self::create_rdb_merger(config).await?;
        if config.checker.is_some() {
            Ok(Box::new(MergeParallelizer::for_check(
                base_parallelizer,
                merger,
                parallel_size,
                config.sinker_basic.clone(),
            )))
        } else {
            Ok(Box::new(MergeParallelizer::for_rdb_merge(
                base_parallelizer,
                merger,
                parallel_size,
                config.sinker_basic.clone(),
                TaskUtil::create_rdb_meta_manager(config).await?,
            )))
        }
    }

    async fn create_mongo_parallelizer(
        config: &TaskConfig,
        base_parallelizer: BaseParallelizer,
        parallel_size: usize,
    ) -> anyhow::Result<Box<dyn Parallelizer + Send + Sync>> {
        if config.checker.is_some() {
            Ok(Box::new(MergeParallelizer::for_check(
                base_parallelizer,
                Self::create_mongo_merger().await?,
                parallel_size,
                config.sinker_basic.clone(),
            )))
        } else {
            Ok(Box::new(MergeParallelizer::for_mongo(
                base_parallelizer,
                parallel_size,
                config.sinker_basic.clone(),
            )))
        }
    }
}
