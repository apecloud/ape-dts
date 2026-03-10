use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use ratelimit::Ratelimiter;

use super::task_util::TaskUtil;
use dt_common::{
    config::{
        config_enums::{DbType, ParallelType},
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    meta::redis::command::key_parser::KeyParser,
    monitor::monitor::Monitor,
    utils::redis_util::RedisUtil,
};
use dt_parallelizer::{
    base_parallelizer::BaseParallelizer,
    foxlake_parallelizer::FoxlakeParallelizer,
    merge_parallelizer::{MergeParallelizer, MergeSinkMode},
    mongo_merger::MongoMerger,
    partition_parallelizer::PartitionParallelizer,
    rdb_merger::RdbMerger,
    rdb_partitioner::RdbPartitioner,
    redis_parallelizer::RedisParallelizer,
    serial_parallelizer::SerialParallelizer,
    snapshot_parallelizer::SnapshotParallelizer,
    table_parallelizer::TableParallelizer,
    Merger, Parallelizer,
};

pub struct ParallelizerUtil;

impl ParallelizerUtil {
    pub async fn create_parallelizer(
        config: &TaskConfig,
        monitor: Arc<Monitor>,
        rps_limiter: Option<Ratelimiter>,
    ) -> anyhow::Result<Box<dyn Parallelizer + Send + Sync>> {
        let parallel_size = config.parallelizer.parallel_size;
        let base_parallelizer = |monitor, rps_limiter| BaseParallelizer {
            popped_data: VecDeque::new(),
            monitor,
            rps_limiter,
        };
        Self::build_parallelizer(
            config,
            parallel_size,
            base_parallelizer(monitor, rps_limiter),
        )
        .await
    }

    async fn build_parallelizer(
        config: &TaskConfig,
        parallel_size: usize,
        base_parallelizer: BaseParallelizer,
    ) -> anyhow::Result<Box<dyn Parallelizer + Send + Sync>> {
        Ok(match &config.parallelizer.parallel_type {
            ParallelType::Snapshot => Box::new(SnapshotParallelizer {
                base_parallelizer,
                parallel_size,
            }),

            ParallelType::RdbPartition => Box::new(PartitionParallelizer {
                base_parallelizer,
                partitioner: RdbPartitioner {
                    meta_manager: TaskUtil::create_rdb_meta_manager(config).await?.unwrap(),
                },
                parallel_size,
            }),

            ParallelType::RdbMerge => Box::new(Self::new_merge_parallelizer(
                config,
                base_parallelizer,
                Self::create_rdb_merger(config).await?,
                parallel_size,
                TaskUtil::create_rdb_meta_manager(config).await?,
                MergeSinkMode::AdaptiveBatch,
            )),

            ParallelType::Serial => Box::new(SerialParallelizer { base_parallelizer }),

            ParallelType::Table => Box::new(TableParallelizer {
                base_parallelizer,
                parallel_size,
            }),

            ParallelType::Mongo => Box::new(Self::new_merge_parallelizer(
                config,
                base_parallelizer,
                Box::new(MongoMerger),
                parallel_size,
                None,
                MergeSinkMode::AdaptiveBatch,
            )),

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

            ParallelType::Foxlake => Box::new(FoxlakeParallelizer {
                task_config: config.clone(),
                snapshot_parallelizer: SnapshotParallelizer {
                    base_parallelizer,
                    parallel_size,
                },
            }),

            ParallelType::RdbCheck => Box::new(Self::new_merge_parallelizer(
                config,
                base_parallelizer,
                match config.sinker_basic.db_type {
                    DbType::Mongo => Box::new(MongoMerger),
                    _ => Self::create_rdb_merger(config).await?,
                },
                parallel_size,
                None,
                MergeSinkMode::Partitioned,
            )),
        })
    }

    fn new_merge_parallelizer(
        config: &TaskConfig,
        base_parallelizer: BaseParallelizer,
        merger: Box<dyn Merger + Send + Sync>,
        parallel_size: usize,
        meta_manager: Option<dt_common::meta::rdb_meta_manager::RdbMetaManager>,
        sink_mode: MergeSinkMode,
    ) -> MergeParallelizer {
        MergeParallelizer {
            base_parallelizer,
            merger,
            parallel_size,
            sinker_basic_config: config.sinker_basic.clone(),
            meta_manager,
            sink_mode,
        }
    }

    async fn create_rdb_merger(
        config: &TaskConfig,
    ) -> anyhow::Result<Box<dyn Merger + Send + Sync>> {
        Ok(Box::new(RdbMerger {
            rdb_meta_manager: TaskUtil::create_rdb_meta_manager(config).await?.unwrap(),
        }))
    }
}
