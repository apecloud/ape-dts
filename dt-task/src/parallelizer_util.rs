use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use ratelimit::Ratelimiter;

use super::task_util::TaskUtil;
use dt_common::{
    config::{
        config_enums::{DbType, ExtractType, ParallelType},
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    meta::redis::command::key_parser::KeyParser,
    monitor::monitor::Monitor,
    utils::redis_util::RedisUtil,
};
use dt_connector::checker::CheckerHandle;
use dt_parallelizer::{
    base_parallelizer::BaseParallelizer, check_parallelizer::CheckParallelizer,
    foxlake_parallelizer::FoxlakeParallelizer, merge_parallelizer::MergeParallelizer,
    mongo_merger::MongoMerger, partition_parallelizer::PartitionParallelizer,
    rdb_merger::RdbMerger, rdb_partitioner::RdbPartitioner, redis_parallelizer::RedisParallelizer,
    serial_parallelizer::SerialParallelizer, snapshot_parallelizer::SnapshotParallelizer,
    table_parallelizer::TableParallelizer, Merger, Parallelizer,
};

pub struct ParallelizerUtil {}

impl ParallelizerUtil {
    pub async fn create_parallelizer(
        config: &TaskConfig,
        monitor: Arc<Monitor>,
        rps_limiter: Option<Ratelimiter>,
        checker: Option<CheckerHandle>,
    ) -> anyhow::Result<(Box<dyn Parallelizer + Send + Sync>, Option<CheckerHandle>)> {
        let parallel_size = config.parallelizer.parallel_size;
        let parallel_type = &config.parallelizer.parallel_type;
        let base_parallelizer = BaseParallelizer {
            popped_data: VecDeque::new(),
            monitor,
            rps_limiter,
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
                let merger = Self::create_rdb_merger(config).await?;
                let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?;
                Box::new(MergeParallelizer {
                    base_parallelizer,
                    merger,
                    parallel_size,
                    sinker_basic_config: config.sinker_basic.clone(),
                    meta_manager,
                })
            }

            ParallelType::RdbCheck => {
                let merger = match config.sinker_basic.db_type {
                    DbType::Mongo => Self::create_mongo_merger().await?,
                    _ => Self::create_rdb_merger(config).await?,
                };
                // Only move checker into CheckParallelizer for CDC tasks,
                // so it checks against merged data. Snapshot check keeps
                // checker in BasePipeline (pre-merger data == actual data).
                let is_cdc = matches!(config.extractor_basic.extract_type, ExtractType::Cdc);
                let (inner_checker, remaining_checker) = if is_cdc {
                    (checker, None)
                } else {
                    (None, checker)
                };
                let parallelizer = Box::new(CheckParallelizer::new(
                    base_parallelizer,
                    merger,
                    parallel_size,
                    inner_checker,
                ));
                return Ok((parallelizer, remaining_checker));
            }

            ParallelType::Serial => Box::new(SerialParallelizer { base_parallelizer }),

            ParallelType::Table => Box::new(TableParallelizer {
                base_parallelizer,
                parallel_size,
            }),

            ParallelType::Mongo => {
                let merger = Box::new(MongoMerger {});
                Box::new(MergeParallelizer {
                    base_parallelizer,
                    merger,
                    parallel_size,
                    sinker_basic_config: config.sinker_basic.clone(),
                    meta_manager: None,
                })
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
        Ok((parallelizer, checker))
    }

    async fn create_rdb_merger(
        config: &TaskConfig,
    ) -> anyhow::Result<Box<dyn Merger + Send + Sync>> {
        let rdb_merger = RdbMerger {
            rdb_meta_manager: TaskUtil::create_rdb_meta_manager(config).await?.unwrap(),
        };
        Ok(Box::new(rdb_merger))
    }

    async fn create_mongo_merger() -> anyhow::Result<Box<dyn Merger + Send + Sync>> {
        let mongo_merger = MongoMerger {};
        Ok(Box::new(mongo_merger))
    }

    async fn create_rdb_partitioner(config: &TaskConfig) -> anyhow::Result<RdbPartitioner> {
        let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?.unwrap();
        Ok(RdbPartitioner { meta_manager })
    }
}
