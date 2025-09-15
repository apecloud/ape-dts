use std::{
    cmp,
    str::FromStr,
    sync::{atomic::AtomicBool, Arc},
};

use tokio::sync::Mutex;

use dt_common::{
    config::{
        config_enums::{DbType, ExtractType},
        extractor_config::ExtractorConfig,
        task_config::TaskConfig,
    },
    meta::{
        avro::avro_converter::AvroConverter, mongo::mongo_cdc_source::MongoCdcSource,
        pg::pg_meta_manager::PgMetaManager, redis::redis_statistic_type::RedisStatisticType,
        syncer::Syncer,
    },
    meta::{
        dt_queue::DtQueue, mysql::mysql_meta_manager::MysqlMetaManager,
        rdb_meta_manager::RdbMetaManager,
    },
    monitor::monitor::Monitor,
    rdb_filter::RdbFilter,
    time_filter::TimeFilter,
    utils::redis_util::RedisUtil,
};
use dt_connector::{
    data_marker::DataMarker,
    extractor::{
        base_extractor::BaseExtractor,
        extractor_monitor::ExtractorMonitor,
        foxlake::foxlake_s3_extractor::FoxlakeS3Extractor,
        kafka::kafka_extractor::KafkaExtractor,
        mongo::{
            mongo_cdc_extractor::MongoCdcExtractor, mongo_check_extractor::MongoCheckExtractor,
            mongo_snapshot_extractor::MongoSnapshotExtractor,
        },
        mysql::{
            mysql_cdc_extractor::MysqlCdcExtractor, mysql_check_extractor::MysqlCheckExtractor,
            mysql_snapshot_extractor::MysqlSnapshotExtractor,
            mysql_struct_extractor::MysqlStructExtractor,
        },
        pg::{
            pg_cdc_extractor::PgCdcExtractor, pg_check_extractor::PgCheckExtractor,
            pg_snapshot_extractor::PgSnapshotExtractor, pg_struct_extractor::PgStructExtractor,
        },
        redis::{
            redis_client::RedisClient, redis_psync_extractor::RedisPsyncExtractor,
            redis_reshard_extractor::RedisReshardExtractor,
            redis_scan_extractor::RedisScanExtractor,
            redis_snapshot_file_extractor::RedisSnapshotFileExtractor,
        },
        resumer::{cdc_resumer::CdcResumer, snapshot_resumer::SnapshotResumer},
    },
    rdb_router::RdbRouter,
    Extractor,
};

use super::task_util::TaskUtil;

pub struct ExtractorUtil {}

impl ExtractorUtil {
    pub async fn create_extractor(
        config: &TaskConfig,
        extractor_config: &ExtractorConfig,
        buffer: Arc<DtQueue>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
        monitor: Arc<Monitor>,
        data_marker: Option<DataMarker>,
        router: RdbRouter,
        snapshot_resumer: SnapshotResumer,
        cdc_resumer: CdcResumer,
    ) -> anyhow::Result<Box<dyn Extractor + Send>> {
        let mut base_extractor = BaseExtractor {
            buffer,
            router,
            shut_down,
            monitor: ExtractorMonitor::new(monitor).await,
            data_marker,
            time_filter: TimeFilter::default(),
        };

        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(&config.runtime.log_level);
        let filter = RdbFilter::from_config(&config.filter, &config.extractor_basic.db_type)?;

        let extractor: Box<dyn Extractor + Send> = match extractor_config.to_owned() {
            ExtractorConfig::MysqlSnapshot {
                url,
                db,
                tb,
                sample_interval,
                parallel_size,
                batch_size,
            } => {
                // max_connections: 1 for extracting data from table, 1 for db-meta-manager
                let max_connections = cmp::max(2, parallel_size as u32 + 1);
                let conn_pool =
                    TaskUtil::create_mysql_conn_pool(&url, max_connections, enable_sqlx_log, false)
                        .await?;
                let meta_manager = TaskUtil::create_mysql_meta_manager(
                    &url,
                    &config.runtime.log_level,
                    DbType::Mysql,
                    config.meta_center.clone(),
                )
                .await?;
                let extractor = MysqlSnapshotExtractor {
                    conn_pool: conn_pool.clone(),
                    meta_manager,
                    resumer: snapshot_resumer,
                    db,
                    tb,
                    batch_size,
                    sample_interval,
                    parallel_size,
                    base_extractor,
                    filter,
                };
                Box::new(extractor)
            }

            ExtractorConfig::MysqlCheck {
                url,
                check_log_dir,
                batch_size,
            } => {
                let conn_pool =
                    TaskUtil::create_mysql_conn_pool(&url, 2, enable_sqlx_log, false).await?;
                let meta_manager = TaskUtil::create_mysql_meta_manager(
                    &url,
                    &config.runtime.log_level,
                    DbType::Mysql,
                    config.meta_center.clone(),
                )
                .await?;
                let extractor = MysqlCheckExtractor {
                    conn_pool,
                    meta_manager,
                    check_log_dir,
                    batch_size,
                    base_extractor,
                    filter,
                };
                Box::new(extractor)
            }

            ExtractorConfig::MysqlCdc {
                url,
                binlog_filename,
                binlog_position,
                server_id,
                gtid_enabled,
                gtid_set,
                binlog_heartbeat_interval_secs,
                binlog_timeout_secs,
                heartbeat_interval_secs,
                heartbeat_tb,
                start_time_utc,
                end_time_utc,
            } => {
                let conn_pool =
                    TaskUtil::create_mysql_conn_pool(&url, 2, enable_sqlx_log, false).await?;
                let meta_manager = TaskUtil::create_mysql_meta_manager(
                    &url,
                    &config.runtime.log_level,
                    DbType::Mysql,
                    config.meta_center.clone(),
                )
                .await?;
                base_extractor.time_filter = TimeFilter::new(&start_time_utc, &end_time_utc)?;
                let extractor = MysqlCdcExtractor {
                    meta_manager,
                    filter,
                    conn_pool,
                    url,
                    binlog_filename,
                    binlog_position,
                    server_id,
                    binlog_heartbeat_interval_secs,
                    binlog_timeout_secs,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    syncer,
                    base_extractor,
                    resumer: cdc_resumer,
                    gtid_enabled,
                    gtid_set,
                };
                Box::new(extractor)
            }

            ExtractorConfig::PgSnapshot {
                url,
                schema,
                tb,
                sample_interval,
                batch_size,
            } => {
                let conn_pool =
                    TaskUtil::create_pg_conn_pool(&url, 2, enable_sqlx_log, false).await?;
                let meta_manager = PgMetaManager::new(conn_pool.clone()).await?;
                let extractor = PgSnapshotExtractor {
                    conn_pool,
                    meta_manager,
                    resumer: snapshot_resumer,
                    batch_size,
                    sample_interval,
                    schema,
                    tb,
                    base_extractor,
                    filter,
                };
                Box::new(extractor)
            }

            ExtractorConfig::PgCheck {
                url,
                check_log_dir,
                batch_size,
            } => {
                let conn_pool =
                    TaskUtil::create_pg_conn_pool(&url, 2, enable_sqlx_log, false).await?;
                let meta_manager = PgMetaManager::new(conn_pool.clone()).await?;
                let extractor = PgCheckExtractor {
                    conn_pool,
                    meta_manager,
                    check_log_dir,
                    batch_size,
                    base_extractor,
                    filter,
                };
                Box::new(extractor)
            }

            ExtractorConfig::PgCdc {
                url,
                slot_name,
                pub_name,
                start_lsn,
                recreate_slot_if_exists,
                keepalive_interval_secs,
                heartbeat_interval_secs,
                heartbeat_tb,
                ddl_meta_tb,
                start_time_utc,
                end_time_utc,
            } => {
                let conn_pool =
                    TaskUtil::create_pg_conn_pool(&url, 2, enable_sqlx_log, false).await?;
                let meta_manager = PgMetaManager::new(conn_pool.clone()).await?;
                base_extractor.time_filter = TimeFilter::new(&start_time_utc, &end_time_utc)?;
                let extractor = PgCdcExtractor {
                    meta_manager,
                    filter,
                    url,
                    conn_pool,
                    slot_name,
                    pub_name,
                    start_lsn,
                    recreate_slot_if_exists,
                    syncer,
                    keepalive_interval_secs,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    ddl_meta_tb,
                    resumer: cdc_resumer,
                    base_extractor,
                };
                Box::new(extractor)
            }

            ExtractorConfig::MongoSnapshot {
                url,
                app_name,
                db,
                tb,
            } => {
                let mongo_client = TaskUtil::create_mongo_client(&url, &app_name).await?;
                let extractor = MongoSnapshotExtractor {
                    resumer: snapshot_resumer,
                    db,
                    tb,
                    mongo_client,
                    base_extractor,
                };
                Box::new(extractor)
            }

            ExtractorConfig::MongoCdc {
                url,
                app_name,
                resume_token,
                start_timestamp,
                source,
                heartbeat_interval_secs,
                heartbeat_tb,
            } => {
                let mongo_client = TaskUtil::create_mongo_client(&url, &app_name).await?;
                let extractor = MongoCdcExtractor {
                    filter,
                    resume_token,
                    start_timestamp,
                    source: MongoCdcSource::from_str(&source)?,
                    mongo_client,
                    app_name,
                    base_extractor,
                    heartbeat_interval_secs,
                    heartbeat_tb,
                    syncer,
                    resumer: cdc_resumer,
                };
                Box::new(extractor)
            }

            ExtractorConfig::MongoCheck {
                url,
                app_name,
                check_log_dir,
                batch_size,
            } => {
                let mongo_client = TaskUtil::create_mongo_client(&url, &app_name).await?;
                let extractor = MongoCheckExtractor {
                    mongo_client,
                    check_log_dir,
                    batch_size,
                    base_extractor,
                };
                Box::new(extractor)
            }

            ExtractorConfig::MysqlStruct { url, db, dbs, .. } => {
                // TODO, pass max_connections as parameter
                let conn_pool =
                    TaskUtil::create_mysql_conn_pool(&url, 2, enable_sqlx_log, false).await?;
                let extractor = MysqlStructExtractor {
                    conn_pool,
                    db,
                    dbs,
                    filter,
                    base_extractor,
                };
                Box::new(extractor)
            }

            ExtractorConfig::PgStruct {
                url,
                schema,
                do_global_structs,
                batch_size,
            } => {
                // TODO, pass max_connections as parameter
                let conn_pool =
                    TaskUtil::create_pg_conn_pool(&url, 2, enable_sqlx_log, false).await?;
                let extractor = PgStructExtractor {
                    conn_pool,
                    schema,
                    do_global_structs,
                    filter,
                    base_extractor,
                };
                Box::new(extractor)
            }

            ExtractorConfig::RedisSnapshot { url, repl_port } => {
                let extractor = RedisPsyncExtractor {
                    conn: RedisClient::new(&url).await?,
                    syncer,
                    repl_port,
                    filter,
                    resumer: cdc_resumer,
                    base_extractor,
                    extract_type: ExtractType::Snapshot,
                    repl_id: String::new(),
                    repl_offset: 0,
                    now_db_id: 0,
                    keepalive_interval_secs: 0,
                    heartbeat_interval_secs: 0,
                    heartbeat_key: String::new(),
                };
                Box::new(extractor)
            }

            ExtractorConfig::RedisSnapshotFile { file_path } => {
                let extractor = RedisSnapshotFileExtractor {
                    file_path,
                    filter,
                    base_extractor,
                };
                Box::new(extractor)
            }

            ExtractorConfig::RedisScan {
                url,
                scan_count,
                statistic_type,
            } => {
                let conn = RedisUtil::create_redis_conn(&url).await?;
                let statistic_type = RedisStatisticType::from_str(&statistic_type)?;
                let extractor = RedisScanExtractor {
                    conn,
                    statistic_type,
                    scan_count,
                    filter,
                    base_extractor,
                };
                Box::new(extractor)
            }

            ExtractorConfig::RedisCdc {
                url,
                repl_id,
                repl_offset,
                now_db_id,
                repl_port,
                keepalive_interval_secs,
                heartbeat_interval_secs,
                heartbeat_key,
            } => {
                let extractor = RedisPsyncExtractor {
                    conn: RedisClient::new(&url).await?,
                    repl_id,
                    repl_offset,
                    keepalive_interval_secs,
                    heartbeat_interval_secs,
                    heartbeat_key,
                    syncer,
                    repl_port,
                    now_db_id,
                    filter,
                    resumer: cdc_resumer,
                    base_extractor,
                    extract_type: ExtractType::Cdc,
                };
                Box::new(extractor)
            }

            ExtractorConfig::RedisSnapshotAndCdc {
                url,
                repl_id,
                repl_port,
                keepalive_interval_secs,
                heartbeat_interval_secs,
                heartbeat_key,
            } => {
                let extractor = RedisPsyncExtractor {
                    conn: RedisClient::new(&url).await?,
                    syncer,
                    repl_port,
                    filter,
                    resumer: cdc_resumer,
                    base_extractor,
                    extract_type: ExtractType::SnapshotAndCdc,
                    repl_id,
                    repl_offset: 0,
                    now_db_id: 0,
                    keepalive_interval_secs,
                    heartbeat_interval_secs,
                    heartbeat_key,
                };
                Box::new(extractor)
            }

            ExtractorConfig::RedisReshard { url } => {
                let extractor = RedisReshardExtractor {
                    base_extractor,
                    url,
                };
                Box::new(extractor)
            }

            ExtractorConfig::Kafka {
                url,
                group,
                topic,
                partition,
                offset,
                ack_interval_secs,
            } => {
                let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?;
                let avro_converter = AvroConverter::new(meta_manager, false);
                let extractor = KafkaExtractor {
                    url,
                    group,
                    topic,
                    partition,
                    offset,
                    ack_interval_secs,
                    avro_converter,
                    syncer,
                    resumer: cdc_resumer,
                    base_extractor,
                };
                Box::new(extractor)
            }

            ExtractorConfig::FoxlakeS3 {
                schema,
                tb,
                s3_config,
                batch_size,
                ..
            } => {
                let s3_client = TaskUtil::create_s3_client(&s3_config);
                let extractor = FoxlakeS3Extractor {
                    schema,
                    tb,
                    s3_client,
                    s3_config,
                    resumer: snapshot_resumer,
                    base_extractor,
                    batch_size,
                };
                Box::new(extractor)
            }
        };
        Ok(extractor)
    }

    pub async fn get_extractor_meta_manager(
        task_config: &TaskConfig,
    ) -> anyhow::Result<Option<RdbMetaManager>> {
        let extractor_url = &task_config.extractor_basic.url;
        let meta_manager = match task_config.extractor_basic.db_type {
            DbType::Mysql => {
                let conn_pool =
                    TaskUtil::create_mysql_conn_pool(extractor_url, 1, true, false).await?;
                let meta_manager = MysqlMetaManager::new(conn_pool.clone()).await?;
                Some(RdbMetaManager::from_mysql(meta_manager))
            }
            DbType::Pg => {
                let conn_pool =
                    TaskUtil::create_pg_conn_pool(extractor_url, 1, true, false).await?;
                let meta_manager = PgMetaManager::new(conn_pool.clone()).await?;
                Some(RdbMetaManager::from_pg(meta_manager))
            }
            _ => None,
        };
        Ok(meta_manager)
    }
}
