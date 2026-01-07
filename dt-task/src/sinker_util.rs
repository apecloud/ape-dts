use std::{str::FromStr, sync::Arc};

use anyhow::{bail, Context};
use kafka::producer::{Producer, RequiredAcks};
use reqwest::{redirect::Policy, Url};
use rusoto_s3::S3Client;
use sqlx::types::chrono::Utc;
use tokio::sync::{Mutex, RwLock};

use dt_common::{
    config::{
        config_enums::DbType, connection_auth_config::ConnectionAuthConfig,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    meta::{
        avro::avro_converter::AvroConverter,
        mysql::mysql_meta_manager::MysqlMetaManager,
        pg::pg_meta_manager::PgMetaManager,
        redis::{
            command::key_parser::KeyParser, redis_statistic_type::RedisStatisticType,
            redis_write_method::RedisWriteMethod,
        },
    },
    monitor::monitor::Monitor,
    rdb_filter::RdbFilter,
    utils::redis_util::RedisUtil,
};

use super::task_util::TaskUtil;
use crate::{extractor_util::ExtractorUtil, task_util::ConnClient};
use dt_connector::{
    data_marker::DataMarker,
    rdb_router::RdbRouter,
    sinker::{
        clickhouse::{
            clickhouse_sinker::ClickhouseSinker, clickhouse_struct_sinker::ClickhouseStructSinker,
        },
        dummy_sinker::DummySinker,
        foxlake::{
            foxlake_merger::FoxlakeMerger, foxlake_pusher::FoxlakePusher,
            foxlake_sinker::FoxlakeSinker, foxlake_struct_sinker::FoxlakeStructSinker,
            orc_sequencer::OrcSequencer,
        },
        kafka::kafka_sinker::KafkaSinker,
        mongo::{mongo_checker::MongoChecker, mongo_sinker::MongoSinker},
        mysql::{
            mysql_checker::MysqlChecker, mysql_sinker::MysqlSinker,
            mysql_struct_sinker::MysqlStructSinker,
        },
        pg::{pg_checker::PgChecker, pg_sinker::PgSinker, pg_struct_sinker::PgStructSinker},
        redis::{redis_sinker::RedisSinker, redis_statistic_sinker::RedisStatisticSinker},
        sql_sinker::SqlSinker,
        starrocks::{
            starrocks_sinker::StarRocksSinker, starrocks_struct_sinker::StarrocksStructSinker,
        },
    },
    Sinker,
};

type Sinkers = Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>;

pub struct SinkerUtil {}

#[macro_export]
macro_rules! create_filter {
    ($task_config:expr,$db_type:ident) => {
        RdbFilter::from_config(&$task_config.filter, &DbType::$db_type)?
    };
}

#[macro_export]
macro_rules! create_router {
    ($task_config:expr,$db_type:ident) => {
        RdbRouter::from_config(&$task_config.router, &DbType::$db_type)?
    };
}

impl SinkerUtil {
    pub async fn create_sinkers(
        task_config: &TaskConfig,
        extractor_config: &ExtractorConfig,
        sinker_client: ConnClient,
        monitor: Arc<Monitor>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
    ) -> anyhow::Result<Sinkers> {
        let log_level = &task_config.runtime.log_level;
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let parallel_size = task_config.parallelizer.parallel_size as u32;
        let monitor_interval = task_config.pipeline.checkpoint_interval_secs;

        let mut sub_sinkers: Sinkers = Vec::new();
        match task_config.sinker.clone() {
            SinkerConfig::Dummy => {
                for _ in 0..parallel_size {
                    let sinker = DummySinker {};
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::Mysql {
                url,
                batch_size,
                replace,
                ..
            } => {
                let router = create_router!(task_config, Mysql);

                let conn_pool = match sinker_client {
                    ConnClient::MySQL(conn_pool) => conn_pool,
                    _ => {
                        bail!("connection pool not found");
                    }
                };
                let meta_manager = MysqlMetaManager::new(conn_pool.clone()).await?;

                for _ in 0..parallel_size {
                    let sinker = MysqlSinker {
                        url: url.to_string(),
                        conn_pool: conn_pool.clone(),
                        meta_manager: meta_manager.clone(),
                        router: router.clone(),
                        batch_size,
                        monitor: monitor.clone(),
                        data_marker: data_marker.clone(),
                        replace,
                        monitor_interval,
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::MysqlCheck { batch_size, .. } => {
                // checker needs the reverse router
                let reverse_router = create_router!(task_config, Mysql).reverse();
                let filter = create_filter!(task_config, Mysql);
                let extractor_meta_manager = ExtractorUtil::get_extractor_meta_manager(task_config)
                    .await?
                    .unwrap();

                let conn_pool = match sinker_client {
                    ConnClient::MySQL(conn_pool) => conn_pool,
                    _ => {
                        bail!("connection pool not found");
                    }
                };
                let meta_manager = MysqlMetaManager::new(conn_pool.clone()).await?;

                for _ in 0..parallel_size {
                    let sinker = MysqlChecker {
                        conn_pool: conn_pool.clone(),
                        meta_manager: meta_manager.clone(),
                        extractor_meta_manager: extractor_meta_manager.clone(),
                        reverse_router: reverse_router.clone(),
                        filter: filter.clone(),
                        batch_size,
                        monitor: monitor.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::Pg {
                url,
                batch_size,
                replace,
                ..
            } => {
                let router = create_router!(task_config, Pg);
                let conn_pool = match sinker_client {
                    ConnClient::PostgreSQL(conn_pool) => conn_pool,
                    _ => {
                        bail!("connection pool not found");
                    }
                };
                let meta_manager = PgMetaManager::new(conn_pool.clone()).await?;

                for _ in 0..parallel_size {
                    let sinker = PgSinker {
                        url: url.to_string(),
                        conn_pool: conn_pool.clone(),
                        meta_manager: meta_manager.clone(),
                        router: router.clone(),
                        batch_size,
                        monitor: monitor.clone(),
                        data_marker: data_marker.clone(),
                        replace,
                        monitor_interval,
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::PgCheck { batch_size, .. } => {
                // checker needs the reverse router
                let reverse_router = create_router!(task_config, Pg).reverse();
                let filter = create_filter!(task_config, Pg);
                let extractor_meta_manager = ExtractorUtil::get_extractor_meta_manager(task_config)
                    .await?
                    .unwrap();

                let conn_pool = match sinker_client {
                    ConnClient::PostgreSQL(conn_pool) => conn_pool,
                    _ => {
                        bail!("connection pool not found");
                    }
                };
                let meta_manager = PgMetaManager::new(conn_pool.clone()).await?;

                for _ in 0..parallel_size {
                    let sinker = PgChecker {
                        conn_pool: conn_pool.clone(),
                        meta_manager: meta_manager.clone(),
                        extractor_meta_manager: extractor_meta_manager.clone(),
                        reverse_router: reverse_router.clone(),
                        filter: filter.clone(),
                        batch_size,
                        monitor: monitor.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::Mongo { batch_size, .. } => {
                let router = create_router!(task_config, Mongo);
                let mongo_client = match sinker_client {
                    ConnClient::MongoDB(mongo_client) => mongo_client,
                    _ => {
                        bail!("connection pool not found");
                    }
                };
                for _ in 0..parallel_size {
                    let sinker = MongoSinker {
                        batch_size,
                        router: router.clone(),
                        mongo_client: mongo_client.clone(),
                        monitor: monitor.clone(),
                        monitor_interval,
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::MongoCheck { batch_size, .. } => {
                let reverse_router = create_router!(task_config, Mongo).reverse();
                let mongo_client = match sinker_client {
                    ConnClient::MongoDB(mongo_client) => mongo_client,
                    _ => {
                        bail!("connection pool not found");
                    }
                };
                for _ in 0..parallel_size {
                    let sinker = MongoChecker {
                        batch_size,
                        reverse_router: reverse_router.clone(),
                        mongo_client: mongo_client.clone(),
                        monitor: monitor.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs,
                required_acks,
                with_field_defs,
            } => {
                let router = RdbRouter::from_config(
                    &task_config.router,
                    // use the db_type of extractor
                    &task_config.extractor_basic.db_type,
                )?;
                // kafka sinker may need meta data from RDB extractor
                let meta_manager = ExtractorUtil::get_extractor_meta_manager(task_config).await?;
                let avro_converter = AvroConverter::new(meta_manager, with_field_defs);

                let brokers = vec![url.to_string()];
                let acks = match required_acks.as_str() {
                    "all" => RequiredAcks::All,
                    "none" => RequiredAcks::None,
                    _ => RequiredAcks::One,
                };

                for _ in 0..parallel_size {
                    // TODO, authentication, https://github.com/kafka-rust/kafka-rust/blob/master/examples/example-ssl.rs
                    let producer = Producer::from_hosts(brokers.clone())
                        .with_ack_timeout(std::time::Duration::from_secs(ack_timeout_secs))
                        .with_required_acks(acks)
                        .create()
                        .with_context(|| {
                            format!("failed to create kafka producer, url: [{}]", url)
                        })?;
                    // the sending performance of RdkafkaSinker is much worse than KafkaSinker
                    let sinker = KafkaSinker {
                        batch_size,
                        router: router.clone(),
                        producer,
                        avro_converter: avro_converter.clone(),
                        monitor: monitor.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::MysqlStruct {
                conflict_policy, ..
            } => {
                let filter = create_filter!(task_config, Mysql);
                let router = create_router!(task_config, Mysql);

                let conn_pool = match sinker_client {
                    ConnClient::MySQL(conn_pool) => conn_pool,
                    _ => {
                        bail!("connection pool not found");
                    }
                };
                let sinker = MysqlStructSinker {
                    conn_pool,
                    conflict_policy: conflict_policy.clone(),
                    filter: filter.clone(),
                    router,
                    monitor: monitor.clone(),
                    monitor_interval,
                };
                sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
            }

            SinkerConfig::PgStruct {
                conflict_policy, ..
            } => {
                let filter = create_filter!(task_config, Pg);
                let router = create_router!(task_config, Pg);

                let conn_pool = match sinker_client {
                    ConnClient::PostgreSQL(conn_pool) => conn_pool,
                    _ => {
                        bail!("connection pool not found");
                    }
                };
                let sinker = PgStructSinker {
                    conn_pool,
                    conflict_policy: conflict_policy.clone(),
                    filter: filter.clone(),
                    router,
                    monitor: monitor.clone(),
                    monitor_interval,
                };
                sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
            }

            SinkerConfig::Redis {
                url,
                connection_auth,
                batch_size,
                method,
                is_cluster,
            } => {
                // redis sinker may need meta data from RDB extractor
                let meta_manager = ExtractorUtil::get_extractor_meta_manager(task_config).await?;
                let mut conn = RedisUtil::create_redis_conn(&url, &connection_auth).await?;
                let version = RedisUtil::get_redis_version(&mut conn)?;
                let method = RedisWriteMethod::from_str(&method)?;

                if is_cluster {
                    let url_info = Url::parse(&url)?;
                    let username = url_info.username();
                    let password = url_info.password().unwrap_or("").to_string();

                    let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)?;
                    for node in nodes.iter() {
                        if !node.is_master {
                            continue;
                        }

                        let new_url = format!("redis://{}:{}@{}", username, password, node.address);
                        let conn = RedisUtil::create_redis_conn(&new_url, &connection_auth).await?;
                        let sinker = RedisSinker {
                            cluster_node: Some(node.clone()),
                            conn,
                            batch_size,
                            now_db_id: -1,
                            version,
                            method: method.clone(),
                            meta_manager: meta_manager.clone(),
                            monitor: monitor.clone(),
                            data_marker: data_marker.clone(),
                            key_parser: KeyParser::new(),
                        };
                        sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                    }
                } else {
                    for _ in 0..parallel_size {
                        let conn = RedisUtil::create_redis_conn(&url, &connection_auth).await?;
                        let sinker = RedisSinker {
                            cluster_node: None,
                            conn,
                            batch_size,
                            now_db_id: -1,
                            version,
                            method: method.clone(),
                            meta_manager: meta_manager.clone(),
                            monitor: monitor.clone(),
                            data_marker: data_marker.clone(),
                            key_parser: KeyParser::new(),
                        };
                        sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                    }
                }
            }

            SinkerConfig::RedisStatistic {
                statistic_type,
                data_size_threshold,
                freq_threshold,
                ..
            } => {
                let statistic_type = RedisStatisticType::from_str(&statistic_type)?;
                for _ in 0..parallel_size {
                    let sinker = RedisStatisticSinker {
                        statistic_type: statistic_type.clone(),
                        data_size_threshold,
                        freq_threshold,
                        monitor: monitor.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::StarRocks {
                url,
                connection_auth,
                batch_size,
                stream_load_url,
                ..
            }
            | SinkerConfig::Doris {
                url,
                connection_auth,
                batch_size,
                stream_load_url,
            } => {
                for _ in 0..parallel_size {
                    let url_info = Url::parse(&stream_load_url)?;
                    let host = url_info.host_str().unwrap().to_string();
                    let port = format!("{}", url_info.port().unwrap());
                    let username = url_info.username().to_string();
                    let password = url_info.password().unwrap_or("").to_string();
                    let custom = Policy::custom(|attempt| attempt.follow());
                    let http_client = reqwest::Client::builder()
                        .http1_title_case_headers()
                        .redirect(custom)
                        .build()?;
                    let conn_pool = TaskUtil::create_mysql_conn_pool(
                        &url,
                        &connection_auth,
                        parallel_size * 2,
                        enable_sqlx_log,
                        None,
                    )
                    .await?;
                    let meta_manager = MysqlMetaManager::new_mysql_compatible(
                        conn_pool.clone(),
                        DbType::StarRocks,
                    )
                    .await?;

                    let mut sinker = StarRocksSinker {
                        db_type: task_config.sinker_basic.db_type.clone(),
                        http_client,
                        host,
                        port,
                        username,
                        password,
                        batch_size,
                        meta_manager,
                        monitor: monitor.clone(),
                        sync_timestamp: Utc::now().timestamp_millis(),
                        hard_delete: false,
                    };
                    if let SinkerConfig::StarRocks { hard_delete, .. } = task_config.sinker {
                        sinker.hard_delete = hard_delete;
                    }

                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::StarRocksStruct {
                url,
                connection_auth,
                conflict_policy,
            }
            | SinkerConfig::DorisStruct {
                url,
                connection_auth,
                conflict_policy,
            } => {
                let conn_pool = TaskUtil::create_mysql_conn_pool(
                    &url,
                    &connection_auth,
                    2,
                    enable_sqlx_log,
                    None,
                )
                .await?;
                let filter = create_filter!(task_config, Mysql);
                let router = create_router!(task_config, Mysql);
                let extractor_meta_manager = ExtractorUtil::get_extractor_meta_manager(task_config)
                    .await?
                    .unwrap();
                let sinker = StarrocksStructSinker {
                    db_type: task_config.sinker_basic.db_type.clone(),
                    conn_pool,
                    conflict_policy,
                    filter,
                    router,
                    extractor_meta_manager,
                    backend_count: 0,
                };
                sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
            }

            SinkerConfig::ClickHouse { url, batch_size } => {
                for _ in 0..parallel_size {
                    let url_info = Url::parse(&url)?;
                    let host = url_info.host_str().unwrap().to_string();
                    let port = format!("{}", url_info.port().unwrap());
                    let username = url_info.username().to_string();
                    let password = url_info.password().unwrap_or("").to_string();
                    let custom = Policy::custom(|attempt| attempt.follow());
                    let http_client = reqwest::Client::builder()
                        .http1_title_case_headers()
                        .redirect(custom)
                        .build()?;
                    let sinker = ClickhouseSinker {
                        http_client,
                        host,
                        port,
                        username,
                        password,
                        batch_size,
                        monitor: monitor.clone(),
                        sync_timestamp: Utc::now().timestamp_millis(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::ClickhouseStruct {
                url,
                conflict_policy,
                engine,
            } => {
                let url_info = Url::parse(&url)?;
                let host = url_info.host_str().unwrap().to_string();
                let port = format!("{}", url_info.port().unwrap());
                let client = clickhouse::Client::default()
                    .with_url(format!("http://{}:{}", host, port))
                    .with_user(url_info.username())
                    .with_password(url_info.password().unwrap_or(""));
                let filter = create_filter!(task_config, Mysql);
                let router = create_router!(task_config, Mysql);
                let extractor_meta_manager = ExtractorUtil::get_extractor_meta_manager(task_config)
                    .await?
                    .unwrap();
                let sinker = ClickhouseStructSinker {
                    client,
                    conflict_policy,
                    engine,
                    filter,
                    router,
                    extractor_meta_manager,
                };
                sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
            }

            SinkerConfig::Sql { reverse } => {
                let router = RdbRouter::from_config(
                    &task_config.router,
                    &task_config.extractor_basic.db_type,
                )?;

                for _ in 0..parallel_size {
                    let meta_manager = ExtractorUtil::get_extractor_meta_manager(task_config)
                        .await?
                        .unwrap();
                    let sinker = SqlSinker {
                        meta_manager,
                        router: router.clone(),
                        reverse,
                        monitor: monitor.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::Foxlake {
                url,
                batch_size,
                batch_memory_mb,
                s3_config,
                engine,
            } => {
                let router = create_router!(task_config, Mysql);
                let reverse_router = router.reverse();
                let conn_pool = TaskUtil::create_mysql_conn_pool(
                    &url,
                    &ConnectionAuthConfig::NoAuth,
                    parallel_size * 2,
                    enable_sqlx_log,
                    None,
                )
                .await?;
                let s3_client = TaskUtil::create_s3_client(&s3_config);
                let orc_sequencer = Arc::new(Mutex::new(OrcSequencer::new()));

                for _ in 0..parallel_size {
                    let meta_manager =
                        MysqlMetaManager::new_mysql_compatible(conn_pool.clone(), DbType::Foxlake)
                            .await?;
                    let (schema, tb) = match extractor_config.to_owned() {
                        ExtractorConfig::MysqlSnapshot { db, tb, .. } => (Some(db), Some(tb)),
                        ExtractorConfig::PgSnapshot { schema, tb, .. } => (Some(schema), Some(tb)),
                        _ => (None, None),
                    };

                    let pusher = FoxlakePusher {
                        url: url.to_string(),
                        extract_type: task_config.extractor_basic.extract_type.clone(),
                        meta_manager: meta_manager.clone(),
                        batch_size,
                        batch_memory_bytes: batch_memory_mb * 1024 * 1024,
                        s3_config: s3_config.clone(),
                        s3_client: s3_client.clone(),
                        monitor: monitor.clone(),
                        schema,
                        tb,
                        reverse_router: reverse_router.clone(),
                        orc_sequencer: orc_sequencer.clone(),
                    };

                    let merger = FoxlakeMerger {
                        batch_size,
                        s3_config: s3_config.clone(),
                        s3_client: s3_client.clone(),
                        monitor: monitor.clone(),
                        conn_pool: conn_pool.clone(),
                        extract_type: task_config.extractor_basic.extract_type.clone(),
                    };

                    let sinker = FoxlakeSinker {
                        url: url.to_string(),
                        meta_manager,
                        batch_size,
                        monitor: monitor.clone(),
                        conn_pool: conn_pool.clone(),
                        router: router.clone(),
                        pusher,
                        merger,
                        engine: engine.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::FoxlakePush {
                url,
                batch_size,
                batch_memory_mb,
                s3_config,
            } => {
                let conn_pool = TaskUtil::create_mysql_conn_pool(
                    &url,
                    &ConnectionAuthConfig::NoAuth,
                    parallel_size * 2,
                    enable_sqlx_log,
                    None,
                )
                .await?;
                let s3_client: S3Client = TaskUtil::create_s3_client(&s3_config);
                let reverse_router = create_router!(task_config, Mysql).reverse();
                let orc_sequencer = Arc::new(Mutex::new(OrcSequencer::new()));

                for _ in 0..parallel_size {
                    let meta_manager =
                        MysqlMetaManager::new_mysql_compatible(conn_pool.clone(), DbType::Foxlake)
                            .await?;
                    let (schema, tb) = match extractor_config.to_owned() {
                        ExtractorConfig::MysqlSnapshot { db, tb, .. } => (Some(db), Some(tb)),
                        ExtractorConfig::PgSnapshot { schema, tb, .. } => (Some(schema), Some(tb)),
                        _ => (None, None),
                    };

                    let sinker = FoxlakePusher {
                        url: url.to_string(),
                        extract_type: task_config.extractor_basic.extract_type.clone(),
                        meta_manager,
                        batch_size,
                        batch_memory_bytes: batch_memory_mb * 1024 * 1024,
                        s3_config: s3_config.clone(),
                        s3_client: s3_client.clone(),
                        monitor: monitor.clone(),
                        schema,
                        tb,
                        reverse_router: reverse_router.clone(),
                        orc_sequencer: orc_sequencer.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::FoxlakeMerge {
                url,
                batch_size,
                s3_config,
            } => {
                let conn_pool = TaskUtil::create_mysql_conn_pool(
                    &url,
                    &ConnectionAuthConfig::NoAuth,
                    parallel_size * 2,
                    enable_sqlx_log,
                    None,
                )
                .await?;
                let s3_client = TaskUtil::create_s3_client(&s3_config);

                for _ in 0..parallel_size {
                    let sinker = FoxlakeMerger {
                        batch_size,
                        s3_config: s3_config.clone(),
                        s3_client: s3_client.clone(),
                        monitor: monitor.clone(),
                        conn_pool: conn_pool.clone(),
                        extract_type: task_config.extractor_basic.extract_type.clone(),
                    };
                    sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
                }
            }

            SinkerConfig::FoxlakeStruct {
                url,
                conflict_policy,
                engine,
            } => {
                let filter = create_filter!(task_config, Mysql);
                let router = create_router!(task_config, Mysql);
                let conn_pool = TaskUtil::create_mysql_conn_pool(
                    &url,
                    &ConnectionAuthConfig::NoAuth,
                    parallel_size * 2,
                    enable_sqlx_log,
                    None,
                )
                .await?;
                let sinker = FoxlakeStructSinker {
                    conn_pool: conn_pool.clone(),
                    conflict_policy: conflict_policy.clone(),
                    filter,
                    router,
                    engine,
                    monitor: monitor.clone(),
                    monitor_interval,
                };
                sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
            }
        };
        Ok(sub_sinkers)
    }
}
