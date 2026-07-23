use std::vec;

use dt_common::{
    config::{config_enums::DbType, task_config::TaskConfig},
    error::{DtError, DtErrorContextExt, EndpointRole, ErrorCode},
    rdb_filter::RdbFilter,
};

use crate::{
    config::precheck_config::PrecheckConfig,
    fetcher::{
        mongo::mongo_fetcher::MongoFetcher, mysql::mysql_fetcher::MysqlFetcher,
        postgresql::pg_fetcher::PgFetcher, redis::redis_fetcher::RedisFetcher,
    },
    meta::check_result::CheckResult,
    prechecker::{
        mongo_prechecker::MongoPrechecker, mysql_prechecker::MySqlPrechecker,
        pg_prechecker::PostgresqlPrechecker, redis_prechecker::RedisPrechecker, traits::Prechecker,
    },
};

pub struct PrecheckerBuilder {
    precheck_config: PrecheckConfig,
    task_config: TaskConfig,
}

impl PrecheckerBuilder {
    pub fn build(precheck_config: PrecheckConfig, task_config: TaskConfig) -> Self {
        Self {
            precheck_config,
            task_config,
        }
    }

    pub fn valid_config(&self) -> bool {
        !self.task_config.extractor_basic.url.is_empty()
            && !self.task_config.sinker_basic.url.is_empty()
    }

    pub fn build_checker(
        &self,
        is_source: bool,
    ) -> anyhow::Result<Option<Box<dyn Prechecker + Send>>> {
        let (db_type, url, connection_auth, is_direct_connection) = if is_source {
            (
                self.task_config.extractor_basic.db_type.clone(),
                self.task_config.extractor_basic.url.clone(),
                self.task_config.extractor_basic.connection_auth.clone(),
                self.task_config.extractor_basic.is_direct_connection,
            )
        } else {
            (
                self.task_config.sinker_basic.db_type.clone(),
                self.task_config.sinker_basic.url.clone(),
                self.task_config.sinker_basic.connection_auth.clone(),
                self.task_config.sinker_basic.is_direct_connection,
            )
        };

        let filter = RdbFilter::from_config(&self.task_config.filter, &db_type)?;
        let checker: Option<Box<dyn Prechecker + Send>> = match db_type {
            DbType::Mysql => Some(Box::new(MySqlPrechecker {
                filter_config: self.task_config.filter.clone(),
                precheck_config: self.precheck_config.clone(),
                is_source,
                fetcher: MysqlFetcher {
                    pool: None,
                    url,
                    connection_auth,
                    is_source,
                    filter,
                },
            })),
            DbType::Pg => Some(Box::new(PostgresqlPrechecker {
                filter_config: self.task_config.filter.clone(),
                precheck_config: self.precheck_config.clone(),
                is_source,
                fetcher: PgFetcher {
                    pool: None,
                    url,
                    connection_auth,
                    is_source,
                    filter,
                },
            })),
            DbType::Mongo => Some(Box::new(MongoPrechecker {
                fetcher: MongoFetcher {
                    pool: None,
                    url,
                    connection_auth,
                    is_direct_connection,
                    is_source,
                    filter,
                },
                filter_config: self.task_config.filter.clone(),
                precheck_config: self.precheck_config.clone(),
                is_source,
            })),
            DbType::Redis => Some(Box::new(RedisPrechecker {
                fetcher: RedisFetcher {
                    conn: None,
                    url,
                    connection_auth,
                    is_source,
                    filter,
                },
                task_config: self.task_config.clone(),
                precheck_config: self.precheck_config.clone(),
                is_source,
            })),
            _ => None,
        };
        Ok(checker)
    }

    pub async fn check(&self) -> anyhow::Result<Vec<anyhow::Result<CheckResult>>> {
        if !self.valid_config() {
            return Err(DtError::InvalidConfig("precheck config is invalid".to_string()).into());
        }
        let source_checker_option = self
            .build_checker(true)
            .map_err(|error| error.with_endpoint(EndpointRole::Source))?;
        let sink_checker_option = self
            .build_checker(false)
            .map_err(|error| error.with_endpoint(EndpointRole::Destination))?;
        let (Some(mut source_checker), Some(mut sink_checker)) =
            (source_checker_option, sink_checker_option)
        else {
            return Err(DtError::InvalidConfig(
                "failed to build precheck checker from database type".to_string(),
            )
            .into());
        };

        println!("[*]begin to check the connection");
        let check_source_connection = source_checker
            .build_connection()
            .await
            .map_err(|error| error.with_endpoint(EndpointRole::Source))?;
        let check_sink_connection = sink_checker
            .build_connection()
            .await
            .map_err(|error| error.with_endpoint(EndpointRole::Destination))?;

        // if connection failed, no need to do other check
        if !check_source_connection.is_validate || !check_sink_connection.is_validate {
            let (error_code, endpoint) = if !check_source_connection.is_validate {
                (
                    check_source_connection
                        .error_code
                        .unwrap_or(ErrorCode::ConnectionFailed),
                    EndpointRole::Source,
                )
            } else {
                (
                    check_sink_connection
                        .error_code
                        .unwrap_or(ErrorCode::ConnectionFailed),
                    EndpointRole::Destination,
                )
            };
            check_source_connection.log();
            check_sink_connection.log();
            return Err(anyhow::anyhow!("database connection precheck failed")
                .with_code(error_code)
                .with_endpoint(endpoint));
        }

        let mut check_results: Vec<anyhow::Result<CheckResult>> = vec![];
        check_results.push(Ok(check_source_connection));
        check_results.push(Ok(check_sink_connection));

        println!("[*]begin to check the database version");
        check_results.push(
            source_checker
                .check_database_version()
                .await
                .map_err(|error| error.with_endpoint(EndpointRole::Source)),
        );
        check_results.push(
            sink_checker
                .check_database_version()
                .await
                .map_err(|error| error.with_endpoint(EndpointRole::Destination)),
        );

        if self.precheck_config.do_cdc {
            println!("[*]begin to check the cdc setting");
            check_results.push(
                source_checker
                    .check_cdc_supported()
                    .await
                    .map_err(|error| error.with_endpoint(EndpointRole::Source)),
            );
        }

        println!("[*]begin to check the if the structs is existed or not");
        check_results.push(
            source_checker
                .check_struct_existed_or_not()
                .await
                .map_err(|error| error.with_endpoint(EndpointRole::Source)),
        );
        check_results.push(
            sink_checker
                .check_struct_existed_or_not()
                .await
                .map_err(|error| error.with_endpoint(EndpointRole::Destination)),
        );

        println!("[*]begin to check the database structs");
        check_results.push(
            source_checker
                .check_table_structs()
                .await
                .map_err(|error| error.with_endpoint(EndpointRole::Source)),
        );
        check_results.push(
            sink_checker
                .check_table_structs()
                .await
                .map_err(|error| error.with_endpoint(EndpointRole::Destination)),
        );

        Ok(check_results)
    }

    pub async fn verify_check_result(&self) -> anyhow::Result<()> {
        let check_results = self.check().await;
        match check_results {
            Ok(results) => {
                println!("check result:");
                let mut error_count = 0;
                let mut first_error_code = None;
                let mut first_error_endpoint = None;
                for check_result in results {
                    match check_result {
                        Ok(result) => {
                            result.log();
                            if !result.is_validate {
                                error_count += 1;
                                first_error_code = first_error_code.or(result.error_code);
                                first_error_endpoint =
                                    first_error_endpoint.or(Some(if result.is_source {
                                        EndpointRole::Source
                                    } else {
                                        EndpointRole::Destination
                                    }));
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
                if error_count > 0 {
                    let mut error = anyhow::anyhow!("one or more prerequisite checks failed")
                        .with_code(first_error_code.unwrap_or(ErrorCode::PrerequisiteNotMet));
                    if let Some(endpoint) = first_error_endpoint {
                        error = error.with_endpoint(endpoint);
                    }
                    Err(error)
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(e),
        }
    }
}
