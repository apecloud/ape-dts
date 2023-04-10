use std::vec;

use dt_common::{
    config::{
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    meta::db_enums::DbType,
};

use crate::{
    checker::{
        mysql_checker::MySqlChecker, postgresql_checker::PostgresqlChecker, traits::Checker,
    },
    config::precheck_config::PrecheckConfig,
    error::Error,
    meta::check_result::CheckResult,
};

pub struct CheckerConnector {
    precheck_config: PrecheckConfig,
    task_config: TaskConfig,
}

impl CheckerConnector {
    pub fn build(precheck_config: PrecheckConfig, task_config: TaskConfig) -> Self {
        Self {
            precheck_config,
            task_config,
        }
    }

    pub fn valid_config(&self) -> Result<bool, Error> {
        match &self.task_config.extractor {
            ExtractorConfig::BasicConfig { url, db_type: _ } => {
                if url.is_empty() {
                    return Ok(false);
                }
            }
            _ => {}
        }
        match &self.task_config.sinker {
            SinkerConfig::BasicConfig { url, db_type: _ } => {
                if url.is_empty() {
                    return Ok(false);
                }
            }
            _ => {}
        }
        Ok(true)
    }

    pub fn build_checker(&self, is_source: bool) -> Option<Box<dyn Checker + Send>> {
        let mut db_type_option: Option<&DbType> = None;
        if is_source {
            match &self.task_config.extractor {
                ExtractorConfig::BasicConfig { url: _, db_type } => db_type_option = Some(db_type),
                _ => {}
            }
        } else {
            match &self.task_config.sinker {
                SinkerConfig::BasicConfig { url: _, db_type } => db_type_option = Some(db_type),
                _ => {}
            }
        }
        if db_type_option.is_none() {
            println!("build checker failed, maybe config is wrong");
            return None;
        }
        let checker: Option<Box<dyn Checker + Send>> = match db_type_option.unwrap() {
            DbType::Mysql => Some(Box::new(MySqlChecker {
                pool: None,
                source_config: self.task_config.extractor.clone(),
                filter_config: self.task_config.filter.clone(),
                sinker_config: self.task_config.sinker.clone(),
                router_config: self.task_config.router.clone(),
                precheck_config: self.precheck_config.clone(),
                db_type_option: None,
                is_source,
            })),
            DbType::Pg => Some(Box::new(PostgresqlChecker {
                pool: None,
                source_config: self.task_config.extractor.clone(),
                filter_config: self.task_config.filter.clone(),
                sinker_config: self.task_config.sinker.clone(),
                router_config: self.task_config.router.clone(),
                precheck_config: self.precheck_config.clone(),
                db_type_option: None,
                is_source,
            })),
        };
        checker
    }

    pub async fn check(&self) -> Result<(), Error> {
        if !self.valid_config().unwrap() {
            return Err(Error::PreCheckError {
                error: "config is invalid.".to_string(),
            });
        }
        let (source_checker_option, sink_checker_option) =
            (self.build_checker(true), self.build_checker(false));
        if source_checker_option.is_none() || sink_checker_option.is_none() {
            return Err(Error::PreCheckError {
                error: "config is invalid when build checker.maybe db_type is wrong.".to_string(),
            });
        }
        let (mut source_checker, mut sink_checker) =
            (source_checker_option.unwrap(), sink_checker_option.unwrap());
        let mut check_results: Vec<Result<CheckResult, Error>> = vec![];

        println!("[*]begin to check the connection");
        let check_source_connection = source_checker.build_connection().await;
        let check_sink_connection = sink_checker.build_connection().await;
        // if connection failed, no need to do other check
        if check_source_connection.is_err() {
            return Err(check_source_connection.err().unwrap());
        }
        if check_sink_connection.is_err() {
            return Err(check_sink_connection.err().unwrap());
        }
        check_results.push(check_source_connection.clone());
        check_results.push(check_sink_connection.clone());
        if !&check_source_connection.unwrap().is_validate
            || !&check_sink_connection.unwrap().is_validate
        {
            for connection_check in check_results {
                let result_tmp = connection_check.unwrap();
                result_tmp.log();
            }
            return Err(Error::PreCheckError {
                error: "connection failed, precheck not passed.".to_string(),
            });
        }

        println!("[*]begin to check the database version");
        check_results.push(source_checker.check_database_version().await);
        check_results.push(sink_checker.check_database_version().await);

        if self.precheck_config.do_cdc {
            println!("[*]begin to check the cdc setting");
            check_results.push(source_checker.check_cdc_supported().await);
        }

        println!("[*]begin to check the database structs");
        check_results.push(source_checker.check_table_structs().await);

        println!("check result:");
        let mut error_count = 0;
        for check_result in check_results {
            match check_result {
                Ok(result) => {
                    result.log();
                    if !result.is_validate {
                        error_count += 1;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        if error_count > 0 {
            Err(Error::PreCheckError {
                error: "precheck not passed.".to_string(),
            })
        } else {
            Ok(())
        }
    }
}