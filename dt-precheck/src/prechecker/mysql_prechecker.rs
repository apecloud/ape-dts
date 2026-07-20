use std::collections::HashSet;

use anyhow::bail;
use async_trait::async_trait;
use dt_common::config::{config_enums::DbType, filter_config::FilterConfig};
use regex::Regex;

use crate::{
    config::precheck_config::PrecheckConfig,
    fetcher::{mysql::mysql_fetcher::MysqlFetcher, traits::Fetcher},
    meta::{check_item::CheckItem, check_result::CheckResult, db_table_model::DbTable},
    prechecker::basic::BasicPrechecker,
};

use super::traits::Prechecker;
use crate::error::failure as precheck_failure;
use dt_common::error::ErrorCode;

const MYSQL_SUPPORT_DB_VERSION_REGEX: &str = r"5\..*|8\..*";

pub struct MySqlPrechecker {
    pub fetcher: MysqlFetcher,
    pub filter_config: FilterConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
}

#[async_trait]
impl Prechecker for MySqlPrechecker {
    async fn build_connection(&mut self) -> anyhow::Result<CheckResult> {
        let check_error = self.fetcher.build_connection().await.err();
        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseConnection,
            self.is_source,
            DbType::Mysql,
            check_error,
            None,
        ))
    }

    // support MySQL 8.*
    async fn check_database_version(&mut self) -> anyhow::Result<CheckResult> {
        let mut check_error = None;

        let result = self.fetcher.fetch_version().await;
        match result {
            Ok(version) => {
                if version.is_empty() {
                    check_error = Some(precheck_failure(
                        ErrorCode::UnsupportedDatabaseVersion,
                        "MySQL returned no version information",
                        self.is_source,
                        "check_mysql_version",
                    ));
                } else {
                    let re = Regex::new(MYSQL_SUPPORT_DB_VERSION_REGEX)?;
                    if !re.is_match(version.as_str()) {
                        check_error = Some(precheck_failure(
                            ErrorCode::UnsupportedDatabaseVersion,
                            format!("MySQL version {version} is not supported"),
                            self.is_source,
                            "check_mysql_version",
                        ));
                    }
                }
            }
            Err(e) => check_error = Some(e),
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseVersionSupported,
            self.is_source,
            DbType::Mysql,
            check_error,
            None,
        ))
    }

    async fn check_permission(&mut self) -> anyhow::Result<CheckResult> {
        Ok(CheckResult::build(
            CheckItem::CheckAccountPermission,
            self.is_source,
        ))
    }

    async fn check_cdc_supported(&mut self) -> anyhow::Result<CheckResult> {
        let mut check_error = None;

        if !self.is_source {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfDatabaseSupportCdc,
                self.is_source,
                DbType::Mysql,
                check_error,
                None,
            ));
        }

        let mut errs: Vec<String> = vec![];
        let cdc_configs = ["log_bin", "binlog_format", "binlog_row_image"]
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<String>>();
        let result = self.fetcher.fetch_configuration(cdc_configs).await;
        match result {
            Ok(configs) => {
                for (k, v) in configs {
                    match k.as_str() {
                        "log_bin" => {
                            if v.to_lowercase() != "on" {
                                errs.push(format!(
                                    "log_bin setting:[{}] is not 'on'.",
                                    v.to_lowercase()
                                ));
                            }
                        }
                        "binlog_row_image" => {
                            if v.to_lowercase() != "full" {
                                errs.push(format!(
                                    "binlog_row_image setting:[{}] is not 'full'",
                                    v.to_lowercase()
                                ));
                            }
                        }
                        "binlog_format" => {
                            if v.to_lowercase() != "row" {
                                errs.push(format!(
                                    "binlog_format setting:[{}] is not 'row'.",
                                    v.to_lowercase()
                                ));
                            }
                        }
                        _ => {
                            bail! {
                            "find database cdc settings meet unknown error"}
                        }
                    }
                }
            }
            Err(e) => bail! {e},
        }
        if !errs.is_empty() {
            check_error = Some(precheck_failure(
                ErrorCode::CdcNotEnabled,
                errs.join(";"),
                self.is_source,
                "check_mysql_cdc_configuration",
            ))
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfDatabaseSupportCdc,
            self.is_source,
            DbType::Mysql,
            check_error,
            None,
        ))
    }

    async fn check_struct_existed_or_not(&mut self) -> anyhow::Result<CheckResult> {
        let mut check_error = None;

        if !self.is_source && self.precheck_config.do_struct_init {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfStructExisted,
                self.is_source,
                DbType::Mysql,
                check_error,
                None,
            ));
        }

        let is_filter_pattern =
            BasicPrechecker::is_filter_pattern(DbType::Mysql, &self.fetcher.filter);
        if is_filter_pattern {
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfStructExisted,
                self.is_source,
                DbType::Mysql,
                check_error,
                Some(precheck_failure(
                    ErrorCode::ObjectNotFound,
                    "structure existence precheck does not support pattern filters",
                    self.is_source,
                    "check_mysql_structure_existence",
                )),
            ));
        }

        let (mut models, mut err_msgs): (Vec<DbTable>, Vec<String>) = (Vec::new(), Vec::new());
        if !self.filter_config.do_tbs.is_empty() {
            DbTable::from_str(&self.filter_config.do_tbs, &mut models)
        } else if !self.filter_config.do_schemas.is_empty() {
            DbTable::from_str(&self.filter_config.do_schemas, &mut models)
        }

        let (dbs, tb_dbs, tbs) = DbTable::get_config_maps(&models)?;
        let mut all_db_names = Vec::new();
        all_db_names.extend(&dbs);
        all_db_names.extend(&tb_dbs);

        // When a specific table to be migrated is specified and the following conditions are met, check the existence of the table
        // 1. this check is for the source database
        // 2. this check is for the sink database, and specified no structure initialization
        if !tbs.is_empty() {
            let mut not_existed_tbs: HashSet<String> = HashSet::new();

            let tables_result = self.fetcher.fetch_tables().await;
            let current_tbs: HashSet<String> = match tables_result {
                Ok(tables) => tables
                    .iter()
                    .map(|t| format!("{}.{}", t.database_name, t.table_name))
                    .collect(),
                Err(e) => bail! {e},
            };
            for tb in tbs {
                if !current_tbs.contains(&tb) {
                    not_existed_tbs.insert(tb);
                }
            }
            if !not_existed_tbs.is_empty() {
                err_msgs.push(format!(
                    "tables not existed: [{}]",
                    not_existed_tbs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(";")
                ));
            }
        }

        if !all_db_names.is_empty() {
            let mut not_existed_dbs: HashSet<String> = HashSet::new();

            let dbs_result = self.fetcher.fetch_databases().await;
            let current_dbs: HashSet<String> = match dbs_result {
                Ok(dbs) => dbs.iter().map(|d| d.database_name.clone()).collect(),
                Err(e) => bail! {e},
            };
            for db_name in all_db_names {
                if !current_dbs.contains(db_name) {
                    not_existed_dbs.insert(db_name.clone());
                }
            }
            if !not_existed_dbs.is_empty() {
                err_msgs.push(format!(
                    "databases not existed: [{}]",
                    not_existed_dbs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(";")
                ));
            }
        }

        if !err_msgs.is_empty() {
            check_error = Some(precheck_failure(
                ErrorCode::ObjectNotFound,
                err_msgs.join("."),
                self.is_source,
                "check_mysql_structure_existence",
            ))
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfStructExisted,
            self.is_source,
            DbType::Mysql,
            check_error,
            None,
        ))
    }

    async fn check_table_structs(&mut self) -> anyhow::Result<CheckResult> {
        let (mut check_error, mut warn_error) = (None, None);

        if !self.is_source && self.precheck_config.do_struct_init {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfTableStructSupported,
                self.is_source,
                DbType::Mysql,
                check_error,
                warn_error,
            ));
        }

        let is_filter_pattern =
            BasicPrechecker::is_filter_pattern(DbType::Mysql, &self.fetcher.filter);
        if is_filter_pattern {
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfTableStructSupported,
                self.is_source,
                DbType::Mysql,
                check_error,
                Some(precheck_failure(
                    ErrorCode::UnsupportedTableStructure,
                    "table structure precheck does not support pattern filters",
                    self.is_source,
                    "check_mysql_table_structures",
                )),
            ));
        }

        let mut models: Vec<DbTable> = Vec::new();
        if !self.filter_config.do_tbs.is_empty() {
            DbTable::from_str(&self.filter_config.do_tbs, &mut models)
        } else if !self.filter_config.do_schemas.is_empty() {
            DbTable::from_str(&self.filter_config.do_schemas, &mut models)
        }
        let (dbs, tb_dbs, _) = DbTable::get_config_maps(&models)?;
        let mut all_db_names = Vec::new();
        all_db_names.extend(&dbs);
        all_db_names.extend(&tb_dbs);

        let (
            mut has_pkuk_tables,
            mut fkref_nonexists_tables,
            mut no_pkuk_tables,
            mut err_msgs,
            mut warn_msgs,
        ): (
            HashSet<String>,
            HashSet<String>,
            HashSet<String>,
            Vec<String>,
            Vec<String>,
        ) = (
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            Vec::new(),
            Vec::new(),
        );

        let constraints_result = self.fetcher.fetch_constraints().await;
        match constraints_result {
            Ok(constraints) => {
                for constraint in constraints {
                    let db_tb_name =
                        format!("{}.{}", constraint.database_name, constraint.table_name);
                    match constraint.constraint_type.as_str() {
                        "PRIMARY KEY" => has_pkuk_tables.insert(db_tb_name),
                        "UNIQUE" => has_pkuk_tables.insert(db_tb_name),
                        "FOREIGN KEY" => {
                            if !constraint.rel_database_name.is_empty()
                                && !constraint.rel_table_name.is_empty()
                            {
                                let rel_db_tb_name = format!(
                                    "{}.{}",
                                    constraint.rel_database_name, constraint.rel_table_name
                                );
                                if self.fetcher.filter.filter_tb(
                                    &constraint.rel_database_name,
                                    &constraint.rel_table_name,
                                ) {
                                    fkref_nonexists_tables.insert(rel_db_tb_name);
                                }
                            }
                            true
                        }
                        _ => true,
                    };
                }
            }
            Err(e) => bail! {e},
        }

        let tables_result = self.fetcher.fetch_tables().await;
        match tables_result {
            Ok(tables) => {
                for table in tables {
                    let db_tb_name = format!("{}.{}", table.database_name, table.table_name);
                    if !has_pkuk_tables.contains(&db_tb_name) {
                        no_pkuk_tables.insert(db_tb_name);
                    }
                }
            }
            Err(e) => bail! {e},
        }

        if !fkref_nonexists_tables.is_empty() {
            err_msgs.push(format!(
                "the following foreign key dependent tables are not defined in the replication object:[{}]",
                fkref_nonexists_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ))
        }

        if !no_pkuk_tables.is_empty() {
            warn_msgs.push(format!(
                "primary key or unique key are needed, but these tables don't have any:[{}]",
                no_pkuk_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ))
        }
        if !err_msgs.is_empty() {
            check_error = Some(precheck_failure(
                ErrorCode::UnsupportedTableStructure,
                err_msgs.join(";"),
                self.is_source,
                "check_mysql_table_structures",
            ))
        }
        if !warn_msgs.is_empty() {
            warn_error = Some(precheck_failure(
                ErrorCode::UnsupportedTableStructure,
                warn_msgs.join(";"),
                self.is_source,
                "check_mysql_table_structures",
            ))
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfTableStructSupported,
            self.is_source,
            DbType::Mysql,
            check_error,
            warn_error,
        ))
    }
}
