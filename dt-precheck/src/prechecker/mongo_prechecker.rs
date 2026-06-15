use async_trait::async_trait;
use dt_common::config::{config_enums::DbType, filter_config::FilterConfig};
use mongodb::bson::Bson;
use regex::Regex;

use crate::{
    config::precheck_config::PrecheckConfig,
    fetcher::{mongo::mongo_fetcher::MongoFetcher, traits::Fetcher},
    meta::{check_item::CheckItem, check_result::CheckResult},
};

use super::traits::Prechecker;

const MONGO_SUPPORTED_VERSION_REGEX: &str = r"4.*|5.0.*|6.0.*|7.0.*";

pub struct MongoPrechecker {
    pub fetcher: MongoFetcher,
    pub filter_config: FilterConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
}

#[async_trait]
impl Prechecker for MongoPrechecker {
    async fn build_connection(&mut self) -> anyhow::Result<CheckResult> {
        self.fetcher.build_connection().await?;
        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseConnection,
            self.is_source,
            DbType::Mongo,
            None,
            None,
        ))
    }

    async fn check_database_version(&mut self) -> anyhow::Result<CheckResult> {
        let mut check_error = None;

        let version = self.fetcher.fetch_version().await?;
        let reg = Regex::new(MONGO_SUPPORTED_VERSION_REGEX).unwrap();
        if !reg.is_match(version.as_str()) {
            check_error = Some(anyhow::Error::msg(format!(
                "mongo version:[{}] is invalid.",
                version
            )));
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseVersionSupported,
            self.is_source,
            DbType::Mongo,
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
                DbType::Mongo,
                check_error,
                None,
            ));
        }

        // 1. replSet used
        // 2. the specify url is the master
        // let random_db = self.fetcher.get_random_db()?;
        let rs_status = self.fetcher.execute_for_admin("hello").await?;

        let (ok, primary, me): (bool, &str, &str) = (
            rs_status.get("ok").and_then(Bson::as_f64).unwrap_or(0.0) >= 1.0,
            rs_status
                .get("primary")
                .and_then(Bson::as_str)
                .unwrap_or(""),
            rs_status.get("me").and_then(Bson::as_str).unwrap_or(""),
        );
        let is_mongos = rs_status.get("msg").and_then(Bson::as_str) == Some("isdbgrid");

        let mut err_msg = "";
        if !ok {
            err_msg = "fetching mongodb instance status with 'db.hello()' failed.";
        } else if is_mongos {
            err_msg = "";
        } else if primary.is_empty() || me.is_empty() {
            err_msg = "mongodb is not a replicaSet architecture.";
        } else if primary != me {
            err_msg = "the mongodb instance is not a master.";
        }

        if !err_msg.is_empty() {
            check_error = Some(anyhow::Error::msg(err_msg));
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfDatabaseSupportCdc,
            self.is_source,
            DbType::Mongo,
            check_error,
            None,
        ))
    }

    async fn check_struct_existed_or_not(&mut self) -> anyhow::Result<CheckResult> {
        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfStructExisted,
            self.is_source,
            DbType::Mongo,
            None,
            None,
        ))
    }

    async fn check_table_structs(&mut self) -> anyhow::Result<CheckResult> {
        let mut check_error = None;

        let invalid_dbs = vec!["admin", "local"];
        for db in invalid_dbs {
            if !self.fetcher.filter.filter_schema(db) {
                check_error = Some(anyhow::Error::msg(
                    "database 'admin' and 'local' are not supported as source and target.",
                ));
                break;
            }
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfTableStructSupported,
            self.is_source,
            DbType::Mongo,
            check_error,
            None,
        ))
    }
}
