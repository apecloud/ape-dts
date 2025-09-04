use std::collections::HashSet;

use anyhow::bail;
use async_trait::async_trait;
use dt_common::config::{config_enums::DbType, filter_config::FilterConfig};

use crate::{
    config::precheck_config::PrecheckConfig,
    fetcher::{postgresql::pg_fetcher::PgFetcher, traits::Fetcher},
    meta::{
        check_item::CheckItem, check_result::CheckResult, db_table_model::DbTable,
        pg_enums::ConstraintTypeEnum,
    },
};

use super::{basic::BasicPrechecker, traits::Prechecker};

const PG_SUPPORT_DB_VERSION_NUM_MIN: i32 = 120000;

pub struct PostgresqlPrechecker {
    pub fetcher: PgFetcher,
    pub filter_config: FilterConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
}

#[async_trait]
impl Prechecker for PostgresqlPrechecker {
    async fn build_connection(&mut self) -> anyhow::Result<CheckResult> {
        let mut check_error = None;
        let result = self.fetcher.build_connection().await;
        match result {
            Ok(_) => {}
            Err(e) => check_error = Some(e),
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseConnection,
            self.is_source,
            DbType::Pg,
            check_error,
            None,
        ))
    }

    async fn check_database_version(&mut self) -> anyhow::Result<CheckResult> {
        let mut check_error = None;

        let result = self.fetcher.fetch_version().await;
        match result {
            Ok(version) => {
                if version.is_empty() {
                    check_error = Some(anyhow::Error::msg("found no version info"));
                } else {
                    let version_i32: i32 = version.parse().unwrap();
                    if version_i32 < PG_SUPPORT_DB_VERSION_NUM_MIN {
                        check_error = Some(anyhow::Error::msg(format!(
                            "version:{} is not supported yet",
                            version_i32
                        )));
                    }
                }
            }
            Err(e) => check_error = Some(e),
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseVersionSupported,
            self.is_source,
            DbType::Pg,
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
            // do nothing when the database is target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfDatabaseSupportCdc,
                self.is_source,
                DbType::Pg,
                check_error,
                None,
            ));
        }

        // check the cdc settings
        let configs: Vec<String> = ["wal_level", "max_wal_senders", "max_replication_slots"]
            .iter()
            .map(|c| c.to_string())
            .collect();
        let (mut max_replication_slots_i32, mut err_msgs): (i32, Vec<String>) = (0, vec![]);
        let result = self.fetcher.fetch_configuration(configs).await;
        match result {
            Ok(rows) => {
                for (k, v) in rows {
                    match k.as_str() {
                        "wal_level" => {
                            if v.to_lowercase() != "logical" {
                                err_msgs.push(format!(
                                    "wal_level should not be '{}', need to be 'logical'.",
                                    v
                                ))
                            }
                        }
                        "max_replication_slots" => {
                            max_replication_slots_i32 = v.parse().unwrap();
                            if max_replication_slots_i32 < 1 {
                                err_msgs.push(format!(
                                    "max_replication_slots needs to be greater than 0. current is '{}'",
                                    max_replication_slots_i32
                                ))
                            }
                        }
                        "max_wal_senders" => {
                            let sender_i32: i32 = v.parse().unwrap();
                            if sender_i32 < 1 {
                                err_msgs.push(format!(
                                    "max_wel_senders needs to be greater than 0, current is '{}'",
                                    sender_i32
                                ))
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => bail! {e},
        }
        if !err_msgs.is_empty() {
            check_error = Some(anyhow::Error::msg(err_msgs.join(";")));
        }

        if check_error.is_none() {
            // check the slot count is less than max_replication_slots or not
            let slot_result = self.fetcher.fetch_slot_names().await;
            match slot_result {
                Ok(slots) => {
                    if max_replication_slots_i32 == (slots.len() as i32) {
                        check_error = Some(anyhow::Error::msg(  format!("the current number of slots:[{}] has reached max_replication_slots, and new slots cannot be created", max_replication_slots_i32) ));
                    }
                }
                Err(e) => check_error = Some(e),
            }
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfDatabaseSupportCdc,
            self.is_source,
            DbType::Pg,
            check_error,
            None,
        ))
    }

    async fn check_struct_existed_or_not(&mut self) -> anyhow::Result<CheckResult> {
        let mut check_error = None;

        if !self.is_source && self.precheck_config.do_struct_init {
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfStructExisted,
                self.is_source,
                DbType::Pg,
                check_error,
                None,
            ));
        }

        let is_filter_pattern =
            BasicPrechecker::is_filter_pattern(DbType::Pg, &self.fetcher.filter);
        if is_filter_pattern {
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfStructExisted,
                self.is_source,
                DbType::Pg,
                check_error,
                Some(anyhow::Error::msg(
                    "CheckIfStructExisted with filter in pattern is not supported.",
                )),
            ));
        }

        let (mut db_tables, mut err_msgs): (Vec<DbTable>, Vec<String>) = (Vec::new(), Vec::new());
        if !self.filter_config.do_tbs.is_empty() {
            DbTable::from_str(&self.filter_config.do_tbs, &mut db_tables)
        } else if !self.filter_config.do_schemas.is_empty() {
            DbTable::from_str(&self.filter_config.do_schemas, &mut db_tables)
        }

        let (schemas, tb_schemas, tbs) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);

        // When a specific table to be migrated is specified and the following conditions are met, check the existence of the table
        // 1. this check is for the source database
        // 2. this check is for the sink database, and specified no structure initialization
        if !tbs.is_empty() {
            let mut not_existed_tbs: HashSet<String> = HashSet::new();

            let table_result = self.fetcher.fetch_tables().await;
            let current_tbs: HashSet<String> = match table_result {
                Ok(tables) => tables
                    .iter()
                    .map(|t| format!("{}.{}", t.schema_name, t.table_name))
                    .collect(),
                Err(e) => bail! {e},
            };
            for tb_key in tbs {
                if !current_tbs.contains(&tb_key) {
                    not_existed_tbs.insert(tb_key);
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

        if !all_schemas.is_empty() {
            let mut not_existed_schema: HashSet<String> = HashSet::new();
            let schema_result = self.fetcher.fetch_schemas().await;
            let current_schemas: HashSet<String> = match schema_result {
                Ok(schemas) => schemas.iter().map(|s| s.schema_name.clone()).collect(),
                Err(e) => bail! {e},
            };

            for schema in all_schemas {
                if !current_schemas.contains(schema) {
                    not_existed_schema.insert(schema.clone());
                }
            }
            if !not_existed_schema.is_empty() {
                err_msgs.push(format!(
                    "schemas not existed: [{}]",
                    not_existed_schema
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(";")
                ));
            }
        }

        if !err_msgs.is_empty() {
            check_error = Some(anyhow::Error::msg(err_msgs.join(".")))
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfStructExisted,
            self.is_source,
            DbType::Pg,
            check_error,
            None,
        ))
    }

    async fn check_table_structs(&mut self) -> anyhow::Result<CheckResult> {
        // all tables have a pk, and have no fk
        let (mut check_error, mut warn_error) = (None, None);

        if !self.is_source && self.precheck_config.do_struct_init {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfTableStructSupported,
                self.is_source,
                DbType::Pg,
                check_error,
                None,
            ));
        }

        let is_filter_pattern =
            BasicPrechecker::is_filter_pattern(DbType::Pg, &self.fetcher.filter);
        if is_filter_pattern {
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfTableStructSupported,
                self.is_source,
                DbType::Pg,
                check_error,
                Some(anyhow::Error::msg(
                    "CheckIfTableStructSupported with filter in pattern is not supported.",
                )),
            ));
        }

        let (mut db_tables, mut err_msgs, mut warn_msgs): (Vec<DbTable>, Vec<String>, Vec<String>) =
            (Vec::new(), Vec::new(), Vec::new());
        if !self.filter_config.do_tbs.is_empty() {
            DbTable::from_str(&self.filter_config.do_tbs, &mut db_tables)
        } else if !self.filter_config.do_schemas.is_empty() {
            DbTable::from_str(&self.filter_config.do_schemas, &mut db_tables)
        }

        let (schemas, tb_schemas, _) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);
        if all_schemas.is_empty() {
            println!("found no schema need to do migrate, very strange");
            bail! {
            "found no schema need to do migrate"};
        }

        let (mut has_pkuk_tables, mut fkref_nonexists_tables): (HashSet<String>, HashSet<String>) =
            (HashSet::new(), HashSet::new());

        let table_result = self.fetcher.fetch_tables().await;
        let current_tables: HashSet<String> = match table_result {
            Ok(tables) => tables
                .iter()
                .map(|t| format!("{}.{}", t.schema_name, t.table_name))
                .collect(),
            Err(e) => bail! {e},
        };

        let constraint_result = self.fetcher.fetch_constraints().await;
        match constraint_result {
            Ok(constraints) => {
                for c in constraints {
                    let schema_table_name = format!("{}.{}", c.schema_name, c.table_name);
                    if c.constraint_type == ConstraintTypeEnum::Primary.to_str().unwrap()
                        || c.constraint_type == ConstraintTypeEnum::Unique.to_str().unwrap()
                    {
                        has_pkuk_tables.insert(schema_table_name);
                    } else if c.constraint_type == ConstraintTypeEnum::Foreign.to_str().unwrap()
                        && self
                            .fetcher
                            .filter
                            .filter_tb(c.rel_schema_name.as_str(), &c.rel_table_name)
                    {
                        fkref_nonexists_tables.insert(schema_table_name);
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
            ));
        }

        let mut no_pkuk_tables: HashSet<String> = HashSet::new();
        for current_table in current_tables {
            if !has_pkuk_tables.contains(&current_table) {
                no_pkuk_tables.insert(current_table);
            }
        }
        if !no_pkuk_tables.is_empty() {
            warn_msgs.push(format!(
                "primary key or unique key are needed, but these tables don't have any:[{}]",
                no_pkuk_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ));
        }
        if !err_msgs.is_empty() {
            check_error = Some(anyhow::Error::msg(err_msgs.join(";")))
        }
        if !warn_msgs.is_empty() {
            warn_error = Some(anyhow::Error::msg(warn_msgs.join(";")))
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfTableStructSupported,
            self.is_source,
            DbType::Pg,
            check_error,
            warn_error,
        ))
    }
}
