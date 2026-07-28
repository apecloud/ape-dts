use dt_common::config::config_enums::DbType;

use super::check_item::CheckItem;

#[derive(Debug, Clone)]
pub struct CheckResult {
    pub check_type_name: String,
    pub check_desc: String,
    pub is_validate: bool,
    pub error_msg: String,
    pub warn_msg: String,
    pub is_source: bool,
    pub advise_msg: String,
}

impl CheckResult {
    pub fn build(check_item: CheckItem, is_source: bool) -> Self {
        Self {
            check_type_name: check_item.to_string(),
            check_desc: String::from(""),
            is_validate: true,
            error_msg: String::from(""),
            warn_msg: String::from(""),
            is_source,
            advise_msg: String::from(""),
        }
    }

    pub fn build_with_err(
        check_item: CheckItem,
        is_source: bool,
        db_type: DbType,
        err_option: Option<anyhow::Error>,
        warn_option: Option<anyhow::Error>,
    ) -> Self {
        let check_desc;
        let mut advise_msg = String::new();
        let mut source_or_sink = String::from("source");
        if !is_source {
            source_or_sink = String::from("destination");
        }

        match check_item {
            CheckItem::CheckDatabaseConnection => {
                check_desc = format!("check if the {} database can be connected.", source_or_sink);
                advise_msg = "(1)check whether the account password is correct.(2)check if the network configuration is correct.".to_string();
            }
            CheckItem::CheckIfDatabaseSupportCdc => {
                check_desc = format!(
                    "check whether the {} database configuration supports cdc",
                    source_or_sink
                );
                match db_type {
                    DbType::Mysql => advise_msg = "(1)open 'log_bin' configuration. (2)set 'binlog_format' configuration to 'row'. (3)set 'binlog_row_image' configuration to 'full'.".to_string(),
                    DbType::Pg => advise_msg = "(1)set 'wal_level' configuration to 'logical'. (2)make sure that the number of 'max_replication_slots' configured is sufficient. (3)make sure that the number of 'max_wal_senders' configured is sufficient.".to_string(),
                    DbType::Mongo => advise_msg = "make sure that the configured link address is the master node under a replica set architecture.".to_string(),
                    _ => {}
                }
            }
            CheckItem::CheckAccountPermission => {
                // Todo:
                check_desc = "check account permission".to_string();
                advise_msg = "advise account permission".to_string();
            }
            CheckItem::CheckIfStructExisted => {
                check_desc = format!(
                    "check whether the data structure of the {} database is existed",
                    source_or_sink
                );
                advise_msg = "manually created the missing struct.".to_string();
            }
            CheckItem::CheckIfTableStructSupported => {
                check_desc = format!(
                    "check whether the data structure of the {} database to be migrated supports",
                    source_or_sink
                );
                advise_msg = "no primary key tables and foreign key tables are currently not supported.these tables can be removed from the migration object.".to_string();
            }
            CheckItem::CheckDatabaseVersionSupported => {
                check_desc = format!("check if the {} database version supports.", source_or_sink);
                let mut advise_version = String::new();
                match db_type {
                    DbType::Mysql => {
                        advise_version = "currently supports version '8.*'.".to_string()
                    }
                    DbType::Pg => advise_version = "currently supports version '14.*'.".to_string(),
                    DbType::Mongo => {
                        advise_version = "currently supports version '5.*', '6.0.*'.".to_string()
                    }
                    _ => {}
                }
                advise_msg = format!("{} wait for the next release.", advise_version);
            }
        }
        let warn_msg = warn_option
            .map(|error| error.to_string())
            .unwrap_or_default();
        let has_warning = !warn_msg.is_empty();
        match err_option {
            Some(error) => Self {
                check_type_name: check_item.to_string(),
                check_desc,
                is_validate: false,
                error_msg: error.to_string(),
                warn_msg,
                is_source,
                advise_msg,
            },
            None => Self {
                check_type_name: check_item.to_string(),
                check_desc,
                is_validate: true,
                error_msg: String::from(""),
                warn_msg,
                is_source,
                advise_msg: if has_warning {
                    advise_msg
                } else {
                    String::new()
                },
            },
        }
    }

    pub fn log(&self) {
        println!("======================================");
        println!("[check_type_name]:{} \n[is_validate]:{} \n[check_desc]:{} \n[error_message]:{} \n[warn_message]:{} \n[advise_message]:{}\n", self.check_type_name, self.is_validate, self.check_desc, self.error_msg, self.warn_msg, self.advise_msg);
    }
}
