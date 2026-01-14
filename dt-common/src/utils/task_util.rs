use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::config::{
    extractor_config::BasicExtractorConfig, filter_config::FilterConfig,
    router_config::RouterConfig, sinker_config::BasicSinkerConfig,
};

pub struct TaskUtil {}

impl TaskUtil {
    pub fn generate_task_id(
        extractor_basic: &BasicExtractorConfig,
        sinker_basic: &BasicSinkerConfig,
        filter: &FilterConfig,
        router: &RouterConfig,
    ) -> String {
        let mut hasher = DefaultHasher::new();
        extractor_basic.hash(&mut hasher);
        sinker_basic.hash(&mut hasher);
        filter.hash(&mut hasher);
        router.hash(&mut hasher);

        format!("{:x}", hasher.finish())
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        config::{
            config_enums::{DbType, ExtractType, SinkType},
            connection_auth_config::ConnectionAuthConfig,
            extractor_config::BasicExtractorConfig,
            filter_config::FilterConfig,
            router_config::RouterConfig,
            sinker_config::BasicSinkerConfig,
        },
        utils::task_util::TaskUtil,
    };

    #[test]
    fn test_generate_task_id() {
        let extractor_config = BasicExtractorConfig {
            db_type: DbType::Mysql,
            extract_type: ExtractType::Snapshot,
            url: "mysql://localhost:3306/test".to_string(),
            connection_auth: ConnectionAuthConfig::NoAuth,
            max_connections: 10,
        };
        let sinker_config = BasicSinkerConfig {
            db_type: DbType::Mysql,
            sink_type: SinkType::Write,
            url: "mysql://localhost:3307/test".to_string(),
            connection_auth: ConnectionAuthConfig::NoAuth,
            batch_size: 0,
            max_connections: 10,
        };
        let mut filter_config = FilterConfig {
            do_schemas: "db1,db2".to_string(),
            ignore_schemas: "db3".to_string(),
            do_tbs: "db4.tb1,db5.*".to_string(),
            ignore_tbs: "".to_string(),
            ignore_cols: "".to_string(),
            do_events: "".to_string(),
            do_structures: "".to_string(),
            do_ddls: "".to_string(),
            do_dcls: "".to_string(),
            ignore_cmds: "".to_string(),
            where_conditions: "".to_string(),
            partition_cols: "".to_string(),
        };
        let router_config = RouterConfig::Rdb {
            schema_map: "db1:db1_tmp".to_string(),
            tb_map: "".to_string(),
            col_map: "".to_string(),
            topic_map: "".to_string(),
        };
        let mut generate_task_id = "".to_string();
        for _i in 0..10 {
            let new_task_id = TaskUtil::generate_task_id(
                &extractor_config,
                &sinker_config,
                &filter_config,
                &router_config,
            );
            if generate_task_id.len() > 0 {
                assert_eq!(new_task_id, generate_task_id);
            }
            generate_task_id = new_task_id
        }

        filter_config.do_schemas = "db1_tmp,db2_tmp".to_string();
        let tmp_task_id = TaskUtil::generate_task_id(
            &extractor_config,
            &sinker_config,
            &filter_config,
            &router_config,
        );
        assert_ne!(tmp_task_id, generate_task_id);
    }
}
