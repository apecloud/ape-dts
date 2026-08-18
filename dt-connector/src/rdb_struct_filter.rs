use dt_common::rdb_filter::RdbFilter;

use crate::rdb_router::RdbRouter;

#[derive(Clone)]
pub struct RdbStructFilter {
    filter: RdbFilter,
    reverse_router: Option<RdbRouter>,
}

impl RdbStructFilter {
    pub fn for_source(filter: RdbFilter) -> Self {
        Self {
            filter,
            reverse_router: None,
        }
    }

    pub fn for_target(filter: RdbFilter, router: Option<RdbRouter>) -> Self {
        Self {
            filter,
            reverse_router: router,
        }
    }

    pub fn filter_schema(&self, schema: &str) -> bool {
        let source_schema = self
            .reverse_router
            .as_ref()
            .map(|router| router.reverse_get_schema_map(schema))
            .unwrap_or(schema);
        self.filter.filter_schema(source_schema)
    }

    pub fn filter_tb(&self, schema: &str, table: &str) -> bool {
        let (source_schema, source_table) = self
            .reverse_router
            .as_ref()
            .map(|router| router.reverse_get_tb_map(schema, table))
            .unwrap_or((schema, table));
        self.filter.filter_tb(source_schema, source_table)
    }
}

#[cfg(test)]
mod tests {
    use dt_common::config::{
        config_enums::DbType, filter_config::FilterConfig, router_config::RouterConfig,
    };

    use super::*;

    fn filter(config: FilterConfig) -> RdbFilter {
        RdbFilter::from_config(&config, &DbType::Pg).unwrap()
    }

    fn router() -> RdbRouter {
        let config = RouterConfig::Rdb {
            schema_map: "src_schema:dst_schema".to_string(),
            tb_map: "src_schema.src_table:dst_schema.dst_table".to_string(),
            col_map: String::new(),
            topic_map: String::new(),
        };
        RdbRouter::from_config(&config, &DbType::Pg)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn target_filter_matches_routed_schema_and_table_by_source_names() {
        let filter = filter(FilterConfig {
            do_schemas: "src_schema".to_string(),
            ..Default::default()
        });
        let target_filter = RdbStructFilter::for_target(filter, Some(router()));

        assert!(!target_filter.filter_schema("dst_schema"));
        assert!(!target_filter.filter_tb("dst_schema", "dst_table"));
        assert!(target_filter.filter_schema("other_schema"));
    }

    #[test]
    fn target_filter_preserves_source_table_rules_after_routing() {
        let filter = filter(FilterConfig {
            do_tbs: "src_schema.src_table,src_schema.ignored_table".to_string(),
            ignore_tbs: "src_schema.ignored_table".to_string(),
            ..Default::default()
        });
        let target_filter = RdbStructFilter::for_target(filter, Some(router()));

        assert!(!target_filter.filter_tb("dst_schema", "dst_table"));
        assert!(target_filter.filter_tb("dst_schema", "ignored_table"));
    }

    #[test]
    fn target_filter_evaluates_patterns_in_the_source_namespace() {
        let filter = filter(FilterConfig {
            do_tbs: "src_*.src_*".to_string(),
            ..Default::default()
        });
        let target_filter = RdbStructFilter::for_target(filter, Some(router()));

        assert!(!target_filter.filter_tb("dst_schema", "dst_table"));
    }

    #[test]
    fn source_filter_does_not_reverse_names() {
        let filter = filter(FilterConfig {
            do_schemas: "src_schema".to_string(),
            ..Default::default()
        });
        let source_filter = RdbStructFilter::for_source(filter);

        assert!(!source_filter.filter_schema("src_schema"));
        assert!(source_filter.filter_schema("dst_schema"));
    }
}
