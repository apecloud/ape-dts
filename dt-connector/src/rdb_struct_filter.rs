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
        self.filter_tb_with_db("", schema, table)
    }

    pub fn filter_tb_with_db(&self, db: &str, schema: &str, table: &str) -> bool {
        let (source_db, source_schema, source_table) = self
            .reverse_router
            .as_ref()
            .map(|router| router.reverse_get_tb_map_with_db(db, schema, table))
            .unwrap_or((db, schema, table));
        self.filter
            .filter_tb_with_db(source_db, source_schema, source_table)
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

    #[test]
    fn target_filter_reverses_mssql_database_schema_and_table() {
        let filter = RdbFilter::from_config(
            &FilterConfig {
                do_schemas: "[source.db?]".to_string(),
                do_tbs: "[source.db?].[schema.*].[table?]".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();
        let config = RouterConfig::Rdb {
            schema_map: "[source.db?]:[target.db?]".to_string(),
            tb_map: "[source.db?].[schema.*].[table?]:[target.db?].[routed.*].[target?]"
                .to_string(),
            col_map: String::new(),
            topic_map: String::new(),
        };
        let router = RdbRouter::from_config(&config, &DbType::Mssql)
            .unwrap()
            .unwrap();
        let target_filter = RdbStructFilter::for_target(filter, Some(router));

        assert!(!target_filter.filter_schema("target.db?"));
        assert!(!target_filter.filter_tb_with_db("target.db?", "routed.*", "target?"));
        assert!(target_filter.filter_tb_with_db("other.db?", "routed.*", "other"));
    }
}
