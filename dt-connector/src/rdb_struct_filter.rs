use dt_common::filter::DtFilter;
use dt_common::{resource::RID, rid, router::DtRouter};

#[derive(Clone)]
pub struct RdbStructFilter {
    filter: DtFilter,
    reverse_router: Option<DtRouter>,
}

impl RdbStructFilter {
    pub fn for_source(filter: DtFilter) -> Self {
        Self {
            filter,
            reverse_router: None,
        }
    }

    pub fn for_target(filter: DtFilter, router: Option<DtRouter>) -> Self {
        Self {
            filter,
            reverse_router: router,
        }
    }

    pub fn filter_schema(&self, schema: &str) -> anyhow::Result<bool> {
        self.filter_id(rid!(schema))
    }

    pub fn filter_tb(&self, schema: &str, table: &str) -> anyhow::Result<bool> {
        self.filter_id(rid!(schema, table))
    }

    pub fn filter_tb_with_db(&self, db: &str, schema: &str, table: &str) -> anyhow::Result<bool> {
        self.filter_id(rid!(db, schema, table))
    }

    fn filter_id(&self, id: RID) -> anyhow::Result<bool> {
        let source = match &self.reverse_router {
            Some(router) => router.reverse(id)?,
            None => id,
        };
        self.filter.filter(source)
    }
}

#[cfg(test)]
mod tests {
    use dt_common::config::{
        config_enums::DbType,
        filter_config::{FilterConfig, LegacyFilterConfig},
        router_config::{LegacyRouterConfig, RouterConfig},
    };

    use super::*;

    #[test]
    fn pg_filters_use_source_names_before_and_after_routing() {
        let router = DtRouter::from_config(
            &RouterConfig::Legacy(LegacyRouterConfig {
                schema_map: "src_schema:dst_schema".into(),
                tb_map: "src_schema.src_table:dst_schema.dst_table".into(),
                col_map: String::new(),
                topic_map: String::new(),
            }),
            DbType::Pg,
            DbType::Pg,
        )
        .unwrap();
        for (do_schemas, do_tbs, ignore_tbs, exclude_ignored_table) in [
            ("src_schema", "", "", false),
            (
                "",
                "src_schema.src_table,src_schema.ignored_table",
                "src_schema.ignored_table",
                true,
            ),
            ("", "src_*.src_*", "", true),
        ] {
            let filter = DtFilter::from_config(
                &FilterConfig::Legacy(LegacyFilterConfig {
                    do_schemas: do_schemas.into(),
                    do_tbs: do_tbs.into(),
                    ignore_tbs: ignore_tbs.into(),
                    ..Default::default()
                }),
                DbType::Pg,
            )
            .unwrap();
            let source_filter = RdbStructFilter::for_source(filter.clone());
            let target_filter = RdbStructFilter::for_target(filter, Some(router.clone()));
            assert!(!source_filter.filter_tb("src_schema", "src_table").unwrap());
            assert!(source_filter.filter_tb("dst_schema", "dst_table").unwrap());
            assert!(!target_filter.filter_tb("dst_schema", "dst_table").unwrap());
            assert_eq!(
                target_filter
                    .filter_tb("dst_schema", "ignored_table")
                    .unwrap(),
                exclude_ignored_table,
                "{do_schemas}/{do_tbs}/{ignore_tbs}"
            );
            if !do_schemas.is_empty() {
                assert!(!source_filter.filter_schema("src_schema").unwrap());
                assert!(source_filter.filter_schema("dst_schema").unwrap());
                assert!(!target_filter.filter_schema("dst_schema").unwrap());
                assert!(target_filter.filter_schema("other_schema").unwrap());
            }
        }
    }

    #[test]
    fn target_filter_reverses_mssql_database_schema_and_table() {
        let filter = DtFilter::from_config(
            &FilterConfig::Legacy(LegacyFilterConfig {
                do_schemas: "[source.db?]".to_string(),
                do_tbs: "[source.db?].[schema.*].[table?]".to_string(),
                ..Default::default()
            }),
            DbType::Mssql,
        )
        .unwrap();
        let config = RouterConfig::Legacy(LegacyRouterConfig {
            schema_map: "[source.db?]:[target.db?]".to_string(),
            tb_map: "[source.db?].[schema.*].[table?]:[target.db?].[routed.*].[target?]"
                .to_string(),
            col_map: String::new(),
            topic_map: String::new(),
        });
        let router = DtRouter::from_config(&config, DbType::Mssql, DbType::Mssql).unwrap();
        let target_filter = RdbStructFilter::for_target(filter, Some(router));

        assert!(!target_filter.filter_schema("target.db?").unwrap());
        assert!(!target_filter
            .filter_tb_with_db("target.db?", "routed.*", "target?")
            .unwrap());
        assert!(target_filter
            .filter_tb_with_db("other.db?", "routed.*", "other")
            .unwrap());
    }
}
