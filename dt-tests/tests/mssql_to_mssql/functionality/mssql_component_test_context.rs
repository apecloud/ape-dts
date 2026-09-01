use std::collections::HashSet;

use anyhow::{ensure, Context};
use dt_common::{
    config::{config_enums::DbType, filter_config::FilterConfig},
    meta::mssql::mssql_connection_pool::MssqlConnectionPool,
    rdb_filter::RdbFilter,
};

use crate::test_runner::{
    mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint},
    rdb_test_runner::RdbTestRunner,
};

#[derive(Clone, Copy)]
pub(super) struct TestTable {
    pub db: &'static str,
    pub schema: &'static str,
    pub tb: &'static str,
}

impl TestTable {
    pub const fn new(db: &'static str, schema: &'static str, tb: &'static str) -> Self {
        Self { db, schema, tb }
    }

    pub fn quoted_name(self) -> String {
        format!("[{}].[{}].[{}]", self.db, self.schema, self.tb)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct TestTableName {
    pub db: String,
    pub schema: String,
    pub tb: String,
}

impl TestTableName {
    fn new(db: &str, schema: &str, tb: &str) -> Self {
        Self {
            db: db.to_string(),
            schema: schema.to_string(),
            tb: tb.to_string(),
        }
    }

    pub fn quoted_name(&self) -> String {
        format!("[{}].[{}].[{}]", self.db, self.schema, self.tb)
    }

    pub fn to_tuple(&self) -> (String, String, String) {
        (self.db.clone(), self.schema.clone(), self.tb.clone())
    }

    pub fn with_database(&self, db: &str) -> Self {
        Self::new(db, &self.schema, &self.tb)
    }
}

pub(super) struct MssqlComponentTestContext {
    pub runner: RdbTestRunner,
    pub source_pool: MssqlConnectionPool,
    pub sinker_pool: MssqlConnectionPool,
    pub source_tables: Vec<TestTableName>,
    pub sinker_tables: Vec<TestTableName>,
}

impl MssqlComponentTestContext {
    pub async fn new(
        case_dir: &str,
        source_selectors: &[TestTable],
        sinker_selectors: &[TestTable],
    ) -> anyhow::Result<Self> {
        let test_dir = format!("mssql_to_mssql/functionality/{case_dir}");
        let runner = RdbTestRunner::new(&test_dir).await?;
        runner.execute_prepare_sqls().await?;
        let source_pool = Self::endpoint(&runner, TaskConfigEndpoint::Extractor)?
            .create_pool()
            .await?;
        let sinker_pool = Self::endpoint(&runner, TaskConfigEndpoint::Sinker)?
            .create_pool()
            .await?;
        let source_tables =
            Self::resolve_tables(&test_dir, &runner, &source_pool, source_selectors).await?;
        let sinker_tables =
            Self::resolve_tables(&test_dir, &runner, &sinker_pool, sinker_selectors).await?;

        Ok(Self {
            runner,
            source_pool,
            sinker_pool,
            source_tables,
            sinker_tables,
        })
    }

    pub fn source_table(&self, db: &str, schema: &str, tb: &str) -> anyhow::Result<&TestTableName> {
        Self::find_table(&self.source_tables, "source", db, schema, tb)
    }

    pub fn sinker_table(&self, db: &str, schema: &str, tb: &str) -> anyhow::Result<&TestTableName> {
        Self::find_table(&self.sinker_tables, "sinker", db, schema, tb)
    }

    pub fn source_endpoint(&self) -> anyhow::Result<&MssqlTestEndpoint> {
        Self::endpoint(&self.runner, TaskConfigEndpoint::Extractor)
    }

    pub async fn execute_test_sqls(&self) -> anyhow::Result<()> {
        self.runner.execute_test_sqls().await
    }

    async fn resolve_tables(
        test_dir: &str,
        runner: &RdbTestRunner,
        pool: &MssqlConnectionPool,
        selectors: &[TestTable],
    ) -> anyhow::Result<Vec<TestTableName>> {
        if selectors.is_empty() {
            return Ok(Vec::new());
        }
        let mut tables = Vec::new();
        let mut seen = HashSet::new();
        let meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;

        for selector in selectors {
            let table_names = if RdbFilter::is_pattern(selector.tb, &DbType::Mssql) {
                let pattern_filter = RdbFilter::from_config(
                    &FilterConfig {
                        do_tbs: format!("{}.{}.{}", selector.db, selector.schema, selector.tb),
                        ..Default::default()
                    },
                    &DbType::Mssql,
                )?;
                let table_names = meta_manager
                    .list_tables(selector.db, selector.schema)
                    .await?
                    .into_iter()
                    .filter(|tb| {
                        !pattern_filter.filter_tb_with_db(selector.db, selector.schema, tb)
                    })
                    .collect::<Vec<_>>();
                ensure!(
                    !table_names.is_empty(),
                    "MSSQL component test table selector matched no tables in {test_dir}: {}.{}.{}",
                    selector.db,
                    selector.schema,
                    selector.tb
                );
                table_names
            } else {
                vec![selector.tb.to_string()]
            };

            for tb in table_names {
                ensure!(
                    !runner
                        .filter
                        .filter_tb_with_db(selector.db, selector.schema, &tb),
                    "MSSQL component test table is excluded by filter in {test_dir}: {}.{}.{}",
                    selector.db,
                    selector.schema,
                    tb
                );
                let table = TestTableName::new(selector.db, selector.schema, &tb);
                if seen.insert(table.clone()) {
                    tables.push(table);
                }
            }
        }

        Ok(tables)
    }

    fn find_table<'a>(
        tables: &'a [TestTableName],
        endpoint: &str,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<&'a TestTableName> {
        tables
            .iter()
            .find(|table| table.db == db && table.schema == schema && table.tb == tb)
            .with_context(|| {
                format!(
                    "MSSQL component test {endpoint} table was not selected: {db}.{schema}.{tb}"
                )
            })
    }

    fn endpoint(
        runner: &RdbTestRunner,
        endpoint: TaskConfigEndpoint,
    ) -> anyhow::Result<&MssqlTestEndpoint> {
        match endpoint {
            TaskConfigEndpoint::Extractor => runner
                .src_mssql_endpoint
                .as_ref()
                .context("MSSQL component test has no source endpoint"),
            TaskConfigEndpoint::Sinker => runner
                .dst_mssql_endpoint
                .as_ref()
                .context("MSSQL component test has no destination endpoint"),
        }
    }
}
