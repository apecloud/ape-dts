use dt_common::{
    config::{config_enums::DbType, task_config::TaskConfig},
    meta::ddl_meta::{ddl_parser::DdlParser, ddl_statement::DdlStatement},
};
use dt_connector::meta_fetcher::{
    mysql::mysql_struct_check_fetcher::MysqlStructCheckFetcher,
    pg::pg_struct_check_fetcher::PgStructCheckFetcher,
};
use std::collections::{HashMap, HashSet};

use super::{base_test_runner::BaseTestRunner, rdb_test_runner::RdbTestRunner};

pub struct RdbStructTestRunner {
    pub base: RdbTestRunner,
}

const PG_GET_INDEXDEF: &str = "pg_get_indexdef";

impl RdbStructTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = RdbTestRunner::new(relative_test_dir).await.unwrap();
        Ok(Self { base })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.base.close().await
    }

    pub async fn run_mysql_struct_test(&mut self) -> anyhow::Result<()> {
        self.base.execute_prepare_sqls().await?;
        self.base.base.start_task().await?;

        let expect_ddl_sqls = self.load_expect_ddl_sqls().await;
        let src_check_fetcher =
            self.base
                .src_conn_pool_mysql
                .as_ref()
                .map(|conn_pool| MysqlStructCheckFetcher {
                    conn_pool: conn_pool.clone(),
                });
        let dst_check_fetcher = MysqlStructCheckFetcher {
            conn_pool: self.base.dst_conn_pool_mysql.as_mut().unwrap().clone(),
        };

        let get_sql_lines = |sql: &str| -> HashSet<String> {
            let mut line_set = HashSet::new();
            let lines: Vec<&str> = sql.split("\n").collect();
            for line in lines {
                line_set.insert(line.trim().trim_end_matches(",").to_owned());
            }
            line_set
        };

        let (src_db_tbs, dst_db_tbs) = self.base.get_compare_db_tbs().unwrap();
        for i in 0..src_db_tbs.len() {
            if let Some(src_check_fetcher) = &src_check_fetcher {
                let src_ddl_sql = src_check_fetcher
                    .fetch_table(&src_db_tbs[i].0, &src_db_tbs[i].1)
                    .await;
                println!("src_ddl_sql: {}\n", src_ddl_sql);
            }

            let dst_ddl_sql = dst_check_fetcher
                .fetch_table(&dst_db_tbs[i].0, &dst_db_tbs[i].1)
                .await;
            let key = format!("{}.{}", &dst_db_tbs[i].0, &dst_db_tbs[i].1);
            let expect_ddl_sql = expect_ddl_sqls.get(&key).unwrap().to_owned();

            println!("dst_ddl_sql: {}\n", dst_ddl_sql);
            println!("expect_ddl_sql: {}\n", expect_ddl_sql);
            // show create table may return sqls with indexes in different orders during tests,
            // so here we just compare all lines of the sqls.
            let dst_ddl_sql_lines = get_sql_lines(&dst_ddl_sql);
            let expect_ddl_sql_lines = get_sql_lines(&expect_ddl_sql);

            for line in dst_ddl_sql_lines.iter() {
                println!("dst_ddl_sql_line: {}", line);
                if !expect_ddl_sql_lines.contains(line) {
                    println!("dst_ddl_sql_line NOT exists in expect_ddl_sql_lines");
                    panic!();
                }
            }
            assert_eq!(dst_ddl_sql_lines.len(), expect_ddl_sql_lines.len());
        }

        // show create database
        let mut tested_dbs = HashSet::new();
        for i in 0..src_db_tbs.len() {
            if tested_dbs.contains(&src_db_tbs[i].0) {
                continue;
            }

            if let Some(src_check_fetcher) = &src_check_fetcher {
                let src_ddl_sql = src_check_fetcher.fetch_database(&src_db_tbs[i].0).await;
                println!("src_ddl_sql: {}\n", src_ddl_sql);
            }

            let dst_ddl_sql = dst_check_fetcher.fetch_database(&dst_db_tbs[i].0).await;
            let key = dst_db_tbs[i].0.to_string();
            let expect_ddl_sql = expect_ddl_sqls.get(&key).unwrap().to_owned();

            println!("dst_ddl_sql: {}\n", dst_ddl_sql);
            println!("expect_ddl_sql: {}\n", expect_ddl_sql);

            assert_eq!(dst_ddl_sql, expect_ddl_sql);
            tested_dbs.insert(&src_db_tbs[i].0);
        }

        Ok(())
    }

    pub async fn run_pg_struct_test(&mut self) -> anyhow::Result<()> {
        self.base.execute_prepare_sqls().await?;
        self.base.base.start_task().await?;

        let src_check_fetcher = PgStructCheckFetcher {
            conn_pool: self.base.src_conn_pool_pg.as_mut().unwrap().clone(),
        };
        let dst_check_fetcher = PgStructCheckFetcher {
            conn_pool: self.base.dst_conn_pool_pg.as_mut().unwrap().clone(),
        };

        let (src_db_tbs, dst_db_tbs) = self.base.get_compare_db_tbs().unwrap();
        for i in 0..src_db_tbs.len() {
            let src_db_tb = &src_db_tbs[i];
            let dst_db_tb = &dst_db_tbs[i];

            let src_table = src_check_fetcher
                .fetch_table(&src_db_tb.0, &src_db_tb.1)
                .await?;
            let mut dst_table = dst_check_fetcher
                .fetch_table(&dst_db_tb.0, &dst_db_tb.1)
                .await?;

            println!(
                "comparing src table: {:?} with dst table: {:?}\n",
                src_db_tb, dst_db_tb
            );

            if src_db_tb == dst_db_tb {
                println!("src_table: {:?}\n", src_table);
                println!("dst_table: {:?}\n", dst_table);
                assert_eq!(src_table, dst_table);
                return Ok(());
            }

            assert_eq!(src_table.columns, dst_table.columns);
            assert_eq!(src_table.summary, dst_table.summary);
            assert_eq!(src_table.constraints, dst_table.constraints);
            assert_eq!(src_table.indexes.len(), dst_table.indexes.len());
            // when table is routed, the dst pg_get_indexdef is different from src
            // src pg_get_indexdef: CREATE UNIQUE INDEX full_column_type_pkey ON struct_it_pg2pg_1.full_column_type USING btree (id)
            // dst pg_get_indexdef: CREATE UNIQUE INDEX full_column_type_pkey ON dst_struct_it_pg2pg_1.full_column_type USING btree (id)
            let parser = DdlParser::new(DbType::Pg);
            for (j, src_index) in src_table.indexes.iter().enumerate() {
                let src_indexdef = src_index.get(PG_GET_INDEXDEF);
                if src_indexdef.is_none() {
                    continue;
                }
                let dst_index = &mut dst_table.indexes[j];

                let src_indexdef = src_indexdef.unwrap();
                let dst_indexdef = dst_index.get(PG_GET_INDEXDEF).unwrap();
                let src_ddl_data = parser.parse(src_indexdef).unwrap().unwrap();
                let dst_ddl_data = parser.parse(dst_indexdef).unwrap().unwrap();

                if let DdlStatement::PgCreateIndex(src) = src_ddl_data.statement {
                    assert_eq!(src.schema, src_db_tb.0);
                    assert_eq!(src.tb, src_db_tb.1);

                    if let DdlStatement::PgCreateIndex(dst) = dst_ddl_data.statement {
                        assert_eq!(dst.schema, dst_db_tb.0);
                        assert_eq!(dst.tb, dst_db_tb.1);

                        assert_eq!(src.index_name, dst.index_name);
                        assert_eq!(src.is_unique, dst.is_unique);
                        assert_eq!(src.is_concurrently, dst.is_concurrently);
                        assert_eq!(src.if_not_exists, dst.if_not_exists);
                        assert_eq!(src.is_only, dst.is_only);
                        assert_eq!(src.unparsed, dst.unparsed);
                    }
                }

                // other properties of src_index and dst_index should be same
                assert_eq!(src_index.len(), dst_index.len());
                for key in src_index.keys() {
                    if key == PG_GET_INDEXDEF {
                        continue;
                    }
                    println!("index property: {}", key);
                    assert_eq!(src_index.get(key), dst_index.get(key));
                }
            }
        }

        println!(
            "summary: src tables: {:?}, dst tables: {:?}",
            src_db_tbs, dst_db_tbs
        );
        Ok(())
    }

    pub async fn run_struct_test_without_check(&mut self) -> anyhow::Result<()> {
        self.base.execute_prepare_sqls().await?;
        self.base.base.start_task().await
    }

    pub async fn load_expect_ddl_sqls(&self) -> HashMap<String, String> {
        let config = TaskConfig::new(&self.base.base.task_config_file).unwrap();
        let dst_db_type = config
            .destination_target()
            .unwrap()
            .map(|target| target.db_type)
            .unwrap_or(config.sinker_basic.db_type);
        let ddl_file = match dst_db_type {
            DbType::Mysql => {
                let version = self.base.get_dst_mysql_version().await;
                if version.starts_with("5.") {
                    format!("{}/expect_ddl_5.7.sql", self.base.base.test_dir)
                } else {
                    format!("{}/expect_ddl_8.0.sql", self.base.base.test_dir)
                }
            }
            _ => format!("{}/expect_ddl.sql", self.base.base.test_dir),
        };

        let mut ddl_sqls = HashMap::new();
        let lines = BaseTestRunner::load_file(&ddl_file);
        let mut lines = lines.iter().peekable();
        while let Some(line) = lines.next() {
            if line.trim().is_empty() {
                continue;
            }

            let key = line.trim().to_owned();
            let mut sql = String::new();
            for line in lines.by_ref() {
                if line.trim().is_empty() {
                    break;
                }
                sql.push_str(line);
                sql.push('\n');
            }
            ddl_sqls.insert(key, sql.trim().to_owned());
        }
        ddl_sqls
    }
}
