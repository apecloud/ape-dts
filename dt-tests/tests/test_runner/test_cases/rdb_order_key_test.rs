use std::collections::HashMap;

use dt_common::{
    config::config_enums::DbType,
    meta::{
        mssql::mssql_meta_manager::MssqlMetaManager,
        mysql::mysql_meta_manager::MysqlMetaManager,
        pg::pg_meta_manager::PgMetaManager,
        rdb_meta_manager::{RdbMetaManager, RDB_PRIMARY_KEY},
        rdb_tb_meta::SortDirection,
    },
};

use crate::{
    test_config_util::TestConfigUtil,
    test_runner::rdb_test_runner::{RdbTestRunner, DST},
};

pub async fn run_order_key_test(db_type: DbType) -> anyhow::Result<()> {
    use SortDirection::*;
    let engine = db_type.to_string();
    let runner =
        RdbTestRunner::new(&format!("{engine}_to_{engine}/snapshot/order_key_test")).await?;
    runner.run_snapshot_test(true).await?;
    let mut manager = match db_type {
        DbType::Mysql => RdbMetaManager::from_mysql(
            MysqlMetaManager::new(runner.src_conn_pool_mysql.clone().unwrap()).await?,
        ),
        DbType::Pg => RdbMetaManager::from_pg(
            PgMetaManager::new(runner.src_conn_pool_pg.clone().unwrap()).await?,
        ),
        DbType::Mssql => RdbMetaManager::from_mssql(
            MssqlMetaManager::new(
                runner
                    .src_mssql_endpoint
                    .as_ref()
                    .unwrap()
                    .create_pool()
                    .await?,
            )
            .await?,
        ),
        _ => unreachable!(),
    };
    let (db, schema) = if matches!(db_type, DbType::Mssql) {
        ("order_key_src", "dbo")
    } else {
        ("", "order_key_src")
    };
    let mut cases: Vec<(&str, &[(&str, SortDirection)])> = vec![
        ("mixed", &[("a", Asc), ("b", Desc), ("c", Asc)]),
        ("descending", &[("id", Desc)]),
        ("selected", &[("id", Asc)]),
        ("scored_key", &[("a", Desc)]),
        ("nullable_key", &[("a", Desc), ("b", Asc)]),
        ("float_key", &[("a", Desc)]),
        ("catalog_key", &[("a", Desc), ("b", Asc)]),
        ("cursor_ignored", &[("a", Asc)]),
    ];
    if matches!(db_type, DbType::Pg) {
        for table in ["network_key", "range_key", "array_key", "bool_key"] {
            cases.push((table, &[("a", Desc)]));
        }
    }
    let example_cols = if matches!(db_type, DbType::Pg) {
        [("id", Asc), ("value", Asc)]
    } else {
        [("id", Desc), ("value", Asc)]
    };
    cases.push(("parse_keys_example", &example_cols));
    for (table, expected) in &cases {
        let meta = manager.get_tb_meta(db, schema, table).await?;
        let expected_cols = expected
            .iter()
            .map(|(col, _)| col.to_string())
            .collect::<Vec<_>>();
        assert_eq!(meta.order_cols, expected_cols, "{table}");
        assert_eq!(meta.id_cols, expected_cols, "{table}");
        assert_eq!(meta.partition_col, expected_cols[0], "{table}");
        assert_eq!(meta.order_col_attrs.len(), expected.len(), "{table}");
        for (col, direction) in *expected {
            assert_eq!(
                meta.order_col_attrs.get(*col),
                Some(direction),
                "{table}.{col}"
            );
        }
        if *table == "selected" {
            assert_eq!(meta.key_map[RDB_PRIMARY_KEY], ["id"], "{table}");
        }
        if matches!(*table, "scored_key" | "catalog_key") {
            assert!(!meta.key_map.contains_key(RDB_PRIMARY_KEY), "{table}");
        }
        if *table == "catalog_key" {
            assert_eq!(meta.key_map.len(), 2, "{table}: only id and uk_catalog");
            assert_eq!(meta.key_map["uk_catalog"], ["a", "b"]);
            assert!(!meta.key_map.contains_key("partial_key"));
            assert!(!meta.key_map.contains_key("expression_key"));
            assert!(!meta.key_map.contains_key("disabled_key"));
        }
    }
    // Check the full parse_keys result, including keys that were not selected.
    // The example DDL and catalog rows are documented beside each engine's parser.
    let key_cases = [
        (
            "parse_keys_example",
            vec![
                (RDB_PRIMARY_KEY, vec!["id", "value"]),
                ("some_uk_name", vec!["value"]),
                ("uk_example", vec!["value", "id"]),
            ],
        ),
        ("mixed", vec![("uk_mixed", vec!["a", "b", "c"])]),
        ("descending", vec![("uk_descending", vec!["id"])]),
        (
            "selected",
            vec![
                (RDB_PRIMARY_KEY, vec!["id"]),
                ("a_selected", vec!["a"]),
                ("z_selected", vec!["a"]),
            ],
        ),
        (
            "nullable_key",
            vec![
                ("uk_nullable", vec!["a", "b"]),
                ("uk_nullable_payload", vec!["payload"]),
            ],
        ),
        ("float_key", vec![("uk_float", vec!["a"])]),
        ("cursor_ignored", vec![(RDB_PRIMARY_KEY, vec!["a"])]),
    ];
    for (table, keys) in key_cases {
        let expected: HashMap<String, Vec<String>> = keys
            .into_iter()
            .map(|(key, cols)| {
                (
                    key.to_string(),
                    cols.into_iter().map(str::to_string).collect(),
                )
            })
            .collect();
        let meta = manager.get_tb_meta(db, schema, table).await?;
        assert_eq!(meta.key_map, expected, "{engine}.{table}: parse_keys");
    }
    manager.close().await?;
    // The same DESC keys must still work when chunk boundaries are constructed ASC.
    TestConfigUtil::update_task_config(
        &runner.base.task_config_file,
        &runner.base.task_config_file,
        &[("extractor".into(), "parallel_type".into(), "chunk".into())],
    );
    runner.run_snapshot_test(true).await?;
    let resume_config = TestConfigUtil::get_absolute_path(&format!(
        "{engine}_to_{engine}/snapshot/order_key_test/resume.config"
    ));
    TestConfigUtil::update_task_config(
        &runner.base.task_config_file,
        &runner.base.task_config_file,
        &[
            ("extractor".into(), "parallel_type".into(), "table".into()),
            ("resumer".into(), "resume_type".into(), "from_log".into()),
            ("resumer".into(), "config_file".into(), resume_config),
            // An empty log_dir defaults to runtime.log_dir and would load the
            // finished-table markers produced by the preceding full migrations.
            (
                "resumer".into(),
                "log_dir".into(),
                runner.base.test_dir.clone(),
            ),
        ],
    );
    runner.run_snapshot_test(false).await?;
    let mut resume_cases = vec![
        ("mixed", "defghij"),
        ("descending", "abc"),
        ("selected", "abc"),
        ("scored_key", "abc"),
        ("nullable_key", "defghi"),
        ("float_key", "abcd"),
        ("catalog_key", "abcd"),
        ("cursor_ignored", "defg"),
    ];
    if matches!(db_type, DbType::Pg) {
        resume_cases.extend([
            ("network_key", "abc"),
            ("range_key", "abc"),
            ("array_key", "abc"),
            ("bool_key", "a"),
        ]);
    }
    for (table, expected_payloads) in resume_cases {
        let target = if matches!(db_type, DbType::Mssql) {
            (
                "order_key_dst".to_string(),
                "dbo".to_string(),
                table.to_string(),
            )
        } else {
            (
                String::new(),
                "order_key_dst".to_string(),
                table.to_string(),
            )
        };
        let rows = runner.fetch_data(&target, DST).await?;
        let mut payloads = rows
            .iter()
            .map(|row| Ok(row.require_after()?["payload"].to_option_string().unwrap()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        payloads.sort();
        assert_eq!(payloads.concat(), expected_payloads, "resumed {table}");
    }
    runner.close().await?;
    Ok(())
}
