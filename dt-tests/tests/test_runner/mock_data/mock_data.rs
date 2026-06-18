use dt_common::config::ini_loader::IniLoader;
use serde::de::DeserializeOwned;

use crate::test_runner::mock_data::{
    context::MockDbContext,
    mock_stmt::{Constraint, MockColType, MockStmt},
    random::Random,
};

pub struct MockData<T: MockColType> {
    pub db_context: MockDbContext,
    pub insert_rows: usize,
    pub mock_stmts: Vec<MockStmt<T>>,
    pub custom_type_ddl_stmts: Vec<String>,
    pub seed: u64,
}

impl<T: MockColType + DeserializeOwned> MockData<T> {
    pub fn new(config_file: &str, db_context: MockDbContext) -> Option<Self> {
        let loader = IniLoader::new(config_file);
        let key_prefix = T::config_key_prefix();
        let col_types = if let Some(config_map) =
            loader.ini.get_map().unwrap_or_default().get("mock")
        {
            // Sort entries by key to ensure deterministic iteration order.
            // HashMap iteration is non-deterministic, which would cause
            // the RNG to be consumed in different orders across runs,
            // producing different INSERT values even with the same seed.
            let mut sorted_entries: Vec<_> = config_map.iter().collect();
            sorted_entries.sort_by_key(|(k, _)| *k);
            let col_types = sorted_entries
                .into_iter()
                .filter(|(k, _v)| k.starts_with(key_prefix))
                .map(|(_, v)| {
                    serde_json::from_str::<Vec<T>>(v.clone().unwrap_or_default().as_str()).unwrap()
                })
                .filter(|v| !v.is_empty())
                .collect::<Vec<Vec<T>>>();
            if col_types.is_empty() {
                return None;
            }
            col_types
        } else {
            return None;
        };
        let db_str = loader.get_with_default("mock", "db", "mock_db_1".to_string());
        let insert_rows = loader.get_with_default("mock", "insert_rows_each_table", 30);
        let seed = loader.get_with_default("mock", "seed", 777);
        let mock_strategy = loader.get_with_default("mock", "strategy", "multi".to_string());
        let custom_type_ddl_stmts = T::custom_type_ddl_stmts(&col_types, &db_str, &db_context);
        let mut tb_suffix = 0usize;
        let mut mock_stmts = Vec::new();
        if mock_strategy == "single" {
            let constraints_str = loader.get_with_default("mock", "constraints", "[]".to_string());
            let nullable_cols_str =
                loader.get_with_default("mock", "nullable_cols", "[]".to_string());
            let constraints: Vec<Constraint> = serde_json::from_str(&constraints_str).unwrap();
            let nullable_cols: Vec<usize> = serde_json::from_str(&nullable_cols_str).unwrap();
            let all_types = col_types
                .iter()
                .flat_map(|types| types.iter().cloned())
                .collect::<Vec<_>>();
            let mut mock_stmt =
                MockStmt::new(&all_types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                    .with_nullable_cols(&nullable_cols);
            for constraint in constraints {
                mock_stmt = mock_stmt.with_index(constraint, &db_context);
            }
            mock_stmts.push(mock_stmt);
            return Some(MockData {
                db_context,
                mock_stmts,
                custom_type_ddl_stmts,
                insert_rows,
                seed,
            });
        }
        // no index, all non-nullable
        mock_stmts.extend(
            col_types.iter().map(|types| {
                MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
            }),
        );
        // no index, all nullable
        mock_stmts.extend(col_types.iter().map(|types| {
            MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
        }));
        // single column primary index, all nullable
        mock_stmts.extend(col_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                stmts.push(
                    MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                        .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                        .with_index(Constraint::Primary(vec![col_idx]), &db_context),
                );
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        // composite primary index, all nullable
        mock_stmts.extend(col_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                for col_idx2 in (col_idx + 1)..types.len() {
                    stmts.push(
                        MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                            .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                            .with_index(Constraint::Primary(vec![col_idx, col_idx2]), &db_context),
                    );
                }
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        // single column unique index, all nullable
        mock_stmts.extend(col_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                stmts.push(
                    MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                        .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                        .with_index(Constraint::Unique(vec![col_idx]), &db_context),
                );
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        // composite unique index, all nullable
        mock_stmts.extend(col_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                for col_idx2 in (col_idx + 1)..types.len() {
                    stmts.push(
                        MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                            .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                            .with_index(Constraint::Unique(vec![col_idx, col_idx2]), &db_context),
                    );
                }
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        // one primary index, all unique index, all nullable
        mock_stmts.extend(col_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                let mut stmt =
                    MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                        .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                        .with_index(Constraint::Primary(vec![col_idx]), &db_context);
                for col_idx2 in 0..types.len() {
                    if col_idx2 != col_idx {
                        stmt = stmt.with_index(Constraint::Unique(vec![col_idx2]), &db_context);
                    }
                }
                stmts.push(stmt);
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        Some(MockData {
            db_context,
            mock_stmts,
            custom_type_ddl_stmts,
            insert_rows,
            seed,
        })
    }

    pub fn mock_schema_stmts(&self) -> Vec<String> {
        self.mock_stmts
            .first()
            .map(|mock_stmt| mock_stmt.create_schema_stmt(&self.db_context))
            .unwrap_or_default()
    }

    pub fn mock_table_ddl_stmts(&self) -> Vec<String> {
        self.mock_stmts
            .iter()
            .map(|mock_stmt| mock_stmt.create_table_stmt(&self.db_context))
            .collect()
    }

    pub fn mock_src_prepare_stmts(&self) -> Vec<String> {
        let mut res = self.mock_schema_stmts();
        res.extend(self.custom_type_ddl_stmts.clone());
        res.extend(self.mock_table_ddl_stmts());
        res
    }

    pub fn mock_dst_prepare_stmts_for_data_task(&self) -> Vec<String> {
        self.mock_src_prepare_stmts()
    }

    pub fn mock_dst_prepare_stmts_for_struct_task(&self) -> Vec<String> {
        let mut res = self.mock_schema_stmts();
        res.extend(self.custom_type_ddl_stmts.clone());
        res
    }

    #[allow(dead_code)]
    pub fn mock_ddl_stmts(&self) -> Vec<String> {
        self.mock_src_prepare_stmts()
    }

    pub fn mock_db_tbs(&self) -> Vec<(String, String)> {
        self.mock_stmts
            .iter()
            .map(|stmt| (stmt.db.clone(), stmt.tb.clone()))
            .collect()
    }

    pub fn mock_dml_stmts(&self) -> Vec<String> {
        let mut res = vec![];
        res.extend(self.mock_insert_stmts());
        let db_tbs = self
            .mock_stmts
            .iter()
            .map(|stmt| (stmt.db.clone(), stmt.tb.clone()))
            .collect::<Vec<_>>();
        res.extend(T::after_all_insert_stmts(&db_tbs, &self.db_context));
        res
    }

    pub fn mock_insert_stmts(&self) -> Vec<String> {
        let mut res = vec![];
        let mut random = Random::new(Some(self.seed));
        for mock_stmt in &self.mock_stmts {
            res.extend(mock_stmt.insert_value_stmt(
                &self.db_context,
                &mut random,
                self.insert_rows,
            ));
        }
        res
    }

    fn gen_mock_tb_name(tb_suffix: &mut usize) -> String {
        let name = format!("mock_tb_{}", tb_suffix);
        *tb_suffix += 1;
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::config::config_enums::DbType;

    use crate::test_runner::mock_data::{
        context::MockDbContext, mysql_type::MysqlType, pg_type::PgType,
    };

    #[test]
    fn test_serialization() {
        let constraints = vec![
            Constraint::Primary(vec![0, 2, 3]),
            Constraint::Unique(vec![1, 4]),
        ];
        let serialized = serde_json::to_string(&constraints).unwrap();
        assert_eq!(serialized, r#"[{"primary":[0,2,3]},{"unique":[1,4]}]"#);
        let serialized1 = "[]".to_string();
        let deserialized: Vec<Constraint> = serde_json::from_str(&serialized1).unwrap();
        assert_eq!(deserialized.len(), 0);
        let nullable_cols = vec![0, 2, 4];
        let serialized_cols = serde_json::to_string(&nullable_cols).unwrap();
        assert_eq!(serialized_cols, "[0,2,4]");
        let serialized_cols1 = "[]".to_string();
        let deserialized_cols: Vec<usize> = serde_json::from_str(&serialized_cols1).unwrap();
        assert_eq!(deserialized_cols.len(), 0);
    }

    #[test]
    fn test_pg_mock_dml_appends_analyze() {
        let mock_data = MockData {
            db_context: MockDbContext::new(DbType::Pg, "16.0"),
            insert_rows: 2,
            mock_stmts: vec![
                MockStmt::new(&[PgType::Int4], "test_db", "test_tb_1"),
                MockStmt::new(&[PgType::Int4], "test_db", "test_tb_2"),
            ],
            custom_type_ddl_stmts: Vec::new(),
            seed: 777,
        };

        let stmts = mock_data.mock_dml_stmts();
        assert_eq!(stmts.len(), 3);
        assert!(stmts[0].starts_with("INSERT INTO test_db.test_tb_1 VALUES "));
        assert!(stmts[1].starts_with("INSERT INTO test_db.test_tb_2 VALUES "));
        assert_eq!(stmts[2], "ANALYZE;");
    }

    #[test]
    fn test_mysql_mock_dml_appends_analyze_table() {
        let mock_data = MockData {
            db_context: MockDbContext::new(DbType::Mysql, "8.0.0"),
            insert_rows: 2,
            mock_stmts: vec![MockStmt::new(&[MysqlType::Int], "test_db", "test_tb")],
            custom_type_ddl_stmts: Vec::new(),
            seed: 777,
        };

        let stmts = mock_data.mock_dml_stmts();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("INSERT INTO `test_db`.`test_tb` VALUES "));
        assert_eq!(stmts[1], "ANALYZE TABLE `test_db`.`test_tb`;");
    }

    #[test]
    fn test_mysql_mock_dml_batches_analyze_tables() {
        let mock_stmts = (0..101)
            .map(|idx| MockStmt::new(&[MysqlType::Int], "test_db", &format!("test_tb_{}", idx)))
            .collect::<Vec<_>>();
        let mock_data = MockData {
            db_context: MockDbContext::new(DbType::Mysql, "8.0.0"),
            insert_rows: 1,
            mock_stmts,
            custom_type_ddl_stmts: Vec::new(),
            seed: 777,
        };

        let stmts = mock_data.mock_dml_stmts();
        assert_eq!(stmts.len(), 103);
        assert!(
            stmts[101].starts_with("ANALYZE TABLE `test_db`.`test_tb_0`, `test_db`.`test_tb_1`")
        );
        assert!(stmts[101].ends_with("`test_db`.`test_tb_99`;"));
        assert_eq!(stmts[102], "ANALYZE TABLE `test_db`.`test_tb_100`;");
    }

    #[test]
    fn test_mock_insert_stmts_excludes_after_insert_analyze() {
        let mock_data = MockData {
            db_context: MockDbContext::new(DbType::Pg, "16.0"),
            insert_rows: 2,
            mock_stmts: vec![MockStmt::new(&[PgType::Int4], "test_db", "test_tb")],
            custom_type_ddl_stmts: Vec::new(),
            seed: 777,
        };

        let stmts = mock_data.mock_insert_stmts();
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].starts_with("INSERT INTO test_db.test_tb VALUES "));
        assert!(!stmts.iter().any(|stmt| stmt == "ANALYZE;"));
    }

    #[test]
    fn test_mock_prepare_stmts_split_struct_and_data_tasks() {
        let mock_data = MockData {
            db_context: MockDbContext::new(DbType::Mysql, "8.0.0"),
            insert_rows: 1,
            mock_stmts: vec![
                MockStmt::new(&[MysqlType::Int], "test_db", "test_tb_1"),
                MockStmt::new(&[MysqlType::TinyBlob], "test_db", "test_tb_2"),
            ],
            custom_type_ddl_stmts: Vec::new(),
            seed: 777,
        };

        let src_prepare = mock_data.mock_src_prepare_stmts();
        let dst_data_prepare = mock_data.mock_dst_prepare_stmts_for_data_task();
        let dst_struct_prepare = mock_data.mock_dst_prepare_stmts_for_struct_task();

        assert_eq!(src_prepare.len(), 4);
        assert_eq!(dst_data_prepare, src_prepare);
        assert_eq!(dst_struct_prepare.len(), 2);
        assert!(dst_struct_prepare
            .iter()
            .all(|stmt| !stmt.to_lowercase().starts_with("create table")));
        assert_eq!(
            mock_data.mock_db_tbs(),
            vec![
                ("test_db".to_string(), "test_tb_1".to_string()),
                ("test_db".to_string(), "test_tb_2".to_string())
            ]
        );
    }

    #[test]
    fn test_pg_custom_type_prepare_stmts_split_struct_and_data_tasks() {
        let config_file = std::env::temp_dir().join("pg_custom_type_prepare_test.ini");
        std::fs::write(
            &config_file,
            r#"
[mock]
db=mock_db_1
insert_rows_each_table=1
pg_types_custom=["int4",{"custom":{"kind":"enum","name":"mock_mood","labels":["sad","ok","happy"]}}]
"#,
        )
        .unwrap();
        let config_file = config_file.to_string_lossy().to_string();
        let mock_data =
            MockData::<PgType>::new(&config_file, MockDbContext::new(DbType::Pg, "17.0")).unwrap();

        let src_prepare = mock_data.mock_src_prepare_stmts();
        let dst_struct_prepare = mock_data.mock_dst_prepare_stmts_for_struct_task();

        assert_eq!(
            mock_data.custom_type_ddl_stmts,
            vec!["CREATE TYPE mock_db_1.mock_mood AS ENUM ('sad', 'ok', 'happy');"]
        );
        assert_eq!(
            &src_prepare[0..3],
            &[
                "DROP SCHEMA IF EXISTS mock_db_1 CASCADE;".to_string(),
                "CREATE SCHEMA IF NOT EXISTS mock_db_1;".to_string(),
                "CREATE TYPE mock_db_1.mock_mood AS ENUM ('sad', 'ok', 'happy');".to_string()
            ]
        );
        assert!(src_prepare
            .iter()
            .any(|stmt| { stmt.contains("CREATE TABLE") && stmt.contains("mock_db_1.mock_mood") }));
        assert_eq!(dst_struct_prepare.len(), 3);
        assert!(dst_struct_prepare
            .iter()
            .all(|stmt| !stmt.to_lowercase().starts_with("create table")));
    }

    #[test]
    fn test_single_strategy_merges_all_mock_type_groups() {
        let config_file = std::env::temp_dir().join("mock_single_strategy_test.ini");
        std::fs::write(
            &config_file,
            r#"
[mock]
strategy=single
db=mock_db_1
mysql_types_a=["int"]
mysql_types_b=["json"]
mysql_types_c=["geometry"]
"#,
        )
        .unwrap();
        let config_file = config_file.to_string_lossy().to_string();
        let mock_data =
            MockData::<MysqlType>::new(&config_file, MockDbContext::new(DbType::Mysql, "8.0.39"))
                .unwrap();

        assert_eq!(mock_data.mock_stmts.len(), 1);
        let type_names = mock_data.mock_stmts[0]
            .included_types
            .iter()
            .map(|ty| ty.name())
            .collect::<Vec<_>>();

        assert!(type_names.contains(&"INT".to_string()));
        assert!(type_names.contains(&"JSON".to_string()));
        assert!(type_names.contains(&"GEOMETRY".to_string()));
    }

    #[test]
    fn test_mysql_snapshot_mock_configs_load_character_attrs() {
        for (relative_config, version, expected_collation) in [
            (
                "tests/mock_test/mysql_to_mysql/5_7_to_5_7/snapshot/table_parallel_test/task_config.ini",
                "5.7.44",
                "utf8mb4_general_ci",
            ),
            (
                "tests/mock_test/mysql_to_mysql/5_7_to_5_7/snapshot/parallel_test/task_config.ini",
                "5.7.44",
                "utf8mb4_general_ci",
            ),
            (
                "tests/mock_test/mysql_to_mysql/8_0_to_8_0/snapshot/table_parallel_test/task_config.ini",
                "8.0.39",
                "utf8mb4_0900_ai_ci",
            ),
            (
                "tests/mock_test/mysql_to_mysql/8_0_to_8_0/snapshot/parallel_test/task_config.ini",
                "8.0.39",
                "utf8mb4_0900_ai_ci",
            ),
        ] {
            let config_file = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), relative_config);
            let mock_data =
                MockData::<MysqlType>::new(&config_file, MockDbContext::new(DbType::Mysql, version))
                    .unwrap();
            let type_names = mock_data
                .mock_stmts
                .iter()
                .flat_map(|stmt| stmt.included_types.iter())
                .map(MysqlType::name)
                .collect::<Vec<_>>()
                .join("\n");

            assert!(type_names.contains(expected_collation));
            assert!(type_names.contains("CHARACTER SET"));
        }
    }

    #[test]
    fn test_pg_mock_configs_load_newly_supported_types() {
        for (relative_config, version, has_multirange) in [
            (
                "tests/mock_test/pg_to_pg/13_3_4_to_13_3_4/snapshot/table_parallel_test/task_config.ini",
                "13.3.4",
                false,
            ),
            (
                "tests/mock_test/pg_to_pg/13_3_4_to_13_3_4/snapshot/parallel_test/task_config.ini",
                "13.3.4",
                false,
            ),
            (
                "tests/mock_test/pg_to_pg/13_3_4_to_13_3_4/cdc/basic_test/task_config.ini",
                "13.3.4",
                false,
            ),
            (
                "tests/mock_test/pg_to_pg/13_3_4_to_13_3_4/struct/basic_test/task_config.ini",
                "13.3.4",
                false,
            ),
            (
                "tests/mock_test/pg_to_pg/17_3_4_to_17_3_4/snapshot/table_parallel_test/task_config.ini",
                "17.3.4",
                true,
            ),
            (
                "tests/mock_test/pg_to_pg/17_3_4_to_17_3_4/snapshot/parallel_test/task_config.ini",
                "17.3.4",
                true,
            ),
            (
                "tests/mock_test/pg_to_pg/17_3_4_to_17_3_4/cdc/basic_test/task_config.ini",
                "17.3.4",
                true,
            ),
            (
                "tests/mock_test/pg_to_pg/17_3_4_to_17_3_4/struct/basic_test/task_config.ini",
                "17.3.4",
                true,
            ),
        ] {
            let config_file = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), relative_config);
            let ctx = MockDbContext::new(DbType::Pg, version);
            let mock_data = MockData::<PgType>::new(&config_file, ctx).unwrap();
            let custom_types_enabled = std::fs::read_to_string(&config_file)
                .unwrap()
                .lines()
                .any(|line| line.starts_with("pg_types_custom="));
            let type_names = mock_data
                .mock_stmts
                .iter()
                .flat_map(|stmt| stmt.included_types.iter())
                .map(|ty| ty.type_name("mock_db_1", &mock_data.db_context))
                .collect::<Vec<_>>();

            for expected_type in [
                "xml",
                "tsvector",
                "tsquery",
                "jsonpath",
                "int4range",
                "int8range",
                "numrange",
                "tsrange",
                "tstzrange",
                "daterange",
            ] {
                assert!(
                    type_names.iter().any(|type_name| type_name == expected_type),
                    "{relative_config} missing {expected_type}"
                );
            }

            assert_eq!(
                type_names
                    .iter()
                    .any(|type_name| type_name == "int4multirange"),
                has_multirange,
                "{relative_config} multirange availability mismatch"
            );

            let prepare_stmts = mock_data.mock_src_prepare_stmts();
            assert!(
                prepare_stmts
                    .iter()
                    .any(|stmt| stmt.contains("CREATE TABLE")),
                "{relative_config} generated no CREATE TABLE statement"
            );
            if custom_types_enabled {
                assert!(
                    prepare_stmts
                        .iter()
                        .any(|stmt| stmt.contains("CREATE TYPE mock_db_1.mock_mood")),
                    "{relative_config} generated no custom enum type"
                );
                assert!(
                    prepare_stmts
                        .iter()
                        .any(|stmt| stmt.contains("CREATE DOMAIN mock_db_1.mock_email")),
                    "{relative_config} generated no custom domain type"
                );
                assert!(
                    prepare_stmts
                        .iter()
                        .any(|stmt| stmt.contains("CREATE TYPE mock_db_1.mock_addr AS")),
                    "{relative_config} generated no custom composite type"
                );
                assert!(
                    prepare_stmts.iter().any(|stmt| stmt
                        .contains("CREATE TYPE mock_db_1.mock_score_range AS RANGE")),
                    "{relative_config} generated no custom range type"
                );
            }
        }
    }
}
