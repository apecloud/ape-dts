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
        let mut tb_suffix = 0usize;
        let mut mock_stmts = Vec::new();
        if mock_strategy == "single" {
            let constraints_str = loader.get_with_default("mock", "constraints", "[]".to_string());
            let nullable_cols_str =
                loader.get_with_default("mock", "nullable_cols", "[]".to_string());
            let constraints: Vec<Constraint> = serde_json::from_str(&constraints_str).unwrap();
            let nullable_cols: Vec<usize> = serde_json::from_str(&nullable_cols_str).unwrap();
            let all_types = col_types.first().unwrap().clone();
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
            insert_rows,
            seed,
        })
    }

    pub fn mock_ddl_stmts(&self) -> Vec<String> {
        let mut res = vec![];
        for (i, mock_stmt) in self.mock_stmts.iter().enumerate() {
            if i == 0 {
                // create database/schema only once
                res.extend(mock_stmt.create_schema_stmt(&self.db_context));
            }
            res.push(mock_stmt.create_table_stmt(&self.db_context));
        }
        res
    }

    pub fn mock_dml_stmts(&self) -> Vec<String> {
        let mut res = vec![];
        let mut random = Random::new(Some(self.seed));
        for mock_stmt in &self.mock_stmts {
            res.extend(mock_stmt.insert_value_stmt(
                &self.db_context,
                &mut random,
                self.insert_rows,
            ));
        }
        let db_tbs = self
            .mock_stmts
            .iter()
            .map(|stmt| (stmt.db.clone(), stmt.tb.clone()))
            .collect::<Vec<_>>();
        res.extend(T::after_all_insert_stmts(&db_tbs, &self.db_context));
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
            seed: 777,
        };

        let stmts = mock_data.mock_dml_stmts();
        assert_eq!(stmts.len(), 3);
        assert!(stmts[0].starts_with("INSERT INTO test_db.test_tb_1 VALUES "));
        assert!(stmts[1].starts_with("INSERT INTO test_db.test_tb_2 VALUES "));
        assert_eq!(stmts[2], "ANALYZE;");
    }

    #[test]
    fn test_mysql_mock_dml_has_no_after_insert_stmt() {
        let mock_data = MockData {
            db_context: MockDbContext::new(DbType::Mysql, "8.0.0"),
            insert_rows: 2,
            mock_stmts: vec![MockStmt::new(&[MysqlType::Int], "test_db", "test_tb")],
            seed: 777,
        };

        let stmts = mock_data.mock_dml_stmts();
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].starts_with("INSERT INTO `test_db`.`test_tb` VALUES "));
    }
}
