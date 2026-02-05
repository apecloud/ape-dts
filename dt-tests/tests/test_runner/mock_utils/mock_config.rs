use dt_common::config::ini_loader::IniLoader;

use crate::test_runner::mock_utils::{
    mock_stmt::{Constraint, MockStmt},
    pg_type::PgType,
    random::Random,
};

pub struct MockConfig {
    pub insert_rows: usize,
    pub mock_stmts: Vec<MockStmt>,
    pub seed: u64,
}

impl MockConfig {
    pub fn new(config_file: &str) -> Option<Self> {
        let loader = IniLoader::new(config_file);
        let pg_types =
            if let Some(config_map) = loader.ini.get_map().unwrap_or_default().get("mock") {
                let pg_types = config_map
                    .iter()
                    .filter(|(k, _v)| k.starts_with("pg_types"))
                    .map(|(_, v)| {
                        serde_json::from_str::<Vec<PgType>>(v.clone().unwrap_or_default().as_str())
                            .unwrap()
                    })
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<Vec<PgType>>>();
                if pg_types.is_empty() {
                    return None;
                }
                pg_types
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
            let all_types = pg_types.first().unwrap().clone();
            let mut mock_stmt =
                MockStmt::new(&all_types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                    .with_nullable_cols(&nullable_cols);
            for constraint in constraints {
                mock_stmt = mock_stmt.with_index(constraint);
            }
            mock_stmts.push(mock_stmt);
            return Some(MockConfig {
                mock_stmts,
                insert_rows,
                seed,
            });
        }
        // no index, all non-nullable
        mock_stmts.extend(
            pg_types.iter().map(|types| {
                MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
            }),
        );
        // no index, all nullable
        mock_stmts.extend(pg_types.iter().map(|types| {
            MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
        }));
        // single column primary index, all nullable
        mock_stmts.extend(pg_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                stmts.push(
                    MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                        .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                        .with_index(Constraint::Primary(vec![col_idx])),
                );
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        // composite primary index, all nullable
        mock_stmts.extend(pg_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                for col_idx2 in (col_idx + 1)..types.len() {
                    stmts.push(
                        MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                            .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                            .with_index(Constraint::Primary(vec![col_idx, col_idx2])),
                    );
                }
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        // single column unique index, all nullable
        mock_stmts.extend(pg_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                stmts.push(
                    MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                        .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                        .with_index(Constraint::Unique(vec![col_idx])),
                );
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        // composite unique index, all nullable
        mock_stmts.extend(pg_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                for col_idx2 in (col_idx + 1)..types.len() {
                    stmts.push(
                        MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                            .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                            .with_index(Constraint::Unique(vec![col_idx, col_idx2])),
                    );
                }
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        // one primary index, all unique index, all nullable
        mock_stmts.extend(pg_types.iter().flat_map(|types| {
            let mut stmts = Vec::new();
            for col_idx in 0..types.len() {
                let mut stmt =
                    MockStmt::new(types, &db_str, &Self::gen_mock_tb_name(&mut tb_suffix))
                        .with_nullable_cols(&(0..types.len()).collect::<Vec<usize>>())
                        .with_index(Constraint::Primary(vec![col_idx]));
                for col_idx2 in 0..types.len() {
                    if col_idx2 != col_idx {
                        stmt = stmt.with_index(Constraint::Unique(vec![col_idx2]));
                    }
                }
                stmts.push(stmt);
            }
            stmts
                .into_iter()
                .filter(|s| !s.indexs.is_empty())
                .collect::<Vec<_>>()
        }));
        Some(MockConfig {
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
                res.extend(mock_stmt.create_schema_stmt());
            }
            res.push(mock_stmt.create_table_stmt());
        }
        res
    }

    pub fn mock_dml_stmts(&self) -> Vec<String> {
        let mut res = vec![];
        let mut random = Random::new(Some(self.seed));
        for mock_stmt in &self.mock_stmts {
            res.extend(mock_stmt.insert_value_stmt(&mut random, self.insert_rows));
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
}
