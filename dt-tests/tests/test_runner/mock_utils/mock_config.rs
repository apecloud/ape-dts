use dt_common::config::ini_loader::IniLoader;

use crate::test_runner::mock_utils::{
    mock_pg_type::PgType,
    mock_stmt::{Constraint, MockStmt},
    random::Random,
};

pub struct MockConfig {
    pub db: String,
    pub pg_types: Vec<PgType>,
    pub insert_rows: usize,
    pub mock_stmts: Vec<MockStmt>,
}

impl MockConfig {
    pub fn new(config_file: &str) -> Option<Self> {
        let loader = IniLoader::new(config_file);
        // todo(wl): use prefix to match `pg_types` config for flexibility
        // like `pg_types_int`, `pg_types_char`
        let pg_types_str: String = loader.get_optional("mock", "pg_types");
        if pg_types_str.is_empty() {
            return None;
        }
        let pg_types: Vec<PgType> = serde_json::from_str(pg_types_str.as_str()).unwrap();
        let db_str = loader.get_with_default("mock", "db", "mock_db_1".to_string());
        let insert_rows = loader.get_with_default("mock", "insert_rows_each_table", 20);
        Some(MockConfig {
            mock_stmts: vec![MockStmt::new(&pg_types, &db_str, "mock_tb_1")
                .with_index(Constraint::Primary(vec![0]))],
            pg_types,
            db: db_str.to_string(),
            insert_rows,
        })
    }

    pub fn mock_ddl_stmts(&self) -> Vec<String> {
        let mut res = vec![];
        for mock_stmt in &self.mock_stmts {
            res.extend(mock_stmt.create_schema_stmt());
            res.push(mock_stmt.create_table_stmt());
        }
        res
    }

    pub fn mock_dml_stmts(&self) -> Vec<String> {
        let mut res = vec![];
        for mock_stmt in &self.mock_stmts {
            res.extend(mock_stmt.insert_value_stmt(&mut Random::new(Some(42)), self.insert_rows));
        }
        res
    }
}
