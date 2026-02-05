use std::{
    collections::{HashMap, HashSet},
    vec,
};

use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use crate::test_runner::mock_utils::{pg_type::PgType, random::Random};

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Constraint {
    Primary(Vec<usize>), // column indices
    Unique(Vec<usize>),  // column indices
    None,
}

#[derive(Debug)]
pub struct MockStmt {
    pub included_types: Vec<PgType>,
    pub db: String,
    pub tb: String,
    pub indexs: Vec<Constraint>,
    pub nullable_cols: HashSet<usize>,
}

impl MockStmt {
    pub fn new(included_types: &[PgType], db: &str, tb: &str) -> Self {
        Self {
            included_types: included_types.to_vec(),
            db: db.into(),
            tb: tb.into(),
            indexs: vec![],
            nullable_cols: HashSet::new(),
        }
    }

    pub fn with_nullable_cols(mut self, nullable_cols: &[usize]) -> Self {
        self.nullable_cols = nullable_cols
            .iter()
            .filter(|&&col_idx| col_idx < self.included_types.len())
            .cloned()
            .collect();
        self
    }

    pub fn with_index(mut self, index: Constraint) -> Self {
        let filtered_index = index;
        match &filtered_index {
            Constraint::Primary(cols) | Constraint::Unique(cols) => {
                if cols.is_empty() {
                    return self;
                }
                let mut set = HashSet::new();
                let filtered_cols = cols
                    .iter()
                    .filter(|&&col_idx| col_idx < self.included_types.len())
                    .filter(|&&col_idx| self.is_bad_col_for_index(col_idx))
                    .filter(|&&col_idx| set.insert(col_idx))
                    .cloned()
                    .collect::<Vec<usize>>();
                if filtered_cols.len() != cols.len() {
                    // println!("bad index cols");
                    return self;
                }
            }
            _ => return self,
        }
        for idx in &self.indexs {
            match (&filtered_index, &idx) {
                (Constraint::Primary(cols1), Constraint::Primary(cols2))
                | (Constraint::Unique(cols1), Constraint::Unique(cols2))
                | (Constraint::Primary(cols1), Constraint::Unique(cols2))
                | (Constraint::Unique(cols1), Constraint::Primary(cols2)) => {
                    for col in cols1 {
                        if cols2.contains(col) {
                            return self;
                        }
                    }
                }
                _ => return self,
            }
        }
        self.indexs.push(filtered_index);
        self
    }

    pub fn create_schema_stmt(&self) -> Vec<String> {
        // create schema statements
        vec![
            format!("DROP SCHEMA IF EXISTS {} CASCADE;", self.db),
            format!("CREATE SCHEMA IF NOT EXISTS {};", self.db),
        ]
    }

    pub fn create_table_stmt(&self) -> String {
        let mut col_defs = vec![];
        let mut col_names = vec![];
        // columns
        for (col_idx, pg_type) in self.included_types.iter().enumerate() {
            let col_name = format!("col_{}", col_idx);
            let is_nullable = if self.nullable_cols.contains(&col_idx) {
                ""
            } else {
                " NOT NULL"
            };
            let col_def = format!("{} {}{}", col_name, pg_type.name(), is_nullable);
            col_names.push(col_name);
            col_defs.push(col_def);
        }
        for index in self.indexs.iter() {
            match index {
                Constraint::Primary(col_idxs) => {
                    let pk_cols = col_idxs
                        .iter()
                        .map(|&i| col_names.get(i).cloned().unwrap())
                        .collect::<Vec<String>>();
                    col_defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
                }
                Constraint::Unique(col_idxs) => {
                    let uq_cols = col_idxs
                        .iter()
                        .map(|&i| col_names.get(i).cloned().unwrap())
                        .collect::<Vec<String>>();
                    col_defs.push(format!("UNIQUE ({})", uq_cols.join(", ")));
                }
                _ => continue,
            }
        }
        format!(
            "CREATE TABLE {}.{} ({});",
            self.db,
            self.tb,
            col_defs.join(", ")
        )
    }

    pub fn insert_value_stmt(&self, random: &mut Random, cnt: usize) -> Vec<String> {
        // println!("Start generating insert statements for table {:?}.{:?}, stmt: {:?}", self.db, self.tb, self);
        let mut stmts = vec![];
        if cnt == 0 {
            return stmts;
        }
        let mut col_index_map = HashMap::new();
        let mut index_col_values: Vec<Vec<Vec<Option<String>>>> = vec![];

        for (i, index) in self.indexs.iter().enumerate() {
            match index {
                Constraint::Primary(cols) | Constraint::Unique(cols) => {
                    let mut set: HashSet<Vec<String>> = HashSet::new();
                    let mut vec: Vec<Vec<Option<String>>> = vec![];
                    let col_types: Vec<&PgType> = cols
                        .iter()
                        .map(|&col_idx| self.included_types.get(col_idx).unwrap())
                        .collect();
                    for (j, col) in cols.iter().enumerate() {
                        col_index_map.insert(*col, (i, j));
                    }

                    // inject constant values first
                    let col_constants = col_types
                        .iter()
                        .map(|pg_type| {
                            let mut values = pg_type.constant_value_str();
                            values.shuffle(&mut random.rng);
                            values
                        })
                        .collect::<Vec<Vec<String>>>();
                    let max_len = col_constants.iter().map(|v| v.len()).max().unwrap_or(0);
                    if max_len > 0 {
                        for row_idx in 0..max_len {
                            let mut row: Vec<String> = vec![];
                            for (col_idx, &col_type) in col_types.iter().enumerate() {
                                let values = col_constants.get(col_idx).unwrap();
                                let value = if row_idx < values.len() {
                                    values[row_idx].clone()
                                } else {
                                    col_type.next_value_str(random)
                                };
                                row.push(value);
                            }
                            // it should be promised that constant values do not generate duplicate rows
                            if !set.insert(row.clone()) {
                                // println!("duplicate constant index values generated, stmt: {:?}, set: {:?}, row: {:?}, consts: {:?}", self, set, row, col_constants);
                                continue;
                            }
                            vec.push(row.into_iter().map(Some).collect());
                        }
                    }

                    // inject NULL values for nullable columns
                    let col_cnt = col_types.len();
                    if let Constraint::Unique(_) = index {
                        for (col, (j, &_pg_type)) in cols.iter().zip(col_types.iter().enumerate()) {
                            let mut null_vec = Vec::with_capacity(col_cnt);
                            if self.nullable_cols.contains(col) {
                                for (k, &pg_type) in col_types.iter().enumerate() {
                                    if k == j {
                                        null_vec.push(None);
                                    } else {
                                        let value_str = pg_type.next_value_str(random);
                                        null_vec.push(Some(value_str));
                                    }
                                }
                            }
                            if null_vec.is_empty() {
                                continue;
                            }
                            vec.push(null_vec);
                        }
                        let mut null_vec = Vec::with_capacity(col_cnt);
                        for (col, (_j, &pg_type)) in cols.iter().zip(col_types.iter().enumerate()) {
                            if self.nullable_cols.contains(col) {
                                null_vec.push(None);
                            } else {
                                let value_str = pg_type.next_value_str(random);
                                null_vec.push(Some(value_str));
                            }
                        }
                        vec.push(null_vec);
                    }

                    // inject random values until reach cnt
                    let starter = if vec.is_empty() { 0 } else { vec.len() };
                    let max_retries = cnt * 10;
                    for _ in starter..cnt {
                        let mut retries = 0;
                        loop {
                            let mut key = vec![];
                            for pg_type in &col_types {
                                let value_str = pg_type.next_value_str(random);
                                key.push(value_str);
                            }
                            if set.insert(key.clone()) {
                                vec.push(key.into_iter().map(Some).collect());
                                break;
                            }
                            retries += 1;
                            if retries >= max_retries {
                                panic!(
                                    "failed to generate enough unique index values, stmt: {:?}, generated: {}, required: {}",
                                    self,
                                    vec.len(),
                                    cnt
                                );
                            }
                        }
                    }
                    vec.truncate(cnt);
                    index_col_values.push(vec);
                }
                _ => continue,
            }
        }

        for i in 0..cnt {
            let mut values = vec![];
            for (idx, _pg_type) in self.included_types.iter().enumerate() {
                if let Some((index_idx, col_idx)) = col_index_map.get(&idx) {
                    let value_str = index_col_values
                        .get(*index_idx)
                        .unwrap()
                        .get(i)
                        .unwrap()
                        .get(*col_idx)
                        .unwrap()
                        .clone();
                    values.push(value_str);
                    continue;
                }
                let value = self.get_next_value_str(idx, random);
                values.push(value);
            }
            stmts.push(format!(
                "INSERT INTO {}.{} VALUES ({});",
                self.db,
                self.tb,
                values
                    .into_iter()
                    .map(|v| v.unwrap_or("NULL".to_string()))
                    .collect::<Vec<String>>()
                    .join(", ")
            ));
        }
        println!(
            "Generated {} insert statements for table {}.{}, stmt: {:?}",
            stmts.len(),
            self.db,
            self.tb,
            self
        );
        stmts
    }

    fn get_next_value_str(&self, col_idx: usize, random: &mut Random) -> Option<String> {
        let pg_type = self.included_types.get(col_idx).unwrap();
        if self.nullable_cols.contains(&col_idx) && random.next_null() {
            return None;
        }
        Some(pg_type.next_value_str(random))
    }

    fn is_bad_col_for_index(&self, col_idx: usize) -> bool {
        let pg_type = self.included_types.get(col_idx).unwrap();
        if !pg_type.support_btree_index() {
            // println!("unsupported type for btree index: {:?}", pg_type);
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_generation() {
        let mock_stmt = MockStmt::new(&[PgType::Int4, PgType::Varchar], "test_db", "test_tb");
        let db_stmts = mock_stmt.create_schema_stmt();
        assert_eq!(db_stmts.len(), 2);
        assert_eq!(db_stmts[0], "DROP SCHEMA IF EXISTS test_db CASCADE;");
        assert_eq!(db_stmts[1], "CREATE SCHEMA IF NOT EXISTS test_db;");

        let table_stmt = mock_stmt.create_table_stmt();
        assert_eq!(
            table_stmt,
            "CREATE TABLE test_db.test_tb (col_0 int4 NOT NULL, col_1 varchar NOT NULL);"
        );
    }

    #[test]
    fn test_full_schema_generation() {
        let mock_stmt = MockStmt::new(
            &[PgType::Int4, PgType::Varchar, PgType::Float8],
            "test_db",
            "test_tb",
        )
        .with_index(Constraint::Primary(vec![0, 1]))
        .with_index(Constraint::Unique(vec![2]))
        .with_nullable_cols(&[1, 2]);
        let table_stmt = mock_stmt.create_table_stmt();
        assert_eq!(
            table_stmt,
            "CREATE TABLE test_db.test_tb (col_0 int4 NOT NULL, col_1 varchar, col_2 float8, PRIMARY KEY (col_0, col_1), UNIQUE (col_2));"
        );
    }

    #[test]
    fn test_insert_value_generation() {
        let mut random = Random::new(Some(42));
        let mock_stmt: MockStmt = MockStmt::new(
            &[PgType::Int4, PgType::Float4, PgType::Bool],
            "test_db",
            "test_tb",
        )
        .with_index(Constraint::Primary(vec![0]))
        .with_index(Constraint::Unique(vec![1]));
        let insert_stmts = mock_stmt.insert_value_stmt(&mut random, 10);
        assert_eq!(insert_stmts.len(), 10);
        for stmt in insert_stmts {
            println!("{}", stmt);
        }
    }

    #[test]
    fn test_insert_value_generation_with_nullable() {
        let mut random = Random::new(Some(42));
        let mock_stmt = MockStmt::new(
            &[PgType::Int4, PgType::Float4, PgType::Int8, PgType::Bool],
            "test_db",
            "test_tb",
        )
        .with_nullable_cols(&[0, 1, 2, 3])
        .with_index(Constraint::Primary(vec![0]))
        .with_index(Constraint::Unique(vec![1, 2]));
        let insert_stmts = mock_stmt.insert_value_stmt(&mut random, 20);
        assert_eq!(insert_stmts.len(), 20);
        for stmt in insert_stmts {
            println!("{}", stmt);
        }
    }
}
