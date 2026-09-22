use std::collections::HashMap;

use anyhow::bail;

use super::{
    ddl_meta::ddl_data::DdlData,
    mssql::mssql_meta_manager::MssqlMetaManager,
    mysql::mysql_meta_manager::MysqlMetaManager,
    pg::pg_meta_manager::PgMetaManager,
    rdb_tb_meta::{RdbTbMeta, SortDirection},
};
use crate::error::DtError;

pub const RDB_PRIMARY_KEY: &str = "_rdb_primary_key_";

#[derive(Clone, Default)]
pub struct RdbMetaManager {
    pub mysql_meta_manager: Option<MysqlMetaManager>,
    pub pg_meta_manager: Option<PgMetaManager>,
    pub mssql_meta_manager: Option<MssqlMetaManager>,
}

impl RdbMetaManager {
    pub fn from_mysql(mysql_meta_manager: MysqlMetaManager) -> Self {
        Self {
            mysql_meta_manager: Some(mysql_meta_manager),
            ..Default::default()
        }
    }

    pub fn from_pg(pg_meta_manager: PgMetaManager) -> Self {
        Self {
            pg_meta_manager: Some(pg_meta_manager),
            ..Default::default()
        }
    }

    pub fn from_mssql(mssql_meta_manager: MssqlMetaManager) -> Self {
        Self {
            mssql_meta_manager: Some(mssql_meta_manager),
            ..Default::default()
        }
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if let Some(mysql_meta_manager) = &self.mysql_meta_manager {
            mysql_meta_manager.close().await?;
        }
        if let Some(pg_meta_manager) = &self.pg_meta_manager {
            pg_meta_manager.close().await?;
        }
        if let Some(mssql_meta_manager) = &self.mssql_meta_manager {
            mssql_meta_manager.close().await?;
        }
        Ok(())
    }

    pub async fn get_tb_meta<'a>(
        &'a mut self,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<&'a RdbTbMeta> {
        if let Some(mysql_meta_manager) = self.mysql_meta_manager.as_mut() {
            let tb_meta = mysql_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok(&tb_meta.basic);
        }

        if let Some(pg_meta_manager) = self.pg_meta_manager.as_mut() {
            let tb_meta = pg_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok(&tb_meta.basic);
        }

        if let Some(mssql_meta_manager) = self.mssql_meta_manager.as_mut() {
            let tb_meta = mssql_meta_manager.get_tb_meta(db, schema, tb).await?;
            return Ok(&tb_meta.basic);
        }

        bail! {DtError::InvariantViolated("no available meta_manager in partitioner".to_string())
        }
    }

    pub fn invalidate_cache_by_ddl_data(&mut self, ddl_data: &DdlData) {
        if let Some(mysql_meta_manager) = &mut self.mysql_meta_manager {
            mysql_meta_manager.invalidate_cache_by_ddl_data(ddl_data);
        }
        if let Some(pg_meta_manager) = &mut self.pg_meta_manager {
            pg_meta_manager.invalidate_cache_by_ddl_data(ddl_data);
        }
        if let Some(mssql_meta_manager) = &mut self.mssql_meta_manager {
            mssql_meta_manager.invalidate_cache_by_ddl_data(ddl_data);
        }
    }

    pub fn invalidate_cache(&mut self, db: &str, schema: &str, tb: &str) {
        if let Some(mysql_meta_manager) = &mut self.mysql_meta_manager {
            mysql_meta_manager.invalidate_cache(schema, tb);
        }
        if let Some(pg_meta_manager) = &mut self.pg_meta_manager {
            pg_meta_manager.invalidate_cache(schema, tb);
        }
        if let Some(mssql_meta_manager) = &mut self.mssql_meta_manager {
            mssql_meta_manager.invalidate_cache(db, schema, tb);
        }
    }

    pub fn invalidate_cache_for_table(&mut self, db: &str, schema: &str, tb: &str) {
        if let Some(mysql_meta_manager) = &mut self.mysql_meta_manager {
            mysql_meta_manager.invalidate_cache_for_table(schema, tb);
        }
        if let Some(pg_meta_manager) = &mut self.pg_meta_manager {
            pg_meta_manager.invalidate_cache_for_table(schema, tb);
        }
        if let Some(mssql_meta_manager) = &mut self.mssql_meta_manager {
            mssql_meta_manager.invalidate_cache_for_table(db, schema, tb);
        }
    }

    pub fn select_order_key(
        tb_meta: &RdbTbMeta,
        key_scores: &HashMap<String, u32>,
    ) -> Option<String> {
        let mut keys = tb_meta
            .key_map
            .iter()
            .filter(|(key, cols)| !cols.is_empty() && key_scores.contains_key(*key))
            .map(|(key, cols)| {
                let has_nullable_col = cols.iter().any(|col| tb_meta.is_col_nullable(col));
                (key, cols, has_nullable_col)
            })
            .collect::<Vec<_>>();

        // The following stable sorts preserve key-name order when priorities tie.
        keys.sort_by(|a, b| a.0.cmp(b.0));
        keys.sort_by_key(|(key, cols, has_nullable_col)| {
            (
                *key != RDB_PRIMARY_KEY,
                key_scores[*key],
                cols.len(),
                *has_nullable_col,
            )
        });
        keys.into_iter().next().map(|(key, _, _)| key.clone())
    }

    pub fn set_order_cols(
        tb_meta: &mut RdbTbMeta,
        key_scores: &HashMap<String, u32>,
        mut key_col_attrs: HashMap<String, HashMap<String, SortDirection>>,
    ) -> anyhow::Result<()> {
        tb_meta.order_cols.clear();
        tb_meta.order_col_attrs.clear();
        if let Some(key) = Self::select_order_key(tb_meta, key_scores) {
            let cols = &tb_meta.key_map[&key];
            let attrs = key_col_attrs.remove(&key).ok_or_else(|| {
                DtError::InvariantViolated(format!("missing order column directions for key {key}"))
            })?;
            if attrs.len() != cols.len() || cols.iter().any(|col| !attrs.contains_key(col)) {
                bail!(DtError::InvariantViolated(format!(
                    "incomplete order column directions for key {key}"
                )));
            }
            tb_meta.order_cols = cols.clone();
            tb_meta.order_col_attrs = attrs;
            crate::log_debug!(
                "snapshot order key for {}.{}.{}: {}, columns: {:?}, score: {}",
                tb_meta.db,
                tb_meta.schema,
                tb_meta.tb,
                key,
                tb_meta
                    .order_cols
                    .iter()
                    .map(|col| (col, tb_meta.order_col_attrs[col]))
                    .collect::<Vec<_>>(),
                key_scores[&key]
            );
        }
        tb_meta.id_cols = if tb_meta.order_cols.is_empty() {
            tb_meta.cols.clone()
        } else {
            tb_meta.order_cols.clone()
        };
        tb_meta.partition_col = tb_meta.id_cols.first().cloned().ok_or_else(|| {
            DtError::InvariantViolated(
                "cannot select identity columns for an empty table definition".to_string(),
            )
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_key_columns() {
        #[derive(Default)]
        struct Case {
            name: &'static str,
            keys: &'static [(&'static str, &'static [&'static str], Option<u32>)],
            nullable: &'static [&'static str],
            expected: Option<&'static str>,
            expected_id: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "primary before score and column count",
                keys: &[
                    (RDB_PRIMARY_KEY, &["a", "b"], Some(9)),
                    ("uk", &["c"], Some(1)),
                ],
                expected: Some(RDB_PRIMARY_KEY),
                expected_id: &["a", "b"],
                ..Default::default()
            },
            Case {
                name: "unsupported primary",
                keys: &[(RDB_PRIMARY_KEY, &["a"], None), ("uk", &["b"], Some(8))],
                expected: Some("uk"),
                expected_id: &["b"],
                ..Default::default()
            },
            Case {
                name: "no eligible keys fall back to all columns",
                keys: &[("uk", &["a"], None), ("empty", &[], Some(0))],
                expected: None,
                expected_id: &["a", "b", "c"],
                ..Default::default()
            },
            Case {
                name: "score before column count and nullability",
                keys: &[("a", &["a", "b"], Some(2)), ("z", &["c"], Some(8))],
                nullable: &["b"],
                expected: Some("a"),
                expected_id: &["a", "b"],
                ..Default::default()
            },
            Case {
                name: "column count before nullable",
                keys: &[("a", &["a", "b"], Some(2)), ("z", &["c"], Some(2))],
                nullable: &["c"],
                expected: Some("z"),
                expected_id: &["c"],
            },
            Case {
                name: "nonnull breaks score and column count tie",
                keys: &[("a", &["a"], Some(1)), ("z", &["b"], Some(1))],
                nullable: &["a"],
                expected: Some("z"),
                expected_id: &["b"],
            },
            Case {
                name: "empty primary does not hide valid unique",
                keys: &[(RDB_PRIMARY_KEY, &[], Some(0)), ("uk", &["b"], Some(8))],
                expected: Some("uk"),
                expected_id: &["b"],
                ..Default::default()
            },
            Case {
                name: "stable key name preserves declared column order",
                keys: &[("z", &["c", "b"], Some(2)), ("a", &["b", "a"], Some(2))],
                expected: Some("a"),
                expected_id: &["b", "a"],
                ..Default::default()
            },
        ];
        for case in cases {
            let mut tb_meta = RdbTbMeta {
                cols: vec!["a".into(), "b".into(), "c".into()],
                key_map: case
                    .keys
                    .iter()
                    .map(|(key, cols, _)| {
                        (
                            key.to_string(),
                            cols.iter().map(|col| col.to_string()).collect(),
                        )
                    })
                    .collect(),
                nullable_cols: case.nullable.iter().map(|col| col.to_string()).collect(),
                ..Default::default()
            };
            let scores = case
                .keys
                .iter()
                .filter_map(|(key, _, score)| score.map(|score| (key.to_string(), score)))
                .collect();
            let expected_order_cols = case
                .expected
                .map(|key| tb_meta.key_map[key].clone())
                .unwrap_or_default();
            let attrs = tb_meta
                .key_map
                .iter()
                .map(|(key, cols)| {
                    (
                        key.clone(),
                        cols.iter()
                            .map(|col| (col.clone(), SortDirection::Asc))
                            .collect(),
                    )
                })
                .collect();
            RdbMetaManager::set_order_cols(&mut tb_meta, &scores, attrs).unwrap();
            assert_eq!(tb_meta.order_cols, expected_order_cols, "{}", case.name);
            assert_eq!(tb_meta.id_cols, case.expected_id, "{}", case.name);
            assert_eq!(tb_meta.partition_col, case.expected_id[0], "{}", case.name);
        }
    }

    #[test]
    fn test_order_directions_belong_to_selected_key() {
        use SortDirection::*;
        let mut meta = RdbTbMeta {
            cols: vec!["a".into(), "b".into()],
            key_map: HashMap::from([
                (RDB_PRIMARY_KEY.into(), vec!["a".into(), "b".into()]),
                ("uk".into(), vec!["b".into(), "a".into()]),
            ]),
            ..Default::default()
        };
        let attrs = HashMap::from([
            (
                RDB_PRIMARY_KEY.into(),
                HashMap::from([("a".into(), Asc), ("b".into(), Desc)]),
            ),
            (
                "uk".into(),
                HashMap::from([("a".into(), Desc), ("b".into(), Asc)]),
            ),
        ]);
        RdbMetaManager::set_order_cols(&mut meta, &HashMap::from([("uk".into(), 2)]), attrs)
            .unwrap();
        assert_eq!(meta.order_cols, ["b", "a"]);
        assert_eq!(meta.order_col_attrs["a"], Desc);
        assert_eq!(meta.order_col_attrs["b"], Asc);
        assert_eq!(meta.id_cols, ["b", "a"]);
        assert_eq!(meta.partition_col, "b");
        assert!(RdbMetaManager::set_order_cols(
            &mut meta,
            &HashMap::from([("uk".into(), 2)]),
            HashMap::new()
        )
        .is_err());
        RdbMetaManager::set_order_cols(&mut meta, &HashMap::new(), HashMap::new()).unwrap();
        assert!(meta.order_cols.is_empty());
        assert!(meta.order_col_attrs.is_empty());
        assert_eq!(meta.id_cols, ["a", "b"]);
        assert_eq!(meta.partition_col, "a");
    }

    #[test]
    fn test_engine_key_scores() {
        use crate::meta::{
            mssql::{mssql_col_type::MssqlColType, mssql_tb_meta::MssqlTbMeta},
            mysql::{mysql_col_type::MysqlColType, mysql_tb_meta::MysqlTbMeta},
            pg::{pg_col_type::PgColType, pg_tb_meta::PgTbMeta, pg_value_type::PgValueType},
        };
        let basic = RdbTbMeta {
            key_map: HashMap::from([
                ("integer".into(), vec!["a".into()]),
                ("float".into(), vec!["b".into()]),
                ("composite".into(), vec!["a".into(), "b".into()]),
                ("unsupported".into(), vec!["c".into(), "a".into()]),
                ("bit".into(), vec!["d".into()]),
                ("empty".into(), vec![]),
            ]),
            ..Default::default()
        };
        let expected = HashMap::from([
            ("integer".into(), 1),
            ("float".into(), 5),
            ("composite".into(), 6),
            ("bit".into(), 6),
        ]);
        let assert_key_priority =
            |meta: &RdbTbMeta, mut scores: HashMap<String, u32>, expected_keys: &[&str]| {
                for key in expected_keys {
                    assert_eq!(
                        RdbMetaManager::select_order_key(meta, &scores).as_deref(),
                        Some(*key)
                    );
                    scores.remove(*key);
                }
                assert!(RdbMetaManager::select_order_key(meta, &scores).is_none());
            };
        let mut mysql = MysqlTbMeta {
            basic: basic.clone(),
            col_type_map: HashMap::from([
                ("a".into(), MysqlColType::Int { unsigned: false }),
                ("b".into(), MysqlColType::Double),
                ("c".into(), MysqlColType::Point),
                ("d".into(), MysqlColType::Bit),
            ]),
        };
        let mysql_scores = MysqlMetaManager::get_key_scores(&mysql).unwrap();
        assert_eq!(mysql_scores, expected);
        assert_key_priority(
            &mysql.basic,
            mysql_scores,
            &["integer", "float", "bit", "composite"],
        );
        mysql
            .basic
            .key_map
            .insert("missing".into(), vec!["c".into(), "missing".into()]);
        assert!(MysqlMetaManager::get_key_scores(&mysql).is_err());

        let mut mssql = MssqlTbMeta {
            basic: basic.clone(),
            col_type_map: HashMap::from([
                ("a".into(), MssqlColType::Int4),
                ("b".into(), MssqlColType::Float8),
                ("c".into(), MssqlColType::AssemblyUdt),
                ("d".into(), MssqlColType::Bitn),
                ("e".into(), MssqlColType::Guid),
            ]),
            ..Default::default()
        };
        mssql.basic.key_map.insert("uuid".into(), vec!["e".into()]);
        let mut expected_mssql = expected.clone();
        expected_mssql.insert("uuid".into(), 4);
        let mssql_scores = MssqlMetaManager::get_key_scores(&mssql).unwrap();
        assert_eq!(mssql_scores, expected_mssql);
        assert_key_priority(
            &mssql.basic,
            mssql_scores,
            &["integer", "uuid", "float", "bit", "composite"],
        );
        mssql
            .basic
            .key_map
            .insert("missing".into(), vec!["c".into(), "missing".into()]);
        assert!(MssqlMetaManager::get_key_scores(&mssql).is_err());

        let pg_col_type = |oid| PgColType {
            oid,
            value_type: PgValueType::from_oid(oid),
            name: String::new(),
            alias: String::new(),
            parent_oid: 0,
            element_oid: 0,
            category: String::new(),
            enum_values: None,
            schema_name: String::new(),
            typmod: -1,
        };
        let mut pg = PgTbMeta {
            basic,
            col_type_map: HashMap::from([
                ("a".into(), pg_col_type(23)),
                ("b".into(), pg_col_type(701)),
                ("c".into(), pg_col_type(99999)),
                ("d".into(), pg_col_type(1560)),
                ("e".into(), pg_col_type(2950)),
                ("flag".into(), pg_col_type(16)),
                ("text".into(), pg_col_type(25)),
            ]),
            ..Default::default()
        };
        pg.basic.key_map.extend([
            ("uuid".into(), vec!["e".into()]),
            ("bool".into(), vec!["flag".into()]),
            ("text".into(), vec!["text".into()]),
        ]);
        let mut expected_pg = expected;
        expected_pg.extend([("uuid".into(), 4), ("bool".into(), 32), ("text".into(), 8)]);
        expected_pg.insert("unsupported".into(), 33); // A catalog-confirmed unique custom type retains its existing path.
        let pg_scores = PgMetaManager::get_key_scores(&pg).unwrap();
        assert_eq!(pg_scores, expected_pg);
        assert_key_priority(
            &pg.basic,
            pg_scores,
            &[
                "integer",
                "uuid",
                "float",
                "bit",
                "composite",
                "text",
                "bool",
                "unsupported",
            ],
        );

        pg.basic
            .key_map
            .insert("missing".into(), vec!["missing".into()]);
        assert!(PgMetaManager::get_key_scores(&pg).is_err());
        for (oid, category, expected_weight) in [
            (701, "N", 5),
            (869, "I", 16),
            (3904, "R", 20),
            (1007, "A", 20),
            (99999, "U", 32),
        ] {
            let mut col = pg_col_type(oid);
            col.category = category.into();
            assert_eq!(col.order_key_weight(), Some(expected_weight));
        }
    }
}
