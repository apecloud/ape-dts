use std::collections::HashMap;

use anyhow::bail;
use dt_common::{
    error::DtError,
    meta::{
        adaptor::{
            sqlx_ext::{SqlxMysqlExt, SqlxPgExt},
            tiberius_ext::TiberiusExt,
        },
        col_value::ColValue,
        mssql::mssql_tb_meta::MssqlTbMeta,
        mysql::mysql_tb_meta::MysqlTbMeta,
        pg::pg_tb_meta::PgTbMeta,
    },
};
use sqlx::{mysql::MySqlArguments, postgres::PgArguments, query::Query, MySql, Postgres};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RdbSnapshotQuery {
    pub sql: String,
    /// One column per placeholder occurrence, including repeated prefix columns.
    /// Range queries contain the complete start binds followed by the end binds.
    pub cols: Vec<String>,
}

impl RdbSnapshotQuery {
    /// Expand one cursor into placeholder order without cloning its column values.
    pub fn get_bind_values<'a>(
        &self,
        col_values: &'a HashMap<String, ColValue>,
    ) -> anyhow::Result<Vec<&'a ColValue>> {
        self.cols
            .iter()
            .map(|col| {
                col_values.get(col).ok_or_else(|| {
                    DtError::InvariantViolated(format!(
                        "snapshot binding column {col} has no cursor value"
                    ))
                    .into()
                })
            })
            .collect()
    }

    pub fn create_mysql_query<'q>(
        &'q self,
        tb_meta: &MysqlTbMeta,
        values: &[&'q ColValue],
    ) -> anyhow::Result<Query<'q, MySql, MySqlArguments>> {
        self.validate_bind_values(values)?;
        let mut query = sqlx::query::<MySql>(&self.sql);
        for (col, value) in self.cols.iter().zip(values) {
            query = query.bind_col_value(Some(*value), tb_meta.get_col_type(col)?);
        }
        Ok(query)
    }

    pub fn create_pg_query<'q>(
        &'q self,
        tb_meta: &PgTbMeta,
        values: &[&'q ColValue],
    ) -> anyhow::Result<Query<'q, Postgres, PgArguments>> {
        self.validate_bind_values(values)?;
        let mut query = sqlx::query::<Postgres>(&self.sql);
        for (col, value) in self.cols.iter().zip(values) {
            query = query.bind_col_value(Some(*value), tb_meta.get_col_type(col)?);
        }
        Ok(query)
    }

    pub fn create_mssql_query<'q>(
        &'q self,
        tb_meta: &MssqlTbMeta,
        values: &[&'q ColValue],
    ) -> anyhow::Result<tiberius::Query<'q>> {
        self.validate_bind_values(values)?;
        let mut query = tiberius::Query::new(self.sql.as_str());
        for (col, value) in self.cols.iter().zip(values) {
            query.bind_col_value(value, tb_meta.get_col_type(col)?)?;
        }
        Ok(query)
    }

    fn validate_bind_values(&self, values: &[&ColValue]) -> anyhow::Result<()> {
        if values.len() != self.cols.len() {
            bail!(DtError::InvariantViolated(format!(
                "snapshot query expects {} bind values, got {}",
                self.cols.len(),
                values.len()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_bind_values() {
        #[derive(Default)]
        struct Case {
            name: &'static str,
            cols: &'static [&'static str],
            expected: Option<&'static [&'static str]>,
        }
        let cursor = HashMap::from([
            ("a".to_string(), ColValue::Long(10)),
            ("b".to_string(), ColValue::Long(90)),
            ("nullable".to_string(), ColValue::None),
        ]);
        let cases = [
            Case {
                name: "no cursor placeholders",
                expected: Some(&[]),
                ..Default::default()
            },
            Case {
                name: "placeholder order with repeated prefix",
                cols: &["b", "b", "a"],
                expected: Some(&["b", "b", "a"]),
            },
            Case {
                name: "explicit null is a present value",
                cols: &["nullable"],
                expected: Some(&["nullable"]),
            },
            Case {
                name: "missing cursor column is not null",
                cols: &["a", "missing"],
                expected: None,
            },
        ];
        for case in cases {
            let query = RdbSnapshotQuery {
                cols: case.cols.iter().map(|col| col.to_string()).collect(),
                ..Default::default()
            };
            let result = query.get_bind_values(&cursor);
            if let Some(expected) = case.expected {
                let values = result.unwrap();
                assert_eq!(values.len(), expected.len(), "{}", case.name);
                for (value, col) in values.into_iter().zip(expected) {
                    assert!(std::ptr::eq(value, &cursor[*col]), "{}", case.name);
                }
            } else {
                assert!(result.is_err(), "{}", case.name);
            }
        }
    }

    #[test]
    fn test_create_query_rejects_missing_or_extra_values() {
        let query = RdbSnapshotQuery {
            cols: vec!["id".into()],
            ..Default::default()
        };
        let values = [ColValue::Long(10), ColValue::Long(20)];
        for binds in [vec![], vec![&values[0], &values[1]]] {
            let results = [
                query
                    .create_mysql_query(&MysqlTbMeta::default(), &binds)
                    .map(|_| ()),
                query
                    .create_pg_query(&PgTbMeta::default(), &binds)
                    .map(|_| ()),
                query
                    .create_mssql_query(&MssqlTbMeta::default(), &binds)
                    .map(|_| ()),
            ];
            for result in results {
                let error = result.unwrap_err().to_string();
                assert!(
                    error.contains("snapshot query expects 1 bind values"),
                    "{error}"
                );
            }
        }
    }
}
