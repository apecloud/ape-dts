use std::collections::HashSet;

use anyhow::{bail, Context};
use tiberius::Query;

use super::mssql_tb_meta::MssqlTbMeta;
use crate::{
    config::config_enums::DbType,
    error::DtError,
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor, col_value::ColValue,
        row_data::RowData, row_type::RowType,
    },
    utils::sql_util::SqlUtil,
};

pub struct MssqlTableQueryInfo<'a> {
    pub sql: String,
    // Batch queries repeat this column layout across multiple rows of binds.
    pub cols: Vec<String>,
    pub binds: Vec<&'a ColValue>,
}

impl MssqlTableQueryInfo<'_> {
    fn validate_bind_layout(&self) -> anyhow::Result<()> {
        if !self.binds.is_empty()
            && (self.cols.is_empty() || self.binds.len() % self.cols.len() != 0)
        {
            bail!(DtError::InvariantViolated(
                "MSSQL query bind column layout does not match bind values".to_string(),
            ));
        }
        Ok(())
    }
}

pub struct MssqlTableSqlBuilder<'a> {
    tb_meta: &'a MssqlTbMeta,
    ignore_cols: Option<&'a HashSet<String>>,
}

impl<'a> MssqlTableSqlBuilder<'a> {
    pub fn new(tb_meta: &'a MssqlTbMeta, ignore_cols: Option<&'a HashSet<String>>) -> Self {
        Self {
            tb_meta,
            ignore_cols,
        }
    }

    pub fn create_query<'q>(
        &self,
        query_info: &'q MssqlTableQueryInfo<'q>,
    ) -> anyhow::Result<Query<'q>> {
        query_info.validate_bind_layout()?;
        let col_types = query_info
            .cols
            .iter()
            .map(|col| self.tb_meta.get_col_type(col))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut query = Query::new(query_info.sql.as_str());
        for (index, value) in query_info.binds.iter().enumerate() {
            MssqlColValueConvertor::bind(&mut query, value, col_types[index % col_types.len()])?;
        }
        Ok(query)
    }

    pub fn get_insert_query<'r>(
        &self,
        row_data: &'r RowData,
        upsert: bool,
    ) -> anyhow::Result<MssqlTableQueryInfo<'r>> {
        if upsert {
            self.get_upsert_query(row_data)
        } else {
            let (query_info, _) = self.get_batch_insert_query(std::slice::from_ref(row_data))?;
            Ok(query_info)
        }
    }

    pub fn get_batch_insert_query<'r>(
        &self,
        data: &'r [RowData],
    ) -> anyhow::Result<(MssqlTableQueryInfo<'r>, usize)> {
        let first_row = data.first().context("MSSQL insert has no rows")?;
        if !matches!(first_row.row_type, RowType::Insert) {
            bail!("MSSQL snapshot sink only supports INSERT rows");
        }
        let first_after = first_row.require_after()?;
        let cols = self
            .tb_meta
            .basic
            .cols
            .iter()
            .filter(|col| first_after.contains_key(*col) && self.tb_meta.is_writable_col(col))
            .cloned()
            .collect::<Vec<_>>();
        if cols.is_empty() {
            bail!(DtError::InvariantViolated(format!(
                "MSSQL insert for {}.{} has no columns",
                self.tb_meta.basic.schema, self.tb_meta.basic.tb
            )));
        }

        let sql = format!(
            "INSERT INTO {}({}) VALUES{}",
            self.table_name(),
            self.escape_cols(&cols).join(","),
            self.get_batch_placeholders(&cols, data.len())?,
        );
        let mut data_size = 0;
        let mut binds = Vec::with_capacity(data.len().saturating_mul(cols.len()));
        for row_data in data {
            data_size += row_data.data_size;
            let after = row_data.require_after()?;
            if self
                .tb_meta
                .basic
                .cols
                .iter()
                .filter(|col| after.contains_key(*col) && self.tb_meta.is_writable_col(col))
                .ne(cols.iter())
            {
                bail!(DtError::InvariantViolated(format!(
                    "MSSQL insert rows have inconsistent column layouts for {}.{}",
                    self.tb_meta.basic.schema, self.tb_meta.basic.tb
                )));
            }
            for col in &cols {
                binds.push(after.get(col).ok_or_else(|| {
                    DtError::InvariantViolated(format!(
                        "MSSQL insert row is missing column {}.{}.{}",
                        self.tb_meta.basic.schema, self.tb_meta.basic.tb, col
                    ))
                })?);
            }
        }

        Ok((MssqlTableQueryInfo { sql, cols, binds }, data_size))
    }

    fn get_upsert_query<'r>(
        &self,
        row_data: &'r RowData,
    ) -> anyhow::Result<MssqlTableQueryInfo<'r>> {
        let (mut query_info, _) = self.get_batch_insert_query(std::slice::from_ref(row_data))?;
        let key_cols = &self.tb_meta.basic.order_cols;
        if key_cols.is_empty()
            || key_cols
                .iter()
                .any(|key_col| !query_info.cols.contains(key_col))
        {
            return Ok(query_info);
        }

        let source_cols = query_info
            .cols
            .iter()
            .enumerate()
            .map(|(index, col)| format!("@P{} AS {}", index + 1, self.escape(col)))
            .collect::<Vec<_>>()
            .join(", ");
        let match_conditions = key_cols
            .iter()
            .map(|col| {
                let escaped_col = self.escape(col);
                if self.tb_meta.basic.is_col_nullable(col) {
                    format!(
                        "(TARGET.{escaped_col} = SOURCE.{escaped_col} OR (TARGET.{escaped_col} IS NULL AND SOURCE.{escaped_col} IS NULL))"
                    )
                } else {
                    format!("TARGET.{escaped_col} = SOURCE.{escaped_col}")
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        // TODO: SQL Server cannot update identity columns, even when IDENTITY_INSERT is ON.
        // Keep them in the generic update list until identity conflicts have a defined policy.
        let update_cols = query_info
            .cols
            .iter()
            .filter(|col| !key_cols.contains(col))
            .collect::<Vec<_>>();
        let update_clause = if update_cols.is_empty() {
            String::new()
        } else {
            format!(
                " WHEN MATCHED THEN UPDATE SET {}",
                update_cols
                    .iter()
                    .map(|col| {
                        let escaped_col = self.escape(col);
                        format!("TARGET.{escaped_col} = SOURCE.{escaped_col}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let escaped_cols = self.escape_cols(&query_info.cols);
        let source_values = escaped_cols
            .iter()
            .map(|col| format!("SOURCE.{col}"))
            .collect::<Vec<_>>()
            .join(", ");
        query_info.sql = format!(
            "MERGE INTO {} AS TARGET USING (SELECT {}) AS SOURCE ON {}{} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});",
            self.table_name(),
            source_cols,
            match_conditions,
            update_clause,
            escaped_cols.join(", "),
            source_values
        );
        Ok(query_info)
    }

    pub fn build_extract_cols_str(&self) -> anyhow::Result<String> {
        Ok(self
            .tb_meta
            .basic
            .cols
            .iter()
            .filter(|col| !self.ignore_cols.is_some_and(|cols| cols.contains(*col)))
            .map(|col| self.escape(col))
            .collect::<Vec<_>>()
            .join(","))
    }

    fn get_batch_placeholders(&self, cols: &[String], batch_size: usize) -> anyhow::Result<String> {
        let mut parameter_index = 1;
        let mut values = String::new();
        for row_index in 0..batch_size {
            if row_index > 0 {
                values.push(',');
            }
            values.push('(');
            for (col_index, col) in cols.iter().enumerate() {
                if col_index > 0 {
                    values.push(',');
                }
                self.tb_meta.get_col_type(col)?;
                values.push_str(&format!("@P{parameter_index}"));
                parameter_index += 1;
            }
            values.push(')');
        }
        Ok(values)
    }

    fn escape(&self, identifier: &str) -> String {
        SqlUtil::escape_by_db_type(identifier, &DbType::Mssql)
    }

    fn escape_cols(&self, cols: &Vec<String>) -> Vec<String> {
        SqlUtil::escape_cols(cols, &DbType::Mssql)
    }

    fn table_name(&self) -> String {
        SqlUtil::render_rdb_table(
            &DbType::Mssql,
            &self.tb_meta.basic.db,
            &self.tb_meta.basic.schema,
            &self.tb_meta.basic.tb,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::MssqlTableSqlBuilder;
    use crate::meta::{
        col_value::ColValue,
        mssql::{mssql_col_type::MssqlColType, mssql_tb_meta::MssqlTbMeta},
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
        row_type::RowType,
    };

    fn build_tb_meta() -> MssqlTbMeta {
        MssqlTbMeta {
            basic: RdbTbMeta {
                schema: "dbo".to_string(),
                tb: "t1".to_string(),
                cols: vec!["id".to_string(), "code".to_string(), "name".to_string()],
                order_cols: vec!["id".to_string()],
                partition_col: "id".to_string(),
                id_cols: vec!["id".to_string()],
                ..Default::default()
            },
            col_type_map: HashMap::from([
                ("id".to_string(), MssqlColType::Int4),
                ("code".to_string(), MssqlColType::NVarchar),
                ("name".to_string(), MssqlColType::NVarchar),
            ]),
            identity_col: None,
            computed_cols: HashSet::new(),
            generated_always_type_map: HashMap::new(),
            rowversion_cols: HashSet::new(),
        }
    }

    fn build_insert_row() -> RowData {
        RowData::new(
            String::new(),
            "dbo".to_string(),
            "t1".to_string(),
            0,
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(1)),
                ("code".to_string(), ColValue::String("xx".to_string())),
                ("name".to_string(), ColValue::String("n1".to_string())),
            ])),
        )
    }

    #[derive(Clone, Copy)]
    enum QuerySetup {
        Default,
        ThreePartTable,
        GeneratedColumns,
        NullableCompositeKey,
        IdentityWithUniqueKey,
        NoKey,
        KeyOnly,
    }

    #[derive(Clone, Copy)]
    enum QueryCall {
        BatchInsert,
        Insert,
        Upsert,
    }

    struct QueryCase {
        name: &'static str,
        setup: QuerySetup,
        row_count: usize,
        call: QueryCall,
        expected_sql: &'static str,
        expected_cols: &'static [&'static str],
        expected_bind_count: usize,
    }

    fn apply_setup(setup: QuerySetup, tb_meta: &mut MssqlTbMeta, data: &mut [RowData]) {
        match setup {
            QuerySetup::Default => {}
            QuerySetup::ThreePartTable => {
                tb_meta.basic.db = "target]db".to_string();
            }
            QuerySetup::GeneratedColumns => {
                tb_meta.basic.cols.extend([
                    "computed_value".to_string(),
                    "valid_from".to_string(),
                    "version".to_string(),
                ]);
                tb_meta.col_type_map.extend([
                    ("computed_value".to_string(), MssqlColType::Int4),
                    ("valid_from".to_string(), MssqlColType::Datetime2),
                    ("version".to_string(), MssqlColType::BigVarBin),
                ]);
                tb_meta.identity_col = Some("id".to_string());
                tb_meta.computed_cols.insert("computed_value".to_string());
                tb_meta
                    .generated_always_type_map
                    .insert("valid_from".to_string(), 1);
                tb_meta.rowversion_cols.insert("version".to_string());
                for row in data {
                    row.after.as_mut().unwrap().extend([
                        ("computed_value".to_string(), ColValue::Long(2)),
                        (
                            "valid_from".to_string(),
                            ColValue::DateTime("2026-08-13 00:00:00".to_string()),
                        ),
                        ("version".to_string(), ColValue::Blob(vec![1; 8])),
                    ]);
                }
            }
            QuerySetup::NullableCompositeKey => {
                tb_meta.basic.order_cols = vec!["id".to_string(), "code".to_string()];
                tb_meta.basic.nullable_cols.insert("code".to_string());
            }
            QuerySetup::IdentityWithUniqueKey => {
                tb_meta.basic.order_cols = vec!["code".to_string()];
                tb_meta.identity_col = Some("id".to_string());
            }
            QuerySetup::NoKey => {
                tb_meta.basic.order_cols.clear();
            }
            QuerySetup::KeyOnly => {
                tb_meta.basic.cols = vec!["id".to_string()];
            }
        }
    }

    #[test]
    fn builds_queries_from_table_cases() {
        let cases = [
            QueryCase {
                name: "batch insert uses unique parameter indexes",
                setup: QuerySetup::Default,
                row_count: 2,
                call: QueryCall::BatchInsert,
                expected_sql: "INSERT INTO [dbo].[t1]([id],[code],[name]) VALUES(@P1,@P2,@P3),(@P4,@P5,@P6)",
                expected_cols: &["id", "code", "name"],
                expected_bind_count: 6,
            },
            QueryCase {
                name: "insert uses three part table name",
                setup: QuerySetup::ThreePartTable,
                row_count: 1,
                call: QueryCall::Insert,
                expected_sql: "INSERT INTO [target]]db].[dbo].[t1]([id],[code],[name]) VALUES(@P1,@P2,@P3)",
                expected_cols: &["id", "code", "name"],
                expected_bind_count: 3,
            },
            QueryCase {
                name: "batch insert filters server generated columns",
                setup: QuerySetup::GeneratedColumns,
                row_count: 2,
                call: QueryCall::BatchInsert,
                expected_sql: "INSERT INTO [dbo].[t1]([id],[code],[name]) VALUES(@P1,@P2,@P3),(@P4,@P5,@P6)",
                expected_cols: &["id", "code", "name"],
                expected_bind_count: 6,
            },
            QueryCase {
                name: "upsert matches nullable composite key",
                setup: QuerySetup::NullableCompositeKey,
                row_count: 1,
                call: QueryCall::Upsert,
                expected_sql: "MERGE INTO [dbo].[t1] AS TARGET USING (SELECT @P1 AS [id], @P2 AS [code], @P3 AS [name]) AS SOURCE ON TARGET.[id] = SOURCE.[id] AND (TARGET.[code] = SOURCE.[code] OR (TARGET.[code] IS NULL AND SOURCE.[code] IS NULL)) WHEN MATCHED THEN UPDATE SET TARGET.[name] = SOURCE.[name] WHEN NOT MATCHED THEN INSERT ([id], [code], [name]) VALUES (SOURCE.[id], SOURCE.[code], SOURCE.[name]);",
                expected_cols: &["id", "code", "name"],
                expected_bind_count: 3,
            },
            QueryCase {
                name: "upsert includes non-key identity column",
                setup: QuerySetup::IdentityWithUniqueKey,
                row_count: 1,
                call: QueryCall::Upsert,
                expected_sql: "MERGE INTO [dbo].[t1] AS TARGET USING (SELECT @P1 AS [id], @P2 AS [code], @P3 AS [name]) AS SOURCE ON TARGET.[code] = SOURCE.[code] WHEN MATCHED THEN UPDATE SET TARGET.[id] = SOURCE.[id], TARGET.[name] = SOURCE.[name] WHEN NOT MATCHED THEN INSERT ([id], [code], [name]) VALUES (SOURCE.[id], SOURCE.[code], SOURCE.[name]);",
                expected_cols: &["id", "code", "name"],
                expected_bind_count: 3,
            },
            QueryCase {
                name: "upsert without key falls back to insert",
                setup: QuerySetup::NoKey,
                row_count: 1,
                call: QueryCall::Upsert,
                expected_sql: "INSERT INTO [dbo].[t1]([id],[code],[name]) VALUES(@P1,@P2,@P3)",
                expected_cols: &["id", "code", "name"],
                expected_bind_count: 3,
            },
            QueryCase {
                name: "upsert key only table omits matched update",
                setup: QuerySetup::KeyOnly,
                row_count: 1,
                call: QueryCall::Upsert,
                expected_sql: "MERGE INTO [dbo].[t1] AS TARGET USING (SELECT @P1 AS [id]) AS SOURCE ON TARGET.[id] = SOURCE.[id] WHEN NOT MATCHED THEN INSERT ([id]) VALUES (SOURCE.[id]);",
                expected_cols: &["id"],
                expected_bind_count: 1,
            },
        ];

        for case in cases {
            let mut tb_meta = build_tb_meta();
            let mut data = (0..case.row_count)
                .map(|_| build_insert_row())
                .collect::<Vec<_>>();
            apply_setup(case.setup, &mut tb_meta, &mut data);
            let builder = MssqlTableSqlBuilder::new(&tb_meta, None);

            let query_info = match case.call {
                QueryCall::BatchInsert => builder
                    .get_batch_insert_query(&data)
                    .map(|(query_info, _)| query_info),
                QueryCall::Insert => builder.get_insert_query(&data[0], false),
                QueryCall::Upsert => builder.get_insert_query(&data[0], true),
            }
            .unwrap_or_else(|error| panic!("case [{}] failed: {error:#}", case.name));

            assert_eq!(query_info.sql, case.expected_sql, "case [{}]", case.name);
            assert_eq!(
                query_info
                    .cols
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                case.expected_cols,
                "case [{}]",
                case.name
            );
            assert_eq!(
                query_info.binds.len(),
                case.expected_bind_count,
                "case [{}]",
                case.name
            );
            builder
                .create_query(&query_info)
                .unwrap_or_else(|error| panic!("case [{}] failed to bind: {error:#}", case.name));
        }
    }

    #[test]
    fn rejects_invalid_query_cases() {
        let cases = [
            (
                "non insert row",
                1,
                QueryCall::Insert,
                true,
                "only supports INSERT rows",
            ),
            (
                "empty batch insert",
                0,
                QueryCall::BatchInsert,
                false,
                "insert has no rows",
            ),
        ];

        for (name, row_count, call, non_insert, expected_error) in cases {
            let tb_meta = build_tb_meta();
            let mut data = (0..row_count)
                .map(|_| build_insert_row())
                .collect::<Vec<_>>();
            if non_insert {
                data[0].row_type = RowType::Delete;
                data[0].before = data[0].after.take();
            }
            let builder = MssqlTableSqlBuilder::new(&tb_meta, None);

            let result = match call {
                QueryCall::BatchInsert => builder
                    .get_batch_insert_query(&data)
                    .map(|(query_info, _)| query_info),
                QueryCall::Insert => builder.get_insert_query(&data[0], false),
                QueryCall::Upsert => builder.get_insert_query(&data[0], true),
            };
            let error = match result {
                Ok(_) => panic!("case [{name}] unexpectedly succeeded"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains(expected_error),
                "case [{name}] returned unexpected error: {error:#}"
            );
        }
    }
}
