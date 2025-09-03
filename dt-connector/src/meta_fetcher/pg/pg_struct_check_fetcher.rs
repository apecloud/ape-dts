use std::collections::HashMap;

use dt_common::meta::{
    adaptor::pg_col_value_convertor::PgColValueConvertor,
    col_value::ColValue,
    pg::{pg_col_type::PgColType, pg_value_type::PgValueType},
};
use futures::TryStreamExt;
use sqlx::{postgres::PgRow, Pool, Postgres, Row};

pub struct PgStructCheckFetcher {
    pub conn_pool: Pool<Postgres>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PgCheckTableInfo {
    pub summary: Vec<HashMap<String, String>>,
    pub columns: Vec<HashMap<String, String>>,
    pub indexes: Vec<HashMap<String, String>>,
    pub constraints: Vec<HashMap<String, String>>,
}

impl PgStructCheckFetcher {
    /// execute the sqls behind "\d table"
    /// refer: https://www.postgresql.org/docs/current/app-psql.html
    pub async fn fetch_table(&self, schema: &str, tb: &str) -> anyhow::Result<PgCheckTableInfo> {
        let oid = self.get_oid(schema, tb).await?;
        if oid.is_empty() {
            anyhow::bail!(
                "Invalid OID: cannot be empty for schema: {} and table: {}",
                schema,
                tb
            );
        }
        let summary = self.get_table_summary(&oid).await?;
        let columns = self.get_table_columns(&oid).await?;
        let indexes = self.get_table_indexes(&oid).await?;
        let mut constraints = self.get_table_check_constraints(&oid).await?;
        let foreign_key_constraints = self.get_table_foreign_key_constraints(&oid).await?;
        constraints.extend_from_slice(&foreign_key_constraints);
        Ok(PgCheckTableInfo {
            summary,
            columns,
            indexes,
            constraints,
        })
    }

    pub async fn get_oid(&self, schema: &str, tb: &str) -> anyhow::Result<String> {
        let sql = format!(
            r#"SELECT c.oid::int8,
                n.nspname,
                c.relname
            FROM pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname OPERATOR(pg_catalog.~) '^({})$' COLLATE pg_catalog.default
                AND n.nspname OPERATOR(pg_catalog.~) '^({})$' COLLATE pg_catalog.default
            ORDER BY 2, 3;"#,
            tb, schema
        );
        let col_names = ["oid", "nspname", "relname"];
        let mut col_types = HashMap::new();
        col_types.insert("oid", Self::mock_col_type("oid"));

        let rows = self.execute_sql(&sql, &col_names, &col_types).await?;
        if !rows.is_empty() {
            return Ok(rows[0].get("oid").unwrap().into());
        }
        Ok(String::new())
    }

    async fn get_table_summary(&self, oid: &str) -> anyhow::Result<Vec<HashMap<String, String>>> {
        let sql = format!(
            r#"SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, 
            c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', 
            c.reltablespace::int8, 
            CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
            LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
            WHERE c.oid = '{}';"#,
            oid
        );
        let col_names = [
            "relchecks",
            "relkind",
            "relhasindex",
            "relhasrules",
            "relhastriggers",
            "relrowsecurity",
            "relforcerowsecurity",
            "relhasoids",
            "relispartition",
            "reltablespace",
            "reloftype",
            "relpersistence",
            "relreplident",
            "amname",
        ];

        let mut col_types = HashMap::new();
        col_types.insert("relchecks", Self::mock_col_type("int2"));
        col_types.insert("relhastriggers", Self::mock_col_type("bool"));
        col_types.insert("relhasoids", Self::mock_col_type("bool"));
        col_types.insert("relhasrules", Self::mock_col_type("bool"));
        col_types.insert("relrowsecurity", Self::mock_col_type("bool"));
        col_types.insert("relhasindex", Self::mock_col_type("bool"));
        col_types.insert("relforcerowsecurity", Self::mock_col_type("bool"));
        col_types.insert("relispartition", Self::mock_col_type("bool"));
        col_types.insert("reltablespace", Self::mock_col_type("oid"));

        self.execute_sql(&sql, &col_names, &col_types).await
    }

    async fn get_table_columns(&self, oid: &str) -> anyhow::Result<Vec<HashMap<String, String>>> {
        let sql = format!(
            r#"SELECT a.attname,
                pg_catalog.format_type(a.atttypid, a.atttypmod),
                (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
                FROM pg_catalog.pg_attrdef d
                WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
                a.attnotnull,
                (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
                WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
                a.attidentity::text,
                a.attgenerated::text
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = '{}' AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum;"#,
            oid
        );
        let col_names = [
            "attname",
            "format_type",
            "pg_get_expr",
            "attnotnull",
            "attcollation",
            "attidentity",
            "attgenerated",
        ];
        let mut col_types = HashMap::new();
        col_types.insert("attnotnull", Self::mock_col_type("bool"));

        self.execute_sql(&sql, &col_names, &col_types).await
    }

    async fn get_table_indexes(&self, oid: &str) -> anyhow::Result<Vec<HashMap<String, String>>> {
        let sql = format!(
            r#"SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
                pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, 
                c2.reltablespace::int8
            FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
                LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
            WHERE c.oid = '{}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid
            ORDER BY i.indisprimary DESC, c2.relname;"#,
            oid
        );
        let col_names = [
            "relname",
            "indisprimary",
            "indisunique",
            "indisclustered",
            "indisvalid",
            "pg_get_indexdef",
            "pg_get_constraintdef",
            "contype",
            "condeferrable",
            "condeferred",
            "indisreplident",
            "reltablespace",
        ];
        let mut col_types = HashMap::new();
        col_types.insert("indisunique", Self::mock_col_type("bool"));
        col_types.insert("reltablespace", Self::mock_col_type("oid"));
        col_types.insert("indisclustered", Self::mock_col_type("bool"));
        col_types.insert("indisvalid", Self::mock_col_type("bool"));
        col_types.insert("indisprimary", Self::mock_col_type("bool"));
        col_types.insert("condeferred", Self::mock_col_type("bool"));
        col_types.insert("condeferrable", Self::mock_col_type("bool"));
        col_types.insert("indisreplident", Self::mock_col_type("bool"));

        self.execute_sql(&sql, &col_names, &col_types).await
    }

    async fn get_table_check_constraints(
        &self,
        oid: &str,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        let sql = format!(
            r#"SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true)
            FROM pg_catalog.pg_constraint r
            WHERE r.conrelid = '{}' AND r.contype = 'c'
            ORDER BY 1;"#,
            oid
        );
        let col_names = ["conname", "pg_get_constraintdef"];
        let col_types = HashMap::new();

        self.execute_sql(&sql, &col_names, &col_types).await
    }

    async fn get_table_foreign_key_constraints(
        &self,
        oid: &str,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        let sql = format!(
            r#"SELECT conname, conrelid::pg_catalog.regclass::text AS ontable,
            pg_catalog.pg_get_constraintdef(oid, true) AS condef
            FROM pg_catalog.pg_constraint c
            WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('{}')
                                UNION ALL VALUES ('{}'::pg_catalog.regclass))
                    AND contype = 'f' AND conparentid = 0
            ORDER BY conname;"#,
            oid, oid
        );
        let col_names = ["conname", "ontable", "condef"];
        let col_types = HashMap::new();

        self.execute_sql(&sql, &col_names, &col_types).await
    }

    async fn execute_sql(
        &self,
        sql: &str,
        col_names: &[&str],
        col_types: &HashMap<&str, PgColType>,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        let mut results = Vec::new();
        let mut rows = sqlx::query(sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let res = Self::parse_row(&row, col_names, col_types)?;
            results.push(res);
        }
        Ok(results)
    }

    fn parse_row(
        row: &PgRow,
        col_names: &[&str],
        col_types: &HashMap<&str, PgColType>,
    ) -> anyhow::Result<HashMap<String, String>> {
        let mut results = HashMap::new();
        for col_name in col_names {
            let col_value = if let Some(col_type) = col_types.get(*col_name) {
                PgColValueConvertor::from_query(row, col_name, col_type)?
            } else {
                let value: Option<String> = row.try_get_unchecked(col_name).unwrap();
                if let Some(v) = value {
                    ColValue::String(v)
                } else {
                    ColValue::None
                }
            };

            if let Some(v) = col_value.to_option_string() {
                results.insert(col_name.to_string(), v);
            } else {
                results.insert(col_name.to_string(), String::new());
            }
        }
        Ok(results)
    }

    fn mock_col_type(alias: &str) -> PgColType {
        let mut col_type = PgColType {
            name: String::new(),
            alias: "varchar".into(),
            value_type: PgValueType::String,
            oid: 0,
            parent_oid: 0,
            element_oid: 0,
            category: String::new(),
            enum_values: None,
        };

        if !alias.is_empty() {
            col_type.alias = alias.into();
            col_type.value_type = PgValueType::from_alias(alias);
        }
        col_type
    }
}
