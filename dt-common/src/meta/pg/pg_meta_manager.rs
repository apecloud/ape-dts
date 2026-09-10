use std::collections::{HashMap, HashSet};

use anyhow::bail;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Row};

use super::{pg_col_type::PgColType, pg_tb_meta::PgTbMeta, type_registry::TypeRegistry};
use crate::meta::{
    foreign_key::ForeignKey,
    rdb_meta_manager::RdbMetaManager,
    rdb_tb_meta::{RdbTbMeta, SortDirection},
    row_data::RowData,
};
use crate::{
    config::config_enums::DbType,
    error::{DtError, DtErrorContextExt, DtOptionExt, DtResultExt, ErrorObject},
    meta::{ddl_meta::ddl_data::DdlData, rdb_meta_manager::RDB_PRIMARY_KEY},
};

#[derive(Clone)]
pub struct PgMetaManager {
    pub conn_pool: Pool<Postgres>,
    pub type_registry: TypeRegistry,
    pub name_to_tb_meta: HashMap<String, PgTbMeta>,
    pub oid_to_tb_meta: HashMap<i32, PgTbMeta>,
}

impl PgMetaManager {
    pub async fn new(conn_pool: Pool<Postgres>) -> anyhow::Result<Self> {
        let type_registry = TypeRegistry::new(conn_pool.clone());
        let mut me = PgMetaManager {
            conn_pool,
            type_registry,
            name_to_tb_meta: HashMap::new(),
            oid_to_tb_meta: HashMap::new(),
        };
        me.type_registry = me.type_registry.init().await?;
        Ok(me)
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn get_col_type_by_oid(&mut self, oid: i32) -> anyhow::Result<PgColType> {
        self
            .type_registry
            .oid_to_type
            .get(&oid)
            .cloned()
            .or_dt_error(DtError::DatabaseUnsupportedTableStructure(
                DbType::Pg,
                format!(
                    "PostgreSQL type ID {oid} is not available in the source type catalog"
                ),
            ))
            .message("A PostgreSQL column type used by the source is not supported")
            .hint(
                "Check the reported source column type and exclude or convert unsupported columns before retrying.",
            )
    }

    pub fn update_tb_meta_by_oid(&mut self, oid: i32, tb_meta: PgTbMeta) -> anyhow::Result<()> {
        self.oid_to_tb_meta.insert(oid, tb_meta.clone());
        let full_name = format!(r#""{}"."{}""#, &tb_meta.basic.schema, &tb_meta.basic.tb);
        self.name_to_tb_meta.insert(full_name, tb_meta);
        Ok(())
    }

    pub fn get_tb_meta_by_oid(&mut self, oid: i32) -> anyhow::Result<PgTbMeta> {
        self
            .oid_to_tb_meta
            .get(&oid)
            .cloned()
            .or_dt_error(DtError::DatabaseStatementFailed(
                DbType::Pg,
                format!(
                    "a change event for source relation ID {oid} arrived before Ape-DTS received its table definition"
                ),
            ))
            .message("A PostgreSQL change event could not be decoded")
            .hint(
                "Restart from an earlier LSN so Ape-DTS can reload the relation definition. If it repeats, check the publication and PostgreSQL replication logs.",
            )
    }

    pub async fn get_tb_meta_by_row_data<'a>(
        &'a mut self,
        row_data: &RowData,
    ) -> anyhow::Result<&'a PgTbMeta> {
        self.get_tb_meta(&row_data.schema, &row_data.tb).await
    }

    pub async fn get_tb_meta<'a>(
        &'a mut self,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<&'a PgTbMeta> {
        let full_name = format!(r#""{}"."{}""#, schema, tb);
        if !self.name_to_tb_meta.contains_key(&full_name) {
            let oid = Self::get_oid(&self.conn_pool, schema, tb).await?;
            let (cols, col_origin_type_map, col_type_map, nullable_cols) =
                Self::parse_cols(&self.conn_pool, &mut self.type_registry, schema, tb).await?;
            let (key_map, key_col_attrs) = Self::parse_keys(&self.conn_pool, schema, tb).await?;
            // disable get_foreign_keys since we don't support foreign key check
            // let (foreign_keys, ref_by_foreign_keys) =
            //     Self::get_foreign_keys(&self.conn_pool, schema, tb).await?;

            let basic = RdbTbMeta {
                schema: schema.to_string(),
                tb: tb.to_string(),
                cols,
                nullable_cols,
                col_origin_type_map,
                key_map,
                ..Default::default()
            };
            let mut tb_meta = PgTbMeta {
                oid,
                col_type_map,
                basic,
            };
            let key_scores = Self::get_key_scores(&tb_meta)?;
            RdbMetaManager::set_order_cols(&mut tb_meta.basic, &key_scores, key_col_attrs)?;
            self.oid_to_tb_meta.insert(oid, tb_meta.clone());
            self.name_to_tb_meta.insert(full_name.clone(), tb_meta);
        }
        self.name_to_tb_meta
            .get(&full_name)
            .or_dt_error(DtError::DatabaseObjectNotFound(DbType::Pg, format!(
                "Ape-DTS could not find the previously loaded definition for source table {full_name}"
            )))
            .message("The source table definition could not be loaded")
            .hint(
                "Verify that the source table still exists and is readable, then restart the task.",
            )
            .object(ErrorObject {
                schema: Some(schema.to_string()),
                table: Some(tb.to_string()),
                ..Default::default()
            })
    }

    pub fn invalidate_cache_for_table(&mut self, schema: &str, tb: &str) {
        if !schema.is_empty() && !tb.is_empty() {
            let full_name = format!(r#""{}"."{}""#, schema, tb);
            if let Some(tb_meta) = self.name_to_tb_meta.remove(&full_name) {
                self.oid_to_tb_meta.remove(&tb_meta.oid);
            }
        }
    }

    pub fn invalidate_cache(&mut self, schema: &str, tb: &str) {
        // TODO, if schema is not empty but tb is empty, only clear cache for the schema
        if !schema.is_empty() && !tb.is_empty() {
            let full_name = format!(r#""{}"."{}""#, schema, tb);
            self.name_to_tb_meta.remove(&full_name);
        } else {
            self.name_to_tb_meta.clear();
        }
    }

    pub fn invalidate_cache_by_ddl_data(&mut self, ddl_data: &DdlData) {
        let (schema, tb) = ddl_data.get_schema_tb();
        self.invalidate_cache(&schema, &tb);
    }

    async fn parse_cols(
        conn_pool: &Pool<Postgres>,
        type_registry: &mut TypeRegistry,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<(
        Vec<String>,
        HashMap<String, String>,
        HashMap<String, PgColType>,
        HashSet<String>,
    )> {
        let mut cols = Vec::new();
        let mut col_origin_type_map = HashMap::new();
        let mut col_type_map = HashMap::new();
        let mut nullable_cols = HashSet::new();

        // get cols of the table
        let sql = format!(
            "SELECT column_name, is_nullable FROM information_schema.columns 
            WHERE table_schema='{}' AND table_name = '{}' 
            ORDER BY ordinal_position;",
            schema, tb
        );
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await? {
            let col: String = row.try_get("column_name")?;
            cols.push(col.clone());

            let is_nullable = row.try_get::<String, _>("is_nullable")?.to_lowercase() == "yes";
            if is_nullable {
                nullable_cols.insert(col);
            }
        }

        // get col_type_oid of the table
        let sql = format!(
            "SELECT a.attname AS col_name, a.atttypid as col_type_oid, a.atttypmod as col_type_mod
            FROM pg_class t, pg_attribute a
            WHERE a.attrelid = t.oid
                AND t.relname = '{}'
                AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}');",
            tb, schema
        );

        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await? {
            let col: String = row.try_get("col_name")?;
            if !cols.contains(&col) {
                continue;
            }

            let col_type_oid: i32 = row.try_get_unchecked("col_type_oid")?;
            let col_type_mod: i32 = row.try_get_unchecked("col_type_mod")?;
            let mut col_type = type_registry
                .oid_to_type
                .get(&col_type_oid)
                .cloned()
                .or_dt_error(DtError::DatabaseUnsupportedTableStructure(
                    DbType::Pg,
                    format!("PostgreSQL type OID {col_type_oid} is missing from the type registry"),
                ))
                .object(ErrorObject {
                    schema: Some(schema.to_string()),
                    table: Some(tb.to_string()),
                    column: Some(col.clone()),
                    ..Default::default()
                })?;
            col_type.typmod = col_type_mod;
            col_origin_type_map.insert(col.clone(), col_type.get_alias());
            col_type_map.insert(col, col_type);
        }

        Ok((cols, col_origin_type_map, col_type_map, nullable_cols))
    }

    pub fn get_key_scores(tb_meta: &PgTbMeta) -> anyhow::Result<HashMap<String, u32>> {
        let mut scores = HashMap::new();
        for (key, cols) in &tb_meta.basic.key_map {
            if cols.is_empty() {
                continue;
            }
            let mut score = Some(0);
            for col in cols {
                let weight = tb_meta.get_col_type(col)?.order_key_weight();
                score = score.zip(weight).map(|(total, weight)| total + weight);
            }
            if let Some(score) = score {
                scores.insert(key.clone(), score);
            }
        }
        Ok(scores)
    }

    // Example (exercised by snapshot/order_key_test):
    // CREATE TABLE order_key_src.parse_keys_example (
    //     id int NOT NULL, value int NOT NULL,
    //     CONSTRAINT some_pk_name PRIMARY KEY (id, value),
    //     CONSTRAINT some_uk_name UNIQUE (value)
    // );
    // CREATE UNIQUE INDEX uk_example ON order_key_src.parse_keys_example (value DESC, id ASC);
    // CREATE INDEX non_unique_key ON order_key_src.parse_keys_example (id);
    // The catalog query below returns these rows (shown grouped by key):
    // key_name     | is_primary | col_name | is_descending
    // some_pk_name | true       | id       | false
    // some_pk_name | true       | value    | false
    // some_uk_name | false      | value    | false
    // uk_example   | false      | value    | true
    // uk_example   | false      | id       | false
    // key_map = {RDB_PRIMARY_KEY: [id, value], some_uk_name: [value], uk_example: [value, id]}
    // key_col_attrs = {RDB_PRIMARY_KEY: {id: Asc, value: Asc},
    //                  some_uk_name: {value: Asc}, uk_example: {value: Desc, id: Asc}}
    // Constraint names and standalone unique indexes are both retained. Composite
    // columns follow indkey ordinality; indoption's low bit supplies DESC.
    // Non-unique, partial, expression and invalid/not-ready indexes are excluded;
    // INCLUDE columns do not become key columns (covered by catalog_key in the test).
    async fn parse_keys(
        conn_pool: &Pool<Postgres>,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<(
        HashMap<String, Vec<String>>,
        HashMap<String, HashMap<String, SortDirection>>,
    )> {
        // A constraint and its backing index share one key. INCLUDE columns are
        // not part of uniqueness; partial/expression indexes cannot identify every row.
        let sql = r#"
            SELECT COALESCE(c.conname, i.relname) AS key_name,
                   ix.indisprimary AS is_primary,
                   a.attname AS col_name,
                   (ix.indoption[(k.ord - 1)::int] & 1) <> 0 AS is_descending
            FROM pg_class t
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_index ix ON ix.indrelid = t.oid
            JOIN pg_class i ON i.oid = ix.indexrelid
            LEFT JOIN pg_constraint c ON c.conindid = ix.indexrelid
                                    AND c.contype IN ('p', 'u')
            CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                               AND NOT a.attisdropped
            WHERE n.nspname = $1 AND t.relname = $2
              AND ix.indisunique AND ix.indisvalid AND ix.indisready
              AND ix.indpred IS NULL AND ix.indexprs IS NULL
              AND k.ord <= ix.indnkeyatts
            ORDER BY i.oid, k.ord"#;
        let mut rows = sqlx::query(sql).bind(schema).bind(tb).fetch(conn_pool);
        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut key_col_attrs: HashMap<String, HashMap<String, SortDirection>> = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            let key: String = if row.try_get("is_primary")? {
                RDB_PRIMARY_KEY.to_string()
            } else {
                row.try_get("key_name")?
            };
            let col: String = row.try_get("col_name")?;
            let direction = if row.try_get("is_descending")? {
                SortDirection::Desc
            } else {
                SortDirection::Asc
            };
            key_map.entry(key.clone()).or_default().push(col.clone());
            key_col_attrs.entry(key).or_default().insert(col, direction);
        }
        Ok((key_map, key_col_attrs))
    }

    async fn get_oid(conn_pool: &Pool<Postgres>, schema: &str, tb: &str) -> anyhow::Result<i32> {
        let sql = format!(r#"SELECT '"{}"."{}"'::regclass::oid;"#, schema, tb);
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        if let Some(row) = rows.try_next().await? {
            let oid: i32 = row.try_get_unchecked("oid")?;
            return Ok(oid);
        }

        bail! {DtError::DatabaseObjectNotFound(DbType::Pg, format!("failed to get oid for: {} by query: {}", tb, sql))
        .object(ErrorObject {
            schema: Some(schema.to_string()),
            table: Some(tb.to_string()),
            ..Default::default()
        })}
    }

    #[allow(dead_code)]
    async fn get_foreign_keys(
        conn_pool: &Pool<Postgres>,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<(Vec<ForeignKey>, Vec<ForeignKey>)> {
        let mut foreign_keys = Vec::new();
        let mut ref_by_foreign_keys = Vec::new();
        let sql = format!(
            "SELECT
            ns.nspname AS schema_name,
            tab.relname AS table_name,
            a1.attname AS column_name,
            ns_ref.nspname AS referenced_schema_name,
            tab_ref.relname AS referenced_table_name,
            a2.attname AS referenced_column_name
        FROM
            pg_constraint c
            INNER JOIN pg_class tab ON tab.oid = c.conrelid
            INNER JOIN pg_namespace ns ON ns.oid = tab.relnamespace
            INNER JOIN pg_attribute a1 ON a1.attnum = ANY(c.conkey) AND a1.attrelid = c.conrelid
            INNER JOIN pg_class tab_ref ON tab_ref.oid = c.confrelid
            INNER JOIN pg_namespace ns_ref ON ns_ref.oid = tab_ref.relnamespace
            INNER JOIN pg_attribute a2 ON a2.attnum = ANY(c.confkey) AND a2.attrelid = c.confrelid
        WHERE
            c.contype = 'f' 
            AND (
                ( ns.nspname = '{}' AND tab.relname = '{}' )
                  OR 
                ( ns_ref.nspname = '{}' AND tab_ref.relname = '{}')
              )
              ",
            schema, tb, schema, tb
        );

        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await? {
            let my_schema: String = row.try_get("schema_name")?;
            let my_tb: String = row.try_get("table_name")?;
            let my_col: String = row.try_get("column_name")?;
            let ref_schema: String = row.try_get("referenced_schema_name")?;
            let ref_tb: String = row.try_get("referenced_table_name")?;
            let ref_col: String = row.try_get("referenced_column_name")?;
            let key = ForeignKey {
                db: String::new(),
                schema: my_schema,
                tb: my_tb,
                col: my_col,
                ref_db: String::new(),
                ref_schema,
                ref_tb,
                ref_col,
            };
            if key.schema == schema && key.tb == tb {
                foreign_keys.push(key.clone());
            }
            if key.ref_schema == schema && key.ref_tb == tb {
                ref_by_foreign_keys.push(key)
            }
        }
        Ok((foreign_keys, ref_by_foreign_keys))
    }
}
