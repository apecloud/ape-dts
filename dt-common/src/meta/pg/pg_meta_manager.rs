use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    error::Error,
    meta::{ddl_meta::ddl_data::DdlData, rdb_meta_manager::RDB_PRIMARY_KEY_FLAG},
};
use anyhow::{bail, Context};
use dashmap::DashMap;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Row};
use tokio::sync::Mutex;

use crate::meta::{
    foreign_key::ForeignKey, rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
};

use super::{pg_col_type::PgColType, pg_tb_meta::PgTbMeta, type_registry::TypeRegistry};

#[derive(Clone)]
pub struct PgMetaManager {
    shared: Arc<PgMetaShared>,
    // FIXME: Only CDC managers initialize this map. Relation ids are scoped to one
    // logical replication stream and must not be mixed into the table-name cache.
    pub oid_to_tb_meta: Option<HashMap<i32, Arc<PgTbMeta>>>,
}

pub struct PgMetaShared {
    pub conn_pool: Pool<Postgres>,
    // FIXME: Initialized once and then shared read-only. If runtime type reload is
    // needed later, replace this with a snapshot lock and reload singleflight.
    pub type_registry: Arc<TypeRegistry>,
    // Shared table-name cache for normal schema metadata.
    pub name_to_tb_meta: DashMap<String, Arc<PgTbMeta>>,
    // Per-table singleflight locks for cold-cache fetches.
    pub locks: DashMap<String, Arc<Mutex<()>>>,
}

impl PgMetaManager {
    pub async fn new(conn_pool: Pool<Postgres>) -> anyhow::Result<Self> {
        Self::new_inner(conn_pool, false).await
    }

    pub async fn new_for_cdc(conn_pool: Pool<Postgres>) -> anyhow::Result<Self> {
        Self::new_inner(conn_pool, true).await
    }

    async fn new_inner(conn_pool: Pool<Postgres>, with_oid_cache: bool) -> anyhow::Result<Self> {
        let type_registry = TypeRegistry::new(conn_pool.clone());
        let type_registry = Arc::new(type_registry.init().await?);
        Ok(Self {
            shared: Arc::new(PgMetaShared {
                conn_pool,
                type_registry,
                name_to_tb_meta: DashMap::new(),
                locks: DashMap::new(),
            }),
            oid_to_tb_meta: with_oid_cache.then(HashMap::new),
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn get_col_type_by_oid(&self, oid: i32) -> anyhow::Result<PgColType> {
        Ok(self
            .shared
            .type_registry
            .oid_to_type
            .get(&oid)
            .with_context(|| format!("no type found for oid: [{}]", oid))?
            .clone())
    }

    pub fn update_tb_meta_by_oid(&mut self, oid: i32, tb_meta: PgTbMeta) -> anyhow::Result<()> {
        let oid_to_tb_meta = self
            .oid_to_tb_meta
            .as_mut()
            .with_context(|| "pg oid metadata is only available for cdc meta manager")?;
        // Do not update name_to_tb_meta here. CDC relation metadata may be
        // mocked for filtered tables or reordered to match WAL column order.
        oid_to_tb_meta.insert(oid, Arc::new(tb_meta));
        Ok(())
    }

    pub fn get_tb_meta_by_oid(&self, oid: i32) -> anyhow::Result<Arc<PgTbMeta>> {
        Ok(self
            .oid_to_tb_meta
            .as_ref()
            .with_context(|| "pg oid metadata is only available for cdc meta manager")?
            .get(&oid)
            .with_context(|| format!("no tb_meta found for oid: [{}]", oid))?
            .clone())
    }

    pub async fn get_tb_meta_by_row_data(
        &self,
        row_data: &RowData,
    ) -> anyhow::Result<Arc<PgTbMeta>> {
        self.get_tb_meta(&row_data.schema, &row_data.tb).await
    }

    pub async fn get_tb_meta(&self, schema: &str, tb: &str) -> anyhow::Result<Arc<PgTbMeta>> {
        let full_name = Self::table_key(schema, tb);
        if let Some(tb_meta) = self.shared.name_to_tb_meta.get(&full_name) {
            return Ok(tb_meta.clone());
        }

        // Use a per-table lock so concurrent misses for the same table perform
        // one metadata fetch, while different tables can still fetch in parallel.
        let lock = {
            let entry = self
                .shared
                .locks
                .entry(full_name.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            entry.value().clone()
        };
        let _guard = lock.lock().await;

        // Double-check after waiting; another worker may have filled the cache.
        if let Some(tb_meta) = self.shared.name_to_tb_meta.get(&full_name) {
            return Ok(tb_meta.clone());
        }

        let tb_meta = Arc::new(self.fetch_tb_meta(schema, tb).await?);
        self.shared
            .name_to_tb_meta
            .insert(full_name, tb_meta.clone());
        Ok(tb_meta)
    }

    fn table_key(schema: &str, tb: &str) -> String {
        format!(r#""{}"."{}""#, schema, tb)
    }

    async fn fetch_tb_meta(&self, schema: &str, tb: &str) -> anyhow::Result<PgTbMeta> {
        let oid = Self::get_oid(&self.shared.conn_pool, schema, tb).await?;
        let (cols, col_origin_type_map, col_type_map, nullable_cols) = Self::parse_cols(
            &self.shared.conn_pool,
            &self.shared.type_registry,
            schema,
            tb,
        )
        .await?;
        let mut key_map = Self::parse_keys(&self.shared.conn_pool, schema, tb).await?;
        // unique indexes (e.g. CREATE UNIQUE INDEX) are not in table_constraints;
        let unique_index_keys =
            Self::parse_unique_index_keys(&self.shared.conn_pool, schema, tb).await?;
        for (k, v) in unique_index_keys {
            key_map.entry(k).or_insert(v);
        }
        let (order_cols, partition_col, id_cols) =
            RdbMetaManager::parse_rdb_cols(&key_map, &cols, &nullable_cols)?;
        // disable get_foreign_keys since we don't support foreign key check
        let (foreign_keys, ref_by_foreign_keys) = (vec![], vec![]);
        // let (foreign_keys, ref_by_foreign_keys) =
        //     Self::get_foreign_keys(&self.shared.conn_pool, schema, tb).await?;

        let basic = RdbTbMeta {
            schema: schema.to_string(),
            tb: tb.to_string(),
            cols,
            nullable_cols,
            col_origin_type_map,
            key_map,
            order_cols,
            partition_col,
            id_cols,
            foreign_keys,
            ref_by_foreign_keys,
        };
        Ok(PgTbMeta {
            oid,
            col_type_map,
            basic,
        })
    }

    pub fn invalidate_cache(&self, schema: &str, tb: &str) {
        // FIXME: This does not prevent an in-flight fetch from inserting stale
        // metadata after invalidate. Add per-table epochs before concurrent CDC
        // metadata refresh/decode is introduced.
        // TODO, if schema is not empty but tb is empty, only clear cache for the schema
        if !schema.is_empty() && !tb.is_empty() {
            let full_name = Self::table_key(schema, tb);
            self.shared.name_to_tb_meta.remove(&full_name);
        } else {
            self.shared.name_to_tb_meta.clear();
        }
    }

    pub fn invalidate_cache_by_ddl_data(&self, ddl_data: &DdlData) {
        let (schema, tb) = ddl_data.get_schema_tb();
        self.invalidate_cache(&schema, &tb);
    }

    async fn parse_cols(
        conn_pool: &Pool<Postgres>,
        type_registry: &TypeRegistry,
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
            "SELECT a.attname AS col_name, a.atttypid as col_type_oid
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
            let col_type = type_registry
                .oid_to_type
                .get(&col_type_oid)
                .unwrap()
                .clone();
            col_origin_type_map.insert(col.clone(), col_type.alias.clone());
            col_type_map.insert(col, col_type);
        }

        Ok((cols, col_origin_type_map, col_type_map, nullable_cols))
    }

    async fn parse_keys(
        conn_pool: &Pool<Postgres>,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let sql = format!(
            "SELECT kcu.column_name as col_name, 
                kcu.constraint_name as constraint_name,
                tc.constraint_type as constraint_type
            FROM 
                information_schema.table_constraints AS tc
            JOIN 
                information_schema.key_column_usage AS kcu
            ON 
                tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE 
                tc.table_schema = '{}' 
                AND tc.table_name = '{}'
                AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
            ORDER BY 
                kcu.ordinal_position;",
            schema, tb
        );

        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await? {
            let col_name: String = row.try_get("col_name")?;
            let constraint_type: String = row.try_get("constraint_type")?;
            let mut key_name: String = row.try_get("constraint_name")?;
            if constraint_type == "PRIMARY KEY" {
                key_name = RDB_PRIMARY_KEY_FLAG.to_string();
            }

            // key_map
            if let Some(key_cols) = key_map.get_mut(&key_name) {
                key_cols.push(col_name);
            } else {
                key_map.insert(key_name, vec![col_name]);
            }
        }
        Ok(key_map)
    }

    async fn parse_unique_index_keys(
        conn_pool: &Pool<Postgres>,
        schema: &str,
        tb: &str,
    ) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let sql = format!(
            r#"
            SELECT
                i.relname AS index_name,
                a.attname AS col_name,
                k.ord AS ord
            FROM pg_class t
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_index ix ON ix.indrelid = t.oid
            JOIN pg_class i ON i.oid = ix.indexrelid
            LEFT JOIN pg_constraint c
                   ON c.conindid = ix.indexrelid
                  AND c.contype IN ('p','u')
            CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                AND a.attnum > 0 AND NOT a.attisdropped
            WHERE n.nspname = '{}' AND t.relname = '{}'
              AND ix.indisunique
              AND c.oid IS NULL
            ORDER BY i.relname, k.ord"#,
            schema, tb
        );
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        let mut key_map: HashMap<String, Vec<String>> = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            let index_name: String = row.try_get("index_name")?;
            let col_name: String = row.try_get("col_name")?;
            key_map.entry(index_name).or_default().push(col_name);
        }
        Ok(key_map)
    }

    async fn get_oid(conn_pool: &Pool<Postgres>, schema: &str, tb: &str) -> anyhow::Result<i32> {
        let sql = format!(r#"SELECT '"{}"."{}"'::regclass::oid;"#, schema, tb);
        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        if let Some(row) = rows.try_next().await? {
            let oid: i32 = row.try_get_unchecked("oid")?;
            return Ok(oid);
        }

        bail! {Error::MetadataError(format!(
            "failed to get oid for: {} by query: {}",
            tb, sql
        ))}
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
                schema: my_schema,
                tb: my_tb,
                col: my_col,
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
