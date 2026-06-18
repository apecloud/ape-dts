use std::collections::HashSet;

use anyhow::Context;
use mongodb::{
    bson::{doc, Bson, Document},
    Client,
};

use dt_common::{
    meta::{
        mongo::mongo_shard::list_shard_collections,
        struct_meta::statement::{
            mongo_create_collection_statement::MongoCreateCollectionStatement,
            mongo_shard_key_statement::MongoShardKeyStatement,
        },
    },
    rdb_filter::RdbFilter,
};

const STRUCT_CURSOR_BATCH_SIZE: i32 = 100;

pub struct MongoStructFetcher {
    pub mongo_client: Client,
    pub dbs: HashSet<String>,
    pub filter: RdbFilter,
}

impl MongoStructFetcher {
    pub async fn get_shard_key_statements(&self) -> anyhow::Result<Vec<MongoShardKeyStatement>> {
        let (_, shard_collections) = list_shard_collections(&self.mongo_client).await?;
        let mut statements = Vec::new();
        for shard_collection in shard_collections.into_values() {
            let Some((db, collection)) = shard_collection.ns.split_once('.') else {
                continue;
            };
            if !self.dbs.contains(db) || self.filter.filter_tb(db, collection) {
                continue;
            }
            statements.push(MongoShardKeyStatement { shard_collection });
        }
        Ok(statements)
    }

    pub async fn get_create_collection_statements(
        &self,
    ) -> anyhow::Result<Vec<MongoCreateCollectionStatement>> {
        let mut statements = Vec::new();
        for db in &self.dbs {
            let collections = self.get_collections(db).await?;
            statements.extend(collections);
        }
        Ok(statements)
    }

    async fn get_collections(
        &self,
        db: &str,
    ) -> anyhow::Result<Vec<MongoCreateCollectionStatement>> {
        let collections = self
            .run_cursor_command(
                db,
                doc! {
                    "listCollections": 1,
                    "filter": { "type": "collection" },
                    "cursor": { "batchSize": STRUCT_CURSOR_BATCH_SIZE },
                },
                &format!("failed to list MongoDB collections for database [{}]", db),
            )
            .await?;

        let mut statements = Vec::new();
        for collection_doc in collections {
            let name = collection_doc.get_str("name").with_context(|| {
                format!(
                    "MongoDB listCollections result missing name: {:?}",
                    collection_doc
                )
            })?;
            if Self::is_system_collection(name) || self.filter.filter_tb(db, name) {
                continue;
            }

            let options = collection_doc
                .get_document("options")
                .cloned()
                .unwrap_or_default();
            let indexes = self.get_indexes(db, name).await?;
            statements.push(MongoCreateCollectionStatement {
                database_name: db.to_string(),
                collection_name: name.to_string(),
                options,
                indexes,
            });
        }

        Ok(statements)
    }

    async fn get_indexes(&self, db: &str, collection: &str) -> anyhow::Result<Vec<Document>> {
        let index_docs = self
            .run_cursor_command(
                db,
                doc! {
                    "listIndexes": collection,
                    "cursor": { "batchSize": STRUCT_CURSOR_BATCH_SIZE },
                },
                &format!(
                    "failed to list MongoDB indexes for [{}].[{}]",
                    db, collection
                ),
            )
            .await?;

        let mut indexes = Vec::new();
        for mut index_doc in index_docs {
            if index_doc.get_str("name").ok() == Some("_id_") {
                continue;
            }
            index_doc.remove("ns");
            index_doc.remove("v");
            indexes.push(index_doc);
        }
        Ok(indexes)
    }

    async fn run_cursor_command(
        &self,
        db: &str,
        command: Document,
        error_context: &str,
    ) -> anyhow::Result<Vec<Document>> {
        let database = self.mongo_client.database(db);
        let mut response = database
            .run_command(command)
            .await
            .with_context(|| error_context.to_string())?;
        let mut results = Vec::new();

        loop {
            let cursor = response
                .get_document("cursor")
                .context("MongoDB cursor command response missing cursor")?;
            let batch = cursor
                .get_array("firstBatch")
                .or_else(|_| cursor.get_array("nextBatch"))
                .context("MongoDB cursor command response missing batch")?;

            for item in batch {
                let Bson::Document(document) = item else {
                    continue;
                };
                results.push(document.clone());
            }

            let cursor_id = cursor
                .get_i64("id")
                .or_else(|_| cursor.get_i32("id").map(i64::from))
                .context("MongoDB cursor command response missing cursor id")?;
            if cursor_id == 0 {
                break;
            }

            let ns = cursor
                .get_str("ns")
                .context("MongoDB cursor command response missing namespace")?;
            let cursor_collection = Self::cursor_collection_from_ns(db, ns)
                .with_context(|| format!("invalid MongoDB cursor namespace [{}]", ns))?;

            response = database
                .run_command(doc! {
                    "getMore": cursor_id,
                    "collection": cursor_collection,
                })
                .await
                .with_context(|| {
                    format!(
                        "failed to getMore MongoDB cursor [{}] for namespace [{}]",
                        cursor_id, ns
                    )
                })?;
        }

        Ok(results)
    }

    fn cursor_collection_from_ns<'a>(db: &str, ns: &'a str) -> anyhow::Result<&'a str> {
        let prefix = format!("{}.", db);
        if let Some(collection) = ns.strip_prefix(&prefix) {
            return Ok(collection);
        }
        anyhow::bail!("namespace does not start with database prefix [{}]", prefix)
    }

    fn is_system_collection(collection: &str) -> bool {
        collection == "system" || collection.starts_with("system.")
    }
}
