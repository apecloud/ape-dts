use std::collections::HashSet;

use anyhow::Context;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Client,
};

use dt_common::{
    meta::{
        mongo::mongo_shard::list_shard_collections_in_namespaces,
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
    pub async fn get_shard_key_statements(
        &self,
        namespaces: Option<&[String]>,
    ) -> anyhow::Result<Vec<MongoShardKeyStatement>> {
        if namespaces.is_some_and(|names| names.is_empty()) {
            return Ok(Vec::new());
        }
        let (_, shard_collections) =
            list_shard_collections_in_namespaces(&self.mongo_client, namespaces).await?;
        let mut statements = Vec::new();
        for shard_collection in shard_collections.into_values() {
            let db = &shard_collection.database_name;
            let collection = &shard_collection.collection_name;
            if namespaces.is_none()
                && (!self.dbs.contains(db) || self.filter.filter_tb(db, collection))
            {
                continue;
            }
            statements.push(MongoShardKeyStatement { shard_collection });
        }
        Ok(statements)
    }

    pub async fn get_create_collection_statements(
        &self,
        with_indexes: bool,
    ) -> anyhow::Result<Vec<MongoCreateCollectionStatement>> {
        let mut statements = Vec::new();
        for db in &self.dbs {
            let collections = self.get_collections(db, with_indexes, None).await?;
            statements.extend(collections);
        }
        Ok(statements)
    }

    pub async fn get_collections(
        &self,
        db: &str,
        with_indexes: bool,
        names: Option<&[String]>,
    ) -> anyhow::Result<Vec<MongoCreateCollectionStatement>> {
        let mut filter = doc! { "type": "collection" };
        if let Some(names) = names {
            filter.insert("name", doc! { "$in": names.to_vec() });
        }
        let collections = self
            .run_cursor_command(
                db,
                doc! {
                    "listCollections": 1,
                    "filter": filter,
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
            if Self::is_system_collection(name)
                || (names.is_none() && self.filter.filter_tb(db, name))
            {
                continue;
            }

            let options = collection_doc
                .get_document("options")
                .context("MongoDB listCollections result missing options")?
                .clone();
            let indexes = if with_indexes {
                self.get_indexes(db, name).await?
            } else {
                Vec::default()
            };
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
        let cursor = self
            .mongo_client
            .database(db)
            .run_cursor_command(command)
            .await
            .with_context(|| error_context.to_string())?;
        cursor
            .try_collect()
            .await
            .with_context(|| error_context.to_string())
    }

    fn is_system_collection(collection: &str) -> bool {
        collection == "system" || collection.starts_with("system.")
    }
}
