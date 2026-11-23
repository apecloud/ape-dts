use std::collections::HashMap;

use anyhow::Context;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Bson, Document},
    Client,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MongoTopology {
    Mongos,
    ReplicaSet,
    Standalone,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MongoShardCollection {
    pub database_name: String,
    pub collection_name: String,
    pub key: Document,
    pub unique: bool,
}

impl MongoShardCollection {
    fn from_config_document(mut doc: Document) -> anyhow::Result<Option<Self>> {
        let ns = doc
            .get_str("_id")
            .context("MongoDB config.collections metadata missing a string namespace")?;
        let (database_name, collection_name) = ns
            .split_once('.')
            .filter(|(database, collection)| !database.is_empty() && !collection.is_empty())
            .with_context(|| format!("invalid MongoDB shard namespace: {ns}"))?;
        let database_name = database_name.to_string();
        let collection_name = collection_name.to_string();

        let Some(Bson::Document(key)) = doc.remove("key") else {
            return Ok(None);
        };
        if key.is_empty() {
            return Ok(None);
        }

        Ok(Some(Self {
            database_name,
            collection_name,
            key,
            unique: doc.get_bool("unique").unwrap_or(false),
        }))
    }

    pub fn get_ns(&self) -> String {
        format!("{}.{}", self.database_name, self.collection_name)
    }
}

pub async fn detect_topology(client: &Client) -> anyhow::Result<MongoTopology> {
    let hello = client
        .database("admin")
        .run_command(doc! { "hello": 1 })
        .await?;
    if hello.get_str("msg").ok() == Some("isdbgrid") {
        return Ok(MongoTopology::Mongos);
    }
    if hello.contains_key("setName") {
        return Ok(MongoTopology::ReplicaSet);
    }
    Ok(MongoTopology::Standalone)
}

pub async fn is_mongos(client: &Client) -> anyhow::Result<bool> {
    Ok(detect_topology(client).await? == MongoTopology::Mongos)
}

pub async fn list_shard_collections(
    client: &Client,
) -> anyhow::Result<(bool, HashMap<String, MongoShardCollection>)> {
    list_shard_collections_in_namespaces(client, None).await
}

pub async fn list_shard_collections_in_namespaces(
    client: &Client,
    namespaces: Option<&[String]>,
) -> anyhow::Result<(bool, HashMap<String, MongoShardCollection>)> {
    if !is_mongos(client).await? {
        return Ok((false, HashMap::new()));
    }

    let collection = client
        .database("config")
        .collection::<Document>("collections");
    let batches: Vec<_> = match namespaces {
        Some(names) => names.chunks(1000).map(Some).collect(),
        None => vec![None],
    };

    let mut result = HashMap::new();
    for batch in batches {
        let mut filter = doc! { "dropped": { "$ne": true } };
        if let Some(names) = batch {
            filter.insert("_id", doc! { "$in": names.to_vec() });
        }
        let mut cursor = collection.find(filter).await?;
        while let Some(doc) = cursor.try_next().await? {
            if let Some(shard_collection) = MongoShardCollection::from_config_document(doc)? {
                result.insert(shard_collection.get_ns(), shard_collection);
            }
        }
    }
    Ok((true, result))
}

pub async fn get_shard_collection(
    client: &Client,
    ns: &str,
) -> anyhow::Result<Option<MongoShardCollection>> {
    if !is_mongos(client).await? {
        return Ok(None);
    }

    let collection = client
        .database("config")
        .collection::<Document>("collections");
    let doc = collection
        .find_one(doc! { "_id": ns, "dropped": { "$ne": true } })
        .await?;
    let Some(doc) = doc else {
        return Ok(None);
    };
    MongoShardCollection::from_config_document(doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_metadata_preserves_dots_in_collection_name() {
        let metadata =
            doc! { "_id": "sales.orders.archive", "key": { "tenant": 1 }, "unique": true };
        let collection = MongoShardCollection::from_config_document(metadata)
            .unwrap()
            .unwrap();
        assert_eq!(collection.database_name, "sales");
        assert_eq!(collection.collection_name, "orders.archive");
        assert_eq!(collection.get_ns(), "sales.orders.archive");
        assert_eq!(collection.key, doc! { "tenant": 1 });
        assert!(collection.unique);
    }

    #[test]
    fn shard_metadata_rejects_malformed_namespaces() {
        for id in [
            Bson::Null,
            Bson::String("orders".into()),
            Bson::String(".orders".into()),
            Bson::String("sales.".into()),
        ] {
            let metadata = doc! { "_id": id, "key": { "tenant": 1 } };
            assert!(MongoShardCollection::from_config_document(metadata).is_err());
        }
    }
}
