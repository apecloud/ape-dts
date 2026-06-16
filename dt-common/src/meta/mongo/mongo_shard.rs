use std::collections::HashMap;

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
    pub ns: String,
    pub key: Document,
    pub unique: bool,
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
) -> anyhow::Result<HashMap<String, MongoShardCollection>> {
    if !is_mongos(client).await? {
        return Ok(HashMap::new());
    }

    let collection = client
        .database("config")
        .collection::<Document>("collections");
    let mut cursor = collection.find(doc! { "dropped": { "$ne": true } }).await?;

    let mut result = HashMap::new();
    while let Some(doc) = cursor.try_next().await? {
        let Some(Bson::String(ns)) = doc.get("_id") else {
            continue;
        };
        let Some(Bson::Document(key)) = doc.get("key") else {
            continue;
        };
        if key.is_empty() {
            continue;
        }
        let unique = doc.get_bool("unique").unwrap_or(false);
        result.insert(
            ns.clone(),
            MongoShardCollection {
                ns: ns.clone(),
                key: key.clone(),
                unique,
            },
        );
    }
    Ok(result)
}

pub async fn get_shard_collection(
    client: &Client,
    ns: &str,
) -> anyhow::Result<Option<MongoShardCollection>> {
    Ok(list_shard_collections(client).await?.remove(ns))
}
