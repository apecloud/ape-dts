use mongodb::bson::{doc, raw::RawDocument, Bson, Document};

use crate::{
    config::config_enums::DbType,
    meta::ddl_meta::{
        ddl_data::DdlData,
        ddl_statement::{DdlStatement, MongoCommandStatement},
        ddl_type::DdlType,
    },
};

const OPERATION_TYPE: &str = "operationType";
const OPERATION_DESCRIPTION: &str = "operationDescription";
const NS: &str = "ns";
const TO: &str = "to";
const DB: &str = "db";
const COLL: &str = "coll";
const SHARD_KEY: &str = "shardKey";
const KEY: &str = "key";

pub fn command_to_query(command: Document) -> String {
    Bson::Document(command).into_canonical_extjson().to_string()
}

pub fn query_to_command(query: &str) -> anyhow::Result<Document> {
    let value: serde_json::Value = serde_json::from_str(query)?;
    match Bson::try_from(value)? {
        Bson::Document(command) => Ok(command),
        other => anyhow::bail!("mongo ddl query is not a document: {:?}", other),
    }
}

pub fn build_shard_collection_ddl(ns: &str, key: Document, unique: bool) -> Option<DdlData> {
    let (db, coll) = split_ns(ns)?;
    let mut command = doc! {
        "shardCollection": ns,
        KEY: key,
    };
    command.insert("unique", unique);
    Some(build_ddl(
        db,
        coll,
        String::new(),
        String::new(),
        DdlType::MongoShardCollection,
        command,
    ))
}

pub fn change_stream_event_to_ddl(event: &Document) -> Option<DdlData> {
    let operation_type = event.get_str(OPERATION_TYPE).ok()?;
    let (db, coll) = parse_ns(event)?;
    let operation_description = event.get_document(OPERATION_DESCRIPTION).ok();

    match operation_type {
        "create" => {
            let mut command = doc! { "create": coll.clone() };
            if let Some(description) = operation_description {
                copy_description_fields(description, &mut command, &["idIndex"]);
            }
            Some(build_ddl(
                db,
                coll,
                String::new(),
                String::new(),
                DdlType::MongoCreateCollection,
                command,
            ))
        }

        "drop" => Some(build_ddl(
            db,
            coll.clone(),
            String::new(),
            String::new(),
            DdlType::MongoDropCollection,
            doc! { "drop": coll },
        )),

        "rename" => {
            let to = event.get_document(TO).ok()?;
            let new_db = to.get_str(DB).ok()?.to_string();
            let new_coll = to.get_str(COLL).ok()?.to_string();
            Some(build_ddl(
                db.clone(),
                coll.clone(),
                new_db.clone(),
                new_coll.clone(),
                DdlType::MongoRenameCollection,
                doc! {
                    "renameCollection": format!("{}.{}", db, coll),
                    "to": format!("{}.{}", new_db, new_coll),
                },
            ))
        }

        "dropDatabase" => Some(build_ddl(
            db,
            String::new(),
            String::new(),
            String::new(),
            DdlType::MongoDropDatabase,
            doc! { "dropDatabase": 1 },
        )),

        "createIndexes" => {
            let description = operation_description?;
            let indexes = description.get("indexes")?.clone();
            Some(build_ddl(
                db,
                coll.clone(),
                String::new(),
                String::new(),
                DdlType::MongoCreateIndex,
                doc! { "createIndexes": coll, "indexes": indexes },
            ))
        }

        "dropIndexes" => {
            let description = operation_description?;
            let index = first_index_name(description)?;
            Some(build_ddl(
                db,
                coll.clone(),
                String::new(),
                String::new(),
                DdlType::MongoDropIndex,
                doc! { "dropIndexes": coll, "index": index },
            ))
        }

        "modify" => {
            let mut command = doc! { "collMod": coll.clone() };
            if let Some(description) = operation_description {
                copy_description_fields(description, &mut command, &[]);
            }
            Some(build_ddl(
                db,
                coll,
                String::new(),
                String::new(),
                DdlType::MongoCollMod,
                command,
            ))
        }

        "shardCollection" | "reshardCollection" | "refineCollectionShardKey" => {
            sharding_event_to_ddl(operation_type, db, coll, operation_description?)
        }

        _ => None,
    }
}

pub fn raw_change_stream_event_to_ddl(event: &RawDocument) -> Option<DdlData> {
    change_stream_event_to_ddl(&Document::try_from(event).ok()?)
}

fn sharding_event_to_ddl(
    operation_type: &str,
    db: String,
    coll: String,
    operation_description: &Document,
) -> Option<DdlData> {
    let shard_key = operation_description.get_document(SHARD_KEY).ok()?.clone();
    let command_name = match operation_type {
        "shardCollection" => "shardCollection",
        "reshardCollection" => "reshardCollection",
        "refineCollectionShardKey" => "refineCollectionShardKey",
        _ => return None,
    };
    let ddl_type = match operation_type {
        "shardCollection" => DdlType::MongoShardCollection,
        "reshardCollection" => DdlType::MongoReshardCollection,
        "refineCollectionShardKey" => DdlType::MongoRefineCollectionShardKey,
        _ => return None,
    };

    let mut command = doc! {
        command_name: format!("{}.{}", db, coll),
        KEY: shard_key,
    };
    let ignored = ["shardKey", "reshardUUID", "oldShardKey"];
    copy_description_fields(operation_description, &mut command, &ignored);
    Some(build_ddl(
        db,
        coll,
        String::new(),
        String::new(),
        ddl_type,
        command,
    ))
}

fn build_ddl(
    db: String,
    coll: String,
    new_db: String,
    new_coll: String,
    ddl_type: DdlType,
    command: Document,
) -> DdlData {
    DdlData {
        default_schema: db.clone(),
        query: command_to_query(command),
        ddl_type,
        db_type: DbType::Mongo,
        statement: DdlStatement::MongoCommand(MongoCommandStatement {
            schema: db,
            tb: coll,
            new_schema: new_db,
            new_tb: new_coll,
        }),
    }
}

fn parse_ns(event: &Document) -> Option<(String, String)> {
    let ns = event.get_document(NS).ok()?;
    Some((
        ns.get_str(DB).ok()?.to_string(),
        ns.get_str(COLL).unwrap_or("").to_string(),
    ))
}

fn split_ns(ns: &str) -> Option<(String, String)> {
    let (db, coll) = ns.split_once('.')?;
    Some((db.to_string(), coll.to_string()))
}

fn copy_description_fields(description: &Document, command: &mut Document, ignored: &[&str]) {
    for (key, value) in description {
        if ignored.iter().any(|ignored_key| ignored_key == key) {
            continue;
        }
        command.insert(key, value.clone());
    }
}

fn first_index_name(description: &Document) -> Option<Bson> {
    match description.get("indexes")? {
        Bson::Array(indexes) => indexes.first().and_then(index_name_from_bson),
        index => index_name_from_bson(index),
    }
}

fn index_name_from_bson(index: &Bson) -> Option<Bson> {
    match index {
        Bson::String(name) => Some(Bson::String(name.clone())),
        Bson::Document(spec) => spec.get("name").cloned(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, raw::RawDocumentBuf};

    #[test]
    fn shard_collection_ddl_round_trips_command() {
        let ddl = build_shard_collection_ddl(
            "db1.tb1",
            doc! {
                "tenant_id": 1,
                "_id": 1,
            },
            false,
        )
        .unwrap();

        assert_eq!(ddl.ddl_type, DdlType::MongoShardCollection);
        assert_eq!(ddl.get_schema_tb(), ("db1".into(), "tb1".into()));
        let command = query_to_command(&ddl.query).unwrap();
        assert_eq!(command.get_str("shardCollection").unwrap(), "db1.tb1");
        assert_eq!(
            command
                .get_document("key")
                .unwrap()
                .get_i32("tenant_id")
                .unwrap(),
            1
        );
    }

    #[test]
    fn change_stream_rename_maps_source_and_target_namespace() {
        let event = doc! {
            "operationType": "rename",
            "ns": { "db": "db1", "coll": "old_tb" },
            "to": { "db": "db2", "coll": "new_tb" },
        };

        let ddl = change_stream_event_to_ddl(&event).unwrap();
        assert_eq!(ddl.ddl_type, DdlType::MongoRenameCollection);
        assert_eq!(ddl.get_schema_tb(), ("db1".into(), "old_tb".into()));
        assert_eq!(
            ddl.get_rename_to_schema_tb(),
            ("db2".into(), "new_tb".into())
        );
        let command = query_to_command(&ddl.query).unwrap();
        assert_eq!(command.get_str("renameCollection").unwrap(), "db1.old_tb");
        assert_eq!(command.get_str("to").unwrap(), "db2.new_tb");
    }

    #[test]
    fn change_stream_create_indexes_keeps_index_specs() {
        let event = doc! {
            "operationType": "createIndexes",
            "ns": { "db": "db1", "coll": "tb1" },
            "operationDescription": {
                "indexes": [
                    {
                        "name": "idx_tenant",
                        "key": { "tenant_id": 1 },
                    }
                ],
            },
        };

        let ddl = change_stream_event_to_ddl(&event).unwrap();
        assert_eq!(ddl.ddl_type, DdlType::MongoCreateIndex);
        let command = query_to_command(&ddl.query).unwrap();
        assert_eq!(command.get_str("createIndexes").unwrap(), "tb1");
        assert!(matches!(command.get("indexes"), Some(Bson::Array(indexes)) if indexes.len() == 1));
    }

    #[test]
    fn raw_change_stream_event_is_parsed_only_for_ddl_conversion() {
        let event = RawDocumentBuf::from_document(&doc! {
            "operationType": "drop",
            "ns": { "db": "db1", "coll": "tb1" },
        })
        .unwrap();

        let ddl = raw_change_stream_event_to_ddl(&event).unwrap();
        assert_eq!(ddl.ddl_type, DdlType::MongoDropCollection);
        assert_eq!(ddl.get_schema_tb(), ("db1".into(), "tb1".into()));
    }
}
