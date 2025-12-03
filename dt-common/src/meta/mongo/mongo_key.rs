use mongodb::bson::{oid::ObjectId, Bson, DateTime, Document, Timestamp};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::mongo_constant::MongoConstants;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MongoKey {
    ObjectId(ObjectId),
    String(String),
    Int32(i32),
    Int64(i64),
    JavaScriptCode(String),
    Timestamp(Timestamp),
    DateTime(DateTime),
    Symbol(String),
}

impl MongoKey {
    pub fn from_doc(doc: &Document) -> Option<MongoKey> {
        doc.get(MongoConstants::ID).and_then(|id| match id {
            Bson::ObjectId(v) => Some(MongoKey::ObjectId(*v)),
            Bson::String(v) => Some(MongoKey::String(v.clone())),
            Bson::Int32(v) => Some(MongoKey::Int32(*v)),
            Bson::Int64(v) => Some(MongoKey::Int64(*v)),
            Bson::JavaScriptCode(v) => Some(MongoKey::JavaScriptCode(v.clone())),
            Bson::Timestamp(v) => Some(MongoKey::Timestamp(*v)),
            Bson::DateTime(v) => Some(MongoKey::DateTime(*v)),
            Bson::Symbol(v) => Some(MongoKey::Symbol(v.clone())),
            // other types don't derive Hash and Eq
            _ => None,
        })
    }

    pub fn to_mongo_id(&self) -> Bson {
        match self {
            MongoKey::ObjectId(v) => Bson::ObjectId(*v),
            MongoKey::String(v) => Bson::String(v.clone()),
            MongoKey::Int32(v) => Bson::Int32(*v),
            MongoKey::Int64(v) => Bson::Int64(*v),
            MongoKey::JavaScriptCode(v) => Bson::JavaScriptCode(v.clone()),
            MongoKey::Timestamp(v) => Bson::Timestamp(*v),
            MongoKey::DateTime(v) => Bson::DateTime(*v),
            MongoKey::Symbol(v) => Bson::Symbol(v.clone()),
        }
    }
}

impl std::fmt::Display for MongoKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}
