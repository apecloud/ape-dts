use mongodb::bson::{oid::ObjectId, Bson, DateTime, Document, Timestamp};
use serde::{Deserialize, Serialize};

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
        if let Some(id) = doc.get(MongoConstants::ID) {
            let value = match id {
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
            };
            return value;
        }
        None
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

    pub fn parse_fuzzy(s: &str) -> Vec<MongoKey> {
        if let Ok(k) = serde_json::from_str::<MongoKey>(s) {
            return vec![k];
        }

        let mut keys = Vec::new();
        keys.push(MongoKey::String(s.to_string()));

        if let Ok(oid) = ObjectId::parse_str(s) {
            keys.push(MongoKey::ObjectId(oid));
        }

        if let Ok(i) = s.parse::<i64>() {
            keys.push(MongoKey::Int64(i));
            if let Ok(i32_val) = s.parse::<i32>() {
                keys.push(MongoKey::Int32(i32_val));
            }
        }

        keys
    }

    pub fn to_plain_string(&self) -> String {
        match self {
            MongoKey::ObjectId(v) => v.to_hex(),
            MongoKey::String(v) => v.clone(),
            MongoKey::Int32(v) => v.to_string(),
            MongoKey::Int64(v) => v.to_string(),
            MongoKey::JavaScriptCode(v) => v.clone(),
            MongoKey::Timestamp(v) => v.to_string(),
            MongoKey::DateTime(v) => v.to_string(),
            MongoKey::Symbol(v) => v.clone(),
        }
    }
}

impl std::fmt::Display for MongoKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MongoKey::ObjectId(v) => write!(f, "{}", v),
            MongoKey::String(v) => write!(f, "{}", v),
            MongoKey::Int32(v) => write!(f, "{}", v),
            MongoKey::Int64(v) => write!(f, "{}", v),
            MongoKey::JavaScriptCode(v) => write!(f, "{}", v),
            MongoKey::Timestamp(v) => write!(f, "{}", v),
            MongoKey::DateTime(v) => write!(f, "{}", v),
            MongoKey::Symbol(v) => write!(f, "{}", v),
        }
    }
}
