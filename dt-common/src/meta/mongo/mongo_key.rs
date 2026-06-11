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

    // use canonical extended JSON string representation for types that don't derive Hash and Eq,
    // such as Decimal128, Binary, etc.
    CanonicalExtJson(serde_json::Value),
}

impl MongoKey {
    pub fn from_doc(doc: &Document) -> Option<MongoKey> {
        doc.get(MongoConstants::ID).map(|id| match id {
            Bson::ObjectId(v) => MongoKey::ObjectId(*v),
            Bson::String(v) => MongoKey::String(v.clone()),
            Bson::Int32(v) => MongoKey::Int32(*v),
            Bson::Int64(v) => MongoKey::Int64(*v),
            Bson::JavaScriptCode(v) => MongoKey::JavaScriptCode(v.clone()),
            Bson::Timestamp(v) => MongoKey::Timestamp(*v),
            Bson::DateTime(v) => MongoKey::DateTime(*v),
            Bson::Symbol(v) => MongoKey::Symbol(v.clone()),
            _ => MongoKey::CanonicalExtJson(id.clone().into_canonical_extjson()),
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
            MongoKey::CanonicalExtJson(v) => Bson::try_from(v.clone()).unwrap_or_else(|_| {
                // if the canonical extended JSON string cannot be parsed back to a Bson, use the original value as a string.
                Bson::String(v.to_string())
            }),
        }
    }
}

impl std::fmt::Display for MongoKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{doc, spec::BinarySubtype, Binary};

    use super::*;

    #[test]
    fn canonical_extjson_key_round_trips_fallback_id_value() {
        let id = Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: vec![1, 2, 3],
        });
        let doc = doc! {
            MongoConstants::ID: id.clone(),
            "name": "ignored",
        };

        let key = MongoKey::from_doc(&doc).unwrap();
        assert_eq!(key.to_mongo_id(), id);

        let serialized = key.to_string();
        let deserialized: MongoKey = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.to_mongo_id(), id);
    }
}
