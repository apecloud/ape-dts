use std::collections::BTreeMap;

use mongodb::bson::Document;
use serde::{ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};

use super::col_value::ColValue;

// Serde definition for tagged ColValue maps, currently used to persist
// checker-state primary keys for inconsistent rows.
#[derive(Serialize, Deserialize)]
#[serde(remote = "ColValue")]
pub enum TaggedColValueDef {
    None,
    UnchangedToast,
    Bool(bool),
    Tiny(i8),
    UnsignedTiny(u8),
    Short(i16),
    UnsignedShort(u16),
    Long(i32),
    UnsignedLong(u32),
    LongLong(i64),
    UnsignedLongLong(u64),
    Float(f32),
    Double(f64),
    Decimal(String),
    Time(String),
    Date(String),
    DateTime(String),
    Timestamp(String),
    Year(u16),
    String(String),
    RawString(Vec<u8>),
    Blob(Vec<u8>),
    Bit(u64),
    Set(u64),
    Enum(u32),
    Set2(String),
    Enum2(String),
    Json(Vec<u8>),
    Json2(String),
    Json3(serde_json::Value),
    MongoDoc(Document),
}

#[derive(Serialize)]
struct TaggedColValueRef<'a>(#[serde(with = "TaggedColValueDef")] &'a ColValue);

#[derive(Deserialize)]
struct TaggedColValue(#[serde(with = "TaggedColValueDef")] ColValue);

pub fn serialize<S>(values: &BTreeMap<String, ColValue>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(values.len()))?;
    for (col, value) in values {
        map.serialize_entry(col, &TaggedColValueRef(value))?;
    }
    map.end()
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<String, ColValue>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = BTreeMap::<String, TaggedColValue>::deserialize(deserializer)?;
    Ok(values
        .into_iter()
        .map(|(col, TaggedColValue(value))| (col, value))
        .collect())
}
