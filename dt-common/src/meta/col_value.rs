use std::{
    cmp,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use anyhow::bail;
use mongodb::bson::{Bson, Document};
use serde::{Deserialize, Serialize, Serializer};

// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// #[serde(tag = "type", content = "value")]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[allow(dead_code)]
pub enum ColValue {
    None,
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

impl std::fmt::Display for ColValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.to_option_string().unwrap_or("NULL".to_string())
        )
    }
}

impl ColValue {
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::Tiny { .. }
                | Self::UnsignedTiny { .. }
                | Self::Short { .. }
                | Self::UnsignedShort { .. }
                | Self::Long { .. }
                | Self::UnsignedLong { .. }
                | Self::LongLong { .. }
                | Self::UnsignedLongLong { .. }
        )
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float { .. } | Self::Double { .. })
    }

    pub fn is_decimal(&self) -> bool {
        matches!(self, Self::Decimal { .. })
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Self::String { .. })
    }

    pub fn convert_into_integer_128(&self) -> anyhow::Result<i128> {
        match self {
            Self::Tiny(v) => Ok(*v as i128),
            Self::UnsignedTiny(v) => Ok(*v as i128),
            Self::Short(v) => Ok(*v as i128),
            Self::UnsignedShort(v) => Ok(*v as i128),
            Self::Long(v) => Ok(*v as i128),
            Self::UnsignedLong(v) => Ok(*v as i128),
            Self::LongLong(v) => Ok(*v as i128),
            Self::UnsignedLongLong(v) => Ok(*v as i128),
            _ => bail!("can not convert {:?} into 128-bit integer", self),
        }
    }

    pub fn add_integer_128(&self, t: i128) -> anyhow::Result<Self> {
        match self {
            Self::Tiny(v) => Ok(Self::Tiny(cmp::min(*v as i128 + t, i8::MAX as i128) as i8)),
            Self::UnsignedTiny(v) => Ok(Self::UnsignedTiny(
                cmp::min(*v as i128 + t, i8::MAX as i128) as u8,
            )),
            Self::Short(v) => Ok(Self::Short(
                cmp::min(*v as i128 + t, i16::MAX as i128) as i16
            )),
            Self::UnsignedShort(v) => Ok(Self::UnsignedShort(cmp::min(
                *v as i128 + t,
                i16::MAX as i128,
            ) as u16)),
            Self::Long(v) => Ok(Self::Long(cmp::min(*v as i128 + t, i32::MAX as i128) as i32)),
            Self::UnsignedLong(v) => Ok(Self::UnsignedLong(cmp::min(
                *v as i128 + t,
                i32::MAX as i128,
            ) as u32)),
            Self::LongLong(v) => Ok(Self::LongLong(
                cmp::min(*v as i128 + t, i64::MAX as i128) as i64
            )),
            Self::UnsignedLongLong(v) => Ok(Self::UnsignedLongLong(cmp::min(
                *v as i128 + t,
                i64::MAX as i128,
            ) as u64)),
            _ => bail!("{} can not add 128-bit integer", self),
        }
    }

    pub fn convert_into_float_64(&self) -> anyhow::Result<f64> {
        match self {
            Self::Float(v) => Ok(*v as f64),
            Self::Double(v) => Ok(*v),
            _ => bail!("can not convert {:?} into double", self),
        }
    }

    pub fn is_same_value(&self, other: &ColValue) -> bool {
        match (self, other) {
            (ColValue::Float(v1), ColValue::Float(v2)) => {
                if v1.is_nan() && v2.is_nan() {
                    true
                } else {
                    v1 == v2
                }
            }
            (ColValue::Double(v1), ColValue::Double(v2)) => {
                if v1.is_nan() && v2.is_nan() {
                    true
                } else {
                    v1 == v2
                }
            }
            // MySQL Binlog VARCHAR/CHAR/TEXT->RawString same as String
            (ColValue::RawString(v1) | ColValue::Blob(v1), ColValue::String(v2)) => {
                if let Ok(s) = String::from_utf8(v1.clone()) {
                    &s == v2
                } else {
                    false
                }
            }
            (ColValue::String(v1), ColValue::RawString(v2) | ColValue::Blob(v2)) => {
                if let Ok(s) = String::from_utf8(v2.clone()) {
                    v1 == &s
                } else {
                    false
                }
            }
            // PostgreSQL inet type normalization: 192.168.1.100 == 192.168.1.100/32
            (ColValue::String(v1), ColValue::String(v2)) => {
                if v1 == v2 {
                    true
                } else {
                    Self::normalize_inet(v1) == Self::normalize_inet(v2)
                }
            }
            _ => self == other,
        }
    }

    /// Normalize PostgreSQL inet type values for comparison
    /// e.g., "192.168.1.100" and "192.168.1.100/32" should be considered equal
    fn normalize_inet(s: &str) -> &str {
        s.strip_suffix("/32").unwrap_or(s)
    }

    pub fn hash_code(&self) -> u64 {
        match self {
            ColValue::None => 0,
            _ => {
                let mut hasher = DefaultHasher::new();
                self.to_option_string().hash(&mut hasher);
                hasher.finish()
            }
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            ColValue::None => "None",
            ColValue::Bool(_) => "Bool",
            ColValue::Tiny(_) => "Tiny",
            ColValue::UnsignedTiny(_) => "UnsignedTiny",
            ColValue::Short(_) => "Short",
            ColValue::UnsignedShort(_) => "UnsignedShort",
            ColValue::Long(_) => "Long",
            ColValue::UnsignedLong(_) => "UnsignedLong",
            ColValue::LongLong(_) => "LongLong",
            ColValue::UnsignedLongLong(_) => "UnsignedLongLong",
            ColValue::Float(_) => "Float",
            ColValue::Double(_) => "Double",
            ColValue::Decimal(_) => "Decimal",
            ColValue::Time(_) => "Time",
            ColValue::Date(_) => "Date",
            ColValue::DateTime(_) => "DateTime",
            ColValue::Timestamp(_) => "Timestamp",
            ColValue::Year(_) => "Year",
            ColValue::String(_) => "String",
            ColValue::RawString(_) => "RawString",
            ColValue::Blob(_) => "Blob",
            ColValue::Bit(_) => "Bit",
            ColValue::Set(_) => "Set",
            ColValue::Enum(_) => "Enum",
            ColValue::Set2(_) => "Set2",
            ColValue::Enum2(_) => "Enum2",
            ColValue::Json(_) => "Json",
            ColValue::Json2(_) => "Json2",
            ColValue::Json3(_) => "Json3",
            ColValue::MongoDoc(_) => "MongoDoc",
        }
    }

    pub fn to_option_string(&self) -> Option<String> {
        match self {
            ColValue::Tiny(v) => Some(v.to_string()),
            ColValue::UnsignedTiny(v) => Some(v.to_string()),
            ColValue::Short(v) => Some(v.to_string()),
            ColValue::UnsignedShort(v) => Some(v.to_string()),
            ColValue::Long(v) => Some(v.to_string()),
            ColValue::UnsignedLong(v) => Some(v.to_string()),
            ColValue::LongLong(v) => Some(v.to_string()),
            ColValue::UnsignedLongLong(v) => Some(v.to_string()),
            ColValue::Float(v) => Some(v.to_string()),
            ColValue::Double(v) => Some(v.to_string()),
            ColValue::Decimal(v) => Some(v.to_string()),
            ColValue::Time(v) => Some(v.to_string()),
            ColValue::Date(v) => Some(v.to_string()),
            ColValue::DateTime(v) => Some(v.to_string()),
            ColValue::Timestamp(v) => Some(v.to_string()),
            ColValue::Year(v) => Some(v.to_string()),
            ColValue::String(v) => Some(v.to_string()),
            ColValue::RawString(v) => Some(hex::encode(v)),
            ColValue::Bit(v) => Some(v.to_string()),
            ColValue::Set(v) => Some(v.to_string()),
            ColValue::Set2(v) => Some(v.to_string()),
            ColValue::Enum(v) => Some(v.to_string()),
            ColValue::Enum2(v) => Some(v.to_string()),
            ColValue::Json(v) => Some(format!("{:?}", v)),
            ColValue::Json2(v) => Some(v.to_string()),
            ColValue::Json3(v) => Some(v.to_string()),
            ColValue::Blob(v) => Some(hex::encode(v)),
            ColValue::MongoDoc(v) => Some(Self::mongo_doc_to_string(v)),
            ColValue::Bool(v) => Some(v.to_string()),
            ColValue::None => Option::None,
        }
    }

    pub fn is_nan(&self) -> bool {
        match &self {
            ColValue::Float(v) => v.is_nan(),
            ColValue::Double(v) => v.is_nan(),
            _ => false,
        }
    }

    pub fn get_malloc_size(&self) -> usize {
        match self {
            ColValue::Tiny(_) | ColValue::UnsignedTiny(_) | ColValue::Bool(_) => 1,
            ColValue::Short(_) | ColValue::UnsignedShort(_) | ColValue::Year(_) => 2,
            ColValue::Long(_)
            | ColValue::UnsignedLong(_)
            | ColValue::Float(_)
            | ColValue::Enum(_) => 4,
            ColValue::LongLong(_)
            | ColValue::UnsignedLongLong(_)
            | ColValue::Double(_)
            | ColValue::Bit(_)
            | ColValue::Set(_) => 8,
            ColValue::Decimal(v)
            | ColValue::Time(v)
            | ColValue::Date(v)
            | ColValue::DateTime(v)
            | ColValue::Timestamp(v)
            | ColValue::String(v)
            | ColValue::Set2(v)
            | ColValue::Enum2(v)
            | ColValue::Json2(v) => v.len(),
            ColValue::Json(v) | ColValue::Blob(v) | ColValue::RawString(v) => v.len(),
            ColValue::Json3(v) => v.to_string().len(),
            ColValue::MongoDoc(v) => Self::get_bson_size_doc(v),
            ColValue::None => 0,
        }
    }

    fn get_bson_size_doc(doc: &Document) -> usize {
        std::mem::size_of::<Document>()
            + doc
                .iter()
                .map(|(k, v)| k.len() + Self::get_bson_size(v))
                .sum::<usize>()
    }

    fn get_bson_size(bson: &Bson) -> usize {
        match bson {
            Bson::String(v) | Bson::Symbol(v) | Bson::JavaScriptCode(v) => v.len(),
            Bson::Array(arr) => arr.iter().map(Self::get_bson_size).sum(),
            Bson::Document(doc) => Self::get_bson_size_doc(doc),
            Bson::Binary(v) => v.bytes.len(),
            Bson::RegularExpression(regex) => regex.pattern.len() + regex.options.len(),
            Bson::JavaScriptCodeWithScope(code_w_scope) => {
                code_w_scope.code.len() + Self::get_bson_size_doc(&code_w_scope.scope)
            }
            Bson::DbPointer(_) => std::mem::size_of::<Bson>(),
            _ => std::mem::size_of_val(bson),
        }
    }

    fn mongo_doc_to_string(doc: &Document) -> String {
        // Use Relaxed Extended JSON to make it more human-readable while preserving necessary types.
        // https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
        let bson = Bson::Document(doc.clone());
        match bson.into_relaxed_extjson() {
            serde_json::Value::Object(map) => serde_json::Value::Object(map).to_string(),
            _ => doc.to_string(),
        }
    }
}

impl Serialize for ColValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // serde json serializer
        // case 1: #[derive(Serialize)]
        //   output: {"title":{"String":"C++ primer"},"author":{"String":"avc"}}
        // case 2: #[derive(Serialize)]
        //         #[serde(tag = "type", content = "value")]
        //   output: {"title":{"type":"String","value":"C++ primer"},"author":{"type":"String","value":"avc"}}
        // case 3: this impl
        //   output: {"title":"C++ primer","author":"avc"}
        match self {
            ColValue::Bool(v) => serializer.serialize_bool(*v),
            ColValue::Tiny(v) => serializer.serialize_i8(*v),
            ColValue::UnsignedTiny(v) => serializer.serialize_u8(*v),
            ColValue::Short(v) => serializer.serialize_i16(*v),
            ColValue::UnsignedShort(v) => serializer.serialize_u16(*v),
            ColValue::Long(v) => serializer.serialize_i32(*v),
            ColValue::UnsignedLong(v) => serializer.serialize_u32(*v),
            ColValue::LongLong(v) => serializer.serialize_i64(*v),
            ColValue::UnsignedLongLong(v) => serializer.serialize_u64(*v),
            ColValue::Float(v) => serializer.serialize_f32(*v),
            ColValue::Double(v) => serializer.serialize_f64(*v),
            ColValue::Decimal(v) => serializer.serialize_str(v),
            ColValue::Time(v) => serializer.serialize_str(v),
            ColValue::Date(v) => serializer.serialize_str(v),
            ColValue::DateTime(v) => serializer.serialize_str(v),
            ColValue::Timestamp(v) => serializer.serialize_str(v),
            ColValue::Year(v) => serializer.serialize_u16(*v),
            ColValue::String(v) => serializer.serialize_str(v),
            ColValue::RawString(v) => serializer.serialize_bytes(v),
            ColValue::Blob(v) => serializer.serialize_bytes(v),
            ColValue::Bit(v) => serializer.serialize_u64(*v),
            ColValue::Set(v) => serializer.serialize_u64(*v),
            ColValue::Set2(v) => serializer.serialize_str(v),
            ColValue::Enum(v) => serializer.serialize_u32(*v),
            ColValue::Enum2(v) => serializer.serialize_str(v),
            ColValue::Json(v) => serializer.serialize_bytes(v),
            ColValue::Json2(v) => serializer.serialize_str(v),
            ColValue::Json3(v) => v.serialize(serializer),
            ColValue::MongoDoc(v) => Bson::Document(v.clone())
                .into_relaxed_extjson()
                .serialize(serializer),
            ColValue::None => serializer.serialize_none(),
        }
    }
}

impl From<Bson> for ColValue {
    fn from(bson: Bson) -> Self {
        match bson {
            Bson::Double(v) => ColValue::Double(v),
            Bson::String(v) => ColValue::String(v),
            Bson::Array(v) => ColValue::Json2(Bson::Array(v).to_string()),
            Bson::Document(v) => ColValue::MongoDoc(v),
            Bson::Boolean(v) => ColValue::Bool(v),
            Bson::Null => ColValue::None,
            Bson::Int32(v) => ColValue::Long(v),
            Bson::Int64(v) => ColValue::LongLong(v),
            Bson::Timestamp(v) => ColValue::Timestamp(format!("{}:{}", v.time, v.increment)),
            Bson::Binary(v) => ColValue::Blob(v.bytes),
            Bson::DateTime(v) => ColValue::DateTime(v.to_string()),
            Bson::Decimal128(v) => ColValue::Decimal(v.to_string()),
            // others types
            Bson::ObjectId(v) => ColValue::String(v.to_hex()),
            Bson::RegularExpression(v) => ColValue::String(v.pattern),
            Bson::JavaScriptCode(v) => ColValue::String(v),
            Bson::JavaScriptCodeWithScope(v) => ColValue::String(v.code),
            Bson::Symbol(v) => ColValue::String(v),
            Bson::Undefined => ColValue::String("Undefined".into()),
            Bson::MaxKey => ColValue::String("MaxKey".into()),
            Bson::MinKey => ColValue::String("MinKey".into()),
            Bson::DbPointer(v) => ColValue::String(format!("{:?}", v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_same_value() {
        let v1 = ColValue::Float(f32::NAN);
        let v2 = ColValue::Double(f64::NAN);
        let v3 = ColValue::None;
        let v4 = ColValue::Long(7);

        assert!(v1.is_same_value(&ColValue::Float(f32::NAN)));
        assert!(v2.is_same_value(&ColValue::Double(f64::NAN)));
        assert!(v3.is_same_value(&ColValue::None));
        assert!(v4.is_same_value(&ColValue::Long(7)));
    }

    #[test]
    fn test_add_integer_128() {
        let cases = vec![
            (ColValue::Tiny(10), 20, Some(ColValue::Tiny(30))),
            (ColValue::Short(1000), 2000, Some(ColValue::Short(3000))),
            (ColValue::Long(50), -20, Some(ColValue::Long(30))),
            (ColValue::Tiny(100), 50, Some(ColValue::Tiny(127))),
            // i64::MAX boundary check
            (
                ColValue::LongLong(i64::MAX - 5),
                10,
                Some(ColValue::LongLong(i64::MAX)),
            ),
            (
                ColValue::UnsignedTiny(100),
                50,
                Some(ColValue::UnsignedTiny(127)),
            ),
            // --- Error Case ---
            (ColValue::String("test".into()), 1, None),
        ];

        for (index, (input, delta, expected)) in cases.into_iter().enumerate() {
            let result = input.add_integer_128(delta);

            match expected {
                Some(exp_val) => {
                    assert_eq!(result.unwrap(), exp_val, "Failed at case #{}", index);
                }
                None => {
                    assert!(result.is_err(), "Case #{} should fail", index);
                }
            }
        }
    }
}
