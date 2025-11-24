use std::collections::HashMap;

use mongodb::bson::{Bson, Document};

use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, row_data::RowData,
};

use crate::check_log::check_log::DiffColValue;

pub fn build_insert_cmd(src_row_data: &RowData) -> Option<String> {
    let after = src_row_data.require_after().ok()?;
    let doc = after.get(MongoConstants::DOC)?;

    match doc {
        ColValue::MongoDoc(bson_doc) => {
            Some(format!("db.{}.insertOne({})", src_row_data.tb, bson_doc))
        }
        _ => None,
    }
}

pub fn build_update_cmd(
    src_row_data: &RowData,
    diff_col_values: &HashMap<String, DiffColValue>,
) -> Option<String> {
    let after = src_row_data.require_after().ok()?;
    let doc = after.get(MongoConstants::DOC)?;

    match doc {
        ColValue::MongoDoc(bson_doc) => {
            let id = bson_doc.get(MongoConstants::ID)?;

            let mut set_doc = Document::new();
            let mut unset_doc = Document::new();
            let mut sorted_keys: Vec<_> = diff_col_values.keys().collect();
            sorted_keys.sort();
            for col in sorted_keys {
                if col == MongoConstants::ID {
                    continue;
                }
                if let Some(value) = bson_doc.get(col) {
                    set_doc.insert(col.clone(), value.clone());
                } else {
                    unset_doc.insert(col.clone(), Bson::Int32(1));
                }
            }

            if set_doc.is_empty() && unset_doc.is_empty() {
                return None;
            }

            let mut parts = Vec::new();
            if !set_doc.is_empty() {
                parts.push(format!("\"$set\": {}", set_doc));
            }
            if !unset_doc.is_empty() {
                parts.push(format!("\"$unset\": {}", unset_doc));
            }

            Some(format!(
                "db.{}.updateOne({{ \"_id\": {} }}, {{ {} }})",
                src_row_data.tb,
                id,
                parts.join(", ")
            ))
        }
        _ => None,
    }
}

pub fn bson_type_name(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "Double",
        Bson::String(_) => "String",
        Bson::Array(_) => "Array",
        Bson::Document(_) => "Document",
        Bson::Boolean(_) => "Boolean",
        Bson::Null => "Null",
        Bson::Int32(_) => "Int32",
        Bson::Int64(_) => "Int64",
        Bson::Timestamp(_) => "Timestamp",
        Bson::Binary(_) => "Binary",
        Bson::ObjectId(_) => "ObjectId",
        Bson::DateTime(_) => "DateTime",
        Bson::Decimal128(_) => "Decimal128",
        Bson::RegularExpression(_) => "RegularExpression",
        Bson::JavaScriptCode(_) => "JavaScriptCode",
        Bson::JavaScriptCodeWithScope(_) => "JavaScriptCodeWithScope",
        Bson::Symbol(_) => "Symbol",
        Bson::Undefined => "Undefined",
        Bson::MinKey => "MinKey",
        Bson::MaxKey => "MaxKey",
        Bson::DbPointer(_) => "DbPointer",
    }
}
