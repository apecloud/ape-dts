use std::collections::HashMap;

use mongodb::bson::{doc, Bson, Document, Regex};

use dt_common::meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, row_data::RowData,
};

use crate::check_log::check_log::DiffColValue;

pub fn build_insert_cmd(src_row_data: &RowData) -> Option<String> {
    let doc = get_doc(src_row_data)?;
    let doc_js = bson_to_js(&Bson::Document(doc.clone()));

    Some(format!("db.{}.insertOne({})", src_row_data.tb, doc_js))
}

pub fn build_update_cmd(
    src_row_data: &RowData,
    diff_col_values: &HashMap<String, DiffColValue>,
) -> Option<String> {
    let full_doc = get_doc(src_row_data)?;
    let id = full_doc.get(MongoConstants::ID)?;

    let mut set_doc = Document::new();
    let mut unset_doc = Document::new();

    // sort keys for test
    let mut sorted_keys: Vec<_> = diff_col_values.keys().collect();
    sorted_keys.sort_unstable();

    for key in sorted_keys {
        if key == MongoConstants::ID {
            continue;
        }
        match full_doc.get(key.as_str()) {
            Some(val) => {
                set_doc.insert(key.clone(), val.clone());
            }
            None => {
                unset_doc.insert(key.clone(), 1);
            }
        }
    }

    if set_doc.is_empty() && unset_doc.is_empty() {
        return None;
    }

    let mut update_action = Document::new();
    if !set_doc.is_empty() {
        update_action.insert("$set", set_doc);
    }
    if !unset_doc.is_empty() {
        update_action.insert("$unset", unset_doc);
    }

    let filter_js = bson_to_js(&Bson::Document(doc! { MongoConstants::ID: id.clone() }));
    let update_js = bson_to_js(&Bson::Document(update_action));

    Some(format!(
        "db.{}.updateOne({}, {})",
        src_row_data.tb, filter_js, update_js
    ))
}

/// Convert BSON into a Mongo Shell-friendly JavaScript literal; strings are quoted.
pub fn bson_to_js(value: &Bson) -> String {
    bson_to_literal(value, true)
}

/// Convert BSON into a log-friendly literal; strings are not quoted.
pub fn bson_to_log_literal(value: &Bson) -> String {
    bson_to_literal(value, false)
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

fn bson_to_literal(value: &Bson, wrap_string: bool) -> String {
    match value {
        Bson::Double(v) => v.to_string(),
        Bson::String(s) => {
            if wrap_string {
                format!("'{}'", escape_js_str(s))
            } else {
                escape_js_str(s)
            }
        }
        Bson::Array(arr) => {
            let items: Vec<String> = arr
                .iter()
                .map(|v| bson_to_literal(v, wrap_string))
                .collect();
            format!("[{}]", items.join(", "))
        }
        Bson::Document(doc) => {
            let items: Vec<String> = doc
                .iter()
                .map(|(k, v)| {
                    format!(
                        "'{}': {}",
                        escape_js_str(k),
                        bson_to_literal(v, wrap_string)
                    )
                })
                .collect();
            format!("{{{}}}", items.join(", "))
        }
        Bson::Boolean(v) => v.to_string(),
        Bson::Null => "null".to_string(),
        Bson::Int32(v) => v.to_string(),
        Bson::Int64(v) => format!("NumberLong({})", v),
        Bson::Timestamp(ts) => format!("Timestamp({}, {})", ts.time, ts.increment),
        Bson::Binary(_) | Bson::DbPointer(_) => bson_to_ejson(value),
        Bson::ObjectId(oid) => format!("ObjectId('{}')", oid.to_hex()),
        Bson::DateTime(dt) => format!("ISODate('{}')", dt),
        Bson::RegularExpression(regex) => format_regex(regex),
        Bson::JavaScriptCode(code) => code.clone(),
        Bson::JavaScriptCodeWithScope(scope) => {
            format!(
                "{} /* scope: {} */",
                scope.code,
                bson_to_literal(&Bson::Document(scope.scope.clone()), wrap_string)
            )
        }
        Bson::Symbol(s) => format!("Symbol('{}')", escape_js_str(s)),
        Bson::Decimal128(v) => format!("NumberDecimal('{}')", v),
        Bson::Undefined => "undefined".to_string(),
        Bson::MinKey => "MinKey".to_string(),
        Bson::MaxKey => "MaxKey".to_string(),
    }
}

fn get_doc(src_row_data: &RowData) -> Option<&Document> {
    match src_row_data
        .require_after()
        .ok()?
        .get(MongoConstants::DOC)?
    {
        ColValue::MongoDoc(doc) => Some(doc),
        _ => None,
    }
}

fn escape_js_str(input: &str) -> String {
    input.replace('\\', "\\\\").replace('\'', "\\'")
}

fn format_regex(regex: &Regex) -> String {
    let safe_pattern = regex.pattern.replace('/', "\\/");
    if regex.options.is_empty() {
        format!("/{}/", safe_pattern)
    } else {
        format!("/{}/{}", safe_pattern, regex.options)
    }
}

fn bson_to_ejson(value: &Bson) -> String {
    let extjson = value.clone().into_relaxed_extjson().to_string();
    format!("EJSON.parse('{}')", escape_js_str(&extjson))
}
