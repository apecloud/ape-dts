use mongodb::bson::Document;

use dt_common::meta::mongo::mongo_constant::MongoConstants;

fn append_diff_path(prefix: &str, field: &str) -> String {
    if prefix.is_empty() {
        field.to_string()
    } else {
        format!("{}.{}", prefix, field)
    }
}

fn flatten_diff(diff: &Document, prefix: &str, set_doc: &mut Document, unset_doc: &mut Document) {
    if let Some(inserted) = diff.get("i").and_then(|value| value.as_document()) {
        for (field, value) in inserted {
            set_doc.insert(append_diff_path(prefix, field), value.clone());
        }
    }

    if let Some(updated) = diff.get("u").and_then(|value| value.as_document()) {
        for (field, value) in updated {
            set_doc.insert(append_diff_path(prefix, field), value.clone());
        }
    }

    if let Some(deleted) = diff.get("d").and_then(|value| value.as_document()) {
        for (field, value) in deleted {
            unset_doc.insert(append_diff_path(prefix, field), value.clone());
        }
    }

    for (field, value) in diff {
        if matches!(field.as_str(), "i" | "u" | "d" | "a") {
            continue;
        }

        let Some(nested_field) = field.strip_prefix('s') else {
            continue;
        };
        if nested_field.is_empty() {
            continue;
        }
        if let Some(nested_diff) = value.as_document() {
            flatten_diff(
                nested_diff,
                &append_diff_path(prefix, nested_field),
                set_doc,
                unset_doc,
            );
        }
    }
}

pub(crate) fn build_update_doc(oplog_doc: &Document) -> Document {
    let mut set_doc = Document::new();
    let mut unset_doc = Document::new();

    if let Some(diff) = oplog_doc.get("diff").and_then(|value| value.as_document()) {
        flatten_diff(diff, "", &mut set_doc, &mut unset_doc);
    } else {
        if let Some(doc) = oplog_doc
            .get(MongoConstants::SET)
            .and_then(|value| value.as_document())
        {
            set_doc.extend(doc.clone());
        }
        if let Some(doc) = oplog_doc
            .get(MongoConstants::UNSET)
            .and_then(|value| value.as_document())
        {
            unset_doc.extend(doc.clone());
        }
    }

    let mut update_doc = Document::new();
    if !set_doc.is_empty() {
        update_doc.insert(MongoConstants::SET, set_doc);
    }
    if !unset_doc.is_empty() {
        update_doc.insert(MongoConstants::UNSET, unset_doc);
    }
    update_doc
}
