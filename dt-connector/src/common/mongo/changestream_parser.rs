use mongodb::bson::{Bson, Document};

fn get_path_value<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut current = doc;
    let mut fields = path.split('.').peekable();
    while let Some(field) = fields.next() {
        let value = current.get(field)?;
        if fields.peek().is_none() {
            return Some(value);
        }
        current = value.as_document()?;
    }
    None
}

pub(crate) fn build_update_doc(
    update_description: &Document,
    full_document: Option<&Document>,
) -> Document {
    let mut set_doc = Document::new();
    let mut unset_doc = Document::new();

    if let Some(updated_fields) = update_description
        .get("updatedFields")
        .and_then(|value| value.as_document())
    {
        set_doc.extend(updated_fields.clone());
    }

    if let Some(removed_fields) = update_description
        .get("removedFields")
        .and_then(|value| value.as_array())
    {
        for field in removed_fields {
            if let Some(field) = field.as_str() {
                unset_doc.insert(field, "");
            }
        }
    }

    if let Some(truncated_arrays) = update_description
        .get("truncatedArrays")
        .and_then(|value| value.as_array())
    {
        for truncated_array in truncated_arrays {
            let Some(truncated_array) = truncated_array.as_document() else {
                continue;
            };
            let Ok(field) = truncated_array.get_str("field") else {
                continue;
            };
            if let Some(value) = full_document.and_then(|doc| get_path_value(doc, field)) {
                set_doc.insert(field, value.clone());
            }
        }
    }

    let mut update_doc = Document::new();
    if !set_doc.is_empty() {
        update_doc.insert("$set", set_doc);
    }
    if !unset_doc.is_empty() {
        update_doc.insert("$unset", unset_doc);
    }
    update_doc
}

pub(crate) fn requires_full_document(update_description: &Document) -> bool {
    update_description
        .get("disambiguatedPaths")
        .and_then(|value| value.as_document())
        .map(|doc| doc.values().any(disambiguated_path_requires_full_document))
        .unwrap_or(false)
}

fn disambiguated_path_requires_full_document(path: &Bson) -> bool {
    let Some(components) = path.as_array() else {
        return true;
    };
    if components.is_empty() {
        return true;
    }

    components.iter().any(|component| match component {
        Bson::String(field) => field.contains('.'),
        Bson::Int32(_) | Bson::Int64(_) => false,
        _ => true,
    })
}
