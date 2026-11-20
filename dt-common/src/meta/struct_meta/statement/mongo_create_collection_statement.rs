use anyhow::Context;
use mongodb::bson::{Bson, Document};

use super::struct_statement::{StructKey, StructKeyType};
use crate::{
    config::config_enums::DbType, filter::DtFilter,
    meta::struct_meta::structure::structure_type::StructureType,
};

#[derive(Debug, Clone)]
pub struct MongoCreateCollectionStatement {
    pub database_name: String,
    pub collection_name: String,
    pub options: Document,
    pub indexes: Vec<Document>,
}

impl MongoCreateCollectionStatement {
    pub fn route(&mut self, dst_db: &str, dst_collection: &str) {
        self.database_name = dst_db.to_string();
        self.collection_name = dst_collection.to_string();
    }

    pub fn to_sqls(&self, filter: &DtFilter) -> anyhow::Result<Vec<(StructKey, String)>> {
        if filter.filter_structure(&StructureType::Collection) {
            return Ok(Vec::new());
        }
        let mut options = self.options.clone();
        for (key, default) in [
            ("capped", Bson::Boolean(false)),
            ("validationLevel", Bson::String("strict".into())),
            ("validationAction", Bson::String("error".into())),
            ("validator", Bson::Document(Document::new())),
        ] {
            if options.get(key) == Some(&default) {
                options.remove(key);
            }
        }
        let mut definitions = vec![(
            StructKey::new(
                DbType::Mongo,
                StructKeyType::Collection,
                [&self.database_name, &self.collection_name],
            ),
            check_definition(options)?,
        )];
        if !filter.filter_structure(&StructureType::Index) {
            for index in &self.indexes {
                let name = index
                    .get_str("name")
                    .context("MongoDB index missing name")?;
                if name == "_id_" {
                    continue;
                }
                let key = index
                    .get_document("key")
                    .context("MongoDB index missing key pattern")?;
                anyhow::ensure!(!key.is_empty(), "empty MongoDB index key pattern: {name}");
                let mut definition = index.clone();
                definition.remove("ns");
                definition.remove("v");
                for key in ["unique", "sparse", "hidden"] {
                    if definition.get_bool(key).ok() == Some(false) {
                        definition.remove(key);
                    }
                }
                definitions.push((
                    StructKey::new(
                        DbType::Mongo,
                        StructKeyType::Index,
                        [
                            self.database_name.as_str(),
                            self.collection_name.as_str(),
                            name,
                        ],
                    ),
                    check_definition(definition)?,
                ));
            }
        }
        Ok(definitions)
    }
}

// Sort only metadata wrapper fields. BSON key patterns and expressions retain their order.
pub(super) fn check_definition(document: Document) -> anyhow::Result<String> {
    let mut fields: Vec<_> = document.into_iter().collect();
    fields.sort_by(|a, b| a.0.cmp(&b.0));
    let document = Bson::Document(fields.into_iter().collect());
    // The BSON library enables serde_json's preserve_order for Extended JSON.
    Ok(serde_json::to_string(&document.into_canonical_extjson())?)
}
