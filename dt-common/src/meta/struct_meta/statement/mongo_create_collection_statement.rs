use mongodb::bson::Document;

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
}
