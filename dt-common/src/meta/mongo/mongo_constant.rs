pub struct MongoConstants {}

impl MongoConstants {
    pub const ID: &'static str = "_id";
    pub const DOC: &'static str = "doc";
    pub const DOCUMENT_KEY: &'static str = "document_key";
    pub const PRE_IMAGE: &'static str = "pre_image";
    pub const DIFF_DOC: &'static str = "diff_doc";
    pub const OPLOG_DIFF_DOC: &'static str = "oplog_diff_doc";
    pub const SET: &'static str = "$set";
    pub const UNSET: &'static str = "$unset";
}
