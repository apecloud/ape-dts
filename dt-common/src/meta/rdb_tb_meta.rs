use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::meta::foreign_key::ForeignKey;

#[derive(Debug, Clone, Default, Serialize)]
pub struct RdbTbMeta {
    pub schema: String,
    pub tb: String,
    pub cols: Vec<String>,
    pub nullable_cols: HashSet<String>,
    pub col_origin_type_map: HashMap<String, String>,
    pub key_map: HashMap<String, Vec<String>>,
    pub order_col: Option<String>,
    pub order_cols: Vec<String>,
    pub partition_col: String,
    pub partition_cols: Vec<String>,
    pub id_cols: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
    pub ref_by_foreign_keys: Vec<ForeignKey>,
}
