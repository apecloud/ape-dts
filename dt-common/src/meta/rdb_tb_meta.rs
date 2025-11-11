use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::{
    config::config_enums::DbType,
    meta::{col_value::ColValue, foreign_key::ForeignKey, position::Position},
};

#[derive(Debug, Clone, Default, Serialize)]
pub struct RdbTbMeta {
    pub schema: String,
    pub tb: String,
    pub cols: Vec<String>,
    pub nullable_cols: HashSet<String>,
    pub col_origin_type_map: HashMap<String, String>,
    pub key_map: HashMap<String, Vec<String>>,
    pub order_cols: Vec<String>,
    pub order_cols_are_nullable: bool,
    pub partition_col: String,
    pub id_cols: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
    pub ref_by_foreign_keys: Vec<ForeignKey>,
}

impl RdbTbMeta {
    #[inline(always)]
    pub fn get_default_order_col_values(&self) -> HashMap<String, ColValue> {
        self.order_cols
            .iter()
            .map(|col| (col.clone(), ColValue::None))
            .collect()
    }

    pub fn build_position(
        &self,
        db_type: &DbType,
        col_values: &HashMap<String, ColValue>,
    ) -> Position {
        let mut order_col_values = HashMap::new();
        for order_col in &self.order_cols {
            if let Some(value) = col_values.get(order_col) {
                order_col_values.insert(order_col.to_string(), value.to_option_string());
            } else {
                // Do not record rows whose composite unique columns have NULL values.
                return Position::None;
            }
        }
        Position::RdbSnapshot {
            db_type: db_type.to_string(),
            schema: self.schema.clone(),
            tb: self.tb.clone(),
            order_col: String::new(),
            value: String::new(),
            order_col_values,
        }
    }
}
