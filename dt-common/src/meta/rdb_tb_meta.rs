use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::{
    config::config_enums::DbType,
    meta::{col_value::ColValue, foreign_key::ForeignKey, order_key::OrderKey, position::Position},
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

    #[inline(always)]
    pub fn has_col(&self, col: &String) -> bool {
        self.cols.contains(col)
    }

    #[inline(always)]
    pub fn is_col_nullable(&self, col: &str) -> bool {
        self.nullable_cols.contains(col)
    }

    pub fn build_position(
        &self,
        db_type: &DbType,
        col_values: &HashMap<String, ColValue>,
    ) -> Position {
        let mut order_col_values = Vec::new();
        for order_col in &self.order_cols {
            if let Some(v) = col_values.get(order_col) {
                order_col_values.push((order_col.clone(), v.to_option_string()));
            } else {
                // Do not record rows whose composite unique columns have NULL values.
                return Position::None;
            }
        }
        let order_key = match order_col_values.len() {
            0 => None,
            1 => Some(OrderKey::Single(order_col_values[0].clone())),
            _ => Some(OrderKey::Composite(order_col_values.clone())),
        };
        Position::RdbSnapshot {
            db_type: db_type.to_string(),
            schema: self.schema.clone(),
            tb: self.tb.clone(),
            order_key,
        }
    }

    pub fn build_position_for_partition(
        &self,
        db_type: &DbType,
        partition_col: &str,
        partition_col_value: &ColValue,
    ) -> Position {
        // partion_col can be defined by user, not necessarily in order_cols
        let order_key = Some(OrderKey::Single((
            partition_col.to_string(),
            partition_col_value.to_option_string(),
        )));
        Position::RdbSnapshot {
            db_type: db_type.to_string(),
            schema: self.schema.clone(),
            tb: self.tb.clone(),
            order_key,
        }
    }
}
