use super::pg_value_type::{PgValueType, BPCHAR_OID, TEXT_OID, VARCHAR_OID};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PgColType {
    pub value_type: PgValueType,
    pub name: String,
    pub alias: String,
    pub oid: i32,
    pub parent_oid: i32,
    pub element_oid: i32,
    pub category: String,
    pub enum_values: Option<Vec<String>>,
    pub schema_name: String,
}

impl std::fmt::Display for PgColType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[allow(dead_code)]
impl PgColType {
    pub fn is_enum(&self) -> bool {
        "E" == self.category
    }

    pub fn is_array(&self) -> bool {
        "A" == self.category
    }

    pub fn is_user_defined(&self) -> bool {
        "U" == self.category
    }

    pub fn is_integer(&self) -> bool {
        self.value_type.is_integer()
    }

    pub fn can_be_splitted(&self) -> bool {
        // Means whether the type can be used in `max`/`min` aggregate operations
        // and `order by` comparisons. Compatible with PostgreSQL 14+.
        // Reference: https://www.postgresql.org/docs/14/functions-aggregate.html
        //
        // Unknown PostgreSQL types are mapped to PgValueType::String by default,
        // but not every unknown type has ordering operators, for example polygon[].
        // Therefore String is splittable only for built-in text-like OIDs.
        matches!(
            self.value_type,
            PgValueType::Int32
                | PgValueType::Int16
                | PgValueType::Int64
                | PgValueType::Float32
                | PgValueType::Float64
                | PgValueType::Numeric
                | PgValueType::TimestampTZ
                | PgValueType::Timestamp
                | PgValueType::Time
                | PgValueType::TimeTZ
                | PgValueType::Date
        ) || (matches!(self.value_type, PgValueType::String) && self.is_builtin_string_oid())
    }

    fn is_builtin_string_oid(&self) -> bool {
        matches!(self.oid, TEXT_OID | VARCHAR_OID | BPCHAR_OID)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg_col_type(value_type: PgValueType, oid: i32) -> PgColType {
        PgColType {
            value_type,
            name: String::new(),
            alias: String::new(),
            oid,
            parent_oid: 0,
            element_oid: 0,
            category: String::new(),
            enum_values: None,
            schema_name: String::new(),
        }
    }

    #[test]
    fn test_builtin_string_oid_can_be_splitted() {
        assert!(pg_col_type(PgValueType::String, TEXT_OID).can_be_splitted());
        assert!(pg_col_type(PgValueType::String, VARCHAR_OID).can_be_splitted());
        assert!(pg_col_type(PgValueType::String, BPCHAR_OID).can_be_splitted());
    }

    #[test]
    fn test_unknown_string_oid_can_not_be_splitted() {
        assert!(!pg_col_type(PgValueType::String, 1027).can_be_splitted());
    }
}
