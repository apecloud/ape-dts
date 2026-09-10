use serde::{Deserialize, Serialize};
use serde_json::json;

use super::pg_value_type::{PgValueType, BPCHAR_OID, TEXT_OID, VARCHAR_OID};

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
    pub typmod: i32,
}

impl std::fmt::Display for PgColType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[allow(dead_code)]
impl PgColType {
    pub fn order_key_weight(&self) -> Option<u32> {
        // Unknown types are represented as strings too. Use catalog OIDs/category,
        // rather than assigning them the cost of a built-in text column.
        Some(match self.oid {
            20 | 21 | 23 | 26 => 1,
            1082 | 1083 | 1114 | 1184 | 1186 | 1266 => 2,
            790 | 1700 => 3,
            16 | 1560 | 1562 | 2950 => 4,
            17 => 6,
            18 | 19 | TEXT_OID | VARCHAR_OID | BPCHAR_OID => 8,
            700 | 701 => 12,
            650 | 774 | 829 | 869 => 16,
            _ if self.is_array() || matches!(self.category.as_str(), "R") => 20,
            // The key catalog guarantees a complete ordinary-column unique index.
            // Keep existing indexable types eligible, including text-roundtripped types.
            _ => 32,
        })
    }

    pub fn get_alias(&self) -> String {
        // PostgreSQL bit string docs:
        // https://www.postgresql.org/docs/current/datatype-bit.html
        // `bit` without a length is `bit(1)`, and explicit casts to `bit` will truncate the value to 1 bit.
        //
        // PostgreSQL stores bit-string typmod as the bit length directly.
        // Reference: https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/varbit.c#L18
        match self.alias.as_str() {
            "bit" if self.typmod > 0 => format!("bit({})", self.typmod),
            "_bit" if self.typmod > 0 => format!("bit({})[]", self.typmod),
            _ => self.alias.clone(),
        }
    }

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
            typmod: 0,
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

    #[test]
    fn test_get_alias_uses_bit_typmod() {
        let mut col_type = pg_col_type(PgValueType::String, 1560);

        col_type.alias = "bit".to_string();
        col_type.typmod = 10;
        assert_eq!("bit(10)", col_type.get_alias());

        col_type.alias = "_bit".to_string();
        assert_eq!("bit(10)[]", col_type.get_alias());

        col_type.alias = "bit".to_string();
        col_type.typmod = -1;
        assert_eq!("bit", col_type.get_alias());

        col_type.alias = "varbit".to_string();
        col_type.typmod = 32;
        assert_eq!("varbit", col_type.get_alias());

        col_type.alias = "_varbit".to_string();
        assert_eq!("_varbit", col_type.get_alias());
    }
}
