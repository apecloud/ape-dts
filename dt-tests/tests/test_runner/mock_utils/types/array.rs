use chrono::format;

use crate::test_runner::mock_utils::{pg_type::PgType, random::Random};

pub struct Array {}

impl Array {
    /// Generate random array length (1-5 elements)
    fn random_len(rand: &mut Random) -> usize {
        (rand.next_u8() % 5 + 1) as usize
    }

    /// Generate array values with optional NULL
    fn gen_array<F>(rand: &mut Random, gen_value: F) -> String
    where
        F: Fn(&mut Random) -> String,
    {
        let len = Self::random_len(rand);
        let values: Vec<String> = (0..len)
            .map(|_| {
                if rand.next_null() {
                    "NULL".to_string()
                } else {
                    gen_value(rand)
                }
            })
            .collect();
        format!("ARRAY[{}]", values.join(", "))
    }

    pub fn next_value_str(pg_type: &PgType, rand: &mut Random) -> String {
        if let Some(elem_pg_type) = Array::element_type(pg_type) {
            return Self::gen_array(rand, |r| PgType::next_value_str(&elem_pg_type, r));
        };
        panic!("unsupported array type: {:?}", pg_type);
    }

    /// Get the element type for an array type
    pub fn element_type(pg_type: &PgType) -> Option<PgType> {
        match pg_type {
            PgType::BoolArray => Some(PgType::Bool),
            PgType::Int2Array => Some(PgType::Int2),
            PgType::Int4Array => Some(PgType::Int4),
            PgType::Int8Array => Some(PgType::Int8),
            PgType::OidArray => Some(PgType::Oid),
            PgType::Float4Array => Some(PgType::Float4),
            PgType::Float8Array => Some(PgType::Float8),
            PgType::NumericArray => Some(PgType::Numeric),
            PgType::TextArray => Some(PgType::Text),
            PgType::VarcharArray => Some(PgType::Varchar),
            PgType::BpcharArray => Some(PgType::Bpchar),
            PgType::CharArray => Some(PgType::Char),
            PgType::NameArray => Some(PgType::Name),
            PgType::ByteaArray => Some(PgType::Bytea),
            PgType::JsonArray => Some(PgType::Json),
            PgType::JsonbArray => Some(PgType::Jsonb),
            PgType::UuidArray => Some(PgType::Uuid),
            PgType::DateArray => Some(PgType::Date),
            PgType::TimeArray => Some(PgType::Time),
            PgType::TimetzArray => Some(PgType::Timetz),
            PgType::TimestampArray => Some(PgType::Timestamp),
            PgType::TimestamptzArray => Some(PgType::Timestamptz),
            PgType::IntervalArray => Some(PgType::Interval),
            PgType::PointArray => Some(PgType::Point),
            PgType::LineArray => Some(PgType::Line),
            PgType::LsegArray => Some(PgType::Lseg),
            PgType::BoxArray => Some(PgType::Box),
            PgType::PathArray => Some(PgType::Path),
            PgType::PolygonArray => Some(PgType::Polygon),
            PgType::CircleArray => Some(PgType::Circle),
            PgType::InetArray => Some(PgType::Inet),
            PgType::CidrArray => Some(PgType::Cidr),
            PgType::MacaddrArray => Some(PgType::Macaddr),
            PgType::Macaddr8Array => Some(PgType::Macaddr8),
            PgType::MoneyArray => Some(PgType::Money),
            PgType::BitArray => Some(PgType::Bit),
            PgType::VarbitArray => Some(PgType::Varbit),
            _ => None,
        }
    }

    pub fn constant_values(pg_type: &PgType) -> Vec<String> {
        // Get element type's constant values
        let element_values: Vec<String> = if let Some(elem_type) = Self::element_type(pg_type) {
            PgType::constant_value_str(&elem_type)
        } else {
            panic!("unsupported array type: {:?}", pg_type)
        };

        if element_values.is_empty() {
            return vec!["ARRAY[]".to_string()];
        }

        // Generate different array patterns
        vec![
            "ARRAY[]".to_string(),
            // Single element (first value)
            format!(
                "ARRAY[{}]",
                element_values.first().unwrap_or(&"NULL".to_string())
            ),
            // Multiple elements (up to 3)
            format!(
                "ARRAY[{}]",
                element_values
                    .iter()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            // Array with NULL
            format!(
                "ARRAY[{}, NULL]",
                element_values.first().unwrap_or(&"NULL".to_string())
            ),
            // All constant values
            format!("ARRAY[{}]", element_values.join(", ")),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_array() {
        let mut rand = Random::new(Some(42));
        for _ in 0..3 {
            println!(
                "BoolArray: {}",
                Array::next_value_str(&PgType::BoolArray, &mut rand)
            );
        }
    }

    #[test]
    fn test_int_arrays() {
        let mut rand = Random::new(Some(42));
        println!(
            "Int2Array: {}",
            Array::next_value_str(&PgType::Int2Array, &mut rand)
        );
        println!(
            "Int4Array: {}",
            Array::next_value_str(&PgType::Int4Array, &mut rand)
        );
        println!(
            "Int8Array: {}",
            Array::next_value_str(&PgType::Int8Array, &mut rand)
        );
    }

    #[test]
    fn test_text_array() {
        let mut rand = Random::new(Some(42));
        for _ in 0..3 {
            println!(
                "TextArray: {}",
                Array::next_value_str(&PgType::TextArray, &mut rand)
            );
        }
    }

    #[test]
    fn test_geo_arrays() {
        let mut rand = Random::new(Some(42));
        println!(
            "PointArray: {}",
            Array::next_value_str(&PgType::PointArray, &mut rand)
        );
        println!(
            "CircleArray: {}",
            Array::next_value_str(&PgType::CircleArray, &mut rand)
        );
    }

    #[test]
    fn test_constant_values() {
        println!(
            "BoolArray constants: {:?}",
            Array::constant_values(&PgType::BoolArray)
        );
        println!(
            "Int4Array constants: {:?}",
            Array::constant_values(&PgType::Int4Array)
        );
        println!(
            "TextArray constants: {:?}",
            Array::constant_values(&PgType::TextArray)
        );
    }
}
