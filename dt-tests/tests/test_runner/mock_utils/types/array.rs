use rust_decimal::Decimal;
use time::{Date, PrimitiveDateTime, Time};
use uuid::Uuid;

use crate::test_runner::mock_utils::{
    pg_type::PgType,
    random::Random,
    types::{
        bytea::Bytea,
        geo::{Box, Circle, Line, LineSegment, Path, Point, Polygon},
        json::Json,
        money::Money,
        net::{Cidr, Inet, MacAddr, MacAddr8},
        time::Interval,
        type_util::TypeUtil,
    },
};

use super::super::random::RandomValue;

macro_rules! single_quote {
    ($s:expr) => {
        format!("'{}'", $s)
    };
}

macro_rules! dollar_quote {
    ($s:expr) => {
        format!("$${}$$", $s)
    };
}

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
        match pg_type {
            // Boolean
            PgType::BoolArray => Self::gen_array(rand, |r| {
                if r.next_u8() % 2 == 0 {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }),

            // Integer types
            PgType::Int2Array => Self::gen_array(rand, |r| r.next_i16().to_string()),
            PgType::Int4Array => Self::gen_array(rand, |r| r.next_i32().to_string()),
            PgType::Int8Array => Self::gen_array(rand, |r| r.next_i64().to_string()),
            PgType::OidArray => Self::gen_array(rand, |r| r.next_u32().to_string()),

            // Floating point types
            PgType::Float4Array => Self::gen_array(rand, |r| r.next_f32().to_string()),
            PgType::Float8Array => Self::gen_array(rand, |r| r.next_f64().to_string()),
            PgType::NumericArray => Self::gen_array(rand, |_| TypeUtil::fake_str::<Decimal>()),

            // String types
            PgType::TextArray | PgType::VarcharArray | PgType::BpcharArray => {
                Self::gen_array(rand, |r| dollar_quote!(r.next_str()))
            }
            PgType::CharArray => Self::gen_array(rand, |r| {
                single_quote!(r.next_str().chars().next().unwrap_or('a'))
            }),
            PgType::NameArray => Self::gen_array(rand, |r| {
                // name is max 63 chars
                let s = r.next_str();
                let truncated: String = s.chars().take(63).collect();
                dollar_quote!(truncated)
            }),

            // Binary
            PgType::ByteaArray => {
                Self::gen_array(rand, |r| format!("'\\x{}'", Bytea::next_value(r)))
            }

            // JSON types
            PgType::JsonArray | PgType::JsonbArray => {
                Self::gen_array(rand, |r| dollar_quote!(Json::next_value(r)))
            }

            // UUID
            PgType::UuidArray => {
                Self::gen_array(rand, |_| single_quote!(TypeUtil::fake_str::<Uuid>()))
            }

            // Date/Time types
            PgType::DateArray => {
                Self::gen_array(rand, |_| single_quote!(TypeUtil::fake_str::<Date>()))
            }
            PgType::TimeArray | PgType::TimetzArray => {
                Self::gen_array(rand, |_| single_quote!(TypeUtil::fake_str::<Time>()))
            }
            PgType::TimestampArray | PgType::TimestamptzArray => Self::gen_array(rand, |_| {
                single_quote!(TypeUtil::fake_str::<PrimitiveDateTime>())
            }),
            PgType::IntervalArray => {
                Self::gen_array(rand, |r| single_quote!(Interval::next_value(r)))
            }

            // Geometric types
            PgType::PointArray => Self::gen_array(rand, |r| single_quote!(Point::next_value(r))),
            PgType::LineArray => Self::gen_array(rand, |r| single_quote!(Line::next_value(r))),
            PgType::LsegArray => {
                Self::gen_array(rand, |r| single_quote!(LineSegment::next_value(r)))
            }
            PgType::BoxArray => Self::gen_array(rand, |r| single_quote!(Box::next_value(r))),
            PgType::PathArray => Self::gen_array(rand, |r| single_quote!(Path::next_value(r))),
            PgType::PolygonArray => {
                Self::gen_array(rand, |r| single_quote!(Polygon::next_value(r)))
            }
            PgType::CircleArray => Self::gen_array(rand, |r| single_quote!(Circle::next_value(r))),

            // Network types
            PgType::InetArray => Self::gen_array(rand, |r| single_quote!(Inet::next_value(r))),
            PgType::CidrArray => Self::gen_array(rand, |r| single_quote!(Cidr::next_value(r))),
            PgType::MacaddrArray => {
                Self::gen_array(rand, |r| single_quote!(MacAddr::next_value(r)))
            }
            PgType::Macaddr8Array => {
                Self::gen_array(rand, |r| single_quote!(MacAddr8::next_value(r)))
            }

            // Money
            PgType::MoneyArray => Self::gen_array(rand, |r| dollar_quote!(Money::next_value(r))),

            // Bit string types
            PgType::BitArray => Self::gen_array(rand, |r| {
                let len = (r.next_u8() % 8 + 1) as usize;
                let bits: String = (0..len)
                    .map(|_| if r.next_u8() % 2 == 0 { '0' } else { '1' })
                    .collect();
                format!("B'{}'", bits)
            }),
            PgType::VarbitArray => Self::gen_array(rand, |r| {
                let len = (r.next_u8() % 16 + 1) as usize;
                let bits: String = (0..len)
                    .map(|_| if r.next_u8() % 2 == 0 { '0' } else { '1' })
                    .collect();
                format!("B'{}'", bits)
            }),

            _ => panic!("unsupported array type: {:?}", pg_type),
        }
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
            // Empty array
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
