use serde::{Deserialize, Serialize};

use crate::test_runner::mock_utils::{constants::Constants, random::Random};

#[derive(Hash, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PgType {
    Bool,
    Bytea,
    Char,
    Name,
    Int8,
    Int2,
    Int4,
    Text,
    Oid,
    Json,
    JsonArray,
    Point,
    Lseg,
    Path,
    Box,
    Polygon,
    Line,
    LineArray,
    Cidr,
    CidrArray,
    Float4,
    Float8,
    Unknown,
    Circle,
    CircleArray,
    Macaddr8,
    Macaddr8Array,
    Macaddr,
    Inet,
    BoolArray,
    ByteaArray,
    CharArray,
    NameArray,
    Int2Array,
    Int4Array,
    TextArray,
    BpcharArray,
    VarcharArray,
    Int8Array,
    PointArray,
    LsegArray,
    PathArray,
    BoxArray,
    Float4Array,
    Float8Array,
    PolygonArray,
    OidArray,
    MacaddrArray,
    InetArray,
    Bpchar,
    Varchar,
    Date,
    Time,
    Timestamp,
    TimestampArray,
    DateArray,
    TimeArray,
    Timestamptz,
    TimestamptzArray,
    Interval,
    IntervalArray,
    NumericArray,
    Timetz,
    TimetzArray,
    Bit,
    BitArray,
    Varbit,
    VarbitArray,
    Numeric,
    Record,
    RecordArray,
    Uuid,
    UuidArray,
    Jsonb,
    JsonbArray,
    Int4Range,
    Int4RangeArray,
    NumRange,
    NumRangeArray,
    TsRange,
    TsRangeArray,
    TstzRange,
    TstzRangeArray,
    DateRange,
    DateRangeArray,
    Int8Range,
    Int8RangeArray,
    Jsonpath,
    JsonpathArray,
    Money,
    MoneyArray,
    // https://www.postgresql.org/docs/9.3/datatype-pseudo.html
    Void,
    // for test, not an offical data type of pg.
    Test {
        oid: u32,
        default: String,
        collation: String,
    },
}

impl PgType {
    pub fn display_name(&self) -> &str {
        match self {
            PgType::Bool => "BOOL",
            PgType::Bytea => "BYTEA",
            PgType::Char => "\"CHAR\"",
            PgType::Name => "NAME",
            PgType::Int8 => "INT8",
            PgType::Int2 => "INT2",
            PgType::Int4 => "INT4",
            PgType::Text => "TEXT",
            PgType::Oid => "OID",
            PgType::Json => "JSON",
            PgType::JsonArray => "JSON[]",
            PgType::Point => "POINT",
            PgType::Lseg => "LSEG",
            PgType::Path => "PATH",
            PgType::Box => "BOX",
            PgType::Polygon => "POLYGON",
            PgType::Line => "LINE",
            PgType::LineArray => "LINE[]",
            PgType::Cidr => "CIDR",
            PgType::CidrArray => "CIDR[]",
            PgType::Float4 => "FLOAT4",
            PgType::Float8 => "FLOAT8",
            PgType::Unknown => "UNKNOWN",
            PgType::Circle => "CIRCLE",
            PgType::CircleArray => "CIRCLE[]",
            PgType::Macaddr8 => "MACADDR8",
            PgType::Macaddr8Array => "MACADDR8[]",
            PgType::Macaddr => "MACADDR",
            PgType::Inet => "INET",
            PgType::BoolArray => "BOOL[]",
            PgType::ByteaArray => "BYTEA[]",
            PgType::CharArray => "\"CHAR\"[]",
            PgType::NameArray => "NAME[]",
            PgType::Int2Array => "INT2[]",
            PgType::Int4Array => "INT4[]",
            PgType::TextArray => "TEXT[]",
            PgType::BpcharArray => "CHAR[]",
            PgType::VarcharArray => "VARCHAR[]",
            PgType::Int8Array => "INT8[]",
            PgType::PointArray => "POINT[]",
            PgType::LsegArray => "LSEG[]",
            PgType::PathArray => "PATH[]",
            PgType::BoxArray => "BOX[]",
            PgType::Float4Array => "FLOAT4[]",
            PgType::Float8Array => "FLOAT8[]",
            PgType::PolygonArray => "POLYGON[]",
            PgType::OidArray => "OID[]",
            PgType::MacaddrArray => "MACADDR[]",
            PgType::InetArray => "INET[]",
            PgType::Bpchar => "CHAR",
            PgType::Varchar => "VARCHAR",
            PgType::Date => "DATE",
            PgType::Time => "TIME",
            PgType::Timestamp => "TIMESTAMP",
            PgType::TimestampArray => "TIMESTAMP[]",
            PgType::DateArray => "DATE[]",
            PgType::TimeArray => "TIME[]",
            PgType::Timestamptz => "TIMESTAMPTZ",
            PgType::TimestamptzArray => "TIMESTAMPTZ[]",
            PgType::Interval => "INTERVAL",
            PgType::IntervalArray => "INTERVAL[]",
            PgType::NumericArray => "NUMERIC[]",
            PgType::Timetz => "TIMETZ",
            PgType::TimetzArray => "TIMETZ[]",
            PgType::Bit => "BIT",
            PgType::BitArray => "BIT[]",
            PgType::Varbit => "VARBIT",
            PgType::VarbitArray => "VARBIT[]",
            PgType::Numeric => "NUMERIC",
            PgType::Record => "RECORD",
            PgType::RecordArray => "RECORD[]",
            PgType::Uuid => "UUID",
            PgType::UuidArray => "UUID[]",
            PgType::Jsonb => "JSONB",
            PgType::JsonbArray => "JSONB[]",
            PgType::Int4Range => "INT4RANGE",
            PgType::Int4RangeArray => "INT4RANGE[]",
            PgType::NumRange => "NUMRANGE",
            PgType::NumRangeArray => "NUMRANGE[]",
            PgType::TsRange => "TSRANGE",
            PgType::TsRangeArray => "TSRANGE[]",
            PgType::TstzRange => "TSTZRANGE",
            PgType::TstzRangeArray => "TSTZRANGE[]",
            PgType::DateRange => "DATERANGE",
            PgType::DateRangeArray => "DATERANGE[]",
            PgType::Int8Range => "INT8RANGE",
            PgType::Int8RangeArray => "INT8RANGE[]",
            PgType::Jsonpath => "JSONPATH",
            PgType::JsonpathArray => "JSONPATH[]",
            PgType::Money => "MONEY",
            PgType::MoneyArray => "MONEY[]",
            PgType::Void => "VOID",
            _ => panic!("unsupported pg type for display name: {:?}", self),
        }
    }

    pub fn next_value_str(&self, random: &mut Random) -> String {
        match self {
            PgType::Bool => {
                if random.next_u8() % 2 == 0 {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            PgType::Int8 => {
                let val = random.next_i64();
                format!("{}", val)
            }
            PgType::Int2 => {
                let val = random.next_i16();
                format!("{}", val)
            }
            PgType::Int4 => {
                let val = random.next_i32();
                format!("{}", val)
            }
            PgType::Float4 => {
                let val = random.next_f32();
                format!("{}", val)
            }
            PgType::Float8 => {
                let val = random.next_f64();
                format!("{}", val)
            }
            _ => panic!("unsupported pg type for mock value generation: {:?}", self),
        }
    }

    pub fn constant_values(&self) -> Vec<String> {
        match self {
            PgType::Int8 => Constants::next_i8()
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>(),
            PgType::Int2 => Constants::next_i16()
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>(),
            PgType::Int4 => Constants::next_i32()
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>(),
            PgType::Float4 => Constants::next_f32()
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>(),
            PgType::Float8 => Constants::next_f64()
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>(),
            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_pg_type_vec_serialization() {
        let supported_pg_types = vec![
            PgType::Bool,
            PgType::Int8,
            PgType::Int2,
            PgType::Int4,
            PgType::Float4,
            PgType::Float8,
            PgType::Test {
                oid: 32,
                default: "NULL".to_string(),
                collation: "utf8".to_string(),
            },
        ];
        let serialized = serde_json::to_string(&supported_pg_types).unwrap();
        assert_eq!(
            serialized,
            r#"["bool","int8","int2","int4","float4","float8",{"test":{"oid":32,"default":"NULL","collation":"utf8"}}]"#
        );
        let deserialized: Vec<PgType> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(supported_pg_types, deserialized);
    }

    #[test]
    fn test_gen_default_pg_types() {
        let default_pg_types = vec![
            PgType::Int8,
            PgType::Int2,
            PgType::Int4,
            PgType::Float4,
            PgType::Float8,
            PgType::Bool,
        ];
        let serialized = serde_json::to_string(&default_pg_types).unwrap();
        assert_eq!(
            serialized,
            r#"["int8","int2","int4","float4","float8","bool"]"#
        );
    }
}
