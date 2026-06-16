use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::test_runner::mock_data::{
    constants::{ConstantValues, Constants},
    context::MockDbContext,
    mock_stmt::MockColType,
    random::{Random, RandomValue},
    types::{
        bytea::Bytea,
        json::Json,
        money::Money,
        net::{Cidr, Inet, MacAddr, MacAddr8},
        pg::{
            array::Array,
            geo::{Box, Circle, Line, LineSegment, Path, Point, Polygon},
        },
        time::{Interval, PgDate, PgDateTime, PgTime},
        type_util::TypeUtil,
    },
};

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
}

impl PgType {
    pub fn name(&self) -> &str {
        match self {
            PgType::Bool => "bool",
            PgType::Bytea => "bytea",
            PgType::Char => "\"char\"",
            PgType::Name => "name",
            PgType::Int8 => "int8",
            PgType::Int2 => "int2",
            PgType::Int4 => "int4",
            PgType::Text => "text",
            PgType::Oid => "oid",
            PgType::Json => "json",
            PgType::JsonArray => "_json",
            PgType::Point => "point",
            PgType::Lseg => "lseg",
            PgType::Path => "path",
            PgType::Box => "box",
            PgType::Polygon => "polygon",
            PgType::Line => "line",
            PgType::LineArray => "_line",
            PgType::Cidr => "cidr",
            PgType::CidrArray => "_cidr",
            PgType::Float4 => "float4",
            PgType::Float8 => "float8",
            PgType::Unknown => "unknown",
            PgType::Circle => "circle",
            PgType::CircleArray => "_circle",
            PgType::Macaddr8 => "macaddr8",
            PgType::Macaddr8Array => "_macaddr8",
            PgType::Macaddr => "macaddr",
            PgType::Inet => "inet",
            PgType::BoolArray => "_bool",
            PgType::ByteaArray => "_bytea",
            PgType::CharArray => "_char",
            PgType::NameArray => "_name",
            PgType::Int2Array => "_int2",
            PgType::Int4Array => "_int4",
            PgType::TextArray => "_text",
            PgType::BpcharArray => "_bpchar",
            PgType::VarcharArray => "_varchar",
            PgType::Int8Array => "_int8",
            PgType::PointArray => "_point",
            PgType::LsegArray => "_lseg",
            PgType::PathArray => "_path",
            PgType::BoxArray => "_box",
            PgType::Float4Array => "_float4",
            PgType::Float8Array => "_float8",
            PgType::PolygonArray => "_polygon",
            PgType::OidArray => "_oid",
            PgType::MacaddrArray => "_macaddr",
            PgType::InetArray => "_inet",
            PgType::Bpchar => "bpchar",
            PgType::Varchar => "varchar",
            PgType::Date => "date",
            PgType::Time => "time",
            PgType::Timestamp => "timestamp",
            PgType::TimestampArray => "_timestamp",
            PgType::DateArray => "_date",
            PgType::TimeArray => "_time",
            PgType::Timestamptz => "timestamptz",
            PgType::TimestamptzArray => "_timestamptz",
            PgType::Interval => "interval",
            PgType::IntervalArray => "_interval",
            PgType::NumericArray => "_numeric",
            PgType::Timetz => "timetz",
            PgType::TimetzArray => "_timetz",
            PgType::Bit => "bit",
            PgType::BitArray => "_bit",
            PgType::Varbit => "varbit",
            PgType::VarbitArray => "_varbit",
            PgType::Numeric => "numeric",
            PgType::Record => "record",
            PgType::RecordArray => "_record",
            PgType::Uuid => "uuid",
            PgType::UuidArray => "_uuid",
            PgType::Jsonb => "jsonb",
            PgType::JsonbArray => "_jsonb",
            PgType::Int4Range => "int4range",
            PgType::Int4RangeArray => "_int4range",
            PgType::NumRange => "numrange",
            PgType::NumRangeArray => "_numrange",
            PgType::TsRange => "tsrange",
            PgType::TsRangeArray => "_tsrange",
            PgType::TstzRange => "tstzrange",
            PgType::TstzRangeArray => "_tstzrange",
            PgType::DateRange => "daterange",
            PgType::DateRangeArray => "_daterange",
            PgType::Int8Range => "int8range",
            PgType::Int8RangeArray => "_int8range",
            PgType::Jsonpath => "jsonpath",
            PgType::JsonpathArray => "_jsonpath",
            PgType::Money => "money",
            PgType::MoneyArray => "_money",
            PgType::Void => "void",
        }
    }

    pub fn support_btree_index(&self) -> bool {
        matches!(
            self,
            PgType::Bytea
                // | PgType::Bool // too small cardinality for test
                | PgType::Char
                | PgType::Name
                | PgType::Int2
                | PgType::Int4
                | PgType::Int8
                | PgType::Text
                | PgType::Bpchar
                | PgType::Varchar
                | PgType::Float4
                | PgType::Float8
                | PgType::Numeric
                | PgType::Money
                | PgType::Oid
                // | PgType::Bit // default Bit(1) is too small cardinality for test
                | PgType::Varbit
                | PgType::Uuid
                | PgType::Date
                | PgType::Time
                | PgType::Timestamp
                | PgType::Timestamptz
                | PgType::Interval
                | PgType::Timetz
                | PgType::Inet
                | PgType::Cidr
                | PgType::Macaddr
                | PgType::Macaddr8
                | PgType::Int4Range
                | PgType::Int8Range
                | PgType::NumRange
                | PgType::TsRange
                | PgType::TstzRange
                | PgType::DateRange
                // | PgType::BoolArray
                | PgType::ByteaArray
                | PgType::CharArray
                | PgType::NameArray
                | PgType::Int2Array
                | PgType::Int4Array
                | PgType::Int8Array
                | PgType::TextArray
                | PgType::BpcharArray
                | PgType::VarcharArray
                | PgType::Float4Array
                | PgType::Float8Array
                | PgType::NumericArray
                | PgType::MoneyArray
                | PgType::OidArray
                // | PgType::BitArray
                | PgType::VarbitArray
                | PgType::UuidArray
                | PgType::InetArray
                | PgType::CidrArray
                | PgType::MacaddrArray
                | PgType::Macaddr8Array
                | PgType::TimestampArray
                | PgType::DateArray
                | PgType::TimeArray
                | PgType::TimestamptzArray
                | PgType::IntervalArray
                | PgType::TimetzArray
                | PgType::Int4RangeArray
                | PgType::Int8RangeArray
                | PgType::NumRangeArray
                | PgType::TsRangeArray
                | PgType::TstzRangeArray
                | PgType::DateRangeArray
        )
    }
    pub fn next_value_str(&self, random: &mut Random) -> String {
        if let Some(_elem_pg_type) = Array::element_type(self) {
            let mut res = Array::next_value_str(self, random);
            res.push_str(format!("::{}", self.name()).as_str());
            return res;
        };
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
            PgType::Oid => {
                format!("{}", random.next_u32())
            }
            PgType::Bpchar => {
                // bpchar will trim trailing spaces
                dollar_quote!(random.next_str().trim_end_matches(' '))
            }
            PgType::Text | PgType::Varchar | PgType::Name => {
                dollar_quote!(random.next_str())
            }
            PgType::Char => {
                dollar_quote!(random.next_str().chars().next().unwrap_or('a'))
            }
            PgType::Bytea => {
                format!("'\\x{}'", Bytea::next_value(random))
            }
            PgType::Json | PgType::Jsonb => {
                dollar_quote!(Json::next_value(random))
            }
            PgType::Uuid => {
                single_quote!(TypeUtil::fake_str::<Uuid>(random))
            }
            PgType::Numeric => TypeUtil::fake_str::<Decimal>(random),
            PgType::Date => single_quote!(PgDate::next_value(random)),
            PgType::Time | PgType::Timetz => single_quote!(PgTime::next_value(random)),
            PgType::Timestamp | PgType::Timestamptz => {
                single_quote!(PgDateTime::next_value(random))
            }
            PgType::Interval => single_quote!(Interval::next_value(random)),
            PgType::Point => {
                single_quote!(Point::next_value(random))
            }
            PgType::Line => {
                single_quote!(Line::next_value(random))
            }
            PgType::Lseg => {
                single_quote!(LineSegment::next_value(random))
            }
            PgType::Box => {
                single_quote!(Box::next_value(random))
            }
            PgType::Path => {
                single_quote!(Path::next_value(random))
            }
            PgType::Polygon => {
                single_quote!(Polygon::next_value(random))
            }
            PgType::Circle => {
                single_quote!(Circle::next_value(random))
            }
            PgType::Inet => {
                single_quote!(Inet::next_value(random))
            }
            PgType::Cidr => {
                single_quote!(Cidr::next_value(random))
            }
            PgType::Macaddr => {
                single_quote!(MacAddr::next_value(random))
            }
            PgType::Macaddr8 => {
                single_quote!(MacAddr8::next_value(random))
            }
            PgType::Money => Money::next_value(random),
            PgType::Bit => {
                let bit = if random.next_u8() % 2 == 0 { '1' } else { '0' };
                format!("B'{}'", bit)
            }
            PgType::Varbit => {
                let len = (random.next_u8() % 10 + 1) as usize;
                let bits: String = (0..len)
                    .map(|_| if random.next_u8() % 2 == 0 { '1' } else { '0' })
                    .collect();
                format!("B'{}'", bits)
            }
            _ => panic!("unsupported pg type for mock value generation: {:?}", self),
        }
    }

    pub fn constant_value_str(&self, ctx: &MockDbContext) -> Vec<String> {
        if let Some(_elem_pg_type) = Array::element_type(self) {
            return Array::constant_values(self, ctx)
                .iter()
                .map(|s| format!("{}::{}", s, self.name()))
                .collect();
        };
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
                .map(|v| single_quote!(v.to_string()))
                .collect::<Vec<String>>(),
            PgType::Float8 => Constants::next_f64()
                .iter()
                .map(|v| single_quote!(v.to_string())) // quote for nan and inf
                .collect::<Vec<String>>(),
            PgType::Numeric => Constants::next_f64()
                .iter()
                .map(|v| single_quote!(v.to_string())) // quote for nan and inf
                .filter(|v| ctx.version.major >= 14 || (v != "'inf'" && v != "'-inf'"))
                .collect::<Vec<String>>(),
            PgType::Bpchar | PgType::Text | PgType::Varchar | PgType::Name => Constants::next_str()
                .iter()
                .map(|s| dollar_quote!(s))
                .collect(),
            PgType::Bytea => Bytea::next_values()
                .into_iter()
                .map(|s| format!("'\\x{}'", s))
                .collect(),
            PgType::Json | PgType::Jsonb => Json::next_values()
                .into_iter()
                .map(|s| dollar_quote!(s))
                .collect(),
            PgType::Date => PgDate::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Time | PgType::Timetz => PgTime::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Timestamp | PgType::Timestamptz => PgDateTime::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Interval => Interval::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Point => Point::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Line => Line::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Lseg => LineSegment::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Box => Box::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Path => Path::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Polygon => Polygon::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Circle => Circle::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Inet => Inet::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Cidr => Cidr::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Macaddr => MacAddr::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Macaddr8 => MacAddr8::next_values()
                .into_iter()
                .map(|s| single_quote!(s))
                .collect(),
            PgType::Money => Money::next_values(),
            _ => vec![],
        }
    }
}

impl MockColType for PgType {
    fn name(&self, _ctx: &MockDbContext) -> String {
        PgType::name(self).to_string()
    }

    fn support_btree_index(&self, _ctx: &MockDbContext) -> bool {
        PgType::support_btree_index(self)
    }

    fn next_value_str(&self, _ctx: &MockDbContext, random: &mut Random) -> String {
        PgType::next_value_str(self, random)
    }

    fn constant_value_str(&self, ctx: &MockDbContext) -> Vec<String> {
        PgType::constant_value_str(self, ctx)
    }

    fn schema_drop_stmt(db: &str, _ctx: &MockDbContext) -> String {
        format!("DROP SCHEMA IF EXISTS {} CASCADE;", db)
    }

    fn schema_create_stmt(db: &str, _ctx: &MockDbContext) -> String {
        format!("CREATE SCHEMA IF NOT EXISTS {};", db)
    }

    fn quote_identifier(name: &str, _ctx: &MockDbContext) -> String {
        name.to_string()
    }

    fn after_all_insert_stmts(_db_tbs: &[(String, String)], _ctx: &MockDbContext) -> Vec<String> {
        vec!["ANALYZE;".to_string()]
    }

    fn config_key_prefix() -> &'static str {
        "pg_types"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dt_common::config::config_enums::DbType;

    #[test]
    fn test_pg_type_vec_serialization() {
        let supported_pg_types = vec![
            PgType::Bool,
            PgType::Int8,
            PgType::Int2,
            PgType::Int4,
            PgType::Float4,
            PgType::Float8,
            PgType::VarcharArray,
        ];
        let serialized = serde_json::to_string(&supported_pg_types).unwrap();
        assert_eq!(
            serialized,
            r#"["bool","int8","int2","int4","float4","float8","varchararray"]"#
        );
        let deserialized: Vec<PgType> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(supported_pg_types, deserialized);
    }

    #[test]
    fn test_numeric_constant_values_filter_infinity_before_pg_14() {
        let pg_13_ctx = MockDbContext::new(DbType::Pg, "13.12");
        let values = PgType::constant_value_str(&PgType::Numeric, &pg_13_ctx);
        assert!(!values.contains(&"'inf'".to_string()));
        assert!(!values.contains(&"'-inf'".to_string()));

        let array_values = PgType::constant_value_str(&PgType::NumericArray, &pg_13_ctx);
        assert!(!array_values.iter().any(|v| v.contains("'inf'")));
        assert!(!array_values.iter().any(|v| v.contains("'-inf'")));

        let pg_14_ctx = MockDbContext::new(DbType::Pg, "14.0");
        let values = PgType::constant_value_str(&PgType::Numeric, &pg_14_ctx);
        assert!(values.contains(&"'inf'".to_string()));
        assert!(values.contains(&"'-inf'".to_string()));
    }
}
