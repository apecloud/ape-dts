use std::collections::HashMap;

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
            custom_types::PgCustomType,
            geo::{Box, Circle, Line, LineSegment, Path, Point, Polygon},
        },
        time::{Interval, PgDate, PgDateTime, PgTime},
        type_util::TypeUtil,
    },
};

const PG_BIT_LEN: usize = 10;
const PG_VARBIT_MAX_LEN: usize = 32;

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
    Xml,
    XmlArray,
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
    TsVector,
    TsVectorArray,
    TsQuery,
    TsQueryArray,
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
    Int4Multirange,
    Int4MultirangeArray,
    NumMultirange,
    NumMultirangeArray,
    TsMultirange,
    TsMultirangeArray,
    TstzMultirange,
    TstzMultirangeArray,
    DateMultirange,
    DateMultirangeArray,
    Int8Multirange,
    Int8MultirangeArray,
    Jsonpath,
    JsonpathArray,
    Money,
    MoneyArray,
    Custom(PgCustomType),
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
            PgType::Xml => "xml",
            PgType::XmlArray => "_xml",
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
            PgType::Bit => "bit(10)",
            PgType::BitArray => "bit(10)[]",
            PgType::Varbit => "varbit(32)",
            PgType::VarbitArray => "varbit(32)[]",
            PgType::Numeric => "numeric",
            PgType::Record => "record",
            PgType::RecordArray => "_record",
            PgType::Uuid => "uuid",
            PgType::UuidArray => "_uuid",
            PgType::Jsonb => "jsonb",
            PgType::JsonbArray => "_jsonb",
            PgType::TsVector => "tsvector",
            PgType::TsVectorArray => "_tsvector",
            PgType::TsQuery => "tsquery",
            PgType::TsQueryArray => "_tsquery",
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
            PgType::Int4Multirange => "int4multirange",
            PgType::Int4MultirangeArray => "_int4multirange",
            PgType::NumMultirange => "nummultirange",
            PgType::NumMultirangeArray => "_nummultirange",
            PgType::TsMultirange => "tsmultirange",
            PgType::TsMultirangeArray => "_tsmultirange",
            PgType::TstzMultirange => "tstzmultirange",
            PgType::TstzMultirangeArray => "_tstzmultirange",
            PgType::DateMultirange => "datemultirange",
            PgType::DateMultirangeArray => "_datemultirange",
            PgType::Int8Multirange => "int8multirange",
            PgType::Int8MultirangeArray => "_int8multirange",
            PgType::Jsonpath => "jsonpath",
            PgType::JsonpathArray => "_jsonpath",
            PgType::Money => "money",
            PgType::MoneyArray => "_money",
            PgType::Custom(_) => panic!("custom pg type name requires db context"),
            PgType::Void => "void",
        }
    }

    pub fn type_name(&self, db: &str, ctx: &MockDbContext) -> String {
        match self {
            PgType::Custom(custom) => custom.type_name(db, ctx),
            _ => self.name().to_string(),
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
                | PgType::Bit
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
                | PgType::Int4Multirange
                | PgType::Int8Multirange
                | PgType::NumMultirange
                | PgType::TsMultirange
                | PgType::TstzMultirange
                | PgType::DateMultirange
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
                | PgType::BitArray
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
                | PgType::Int4MultirangeArray
                | PgType::Int8MultirangeArray
                | PgType::NumMultirangeArray
                | PgType::TsMultirangeArray
                | PgType::TstzMultirangeArray
                | PgType::DateMultirangeArray
        )
    }
    pub fn next_value_str(&self, db: &str, ctx: &MockDbContext, random: &mut Random) -> String {
        if let PgType::Custom(custom) = self {
            return custom.next_value_str(db, ctx, random);
        }
        if let Some(_elem_pg_type) = Array::element_type(self) {
            let mut res = Array::next_value_str(self, db, ctx, random);
            res.push_str(format!("::{}", self.type_name(db, ctx)).as_str());
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
                format!("B'{}'", Self::next_bits(random, PG_BIT_LEN))
            }
            PgType::Varbit => {
                let len = random.random_range(1..(PG_VARBIT_MAX_LEN + 1) as i32) as usize;
                format!("B'{}'", Self::next_bits(random, len))
            }
            PgType::Xml => Self::pick_value(Self::xml_values(), random),
            PgType::Jsonpath => Self::pick_value(Self::jsonpath_values(), random),
            PgType::TsVector => Self::pick_value(Self::tsvector_values(), random),
            PgType::TsQuery => Self::pick_value(Self::tsquery_values(), random),
            PgType::Int4Range => Self::next_int4_range_value(random),
            PgType::Int8Range => Self::next_int8_range_value(random),
            PgType::NumRange => Self::next_num_range_value(random),
            PgType::TsRange => Self::next_ts_range_value(random),
            PgType::TstzRange => Self::next_tstz_range_value(random),
            PgType::DateRange => Self::next_date_range_value(random),
            PgType::Int4Multirange => Self::next_int4_multirange_value(random),
            PgType::Int8Multirange => Self::next_int8_multirange_value(random),
            PgType::NumMultirange => Self::next_num_multirange_value(random),
            PgType::TsMultirange => Self::next_ts_multirange_value(random),
            PgType::TstzMultirange => Self::next_tstz_multirange_value(random),
            PgType::DateMultirange => Self::next_date_multirange_value(random),
            _ => panic!("unsupported pg type for mock value generation: {:?}", self),
        }
    }

    pub fn constant_value_str(&self, db: &str, ctx: &MockDbContext) -> Vec<String> {
        if let PgType::Custom(custom) = self {
            return custom.constant_value_str(db, ctx);
        }
        if let Some(_elem_pg_type) = Array::element_type(self) {
            return Array::constant_values(self, db, ctx)
                .iter()
                .map(|s| format!("{}::{}", s, self.type_name(db, ctx)))
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
            PgType::Bit => Self::bit_values(),
            PgType::Varbit => Self::varbit_values(),
            PgType::Xml => Self::xml_values(),
            PgType::Jsonpath => Self::jsonpath_values(),
            PgType::TsVector => Self::tsvector_values(),
            PgType::TsQuery => Self::tsquery_values(),
            PgType::Int4Range => Self::int4_range_values(),
            PgType::Int8Range => Self::int8_range_values(),
            PgType::NumRange => Self::num_range_values(),
            PgType::TsRange => Self::ts_range_values(),
            PgType::TstzRange => Self::tstz_range_values(),
            PgType::DateRange => Self::date_range_values(),
            PgType::Int4Multirange => Self::int4_multirange_values(),
            PgType::Int8Multirange => Self::int8_multirange_values(),
            PgType::NumMultirange => Self::num_multirange_values(),
            PgType::TsMultirange => Self::ts_multirange_values(),
            PgType::TstzMultirange => Self::tstz_multirange_values(),
            PgType::DateMultirange => Self::date_multirange_values(),
            _ => vec![],
        }
    }

    fn next_bits(random: &mut Random, len: usize) -> String {
        (0..len)
            .map(|_| if random.next_u8() % 2 == 0 { '1' } else { '0' })
            .collect()
    }

    fn pick_value(values: Vec<String>, random: &mut Random) -> String {
        values[random.random_range(0..values.len() as i32) as usize].clone()
    }

    fn bit_values() -> Vec<String> {
        [
            "B'0000000000'",
            "B'0000000001'",
            "B'0101010101'",
            "B'1111111111'",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn varbit_values() -> Vec<String> {
        [
            "B''",
            "B'0'",
            "B'1'",
            "B'0101010101'",
            "B'11111111111111111111111111111111'",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn xml_values() -> Vec<String> {
        [
            r#"<root/>"#,
            r#"<root><item id="1">alpha</item></root>"#,
            r#"<root><item><![CDATA[O'Neil & Sons]]></item></root>"#,
            r#"<ns:root xmlns:ns="urn:test"><ns:item>unicode text</ns:item></ns:root>"#,
        ]
        .iter()
        .map(|s| format!("XMLPARSE(DOCUMENT {})", dollar_quote!(s)))
        .collect()
    }

    fn jsonpath_values() -> Vec<String> {
        ["$", "$.a", "$.a[*]", "$.a ? (@ > 1)", "$.a.b"]
            .iter()
            .map(|s| single_quote!(s))
            .collect()
    }

    fn tsvector_values() -> Vec<String> {
        [
            "quick brown fox",
            "database migration test",
            "postgres full text search",
            "unicode token",
        ]
        .iter()
        .map(|s| format!("to_tsvector('simple', {})", dollar_quote!(s)))
        .collect()
    }

    fn tsquery_values() -> Vec<String> {
        [
            "quick fox",
            "database migration",
            "postgres search",
            "unicode",
        ]
        .iter()
        .map(|s| format!("plainto_tsquery('simple', {})", dollar_quote!(s)))
        .collect()
    }

    fn next_int4_range_value(random: &mut Random) -> String {
        let lower = random.random_range(-100_000..100_000);
        let upper = lower + random.random_range(1..1000);
        single_quote!(format!("[{},{})", lower, upper))
    }

    fn next_int8_range_value(random: &mut Random) -> String {
        let lower = random.next_i64() % 1_000_000_000;
        let upper = lower + random.random_range(1..1000) as i64;
        single_quote!(format!("[{},{})", lower, upper))
    }

    fn next_num_range_value(random: &mut Random) -> String {
        let lower = random.random_range(-100_000..100_000);
        let upper = lower + random.random_range(1..1000);
        single_quote!(format!("[{}.00,{}.00)", lower, upper))
    }

    fn next_ts_range_value(random: &mut Random) -> String {
        let offset = random.random_range(0..335);
        single_quote!(format!(
            "[{} 00:00:00,{} 00:00:00)",
            Self::date_from_offset(offset),
            Self::date_from_offset(offset + 1)
        ))
    }

    fn next_tstz_range_value(random: &mut Random) -> String {
        let offset = random.random_range(0..335);
        single_quote!(format!(
            "[{} 00:00:00+00,{} 00:00:00+00)",
            Self::date_from_offset(offset),
            Self::date_from_offset(offset + 1)
        ))
    }

    fn next_date_range_value(random: &mut Random) -> String {
        let offset = random.random_range(0..335);
        single_quote!(format!(
            "[{}, {})",
            Self::date_from_offset(offset),
            Self::date_from_offset(offset + 1)
        ))
    }

    fn date_from_offset(offset: i32) -> String {
        let month = offset / 28 + 1;
        let day = offset % 28 + 1;
        format!("2024-{month:02}-{day:02}")
    }

    fn int4_range_values() -> Vec<String> {
        [
            "empty",
            "[0,1)",
            "[-2147483648,2147483647)",
            "[1,10]",
            "(10,20]",
        ]
        .iter()
        .map(|s| single_quote!(s))
        .collect()
    }

    fn int8_range_values() -> Vec<String> {
        [
            "empty",
            "[0,1)",
            "[-9223372036854775808,9223372036854775807)",
            "[1000000,6000000)",
        ]
        .iter()
        .map(|s| single_quote!(s))
        .collect()
    }

    fn num_range_values() -> Vec<String> {
        [
            "empty",
            "[0,1)",
            "[-99999999.99,99999999.99)",
            "[1.23,45.67]",
        ]
        .iter()
        .map(|s| single_quote!(s))
        .collect()
    }

    fn ts_range_values() -> Vec<String> {
        [
            "empty",
            "[2024-01-01 00:00:00,2024-01-02 00:00:00)",
            "[2024-02-29 12:00:00,infinity)",
        ]
        .iter()
        .map(|s| single_quote!(s))
        .collect()
    }

    fn tstz_range_values() -> Vec<String> {
        [
            "empty",
            "[2024-01-01 00:00:00+00,2024-01-02 00:00:00+00)",
            "[2024-02-29 12:00:00+08,infinity)",
        ]
        .iter()
        .map(|s| single_quote!(s))
        .collect()
    }

    fn date_range_values() -> Vec<String> {
        ["empty", "[2024-01-01,2024-01-02)", "[2024-02-29,infinity)"]
            .iter()
            .map(|s| single_quote!(s))
            .collect()
    }

    fn next_int4_multirange_value(random: &mut Random) -> String {
        let lower = random.random_range(-100_000..100_000);
        let mid = lower + random.random_range(1..1000);
        let upper = mid + random.random_range(1..1000);
        format!(
            "int4multirange(int4range({}, {}, '[)'), int4range({}, {}, '[)'))",
            lower,
            mid,
            mid + 1,
            upper + 1
        )
    }

    fn next_int8_multirange_value(random: &mut Random) -> String {
        let lower = random.next_i64() % 1_000_000_000;
        let mid = lower + random.random_range(1..1000) as i64;
        let upper = mid + random.random_range(1..1000) as i64;
        format!(
            "int8multirange(int8range({}, {}, '[)'), int8range({}, {}, '[)'))",
            lower,
            mid,
            mid + 1,
            upper + 1
        )
    }

    fn next_num_multirange_value(random: &mut Random) -> String {
        let lower = random.random_range(-100_000..100_000);
        let mid = lower + random.random_range(1..1000);
        let upper = mid + random.random_range(1..1000);
        format!(
            "nummultirange(numrange({}.00, {}.00, '[)'), numrange({}.00, {}.00, '[)'))",
            lower,
            mid,
            mid + 1,
            upper + 1
        )
    }

    fn next_ts_multirange_value(random: &mut Random) -> String {
        let offset = random.random_range(0..333);
        format!(
            "tsmultirange(tsrange('{}'::timestamp, '{}'::timestamp, '[)'), tsrange('{}'::timestamp, '{}'::timestamp, '[)'))",
            Self::date_from_offset(offset),
            Self::date_from_offset(offset + 1),
            Self::date_from_offset(offset + 2),
            Self::date_from_offset(offset + 3)
        )
    }

    fn next_tstz_multirange_value(random: &mut Random) -> String {
        let offset = random.random_range(0..333);
        format!(
            "tstzmultirange(tstzrange('{} 00:00:00+00'::timestamptz, '{} 00:00:00+00'::timestamptz, '[)'), tstzrange('{} 00:00:00+00'::timestamptz, '{} 00:00:00+00'::timestamptz, '[)'))",
            Self::date_from_offset(offset),
            Self::date_from_offset(offset + 1),
            Self::date_from_offset(offset + 2),
            Self::date_from_offset(offset + 3)
        )
    }

    fn next_date_multirange_value(random: &mut Random) -> String {
        let offset = random.random_range(0..333);
        format!(
            "datemultirange(daterange('{}'::date, '{}'::date, '[)'), daterange('{}'::date, '{}'::date, '[)'))",
            Self::date_from_offset(offset),
            Self::date_from_offset(offset + 1),
            Self::date_from_offset(offset + 2),
            Self::date_from_offset(offset + 3)
        )
    }

    fn int4_multirange_values() -> Vec<String> {
        vec![
            "int4multirange()".to_string(),
            "int4multirange(int4range(0, 1, '[)'))".to_string(),
            "int4multirange(int4range(1, 5, '[)'), int4range(10, 20, '[)'))".to_string(),
        ]
    }

    fn int8_multirange_values() -> Vec<String> {
        vec![
            "int8multirange()".to_string(),
            "int8multirange(int8range(0, 1, '[)'))".to_string(),
            "int8multirange(int8range(1000000, 6000000, '[)'), int8range(7000000, 9000000, '[)'))"
                .to_string(),
        ]
    }

    fn num_multirange_values() -> Vec<String> {
        vec![
            "nummultirange()".to_string(),
            "nummultirange(numrange(0.00, 1.00, '[)'))".to_string(),
            "nummultirange(numrange(1.23, 4.56, '[)'), numrange(10.00, 20.00, '[)'))".to_string(),
        ]
    }

    fn ts_multirange_values() -> Vec<String> {
        vec![
            "tsmultirange()".to_string(),
            "tsmultirange(tsrange('2024-01-01 00:00:00'::timestamp, '2024-01-02 00:00:00'::timestamp, '[)'))"
                .to_string(),
            "tsmultirange(tsrange('2024-02-01 00:00:00'::timestamp, '2024-02-02 00:00:00'::timestamp, '[)'), tsrange('2024-03-01 00:00:00'::timestamp, '2024-03-02 00:00:00'::timestamp, '[)'))"
                .to_string(),
        ]
    }

    fn tstz_multirange_values() -> Vec<String> {
        vec![
            "tstzmultirange()".to_string(),
            "tstzmultirange(tstzrange('2024-01-01 00:00:00+00'::timestamptz, '2024-01-02 00:00:00+00'::timestamptz, '[)'))"
                .to_string(),
            "tstzmultirange(tstzrange('2024-02-01 00:00:00+00'::timestamptz, '2024-02-02 00:00:00+00'::timestamptz, '[)'), tstzrange('2024-03-01 00:00:00+00'::timestamptz, '2024-03-02 00:00:00+00'::timestamptz, '[)'))"
                .to_string(),
        ]
    }

    fn date_multirange_values() -> Vec<String> {
        vec![
            "datemultirange()".to_string(),
            "datemultirange(daterange('2024-01-01'::date, '2024-01-02'::date, '[)'))"
                .to_string(),
            "datemultirange(daterange('2024-02-01'::date, '2024-02-02'::date, '[)'), daterange('2024-03-01'::date, '2024-03-02'::date, '[)'))"
                .to_string(),
        ]
    }

    pub(crate) fn collect_custom_type_ddls(
        ty: &PgType,
        db: &str,
        ctx: &MockDbContext,
        ddl_by_name: &mut HashMap<String, String>,
        ordered_names: &mut Vec<String>,
    ) {
        if let PgType::Custom(custom) = ty {
            custom.collect_type_ddls(db, ctx, ddl_by_name, ordered_names);
        }
    }
}

impl MockColType for PgType {
    fn name(&self, _ctx: &MockDbContext) -> String {
        match self {
            PgType::Custom(custom) => custom.name().to_string(),
            _ => PgType::name(self).to_string(),
        }
    }

    fn type_name(&self, db: &str, ctx: &MockDbContext) -> String {
        PgType::type_name(self, db, ctx)
    }

    fn support_btree_index(&self, _ctx: &MockDbContext) -> bool {
        PgType::support_btree_index(self)
    }

    fn next_value_str(&self, db: &str, ctx: &MockDbContext, random: &mut Random) -> String {
        PgType::next_value_str(self, db, ctx, random)
    }

    fn constant_value_str(&self, db: &str, ctx: &MockDbContext) -> Vec<String> {
        PgType::constant_value_str(self, db, ctx)
    }

    fn custom_type_ddl_stmts(types: &[Vec<Self>], db: &str, ctx: &MockDbContext) -> Vec<String> {
        let mut ddl_by_name = HashMap::new();
        let mut ordered_names = Vec::new();
        for ty in types.iter().flatten() {
            Self::collect_custom_type_ddls(ty, db, ctx, &mut ddl_by_name, &mut ordered_names);
        }
        ordered_names
            .into_iter()
            .map(|name| ddl_by_name.remove(&name).unwrap())
            .collect()
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

    fn pg_ctx() -> MockDbContext {
        MockDbContext::new(DbType::Pg, "17.0")
    }

    fn mockable_pg_types() -> Vec<PgType> {
        vec![
            PgType::Bool,
            PgType::Bytea,
            PgType::Char,
            PgType::Name,
            PgType::Int8,
            PgType::Int2,
            PgType::Int4,
            PgType::Text,
            PgType::Oid,
            PgType::Json,
            PgType::JsonArray,
            PgType::Xml,
            PgType::XmlArray,
            PgType::Point,
            PgType::Lseg,
            PgType::Path,
            PgType::Box,
            PgType::Polygon,
            PgType::Line,
            PgType::LineArray,
            PgType::Cidr,
            PgType::CidrArray,
            PgType::Float4,
            PgType::Float8,
            PgType::Circle,
            PgType::CircleArray,
            PgType::Macaddr8,
            PgType::Macaddr8Array,
            PgType::Macaddr,
            PgType::Inet,
            PgType::BoolArray,
            PgType::ByteaArray,
            PgType::CharArray,
            PgType::NameArray,
            PgType::Int2Array,
            PgType::Int4Array,
            PgType::TextArray,
            PgType::BpcharArray,
            PgType::VarcharArray,
            PgType::Int8Array,
            PgType::PointArray,
            PgType::LsegArray,
            PgType::PathArray,
            PgType::BoxArray,
            PgType::Float4Array,
            PgType::Float8Array,
            PgType::PolygonArray,
            PgType::OidArray,
            PgType::MacaddrArray,
            PgType::InetArray,
            PgType::Bpchar,
            PgType::Varchar,
            PgType::Date,
            PgType::Time,
            PgType::Timestamp,
            PgType::TimestampArray,
            PgType::DateArray,
            PgType::TimeArray,
            PgType::Timestamptz,
            PgType::TimestamptzArray,
            PgType::Interval,
            PgType::IntervalArray,
            PgType::NumericArray,
            PgType::Timetz,
            PgType::TimetzArray,
            PgType::Bit,
            PgType::BitArray,
            PgType::Varbit,
            PgType::VarbitArray,
            PgType::Numeric,
            PgType::Uuid,
            PgType::UuidArray,
            PgType::Jsonb,
            PgType::JsonbArray,
            PgType::TsVector,
            PgType::TsVectorArray,
            PgType::TsQuery,
            PgType::TsQueryArray,
            PgType::Int4Range,
            PgType::Int4RangeArray,
            PgType::NumRange,
            PgType::NumRangeArray,
            PgType::TsRange,
            PgType::TsRangeArray,
            PgType::TstzRange,
            PgType::TstzRangeArray,
            PgType::DateRange,
            PgType::DateRangeArray,
            PgType::Int8Range,
            PgType::Int8RangeArray,
            PgType::Int4Multirange,
            PgType::Int4MultirangeArray,
            PgType::NumMultirange,
            PgType::NumMultirangeArray,
            PgType::TsMultirange,
            PgType::TsMultirangeArray,
            PgType::TstzMultirange,
            PgType::TstzMultirangeArray,
            PgType::DateMultirange,
            PgType::DateMultirangeArray,
            PgType::Int8Multirange,
            PgType::Int8MultirangeArray,
            PgType::Jsonpath,
            PgType::JsonpathArray,
            PgType::Money,
            PgType::MoneyArray,
        ]
    }

    fn newly_supported_pg_types() -> Vec<PgType> {
        vec![
            PgType::Xml,
            PgType::XmlArray,
            PgType::TsVector,
            PgType::TsVectorArray,
            PgType::TsQuery,
            PgType::TsQueryArray,
            PgType::Int4Range,
            PgType::Int4RangeArray,
            PgType::NumRange,
            PgType::NumRangeArray,
            PgType::TsRange,
            PgType::TsRangeArray,
            PgType::TstzRange,
            PgType::TstzRangeArray,
            PgType::DateRange,
            PgType::DateRangeArray,
            PgType::Int8Range,
            PgType::Int8RangeArray,
            PgType::Int4Multirange,
            PgType::Int4MultirangeArray,
            PgType::NumMultirange,
            PgType::NumMultirangeArray,
            PgType::TsMultirange,
            PgType::TsMultirangeArray,
            PgType::TstzMultirange,
            PgType::TstzMultirangeArray,
            PgType::DateMultirange,
            PgType::DateMultirangeArray,
            PgType::Int8Multirange,
            PgType::Int8MultirangeArray,
            PgType::Jsonpath,
            PgType::JsonpathArray,
        ]
    }

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
    fn test_mockable_pg_types_generate_values() {
        let ctx = pg_ctx();
        let mut random = Random::new(Some(42));
        for pg_type in mockable_pg_types() {
            let name = pg_type.name();
            assert!(!name.is_empty(), "{:?} has empty name", pg_type);
            let value = pg_type.next_value_str("test_db", &ctx, &mut random);
            assert!(!value.is_empty(), "{:?} generated empty value", pg_type);
        }
    }

    #[test]
    fn test_newly_supported_pg_types_have_constants() {
        let ctx = pg_ctx();
        for pg_type in newly_supported_pg_types() {
            let constants = pg_type.constant_value_str("test_db", &ctx);
            assert!(!constants.is_empty(), "{:?} has no constants", pg_type);
        }
    }

    #[test]
    fn test_bit_types_use_indexable_typmods() {
        assert_eq!(PgType::Bit.name(), "bit(10)");
        assert_eq!(PgType::Varbit.name(), "varbit(32)");
        assert!(PgType::Bit.support_btree_index());
        assert!(PgType::Varbit.support_btree_index());
        assert!(PgType::BitArray.support_btree_index());
        assert!(PgType::VarbitArray.support_btree_index());
    }

    #[test]
    fn test_numeric_constant_values_filter_infinity_before_pg_14() {
        let pg_13_ctx = MockDbContext::new(DbType::Pg, "13.12");
        let values = PgType::constant_value_str(&PgType::Numeric, "test_db", &pg_13_ctx);
        assert!(!values.contains(&"'inf'".to_string()));
        assert!(!values.contains(&"'-inf'".to_string()));

        let array_values = PgType::constant_value_str(&PgType::NumericArray, "test_db", &pg_13_ctx);
        assert!(!array_values.iter().any(|v| v.contains("'inf'")));
        assert!(!array_values.iter().any(|v| v.contains("'-inf'")));

        let pg_14_ctx = MockDbContext::new(DbType::Pg, "14.0");
        let values = PgType::constant_value_str(&PgType::Numeric, "test_db", &pg_14_ctx);
        assert!(values.contains(&"'inf'".to_string()));
        assert!(values.contains(&"'-inf'".to_string()));
    }

    #[test]
    fn test_custom_pg_type_deserialization_and_ddl() {
        let pg_types: Vec<PgType> = serde_json::from_str(
            r#"[
                "int4",
                {"custom":{"kind":"enum","name":"mock_mood","labels":["sad","ok","happy"]}},
                {"custom":{"kind":"domain","name":"mock_email","base":"text","check":"VALUE LIKE '%@%'","values":["$$a@test.com$$","$$b@test.com$$"]}},
                {"custom":{"kind":"composite","name":"mock_addr","fields":[{"name":"city","type":"text"},{"name":"zip","type":"int4"}],"values":[["$$Shanghai$$","200000"],["$$Beijing$$","100000"]]}},
                {"custom":{"kind":"range","name":"mock_score_range","subtype":"int4","values":["'[1,10)'","'[20,30)'"]}}
            ]"#,
        )
        .unwrap();
        let ctx = pg_ctx();
        let ddls = PgType::custom_type_ddl_stmts(&[pg_types.clone()], "test_db", &ctx);

        assert_eq!(
            ddls,
            vec![
                "CREATE TYPE test_db.mock_mood AS ENUM ('sad', 'ok', 'happy');",
                "CREATE DOMAIN test_db.mock_email AS text CHECK (VALUE LIKE '%@%');",
                "CREATE TYPE test_db.mock_addr AS (city text, zip int4);",
                "CREATE TYPE test_db.mock_score_range AS RANGE (SUBTYPE = int4);",
            ]
        );
        assert_eq!(pg_types[1].type_name("test_db", &ctx), "test_db.mock_mood");
        assert!(!pg_types[1].support_btree_index());
    }

    #[test]
    fn test_custom_pg_type_values_are_casted() {
        let pg_types: Vec<PgType> = serde_json::from_str(
            r#"[
                {"custom":{"kind":"enum","name":"mock_mood","labels":["sad","ok","happy"]}},
                {"custom":{"kind":"domain","name":"mock_email","base":"text","check":"VALUE LIKE '%@%'","values":["$$a@test.com$$","$$b@test.com$$"]}},
                {"custom":{"kind":"composite","name":"mock_addr","fields":[{"name":"city","type":"text"},{"name":"zip","type":"int4"}],"values":[["$$Shanghai$$","200000"]]}},
                {"custom":{"kind":"range","name":"mock_score_range","subtype":"int4","values":["'[1,10)'","'[20,30)'"]}}
            ]"#,
        )
        .unwrap();
        let ctx = pg_ctx();
        let mut random = Random::new(Some(42));

        for pg_type in &pg_types {
            let value = pg_type.next_value_str("test_db", &ctx, &mut random);
            assert!(value.contains("::test_db."));
            let constants = pg_type.constant_value_str("test_db", &ctx);
            assert!(!constants.is_empty());
            assert!(constants.iter().all(|value| value.contains("::test_db.")));
        }
    }

    #[test]
    fn test_custom_pg_range_type_collects_custom_subtype_first() {
        let pg_types: Vec<PgType> = serde_json::from_str(
            r#"[
                {"custom":{"kind":"range","name":"mock_email_range","subtype":{"custom":{"kind":"domain","name":"mock_email_key","base":"text","check":null,"values":["$$a@test.com$$"]}},"subtype_opclass":"text_ops","collation":"pg_catalog.\"default\"","values":["'[a@test.com,z@test.com)'"]}}
            ]"#,
        )
        .unwrap();
        let ctx = pg_ctx();
        let ddls = PgType::custom_type_ddl_stmts(&[pg_types], "test_db", &ctx);

        assert_eq!(
            ddls,
            vec![
                "CREATE DOMAIN test_db.mock_email_key AS text;",
                "CREATE TYPE test_db.mock_email_range AS RANGE (SUBTYPE = test_db.mock_email_key, SUBTYPE_OPCLASS = text_ops, COLLATION = pg_catalog.\"default\");",
            ]
        );
    }
}
