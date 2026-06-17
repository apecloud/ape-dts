use serde::{Deserialize, Serialize};

use crate::test_runner::mock_data::{
    constants::{ConstantValues, Constants},
    context::MockDbContext,
    mock_stmt::MockColType,
    random::{Random, RandomValue},
    types::{
        bytea::Bytea,
        json::Json,
        mysql::{
            charset::{MysqlCharAttrs, MysqlTextAttrs},
            geo::{
                WktGeometryCollection, WktLineString, WktMultiLineString, WktMultiPoint,
                WktMultiPolygon, WktPoint, WktPolygon,
            },
        },
    },
};

// MySQL MEDIUMINT bounds (no native Rust 3-byte integer type)
const MEDIUMINT_MIN: i32 = -8388608;
const MEDIUMINT_MAX: i32 = 8388607;
const MEDIUMINT_UNSIGNED_MAX: u32 = 16777215;

// MySQL YEAR bounds (13.2.4)
const YEAR_MIN: i32 = 1901;
const YEAR_MAX: i32 = 2155;

// Predefined values for ENUM and SET types (13.3.5, 13.3.6)
const ENUM_VALUES: &[&str] = &["v1", "v2", "v3", "v4", "v5"];
const SET_VALUES: &[&str] = &["s1", "s2", "s3", "s4", "s5"];
const ENUM_SET_CHARSET: &str = "utf8";
const ENUM_SET_COLLATION: &str = "utf8_bin";

/// Escape a string for use in MySQL single-quoted string literals.
/// Handles: single quotes (' → ''), backslashes (\ → \\)
fn mysql_quote(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{}'", escaped)
}

/// All MySQL data types from Chapter 13 of the MySQL 8.4 Reference Manual.
///
/// - 13.1: Numeric Data Types
/// - 13.2: Date and Time Data Types
/// - 13.3: String Data Types
/// - 13.4: Spatial Data Types
/// - 13.5: JSON Data Type
#[derive(Hash, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MysqlType {
    // --- 13.1.2 Integer Types (Exact Value) ---
    TinyInt,
    TinyIntUnsigned,
    SmallInt,
    SmallIntUnsigned,
    MediumInt,
    MediumIntUnsigned,
    Int,
    IntUnsigned,
    BigInt,
    BigIntUnsigned,

    // --- 13.1.3 Fixed-Point Types (Exact Value) ---
    Decimal,

    // --- 13.1.4 Floating-Point Types (Approximate Value) ---
    Float,
    Double,

    // --- 13.1.5 Bit-Value Type ---
    Bit,

    // --- 13.2 Date and Time Data Types ---
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,

    // --- 13.3.2 CHAR and VARCHAR ---
    Char(MysqlCharAttrs),
    Varchar(MysqlCharAttrs),

    // --- 13.3.3 BINARY and VARBINARY ---
    Binary,
    Varbinary,

    // --- 13.3.4 BLOB and TEXT ---
    TinyBlob,
    Blob,
    MediumBlob,
    LongBlob,
    TinyText(MysqlTextAttrs),
    Text(MysqlTextAttrs),
    MediumText(MysqlTextAttrs),
    LongText(MysqlTextAttrs),

    // --- 13.3.5 ENUM Type ---
    Enum,

    // --- 13.3.6 SET Type ---
    Set,

    // --- 13.4 Spatial Data Types ---
    Geometry,
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,

    // --- 13.5 JSON Data Type ---
    Json,
}

impl MysqlType {
    /// Returns the MySQL DDL type name (used in CREATE TABLE).
    /// Types with size parameters use reasonable test defaults.
    pub fn name(&self) -> String {
        match self {
            // Integer types (13.1.2)
            MysqlType::TinyInt => "TINYINT".to_string(),
            MysqlType::TinyIntUnsigned => "TINYINT UNSIGNED".to_string(),
            MysqlType::SmallInt => "SMALLINT".to_string(),
            MysqlType::SmallIntUnsigned => "SMALLINT UNSIGNED".to_string(),
            MysqlType::MediumInt => "MEDIUMINT".to_string(),
            MysqlType::MediumIntUnsigned => "MEDIUMINT UNSIGNED".to_string(),
            MysqlType::Int => "INT".to_string(),
            MysqlType::IntUnsigned => "INT UNSIGNED".to_string(),
            MysqlType::BigInt => "BIGINT".to_string(),
            MysqlType::BigIntUnsigned => "BIGINT UNSIGNED".to_string(),

            // Fixed-point (13.1.3): precision=10, scale=2
            MysqlType::Decimal => "DECIMAL(10,2)".to_string(),

            // Floating-point (13.1.4)
            MysqlType::Float => "FLOAT".to_string(),
            MysqlType::Double => "DOUBLE".to_string(),

            // Bit (13.1.5): 8 bits for testing
            MysqlType::Bit => "BIT(8)".to_string(),

            // Date/Time (13.2): use fsp=6 for fractional seconds
            MysqlType::Date => "DATE".to_string(),
            MysqlType::DateTime => "DATETIME(6)".to_string(),
            MysqlType::Timestamp => "TIMESTAMP(6)".to_string(),
            MysqlType::Time => "TIME(6)".to_string(),
            MysqlType::Year => "YEAR".to_string(),

            // Character strings (13.3.2)
            MysqlType::Char(attrs) => format!("CHAR({}) {}", attrs.length, attrs.ddl_suffix()),
            MysqlType::Varchar(attrs) => {
                format!("VARCHAR({}) {}", attrs.length, attrs.ddl_suffix())
            }

            // Binary strings (13.3.3)
            MysqlType::Binary => "BINARY(255)".to_string(),
            MysqlType::Varbinary => "VARBINARY(255)".to_string(),

            // BLOB types (13.3.4)
            MysqlType::TinyBlob => "TINYBLOB".to_string(),
            MysqlType::Blob => "BLOB".to_string(),
            MysqlType::MediumBlob => "MEDIUMBLOB".to_string(),
            MysqlType::LongBlob => "LONGBLOB".to_string(),

            // TEXT types (13.3.4)
            MysqlType::TinyText(attrs) => format!("TINYTEXT {}", attrs.ddl_suffix()),
            MysqlType::Text(attrs) => format!("TEXT {}", attrs.ddl_suffix()),
            MysqlType::MediumText(attrs) => format!("MEDIUMTEXT {}", attrs.ddl_suffix()),
            MysqlType::LongText(attrs) => format!("LONGTEXT {}", attrs.ddl_suffix()),

            // ENUM (13.3.5)
            MysqlType::Enum => format!(
                "ENUM('v1','v2','v3','v4','v5') CHARACTER SET {} COLLATE {}",
                ENUM_SET_CHARSET, ENUM_SET_COLLATION
            ),

            // SET (13.3.6)
            MysqlType::Set => format!(
                "SET('s1','s2','s3','s4','s5') CHARACTER SET {} COLLATE {}",
                ENUM_SET_CHARSET, ENUM_SET_COLLATION
            ),

            // Spatial types (13.4)
            MysqlType::Geometry => "GEOMETRY".to_string(),
            MysqlType::Point => "POINT".to_string(),
            MysqlType::LineString => "LINESTRING".to_string(),
            MysqlType::Polygon => "POLYGON".to_string(),
            MysqlType::MultiPoint => "MULTIPOINT".to_string(),
            MysqlType::MultiLineString => "MULTILINESTRING".to_string(),
            MysqlType::MultiPolygon => "MULTIPOLYGON".to_string(),
            MysqlType::GeometryCollection => "GEOMETRYCOLLECTION".to_string(),

            // JSON (13.5)
            MysqlType::Json => "JSON".to_string(),
        }
    }

    /// Whether this type supports B-tree indexing (usable as PRIMARY KEY / UNIQUE).
    ///
    /// Excluded:
    /// - TEXT/BLOB types: require prefix length for indexing
    /// - JSON: cannot be indexed directly
    /// - BIT: too small cardinality for unique test values
    /// - ENUM/SET: too small cardinality for unique test values
    /// - Spatial types: use SPATIAL indexes, not B-tree
    /// - BINARY: right-pads with \x00 to full width, causing constant values
    ///   like X'', X'00', X'00000000' to collide after padding
    pub fn support_btree_index(&self) -> bool {
        match self {
            MysqlType::Char(attrs) | MysqlType::Varchar(attrs) => {
                return attrs.can_be_unique_key();
            }
            _ => {}
        }

        matches!(
            self,
            MysqlType::TinyInt
                | MysqlType::TinyIntUnsigned
                | MysqlType::SmallInt
                | MysqlType::SmallIntUnsigned
                | MysqlType::MediumInt
                | MysqlType::MediumIntUnsigned
                | MysqlType::Int
                | MysqlType::IntUnsigned
                | MysqlType::BigInt
                | MysqlType::BigIntUnsigned
                | MysqlType::Decimal
                | MysqlType::Float
                | MysqlType::Double
                | MysqlType::Date
                | MysqlType::DateTime
                | MysqlType::Timestamp
                | MysqlType::Time
                | MysqlType::Year
                | MysqlType::Varbinary
        )
    }

    /// Generate a random SQL literal value for this MySQL type.
    pub fn next_value_str(&self, random: &mut Random) -> String {
        match self {
            // --- Integer types (13.1.2) ---
            // TINYINT: -128 to 127
            MysqlType::TinyInt => format!("{}", random.next_i8()),
            // TINYINT UNSIGNED: 0 to 255
            MysqlType::TinyIntUnsigned => format!("{}", random.next_u8()),
            // SMALLINT: -32768 to 32767
            MysqlType::SmallInt => format!("{}", random.next_i16()),
            // SMALLINT UNSIGNED: 0 to 65535
            MysqlType::SmallIntUnsigned => format!("{}", random.next_u16()),
            // MEDIUMINT: -8388608 to 8388607 (3 bytes, no Rust native type)
            MysqlType::MediumInt => {
                let val = random.next_i32() % (MEDIUMINT_MAX + 1);
                format!("{}", val)
            }
            // MEDIUMINT UNSIGNED: 0 to 16777215
            MysqlType::MediumIntUnsigned => {
                let val = random.next_u32() % (MEDIUMINT_UNSIGNED_MAX + 1);
                format!("{}", val)
            }
            // INT: -2147483648 to 2147483647
            MysqlType::Int => format!("{}", random.next_i32()),
            // INT UNSIGNED: 0 to 4294967295
            MysqlType::IntUnsigned => format!("{}", random.next_u32()),
            // BIGINT: -2^63 to 2^63-1
            MysqlType::BigInt => format!("{}", random.next_i64()),
            // BIGINT UNSIGNED: 0 to 2^64-1
            MysqlType::BigIntUnsigned => format!("{}", random.next_u64()),

            // --- Fixed-point (13.1.3) ---
            // DECIMAL(10,2): -99999999.99 to 99999999.99
            MysqlType::Decimal => {
                let integer_part = random.next_i32() % 100_000_000;
                let decimal_part = (random.next_u32() % 100) as i32;
                format!("{}.{:02}", integer_part, decimal_part.abs())
            }

            // --- Floating-point (13.1.4) ---
            // MySQL does NOT support NaN, Infinity, or -Infinity
            MysqlType::Float => {
                let mut val = random.next_f32();
                while val.is_nan() || val.is_infinite() {
                    val = random.next_f32();
                }
                format!("{}", val)
            }
            MysqlType::Double => {
                let mut val = random.next_f64();
                while val.is_nan() || val.is_infinite() {
                    val = random.next_f64();
                }
                format!("{}", val)
            }

            // --- Bit (13.1.5) ---
            // BIT(8): 0 to 255 in binary literal format
            MysqlType::Bit => {
                let val = random.next_u8();
                format!("b'{:08b}'", val)
            }

            // --- Date/Time types (13.2) ---
            // DATE: '1000-01-01' to '9999-12-31'
            MysqlType::Date => {
                let year = random.random_range(1000..10000);
                let month = random.random_range(1..13);
                let day = random.random_range(1..29); // safe for all months
                mysql_quote(&format!("{:04}-{:02}-{:02}", year, month, day))
            }
            // DATETIME(6): '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'
            MysqlType::DateTime => {
                let year = random.random_range(1000..10000);
                let month = random.random_range(1..13);
                let day = random.random_range(1..29);
                let hour = random.random_range(0..24);
                let minute = random.random_range(0..60);
                let second = random.random_range(0..60);
                let frac = random.next_u32() % 1_000_000;
                mysql_quote(&format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    year, month, day, hour, minute, second, frac
                ))
            }
            // TIMESTAMP(6): '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'
            // Cap year at 2037 to avoid generating dates past the 2038-01-19 boundary.
            MysqlType::Timestamp => {
                let year = random.random_range(1970..2038);
                let month = random.random_range(1..13);
                let day = random.random_range(1..29);
                let hour = random.random_range(0..24);
                let minute = random.random_range(0..60);
                let second = random.random_range(1..60); // avoid 00:00:00 (reserved for zero-value)
                let frac = random.next_u32() % 1_000_000;
                mysql_quote(&format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    year, month, day, hour, minute, second, frac
                ))
            }
            // TIME(6): '-838:59:59.000000' to '838:59:59.000000'
            MysqlType::Time => {
                let sign = if random.next_u8() % 2 == 0 { "" } else { "-" };
                let hours = random.random_range(0..839);
                let minutes = random.random_range(0..60);
                let seconds = random.random_range(0..60);
                // At boundary ±838:59:59, fractional must be 0
                let frac = if hours == 838 && minutes == 59 && seconds == 59 {
                    0
                } else {
                    random.next_u32() % 1_000_000
                };
                mysql_quote(&format!(
                    "{}{:02}:{:02}:{:02}.{:06}",
                    sign, hours, minutes, seconds, frac
                ))
            }
            // YEAR: 1901 to 2155
            MysqlType::Year => {
                let year = random.random_range(YEAR_MIN..(YEAR_MAX + 1));
                format!("{}", year)
            }

            // --- Character string types (13.3.2, 13.3.4) ---
            // CHAR right-pads with spaces; trim trailing spaces like PG bpchar
            MysqlType::Char(attrs) => {
                let value = attrs.normalize_value(random.next_str().trim_end_matches(' '));
                mysql_quote(&value)
            }
            MysqlType::Varchar(attrs) => mysql_quote(&attrs.normalize_value(&random.next_str())),
            MysqlType::TinyText(attrs)
            | MysqlType::Text(attrs)
            | MysqlType::MediumText(attrs)
            | MysqlType::LongText(attrs) => mysql_quote(&attrs.normalize_value(&random.next_str())),

            // --- Binary string types (13.3.3, 13.3.4) ---
            MysqlType::Binary
            | MysqlType::Varbinary
            | MysqlType::TinyBlob
            | MysqlType::Blob
            | MysqlType::MediumBlob
            | MysqlType::LongBlob => {
                format!("X'{}'", Bytea::next_value(random))
            }

            // --- ENUM (13.3.5) ---
            MysqlType::Enum => {
                let idx = random.next_u8() as usize % ENUM_VALUES.len();
                mysql_quote(ENUM_VALUES[idx])
            }

            // --- SET (13.3.6) ---
            // SET values are comma-separated combinations of the defined members
            MysqlType::Set => {
                let mut selected = Vec::new();
                for val in SET_VALUES.iter() {
                    if random.next_u8() % 2 == 0 {
                        selected.push(*val);
                    }
                }
                if selected.is_empty() {
                    selected.push(SET_VALUES[0]);
                }
                mysql_quote(&selected.join(","))
            }

            // --- Spatial types (13.4) ---
            // Values use ST_GeomFromText() with WKT (Well-Known Text) format.
            // Random geometries generated via geo_types + fake crate (see types::mysql::geo).
            MysqlType::Point => {
                format!("ST_GeomFromText('{}')", WktPoint::next_value(random))
            }
            MysqlType::LineString => {
                format!("ST_GeomFromText('{}')", WktLineString::next_value(random))
            }
            MysqlType::Polygon => {
                format!("ST_GeomFromText('{}')", WktPolygon::next_value(random))
            }
            MysqlType::MultiPoint => {
                format!("ST_GeomFromText('{}')", WktMultiPoint::next_value(random))
            }
            MysqlType::MultiLineString => {
                format!(
                    "ST_GeomFromText('{}')",
                    WktMultiLineString::next_value(random)
                )
            }
            MysqlType::MultiPolygon => {
                format!("ST_GeomFromText('{}')", WktMultiPolygon::next_value(random))
            }
            MysqlType::GeometryCollection => {
                format!(
                    "ST_GeomFromText('{}')",
                    WktGeometryCollection::next_value(random)
                )
            }
            // GEOMETRY accepts any geometry; generate a random Point
            MysqlType::Geometry => {
                format!("ST_GeomFromText('{}')", WktPoint::next_value(random))
            }

            // --- JSON (13.5) ---
            MysqlType::Json => mysql_quote(&Json::next_value(random)),
        }
    }

    /// Returns boundary/edge-case constant values for this MySQL type.
    /// These are injected into test data to verify correctness at limits.
    pub fn constant_value_str(&self) -> Vec<String> {
        match self {
            // --- Integer types (13.1.2) ---
            // TINYINT: -128, 0, 127
            MysqlType::TinyInt => Constants::next_i8().iter().map(|v| v.to_string()).collect(),
            // TINYINT UNSIGNED: 0, 255
            MysqlType::TinyIntUnsigned => {
                Constants::next_u8().iter().map(|v| v.to_string()).collect()
            }
            // SMALLINT: -32768, 0, 32767
            MysqlType::SmallInt => Constants::next_i16()
                .iter()
                .map(|v| v.to_string())
                .collect(),
            // SMALLINT UNSIGNED: 0, 65535
            MysqlType::SmallIntUnsigned => Constants::next_u16()
                .iter()
                .map(|v| v.to_string())
                .collect(),
            // MEDIUMINT: -8388608, 0, 8388607
            MysqlType::MediumInt => vec![
                MEDIUMINT_MIN.to_string(),
                "0".to_string(),
                MEDIUMINT_MAX.to_string(),
            ],
            // MEDIUMINT UNSIGNED: 0, 16777215
            MysqlType::MediumIntUnsigned => {
                vec!["0".to_string(), MEDIUMINT_UNSIGNED_MAX.to_string()]
            }
            // INT: -2147483648, -1, 0, 1, 2147483647
            MysqlType::Int => Constants::next_i32()
                .iter()
                .map(|v| v.to_string())
                .collect(),
            // INT UNSIGNED: 0, 1, 4294967295
            MysqlType::IntUnsigned => Constants::next_u32()
                .iter()
                .map(|v| v.to_string())
                .collect(),
            // BIGINT: -9223372036854775808, -1, 0, 1, 9223372036854775807
            MysqlType::BigInt => Constants::next_i64()
                .iter()
                .map(|v| v.to_string())
                .collect(),
            // BIGINT UNSIGNED: 0, 1, 18446744073709551615
            MysqlType::BigIntUnsigned => Constants::next_u64()
                .iter()
                .map(|v| v.to_string())
                .collect(),

            // --- Fixed-point (13.1.3) ---
            // DECIMAL(10,2) boundary values
            MysqlType::Decimal => vec![
                "-99999999.99".to_string(),
                "-1.00".to_string(),
                "0.00".to_string(),
                "1.00".to_string(),
                "99999999.99".to_string(),
            ],

            // --- Floating-point (13.1.4) ---
            // MySQL does NOT support NaN, Infinity.
            // f32::MAX / f32::MIN are also excluded because Rust's
            // `format!("{}", f32::MAX)` produces a decimal string that
            // MySQL parses as slightly out-of-range for FLOAT.
            // Use safe boundary values instead.
            MysqlType::Float => {
                let mut vals: Vec<String> = Constants::next_f32()
                    .iter()
                    .filter(|v| v.is_finite() && v.abs() < f32::MAX)
                    .map(|v| v.to_string())
                    .collect();
                // MySQL-safe FLOAT boundaries
                vals.push("3.4028e38".to_string());
                vals.push("-3.4028e38".to_string());
                vals
            }
            MysqlType::Double => {
                let mut vals: Vec<String> = Constants::next_f64()
                    .iter()
                    .filter(|v| v.is_finite() && v.abs() < f64::MAX)
                    .map(|v| v.to_string())
                    .collect();
                // MySQL-safe DOUBLE boundaries
                vals.push("1.7976931348623e308".to_string());
                vals.push("-1.7976931348623e308".to_string());
                vals
            }

            // --- Bit (13.1.5) ---
            MysqlType::Bit => vec![
                "b'00000000'".to_string(),
                "b'00000001'".to_string(),
                "b'01010101'".to_string(),
                "b'11111111'".to_string(),
            ],

            // --- Date/Time (13.2) ---
            // DATE: '1000-01-01' to '9999-12-31'
            MysqlType::Date => [
                "'1000-01-01'", // minimum
                "'1970-01-01'", // Unix epoch
                "'2000-02-29'", // leap year Feb 29
                "'2001-02-28'", // non-leap year Feb end
                "'2000-12-31'", // year-end boundary
                "'9999-12-31'", // maximum
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),

            // DATETIME(6): '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
            MysqlType::DateTime => [
                "'1000-01-01 00:00:00.000000'", // minimum
                "'1970-01-01 00:00:00.000000'", // epoch midnight
                "'2000-02-29 23:59:59.999999'", // leap day + max time + max fsp
                "'2000-01-01 12:00:00.000001'", // minimal fractional second
                "'9999-12-31 23:59:59.999999'", // maximum
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),

            // TIMESTAMP(6): '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'
            MysqlType::Timestamp => [
                "'1970-01-01 00:00:01.000000'", // minimum
                "'1970-01-01 00:00:01.000001'", // min + minimal fsp
                "'2000-01-01 00:00:00.000000'", // Y2K midnight
                "'2038-01-19 03:14:07.000000'", // max second boundary (no fsp)
                "'2038-01-19 03:14:07.999999'", // absolute maximum
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),

            // TIME(6): '-838:59:59.000000' to '838:59:59.000000'
            MysqlType::Time => [
                "'-838:59:59.000000'", // minimum
                "'-00:00:00.000001'",  // smallest negative
                "'00:00:00.000000'",   // zero
                "'00:00:00.000001'",   // smallest positive
                "'23:59:59.999999'",   // max normal day time
                "'838:59:59.000000'",  // absolute maximum
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),

            // YEAR: 1901 to 2155
            MysqlType::Year => vec![
                YEAR_MIN.to_string(), // minimum (1901)
                "1970".to_string(),   // Unix epoch year
                "2000".to_string(),   // Y2K
                YEAR_MAX.to_string(), // maximum (2155)
            ],

            // --- Character string types (13.3.2, 13.3.4) ---
            MysqlType::Char(attrs) | MysqlType::Varchar(attrs) => attrs
                .constant_values()
                .into_iter()
                .map(|s| mysql_quote(&s))
                .collect(),
            MysqlType::TinyText(attrs)
            | MysqlType::Text(attrs)
            | MysqlType::MediumText(attrs)
            | MysqlType::LongText(attrs) => attrs
                .constant_values()
                .into_iter()
                .map(|s| mysql_quote(&s))
                .collect(),

            // --- Binary string types (13.3.3, 13.3.4) ---
            MysqlType::Binary
            | MysqlType::Varbinary
            | MysqlType::TinyBlob
            | MysqlType::Blob
            | MysqlType::MediumBlob
            | MysqlType::LongBlob => Bytea::next_values()
                .into_iter()
                .map(|s| format!("X'{}'", s))
                .collect(),

            // --- ENUM (13.3.5) ---
            MysqlType::Enum => ENUM_VALUES.iter().map(|v| mysql_quote(v)).collect(),

            // --- SET (13.3.6) ---
            // Individual values, combined, and empty
            MysqlType::Set => {
                let mut values: Vec<String> = SET_VALUES.iter().map(|v| mysql_quote(v)).collect();
                values.push(mysql_quote(&SET_VALUES.join(",")));
                values.push(mysql_quote(""));
                values
            }

            // --- Spatial types (13.4) ---
            // Constant WKT values using ST_GeomFromText(), sourced from types::mysql::geo.
            MysqlType::Geometry => {
                let mut v = WktPoint::next_values();
                v.truncate(2);
                v.extend(WktLineString::next_values().into_iter().take(1));
                v.into_iter()
                    .map(|wkt| format!("ST_GeomFromText('{}')", wkt))
                    .collect()
            }
            MysqlType::Point => WktPoint::next_values()
                .into_iter()
                .map(|wkt| format!("ST_GeomFromText('{}')", wkt))
                .collect(),
            MysqlType::LineString => WktLineString::next_values()
                .into_iter()
                .map(|wkt| format!("ST_GeomFromText('{}')", wkt))
                .collect(),
            MysqlType::Polygon => WktPolygon::next_values()
                .into_iter()
                .map(|wkt| format!("ST_GeomFromText('{}')", wkt))
                .collect(),
            MysqlType::MultiPoint => WktMultiPoint::next_values()
                .into_iter()
                .map(|wkt| format!("ST_GeomFromText('{}')", wkt))
                .collect(),
            MysqlType::MultiLineString => WktMultiLineString::next_values()
                .into_iter()
                .map(|wkt| format!("ST_GeomFromText('{}')", wkt))
                .collect(),
            MysqlType::MultiPolygon => WktMultiPolygon::next_values()
                .into_iter()
                .map(|wkt| format!("ST_GeomFromText('{}')", wkt))
                .collect(),
            MysqlType::GeometryCollection => WktGeometryCollection::next_values()
                .into_iter()
                .map(|wkt| format!("ST_GeomFromText('{}')", wkt))
                .collect(),

            // --- JSON (13.5) ---
            MysqlType::Json => Json::next_values()
                .into_iter()
                .map(|s| mysql_quote(&s))
                .collect(),
        }
    }

    fn needs_explicit_timestamp_defaults(ctx: &MockDbContext) -> bool {
        ctx.version.major < 8
    }
}

impl MockColType for MysqlType {
    fn name(&self, _ctx: &MockDbContext) -> String {
        MysqlType::name(self)
    }

    fn support_btree_index(&self, _ctx: &MockDbContext) -> bool {
        MysqlType::support_btree_index(self)
    }

    fn next_value_str(&self, _ctx: &MockDbContext, random: &mut Random) -> String {
        MysqlType::next_value_str(self, random)
    }

    fn constant_value_str(&self, _ctx: &MockDbContext) -> Vec<String> {
        MysqlType::constant_value_str(self)
    }

    fn column_def(&self, quoted_col: &str, nullable: bool, ctx: &MockDbContext) -> String {
        if matches!(self, MysqlType::Timestamp) && Self::needs_explicit_timestamp_defaults(ctx) {
            // MySQL 5.x may implicitly turn TIMESTAMP NULL inserts into CURRENT_TIMESTAMP.
            // ref https://dev.mysql.com/doc/refman/5.7/en/timestamp-initialization.html
            let suffix = if nullable {
                "NULL DEFAULT NULL".to_string()
            } else {
                "NOT NULL DEFAULT '2000-01-01 00:00:00.000000'".to_string()
            };
            return format!("{} {} {}", quoted_col, MysqlType::name(self), suffix);
        }

        let is_nullable = if nullable { "" } else { " NOT NULL" };
        format!("{} {}{}", quoted_col, MysqlType::name(self), is_nullable)
    }

    fn schema_drop_stmt(db: &str, _ctx: &MockDbContext) -> String {
        format!("DROP DATABASE IF EXISTS `{}`;", db)
    }

    fn schema_create_stmt(db: &str, _ctx: &MockDbContext) -> String {
        format!("CREATE DATABASE IF NOT EXISTS `{}`;", db)
    }

    fn quote_identifier(name: &str, _ctx: &MockDbContext) -> String {
        format!("`{}`", name)
    }

    fn after_all_insert_stmts(db_tbs: &[(String, String)], ctx: &MockDbContext) -> Vec<String> {
        const ANALYZE_TABLE_BATCH_SIZE: usize = 100;

        db_tbs
            .chunks(ANALYZE_TABLE_BATCH_SIZE)
            .map(|chunk| {
                let tables = chunk
                    .iter()
                    .map(|(db, tb)| {
                        format!(
                            "{}.{}",
                            Self::quote_identifier(db, ctx),
                            Self::quote_identifier(tb, ctx)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("ANALYZE TABLE {};", tables)
            })
            .collect()
    }

    fn config_key_prefix() -> &'static str {
        "mysql_types"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_types() -> Vec<MysqlType> {
        vec![
            // 13.1 Numeric
            MysqlType::TinyInt,
            MysqlType::TinyIntUnsigned,
            MysqlType::SmallInt,
            MysqlType::SmallIntUnsigned,
            MysqlType::MediumInt,
            MysqlType::MediumIntUnsigned,
            MysqlType::Int,
            MysqlType::IntUnsigned,
            MysqlType::BigInt,
            MysqlType::BigIntUnsigned,
            MysqlType::Decimal,
            MysqlType::Float,
            MysqlType::Double,
            MysqlType::Bit,
            // 13.2 Date/Time
            MysqlType::Date,
            MysqlType::DateTime,
            MysqlType::Timestamp,
            MysqlType::Time,
            MysqlType::Year,
            // 13.3 String
            MysqlType::Char(MysqlCharAttrs::default_with_length(255)),
            MysqlType::Varchar(MysqlCharAttrs::default_with_length(255)),
            MysqlType::Binary,
            MysqlType::Varbinary,
            MysqlType::TinyBlob,
            MysqlType::Blob,
            MysqlType::MediumBlob,
            MysqlType::LongBlob,
            MysqlType::TinyText(MysqlTextAttrs::default()),
            MysqlType::Text(MysqlTextAttrs::default()),
            MysqlType::MediumText(MysqlTextAttrs::default()),
            MysqlType::LongText(MysqlTextAttrs::default()),
            MysqlType::Enum,
            MysqlType::Set,
            // 13.4 Spatial
            MysqlType::Geometry,
            MysqlType::Point,
            MysqlType::LineString,
            MysqlType::Polygon,
            MysqlType::MultiPoint,
            MysqlType::MultiLineString,
            MysqlType::MultiPolygon,
            MysqlType::GeometryCollection,
            // 13.5 JSON
            MysqlType::Json,
        ]
    }

    #[test]
    fn test_character_type_names_include_attrs() {
        let varchar: MysqlType = serde_json::from_str(
            r#"{"varchar":{"length":191,"charset":"utf8mb4","collation":"utf8mb4_bin"}}"#,
        )
        .unwrap();
        assert_eq!(
            varchar.name(),
            "VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        );

        let text: MysqlType =
            serde_json::from_str(r#"{"text":{"charset":"utf8","collation":"utf8_bin"}}"#).unwrap();
        assert_eq!(text.name(), "TEXT CHARACTER SET utf8 COLLATE utf8_bin");
    }

    #[test]
    fn test_enum_and_set_type_names_include_fixed_charset_attrs() {
        assert_eq!(
            MysqlType::Enum.name(),
            "ENUM('v1','v2','v3','v4','v5') CHARACTER SET utf8 COLLATE utf8_bin"
        );
        assert_eq!(
            MysqlType::Set.name(),
            "SET('s1','s2','s3','s4','s5') CHARACTER SET utf8 COLLATE utf8_bin"
        );
    }

    #[test]
    fn test_utf8mb3_character_constants_exclude_4_byte_values() {
        let utf8_text: MysqlType =
            serde_json::from_str(r#"{"text":{"charset":"utf8","collation":"utf8_bin"}}"#).unwrap();
        let utf8mb4_text: MysqlType =
            serde_json::from_str(r#"{"text":{"charset":"utf8mb4","collation":"utf8mb4_bin"}}"#)
                .unwrap();

        assert!(!utf8_text
            .constant_value_str()
            .contains(&mysql_quote("🔥🚀")));
        assert!(utf8mb4_text
            .constant_value_str()
            .contains(&mysql_quote("🔥🚀")));
    }

    #[test]
    fn test_character_btree_index_allows_all_char_attrs() {
        let bin_varchar: MysqlType = serde_json::from_str(
            r#"{"varchar":{"length":255,"charset":"utf8mb4","collation":"utf8mb4_bin"}}"#,
        )
        .unwrap();
        let ci_varchar: MysqlType = serde_json::from_str(
            r#"{"varchar":{"length":255,"charset":"utf8mb4","collation":"utf8mb4_0900_ai_ci"}}"#,
        )
        .unwrap();
        let cs_varchar: MysqlType = serde_json::from_str(
            r#"{"varchar":{"length":255,"charset":"utf8mb4","collation":"utf8mb4_0900_as_cs"}}"#,
        )
        .unwrap();

        assert!(bin_varchar.support_btree_index());
        assert!(ci_varchar.support_btree_index());
        assert!(cs_varchar.support_btree_index());
    }

    /// Verify every variant survives serialize → deserialize round-trip.
    #[test]
    fn test_serde_roundtrip() {
        for t in all_types() {
            let json = serde_json::to_string(&t).unwrap();
            let back: MysqlType = serde_json::from_str(&json).unwrap();
            assert_eq!(t, back, "serde round-trip failed for {:?}", t);
        }
    }

    /// Verify every variant has a non-empty DDL type name.
    #[test]
    fn test_type_names() {
        for t in all_types() {
            let name = t.name();
            assert!(!name.is_empty(), "{:?} has empty name", t);
        }
    }

    /// Verify every variant can generate a random value.
    #[test]
    fn test_random_value_generation() {
        let mut random = Random::new(Some(42));
        for t in all_types() {
            let val = t.next_value_str(&mut random);
            assert!(!val.is_empty(), "{:?} generated empty value", t);
        }
    }

    /// Verify every variant has at least one constant boundary value.
    #[test]
    fn test_constant_values() {
        for t in all_types() {
            let vals = t.constant_value_str();
            assert!(!vals.is_empty(), "{:?} has no constant values", t);
        }
    }
}
