use serde::{Deserialize, Serialize};

const BOOL_OID: i32 = 16;
const BYTEA_OID: i32 = 17;
const CHAR_OID: i32 = 18;
// const NAME_OID: i32 = 19;
const INT8_OID: i32 = 20;
const INT2_OID: i32 = 21;
const INT4_OID: i32 = 23;
const TEXT_OID: i32 = 25;
const OID_OID: i32 = 26;
// const TID_OID: i32 = 27;
// const XID_OID: i32 = 28;
// const CID_OID: i32 = 29;
const JSON_OID: i32 = 114;
// const XML_OID: i32 = 142;
// const XML_ARRAY_OID: i32 = 143;
// const JSON_ARRAY_OID: i32 = 199;
const POINT_OID: i32 = 600;
// const LSEG_OID: i32 = 601;
// const PATH_OID: i32 = 602;
// const BOX_OID: i32 = 603;
// const POLYGON_OID: i32 = 604;
// const LINE_OID: i32 = 628;
// const LINE_ARRAY_OID: i32 = 629;
const CIDR_OID: i32 = 650;
// const CIDR_ARRAY_OID: i32 = 651;
const FLOAT4_OID: i32 = 700;
const FLOAT8_OID: i32 = 701;
// const CIRCLE_OID: i32 = 718;
// const CIRCLE_ARRAY_OID: i32 = 719;
// const UNKNOWN_OID: i32 = 705;
// const MACADDR8_OID: i32 = 774;
const MACADDR_OID: i32 = 829;
const INET_OID: i32 = 869;
const BOOL_ARRAY_OID: i32 = 1000;
// const QCHAR_ARRAY_OID: i32 = 1002;
// const NAME_ARRAY_OID: i32 = 1003;
const INT2_ARRAY_OID: i32 = 1005;
const INT4_ARRAY_OID: i32 = 1007;
const TEXT_ARRAY_OID: i32 = 1009;
// const TID_ARRAY_OID: i32 = 1010;
// const BYTEA_ARRAY_OID: i32 = 1001;
// const XID_ARRAY_OID: i32 = 1011;
// const CID_ARRAY_OID: i32 = 1012;
const BPCHAR_ARRAY_OID: i32 = 1014;
const VARCHAR_ARRAY_OID: i32 = 1015;
const INT8_ARRAY_OID: i32 = 1016;
// const POINT_ARRAY_OID: i32 = 1017;
// const LSEG_ARRAY_OID: i32 = 1018;
// const PATH_ARRAY_OID: i32 = 1019;
// const BOX_ARRAY_OID: i32 = 1020;
const FLOAT4_ARRAY_OID: i32 = 1021;
const FLOAT8_ARRAY_OID: i32 = 1022;
// const POLYGON_ARRAY_OID: i32 = 1027;
// const OID_ARRAY_OID: i32 = 1028;
// const ACLITEM_OID: i32 = 1033;
// const ACLITEM_ARRAY_OID: i32 = 1034;
// const MACADDR_ARRAY_OID: i32 = 1040;
// const INET_ARRAY_OID: i32 = 1041;
const BPCHAR_OID: i32 = 1042;
const VARCHAR_OID: i32 = 1043;
const DATE_OID: i32 = 1082;
const TIME_OID: i32 = 1083;
const TIMESTAMP_OID: i32 = 1114;
const TIMESTAMP_ARRAY_OID: i32 = 1115;
const DATE_ARRAY_OID: i32 = 1182;
// const TIME_ARRAY_OID: i32 = 1183;
const TIMESTAMPTZ_OID: i32 = 1184;
const TIMESTAMPTZ_ARRAY_OID: i32 = 1185;
const INTERVAL_OID: i32 = 1186;
// const INTERVAL_ARRAY_OID: i32 = 1187;
// const NUMERIC_ARRAY_OID: i32 = 1231;
const TIMETZ_OID: i32 = 1266;
// const TIMETZ_ARRAY_OID: i32 = 1270;
// const BIT_OID: i32 = 1560;
// const BIT_ARRAY_OID: i32 = 1561;
// const VARBIT_OID: i32 = 1562;
// const VARBIT_ARRAY_OID: i32 = 1563;
const NUMERIC_OID: i32 = 1700;
// const RECORD_OID: i32 = 2249;
// const RECORD_ARRAY_OID: i32 = 2287;
const UUID_OID: i32 = 2950;
// const UUID_ARRAY_OID: i32 = 2951;
const JSONB_OID: i32 = 3802;
// const JSONB_ARRAY_OID: i32 = 3807;
// const DATERANGE_OID: i32 = 3912;
// const DATERANGE_ARRAY_OID: i32 = 3913;
// const INT4RANGE_OID: i32 = 3904;
// const INT4RANGE_ARRAY_OID: i32 = 3905;
// const NUMRANGE_OID: i32 = 3906;
// const NUMRANGE_ARRAY_OID: i32 = 3907;
// const TSRANGE_OID: i32 = 3908;
// const TSRANGE_ARRAY_OID: i32 = 3909;
// const TSTZRANGE_OID: i32 = 3910;
// const TSTZRANGE_ARRAY_OID: i32 = 3911;
// const INT8RANGE_OID: i32 = 3926;
// const INT8RANGE_ARRAY_OID: i32 = 3927;
// const JSONPATH_OID: i32 = 4072;
// const JSONPATH_ARRAY_OID: i32 = 4073;
// const INT4MULTIRANGE_OID: i32 = 4451;
// const NUMMULTIRANGE_OID: i32 = 4532;
// const TSMULTIRANGE_OID: i32 = 4533;
// const TSTZMULTIRANGE_OID: i32 = 4534;
// const DATEMULTIRANGE_OID: i32 = 4535;
// const INT8MULTIRANGE_OID: i32 = 4536;
// const INT4MULTIRANGE_ARRAY_OID: i32 = 6150;
// const NUMMULTIRANGE_ARRAY_OID: i32 = 6151;
// const TSMULTIRANGE_ARRAY_OID: i32 = 6152;
// const TSTZMULTIRANGE_ARRAY_OID: i32 = 6153;
// const DATEMULTIRANGE_ARRAY_OID: i32 = 6155;
// const INT8MULTIRANGE_ARRAY_OID: i32 = 6157;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum PgValueType {
    Float32,
    Float64,
    Int16,
    Int32,
    Int64,
    Boolean,
    Struct,
    Char,
    String,
    Timestamp,
    TimestampTZ,
    Date,
    Time,
    TimeTZ,
    Interval,
    Numeric,
    Bytes,
    UUID,
    JSON,
    HStore,
    Geography,
    Geometry,
    Point,
    CIDR,
    INET,
    Macaddr,
    ArrayFloat32,
    ArrayFloat64,
    ArrayInt16,
    ArrayInt32,
    ArrayInt64,
    ArrayString,
    ArrayDate,
    ArrayTimestamp,
    ArrayTimestampTZ,
    ArrayBoolean,
}

impl PgValueType {
    pub fn from_oid(oid: i32) -> Self {
        match oid {
            BOOL_OID => PgValueType::Boolean,
            INT2_OID => PgValueType::Int16,
            INT4_OID => PgValueType::Int32,
            INT8_OID | OID_OID => PgValueType::Int64,
            FLOAT4_OID => PgValueType::Float32,
            FLOAT8_OID => PgValueType::Float64,
            CHAR_OID => PgValueType::Char,
            TEXT_OID | VARCHAR_OID | BPCHAR_OID => PgValueType::String,
            BYTEA_OID => PgValueType::Bytes,
            JSON_OID | JSONB_OID => PgValueType::JSON,
            UUID_OID => PgValueType::UUID,
            CIDR_OID => PgValueType::CIDR,
            MACADDR_OID => PgValueType::Macaddr,
            INET_OID => PgValueType::INET,
            INTERVAL_OID => PgValueType::Interval,
            DATE_OID => PgValueType::Date,
            TIME_OID => PgValueType::Time,
            TIMETZ_OID => PgValueType::TimeTZ,
            TIMESTAMP_OID => PgValueType::Timestamp,
            TIMESTAMPTZ_OID => PgValueType::TimestampTZ,
            NUMERIC_OID => PgValueType::Numeric,
            POINT_OID => PgValueType::Point,
            INT2_ARRAY_OID => PgValueType::ArrayInt16,
            INT4_ARRAY_OID => PgValueType::ArrayInt32,
            INT8_ARRAY_OID => PgValueType::ArrayInt64,
            FLOAT4_ARRAY_OID => PgValueType::ArrayFloat32,
            FLOAT8_ARRAY_OID => PgValueType::ArrayFloat64,
            BOOL_ARRAY_OID => PgValueType::ArrayBoolean,
            DATE_ARRAY_OID => PgValueType::ArrayDate,
            TIMESTAMP_ARRAY_OID => PgValueType::ArrayTimestamp,
            TIMESTAMPTZ_ARRAY_OID => PgValueType::ArrayTimestampTZ,
            TEXT_ARRAY_OID | VARCHAR_ARRAY_OID | BPCHAR_ARRAY_OID => PgValueType::ArrayString,
            _ => PgValueType::String,
        }
    }

    pub fn from_alias(alias: &str) -> Self {
        match alias {
            "bool" => PgValueType::Boolean,
            "int2" => PgValueType::Int16,
            "int4" => PgValueType::Int32,
            "int8" | "oid" => PgValueType::Int64,
            "float4" => PgValueType::Float32,
            "float8" => PgValueType::Float64,
            "\"char\"" => PgValueType::Char,
            "text" | "varchar" | "bpchar" => PgValueType::String,
            "bytea" => PgValueType::Bytes,
            "json" | "jsonb" => PgValueType::JSON,
            "uuid" => PgValueType::UUID,
            "cidr" => PgValueType::CIDR,
            "macaddr" => PgValueType::Macaddr,
            "inet" => PgValueType::INET,
            "interval" => PgValueType::Interval,
            "date" => PgValueType::Date,
            "time" => PgValueType::Time,
            "timetz" => PgValueType::TimeTZ,
            "timestamp" => PgValueType::Timestamp,
            "timestamptz" => PgValueType::TimestampTZ,
            "numeric" => PgValueType::Numeric,
            "point" => PgValueType::Point,
            "_int2" => PgValueType::ArrayInt16,
            "_int4" => PgValueType::ArrayInt32,
            "_int8" => PgValueType::ArrayInt64,
            "_float4" => PgValueType::ArrayFloat32,
            "_float8" => PgValueType::ArrayFloat64,
            "_bool" => PgValueType::ArrayBoolean,
            "_date" => PgValueType::ArrayDate,
            "_timestamp" => PgValueType::ArrayTimestamp,
            "_timestamptz" => PgValueType::ArrayTimestampTZ,
            "_text" | "_varchar" | "_bpchar" => PgValueType::ArrayString,
            _ => PgValueType::String,
        }
    }
}
