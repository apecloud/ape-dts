use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Display)]
pub enum MysqlColType {
    Unknown,
    TinyInt {
        unsigned: bool,
    },
    SmallInt {
        unsigned: bool,
    },
    MediumInt {
        unsigned: bool,
    },
    Int {
        unsigned: bool,
    },
    BigInt {
        unsigned: bool,
    },
    Float,
    Double,
    Decimal {
        precision: u32,
        scale: u32,
    },
    Time {
        precision: u32,
    },
    Date {
        is_nullable: bool,
    },
    DateTime {
        precision: u32,
        is_nullable: bool,
    },
    // timezone diff with utc in seconds
    // refer: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    Timestamp {
        precision: u32,
        timezone_offset: i64,
        is_nullable: bool,
    },
    Year,
    // for char(length), the maximum length is 255,
    // for varchar(length), the maximum length is 65535
    // refer: https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
    Char {
        length: u64,
        charset: String,
    },
    Varchar {
        length: u64,
        charset: String,
    },
    TinyText {
        length: u64,
        charset: String,
    },
    MediumText {
        length: u64,
        charset: String,
    },
    Text {
        length: u64,
        charset: String,
    },
    LongText {
        length: u64,
        charset: String,
    },
    Binary {
        length: u8,
    },
    VarBinary {
        length: u16,
    },
    TinyBlob,
    MediumBlob,
    LongBlob,
    Blob,
    Bit,
    Set {
        items: HashMap<u64, String>,
    },
    Enum {
        items: Vec<String>,
    },
    Json,
}

impl MysqlColType {
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::TinyInt { .. }
                | Self::SmallInt { .. }
                | Self::MediumInt { .. }
                | Self::Int { .. }
                | Self::BigInt { .. }
        )
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float | Self::Double)
    }

    pub fn is_decimal(&self) -> bool {
        matches!(self, Self::Decimal { .. })
    }

    pub fn is_string(&self) -> bool {
        matches!(
            self,
            Self::Char { .. }
                | Self::Varchar { .. }
                | Self::TinyText { .. }
                | Self::MediumText { .. }
                | Self::Text { .. }
                | Self::LongText { .. }
        )
    }
}
