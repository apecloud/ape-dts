use sqlx::{mysql::MySqlArguments, postgres::PgArguments, query::Query, MySql, Postgres};

use crate::meta::{
    col_value::ColValue,
    mysql::mysql_col_type::MysqlColType,
    pg::{pg_col_type::PgColType, pg_value_type::PgValueType},
};

pub trait SqlxPgExt<'q> {
    fn bind_col_value<'b: 'q>(self, col_value: Option<&'b ColValue>, col_type: &PgColType) -> Self;
}

impl<'q> SqlxPgExt<'q> for Query<'q, Postgres, PgArguments> {
    fn bind_col_value<'b: 'q>(self, col_value: Option<&'b ColValue>, col_type: &PgColType) -> Self {
        if let Some(value) = col_value {
            match value {
                ColValue::Bool(v) => self.bind(v),
                ColValue::Tiny(v) => self.bind(v),
                ColValue::Short(v) => self.bind(v),
                ColValue::Long(v) => self.bind(v),
                ColValue::LongLong(v) => self.bind(v),
                ColValue::Float(v) => self.bind(v),
                ColValue::Double(v) => self.bind(v),
                ColValue::Decimal(v) => self.bind(v),
                ColValue::Time(v) => self.bind(v),
                ColValue::Date(v) => self.bind(v),
                ColValue::DateTime(v) => self.bind(v),
                ColValue::Timestamp(v) => self.bind(v),
                ColValue::String(v) => self.bind(v),
                ColValue::Json2(v) => self.bind(v),
                ColValue::RawString(v) => self.bind(v),
                ColValue::Blob(v) => {
                    if col_type.value_type == PgValueType::Bytes {
                        let bytea_str = format!(r#"\x{}"#, hex::encode(v));
                        self.bind(bytea_str)
                    } else {
                        self.bind(v)
                    }
                }
                ColValue::Set2(v) => self.bind(v),
                ColValue::Enum2(v) => self.bind(v),
                ColValue::Json(v) => self.bind(v),
                _ => match col_type.value_type {
                    PgValueType::Boolean => {
                        let none: Option<bool> = Option::None;
                        self.bind(none)
                    }

                    PgValueType::Int32 => {
                        let none: Option<i32> = Option::None;
                        self.bind(none)
                    }

                    PgValueType::Int16 => {
                        let none: Option<i16> = Option::None;
                        self.bind(none)
                    }

                    PgValueType::Int64 => {
                        let none: Option<i64> = Option::None;
                        self.bind(none)
                    }

                    PgValueType::Float32 => {
                        let none: Option<f32> = Option::None;
                        self.bind(none)
                    }

                    PgValueType::Float64 => {
                        let none: Option<f64> = Option::None;
                        self.bind(none)
                    }

                    _ => {
                        let none: Option<String> = Option::None;
                        self.bind(none)
                    }
                },
            }
        } else {
            let none: Option<String> = Option::None;
            self.bind(none)
        }
    }
}

pub trait SqlxMysqlExt<'q> {
    fn bind_col_value<'b: 'q>(
        self,
        col_value: Option<&'b ColValue>,
        col_type: &MysqlColType,
    ) -> Self;
}

impl<'q> SqlxMysqlExt<'q> for Query<'q, MySql, MySqlArguments> {
    fn bind_col_value<'b: 'q>(
        self,
        col_value: Option<&'b ColValue>,
        _col_type: &MysqlColType,
    ) -> Self {
        if let Some(value) = col_value {
            match value {
                ColValue::Tiny(v) => self.bind(v),
                ColValue::UnsignedTiny(v) => self.bind(v),
                ColValue::Short(v) => self.bind(v),
                ColValue::UnsignedShort(v) => self.bind(v),
                ColValue::Long(v) => self.bind(v),
                ColValue::UnsignedLong(v) => self.bind(v),
                ColValue::LongLong(v) => self.bind(v),
                ColValue::UnsignedLongLong(v) => self.bind(v),
                ColValue::Float(v) => self.bind(v),
                ColValue::Double(v) => self.bind(v),
                ColValue::Decimal(v) => self.bind(v),
                ColValue::Time(v) => self.bind(v),
                ColValue::Date(v) => self.bind(v),
                ColValue::DateTime(v) => self.bind(v),
                ColValue::Timestamp(v) => self.bind(v),
                ColValue::Year(v) => self.bind(v),
                ColValue::String(v) => self.bind(v),
                ColValue::RawString(v) => self.bind(v),
                ColValue::Blob(v) => self.bind(v),
                ColValue::Bit(v) => self.bind(v),
                ColValue::Set(v) => self.bind(v),
                ColValue::Set2(v) => self.bind(v),
                ColValue::Enum(v) => self.bind(v),
                ColValue::Enum2(v) => self.bind(v),
                ColValue::Json(v) => self.bind(v),
                ColValue::Json2(v) => self.bind(v),
                _ => {
                    let none: Option<String> = Option::None;
                    self.bind(none)
                }
            }
        } else {
            let none: Option<String> = Option::None;
            self.bind(none)
        }
    }
}
