use sqlx::{
    mysql::MySqlArguments,
    postgres::{types::Oid, PgArguments},
    query::Query,
    MySql, Postgres,
};

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
            if matches!(value, ColValue::None | ColValue::UnchangedToast) {
                return bind_pg_null(self, col_type);
            }

            match col_type.value_type {
                PgValueType::Boolean => return self.bind(as_bool(value)),
                PgValueType::Int16 => return self.bind(as_i16(value)),
                PgValueType::Int32 => return self.bind(as_i32(value)),
                PgValueType::Int64 => {
                    if col_type.alias == "oid" {
                        return self.bind(Oid(as_u32(value)));
                    }
                    return self.bind(as_i64(value));
                }
                PgValueType::Float32 => return self.bind(as_f32(value)),
                PgValueType::Float64 => return self.bind(as_f64(value)),
                _ => {}
            }

            match value {
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
                _ => bind_pg_null(self, col_type),
            }
        } else {
            bind_pg_null(self, col_type)
        }
    }
}

fn bind_pg_null<'q>(
    query: Query<'q, Postgres, PgArguments>,
    col_type: &PgColType,
) -> Query<'q, Postgres, PgArguments> {
    match col_type.value_type {
        PgValueType::Boolean => query.bind(Option::<bool>::None),
        PgValueType::Int16 => query.bind(Option::<i16>::None),
        PgValueType::Int32 => query.bind(Option::<i32>::None),
        PgValueType::Int64 if col_type.alias == "oid" => query.bind(Option::<Oid>::None),
        PgValueType::Int64 => query.bind(Option::<i64>::None),
        PgValueType::Float32 => query.bind(Option::<f32>::None),
        PgValueType::Float64 => query.bind(Option::<f64>::None),
        _ => query.bind(Option::<String>::None),
    }
}

fn as_bool(value: &ColValue) -> bool {
    match value {
        ColValue::Bool(v) => *v,
        ColValue::Tiny(v) => *v != 0,
        ColValue::UnsignedTiny(v) => *v != 0,
        ColValue::Short(v) => *v != 0,
        ColValue::UnsignedShort(v) => *v != 0,
        ColValue::Long(v) => *v != 0,
        ColValue::UnsignedLong(v) => *v != 0,
        ColValue::LongLong(v) => *v != 0,
        ColValue::UnsignedLongLong(v) => *v != 0,
        ColValue::String(v) => matches!(v.to_ascii_lowercase().as_str(), "t" | "true" | "1"),
        _ => false,
    }
}

fn as_i16(value: &ColValue) -> i16 {
    match value {
        ColValue::Tiny(v) => *v as i16,
        ColValue::UnsignedTiny(v) => *v as i16,
        ColValue::Short(v) => *v,
        ColValue::UnsignedShort(v) => *v as i16,
        ColValue::Long(v) => *v as i16,
        ColValue::UnsignedLong(v) => *v as i16,
        ColValue::LongLong(v) => *v as i16,
        ColValue::UnsignedLongLong(v) => *v as i16,
        ColValue::String(v) => v.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn as_i32(value: &ColValue) -> i32 {
    match value {
        ColValue::Tiny(v) => *v as i32,
        ColValue::UnsignedTiny(v) => *v as i32,
        ColValue::Short(v) => *v as i32,
        ColValue::UnsignedShort(v) => *v as i32,
        ColValue::Long(v) => *v,
        ColValue::UnsignedLong(v) => *v as i32,
        ColValue::LongLong(v) => *v as i32,
        ColValue::UnsignedLongLong(v) => *v as i32,
        ColValue::String(v) => v.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn as_i64(value: &ColValue) -> i64 {
    match value {
        ColValue::Tiny(v) => *v as i64,
        ColValue::UnsignedTiny(v) => *v as i64,
        ColValue::Short(v) => *v as i64,
        ColValue::UnsignedShort(v) => *v as i64,
        ColValue::Long(v) => *v as i64,
        ColValue::UnsignedLong(v) => *v as i64,
        ColValue::LongLong(v) => *v,
        ColValue::UnsignedLongLong(v) => *v as i64,
        ColValue::String(v) => v.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn as_u32(value: &ColValue) -> u32 {
    match value {
        ColValue::Tiny(v) => *v as u32,
        ColValue::UnsignedTiny(v) => *v as u32,
        ColValue::Short(v) => *v as u32,
        ColValue::UnsignedShort(v) => *v as u32,
        ColValue::Long(v) => *v as u32,
        ColValue::UnsignedLong(v) => *v,
        ColValue::LongLong(v) => *v as u32,
        ColValue::UnsignedLongLong(v) => *v as u32,
        ColValue::String(v) => v.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn as_f32(value: &ColValue) -> f32 {
    match value {
        ColValue::Tiny(v) => *v as f32,
        ColValue::UnsignedTiny(v) => *v as f32,
        ColValue::Short(v) => *v as f32,
        ColValue::UnsignedShort(v) => *v as f32,
        ColValue::Long(v) => *v as f32,
        ColValue::UnsignedLong(v) => *v as f32,
        ColValue::LongLong(v) => *v as f32,
        ColValue::UnsignedLongLong(v) => *v as f32,
        ColValue::Float(v) => *v,
        ColValue::Double(v) => *v as f32,
        ColValue::String(v) => v.parse().unwrap_or_default(),
        _ => 0.0,
    }
}

fn as_f64(value: &ColValue) -> f64 {
    match value {
        ColValue::Tiny(v) => *v as f64,
        ColValue::UnsignedTiny(v) => *v as f64,
        ColValue::Short(v) => *v as f64,
        ColValue::UnsignedShort(v) => *v as f64,
        ColValue::Long(v) => *v as f64,
        ColValue::UnsignedLong(v) => *v as f64,
        ColValue::LongLong(v) => *v as f64,
        ColValue::UnsignedLongLong(v) => *v as f64,
        ColValue::Float(v) => *v as f64,
        ColValue::Double(v) => *v,
        ColValue::String(v) => v.parse().unwrap_or_default(),
        _ => 0.0,
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
