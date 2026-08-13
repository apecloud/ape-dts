use std::{borrow::Cow, str::FromStr};

use anyhow::{bail, Context};
use tiberius::{
    numeric::BigDecimal,
    time::chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime},
    xml::XmlData,
    FromSql, IntoSql, Query, Row, Uuid,
};

use crate::{
    config::config_enums::DbType,
    error::DtError,
    meta::{col_value::ColValue, mssql::mssql_col_type::MssqlColType},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MssqlBindKind {
    Bool,
    U8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Money,
    Decimal,
    Text,
    Binary,
    Uuid,
    Xml,
    Date,
    Time,
    NaiveDateTime,
    DateTimeOffset,
}

pub struct MssqlColValueConvertor;

impl MssqlColValueConvertor {
    pub fn from_query(row: &Row, col: &str) -> anyhow::Result<ColValue> {
        let mut matching_columns = row
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| column.name() == col);
        let index = matching_columns
            .next()
            .map(|(index, _)| index)
            .ok_or_else(|| {
                DtError::DatabaseInvariant(
                    DbType::Mssql,
                    format!("query result does not contain column {col}"),
                )
            })?;

        if matching_columns.next().is_some() {
            bail!(DtError::DatabaseInvariant(
                DbType::Mssql,
                format!("query result contains duplicate column name {col}"),
            ));
        }

        Self::from_query_at(row, index)
    }

    pub fn from_query_required_string(row: &Row, col: &str) -> anyhow::Result<String> {
        Self::from_query_required(row, col, "String", |value| match value {
            ColValue::String(value) => Some(value),
            _ => None,
        })
    }

    pub fn from_query_optional_string(row: &Row, col: &str) -> anyhow::Result<Option<String>> {
        let value = Self::from_query(row, col)?;
        let actual_type = value.type_name();
        match value {
            ColValue::String(value) => Ok(Some(value)),
            ColValue::None => Ok(None),
            _ => Err(DtError::DatabaseInvariant(
                DbType::Mssql,
                format!(
                    "query result column {col} returned ColValue::{actual_type}, expected \
                     ColValue::String or ColValue::None"
                ),
            )
            .into()),
        }
    }

    pub fn from_query_required_i16(row: &Row, col: &str) -> anyhow::Result<i16> {
        Self::from_query_required(row, col, "Short", |value| match value {
            ColValue::Short(value) => Some(value),
            _ => None,
        })
    }

    pub fn from_query_required_u8(row: &Row, col: &str) -> anyhow::Result<u8> {
        Self::from_query_required(row, col, "UnsignedTiny", |value| match value {
            ColValue::UnsignedTiny(value) => Some(value),
            _ => None,
        })
    }

    pub fn from_query_required_i64(row: &Row, col: &str) -> anyhow::Result<i64> {
        Self::from_query_required(row, col, "LongLong", |value| match value {
            ColValue::LongLong(value) => Some(value),
            _ => None,
        })
    }

    pub fn from_query_required_bool(row: &Row, col: &str) -> anyhow::Result<bool> {
        Self::from_query_required(row, col, "Bool", |value| match value {
            ColValue::Bool(value) => Some(value),
            _ => None,
        })
    }

    fn from_query_required<T>(
        row: &Row,
        col: &str,
        expected_type: &str,
        convert: impl FnOnce(ColValue) -> Option<T>,
    ) -> anyhow::Result<T> {
        let value = Self::from_query(row, col)?;
        let actual_type = value.type_name();
        convert(value).ok_or_else(|| {
            DtError::DatabaseInvariant(
                DbType::Mssql,
                format!(
                    "query result column {col} returned ColValue::{actual_type}, expected \
                     ColValue::{expected_type}"
                ),
            )
            .into()
        })
    }

    fn from_query_at(row: &Row, index: usize) -> anyhow::Result<ColValue> {
        let col_type = row.columns()[index].column_type();
        match col_type {
            MssqlColType::Null => {
                let value = row.try_get::<bool, _>(index)?;
                if value.is_some() {
                    bail!(DtError::DatabaseInvariant(
                        DbType::Mssql,
                        "Tiberius returned a value for ColumnType::Null".to_string(),
                    ));
                }
                Ok(ColValue::None)
            }
            MssqlColType::Bit | MssqlColType::Bitn => {
                Self::try_get_as::<bool>(row, index, ColValue::Bool)
            }
            MssqlColType::Int1 => Self::try_get_as::<u8>(row, index, ColValue::UnsignedTiny),
            MssqlColType::Int2 => Self::try_get_as::<i16>(row, index, ColValue::Short),
            MssqlColType::Int4 => Self::try_get_as::<i32>(row, index, ColValue::Long),
            MssqlColType::Int8 => Self::try_get_as::<i64>(row, index, ColValue::LongLong),
            MssqlColType::Float4 => Self::try_get_as::<f32>(row, index, ColValue::Float),
            MssqlColType::Float8 | MssqlColType::Money | MssqlColType::Money4 => {
                Self::try_get_as::<f64>(row, index, ColValue::Double)
            }
            MssqlColType::Decimaln | MssqlColType::Numericn => {
                Self::try_get_as::<BigDecimal>(row, index, |value| {
                    ColValue::Decimal(value.to_string())
                })
            }
            MssqlColType::BigVarChar
            | MssqlColType::BigChar
            | MssqlColType::NVarchar
            | MssqlColType::NChar
            | MssqlColType::Text
            | MssqlColType::NText => {
                Self::try_get_as::<&str>(row, index, |value| ColValue::String(value.to_owned()))
            }
            MssqlColType::BigVarBin | MssqlColType::BigBinary | MssqlColType::Image => {
                Self::try_get_as::<&[u8]>(row, index, |value| ColValue::Blob(value.to_vec()))
            }
            MssqlColType::Guid => {
                Self::try_get_as::<Uuid>(row, index, |value| ColValue::String(value.to_string()))
            }
            MssqlColType::Xml => Self::try_get_as::<&XmlData>(row, index, |value| {
                ColValue::String(value.to_string())
            }),
            MssqlColType::Datetime4
            | MssqlColType::Datetime
            | MssqlColType::Datetimen
            | MssqlColType::Datetime2 => Self::try_get_as::<NaiveDateTime>(row, index, |value| {
                ColValue::DateTime(value.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            }),
            MssqlColType::Daten => Self::try_get_as::<NaiveDate>(row, index, |value| {
                ColValue::Date(value.format("%Y-%m-%d").to_string())
            }),
            MssqlColType::Timen => Self::try_get_as::<NaiveTime>(row, index, |value| {
                ColValue::Time(value.format("%H:%M:%S%.f").to_string())
            }),
            MssqlColType::DatetimeOffsetn => {
                Self::try_get_as::<DateTime<FixedOffset>>(row, index, |value| {
                    ColValue::Timestamp(value.to_rfc3339())
                })
            }
            MssqlColType::Intn
            | MssqlColType::Floatn
            | MssqlColType::Udt
            | MssqlColType::SSVariant => {
                bail!(DtError::DatabaseUnsupportedTableStructure(
                    DbType::Mssql,
                    format!("MSSQL column type {col_type:?} is not supported"),
                ))
            }
        }
    }

    fn try_get_as<'a, T>(
        row: &'a Row,
        index: usize,
        map: impl FnOnce(T) -> ColValue,
    ) -> anyhow::Result<ColValue>
    where
        T: FromSql<'a>,
    {
        let value = row.try_get::<T, _>(index)?;
        Ok(value.map(map).unwrap_or(ColValue::None))
    }

    pub fn from_str(col_type: &MssqlColType, value: &str) -> anyhow::Result<ColValue> {
        let parsed = match bind_kind(col_type)? {
            MssqlBindKind::Bool => ColValue::Bool(parse_bool(value).ok_or_else(|| {
                DtError::DatabaseStatementFailed(
                    DbType::Mssql,
                    format!("invalid bit value: {value}"),
                )
            })?),
            MssqlBindKind::U8 => ColValue::UnsignedTiny(value.parse()?),
            MssqlBindKind::I16 => ColValue::Short(value.parse()?),
            MssqlBindKind::I32 => ColValue::Long(value.parse()?),
            MssqlBindKind::I64 => ColValue::LongLong(value.parse()?),
            MssqlBindKind::F32 => ColValue::Float(parse_finite_f32(value)?),
            MssqlBindKind::F64 => ColValue::Double(parse_finite_f64(value)?),
            MssqlBindKind::Money => ColValue::Decimal(parse_decimal(value)?.to_string()),
            MssqlBindKind::Decimal => ColValue::Decimal(parse_decimal(value)?.to_string()),
            MssqlBindKind::Text => ColValue::String(value.to_string()),
            MssqlBindKind::Binary => ColValue::Blob(hex::decode(value)?),
            MssqlBindKind::Uuid => ColValue::String(Uuid::parse_str(value)?.to_string()),
            MssqlBindKind::Xml => ColValue::String(XmlData::new(value).to_string()),
            MssqlBindKind::Date => ColValue::Date(
                NaiveDate::parse_from_str(value, "%Y-%m-%d")?
                    .format("%Y-%m-%d")
                    .to_string(),
            ),
            MssqlBindKind::Time => ColValue::Time(
                NaiveTime::parse_from_str(value, "%H:%M:%S%.f")?
                    .format("%H:%M:%S%.f")
                    .to_string(),
            ),
            MssqlBindKind::NaiveDateTime => ColValue::DateTime(
                NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")?
                    .format("%Y-%m-%d %H:%M:%S%.f")
                    .to_string(),
            ),
            MssqlBindKind::DateTimeOffset => {
                ColValue::Timestamp(DateTime::parse_from_rfc3339(value)?.to_rfc3339())
            }
        };
        Ok(parsed)
    }

    pub fn bind<'a>(
        query: &mut Query<'a>,
        value: &'a ColValue,
        col_type: &MssqlColType,
    ) -> anyhow::Result<()> {
        let result = match bind_kind(col_type)? {
            MssqlBindKind::Bool => Self::bind_as(query, value, as_bool_checked),
            MssqlBindKind::U8 => Self::bind_as(query, value, as_u8_checked),
            MssqlBindKind::I16 => Self::bind_as(query, value, as_i16_checked),
            MssqlBindKind::I32 => Self::bind_as(query, value, as_i32_checked),
            MssqlBindKind::I64 => Self::bind_as(query, value, as_i64_checked),
            MssqlBindKind::F32 => Self::bind_as(query, value, as_f32_checked),
            MssqlBindKind::F64 => Self::bind_as(query, value, as_f64_checked),
            MssqlBindKind::Money => Self::bind_as(query, value, as_money_decimal),
            MssqlBindKind::Decimal => Self::bind_as(query, value, as_big_decimal),
            MssqlBindKind::Text => Self::bind_as(query, value, as_text),
            MssqlBindKind::Binary => Self::bind_as(query, value, as_binary),
            MssqlBindKind::Uuid => Self::bind_as(query, value, parse_uuid),
            MssqlBindKind::Xml => Self::bind_as(query, value, parse_xml),
            MssqlBindKind::Date => Self::bind_as(query, value, parse_date),
            MssqlBindKind::Time => Self::bind_as(query, value, parse_time),
            MssqlBindKind::NaiveDateTime => Self::bind_as(query, value, parse_datetime),
            MssqlBindKind::DateTimeOffset => Self::bind_as(query, value, parse_datetime_offset),
        };

        result.with_context(|| {
            format!(
                "failed to bind ColValue::{} as MSSQL {col_type:?}",
                value.type_name()
            )
        })
    }

    fn bind_as<'a, T>(
        query: &mut Query<'a>,
        value: &'a ColValue,
        convert: impl FnOnce(&'a ColValue) -> anyhow::Result<T>,
    ) -> anyhow::Result<()>
    where
        Option<T>: IntoSql<'a> + 'a,
    {
        let value = match value {
            ColValue::None => None,
            ColValue::UnchangedToast => bail!(DtError::InvariantViolated(
                "cannot bind ColValue::UnchangedToast to MSSQL".to_string(),
            )),
            value => Some(convert(value)?),
        };
        query.bind(value);
        Ok(())
    }
}

fn bind_kind(col_type: &MssqlColType) -> anyhow::Result<MssqlBindKind> {
    let kind = match col_type {
        MssqlColType::Bit | MssqlColType::Bitn => MssqlBindKind::Bool,
        MssqlColType::Int1 => MssqlBindKind::U8,
        MssqlColType::Int2 => MssqlBindKind::I16,
        MssqlColType::Int4 => MssqlBindKind::I32,
        MssqlColType::Int8 => MssqlBindKind::I64,
        MssqlColType::Float4 => MssqlBindKind::F32,
        MssqlColType::Float8 => MssqlBindKind::F64,
        MssqlColType::Money | MssqlColType::Money4 => MssqlBindKind::Money,
        MssqlColType::Decimaln | MssqlColType::Numericn => MssqlBindKind::Decimal,
        MssqlColType::BigVarChar
        | MssqlColType::BigChar
        | MssqlColType::NVarchar
        | MssqlColType::NChar
        | MssqlColType::Text
        | MssqlColType::NText => MssqlBindKind::Text,
        MssqlColType::BigVarBin | MssqlColType::BigBinary | MssqlColType::Image => {
            MssqlBindKind::Binary
        }
        MssqlColType::Guid => MssqlBindKind::Uuid,
        MssqlColType::Xml => MssqlBindKind::Xml,
        MssqlColType::Daten => MssqlBindKind::Date,
        MssqlColType::Timen => MssqlBindKind::Time,
        MssqlColType::Datetime4
        | MssqlColType::Datetime
        | MssqlColType::Datetimen
        | MssqlColType::Datetime2 => MssqlBindKind::NaiveDateTime,
        MssqlColType::DatetimeOffsetn => MssqlBindKind::DateTimeOffset,
        MssqlColType::Null
        | MssqlColType::Intn
        | MssqlColType::Floatn
        | MssqlColType::Udt
        | MssqlColType::SSVariant => bail!(DtError::DatabaseUnsupportedTableStructure(
            DbType::Mssql,
            format!("column type {col_type:?} is not supported"),
        )),
    };
    Ok(kind)
}

fn invalid_value(value: &ColValue, target: &str, detail: impl std::fmt::Display) -> anyhow::Error {
    DtError::DatabaseStatementFailed(
        DbType::Mssql,
        format!(
            "cannot convert ColValue::{} to {target}: {detail}",
            value.type_name()
        ),
    )
    .into()
}

fn as_bool_checked(value: &ColValue) -> anyhow::Result<bool> {
    match value {
        ColValue::Bool(value) => Ok(*value),
        value if value.is_integer() => match value.convert_into_integer_128()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid_value(value, "bit", "integer must be 0 or 1")),
        },
        original @ ColValue::String(value) => parse_bool(value)
            .ok_or_else(|| invalid_value(original, "bit", "invalid boolean string")),
        original @ ColValue::RawString(value) => {
            let value_str = std::str::from_utf8(value)?;
            parse_bool(value_str)
                .ok_or_else(|| invalid_value(original, "bit", "invalid boolean string"))
        }
        _ => Err(invalid_value(value, "bit", "incompatible value type")),
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    if value == "1" || value.eq_ignore_ascii_case("true") {
        Some(true)
    } else if value == "0" || value.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

fn parse_finite_f32(value: &str) -> anyhow::Result<f32> {
    let parsed = value.parse::<f32>()?;
    if parsed.is_finite() {
        Ok(parsed)
    } else {
        bail!(DtError::DatabaseStatementFailed(
            DbType::Mssql,
            format!("real value must be finite: {value}"),
        ))
    }
}

fn parse_finite_f64(value: &str) -> anyhow::Result<f64> {
    let parsed = value.parse::<f64>()?;
    if parsed.is_finite() {
        Ok(parsed)
    } else {
        bail!(DtError::DatabaseStatementFailed(
            DbType::Mssql,
            format!("float value must be finite: {value}"),
        ))
    }
}

fn as_u8_checked(value: &ColValue) -> anyhow::Result<u8> {
    u8::try_from(value.convert_into_integer_128()?)
        .map_err(|error| invalid_value(value, "tinyint", error))
}

fn as_i16_checked(value: &ColValue) -> anyhow::Result<i16> {
    i16::try_from(value.convert_into_integer_128()?)
        .map_err(|error| invalid_value(value, "smallint", error))
}

fn as_i32_checked(value: &ColValue) -> anyhow::Result<i32> {
    i32::try_from(value.convert_into_integer_128()?)
        .map_err(|error| invalid_value(value, "int", error))
}

fn as_i64_checked(value: &ColValue) -> anyhow::Result<i64> {
    i64::try_from(value.convert_into_integer_128()?)
        .map_err(|error| invalid_value(value, "bigint", error))
}

fn as_f32_checked(value: &ColValue) -> anyhow::Result<f32> {
    let converted = as_f64_checked(value)? as f32;
    if converted.is_finite() {
        Ok(converted)
    } else {
        Err(invalid_value(value, "real", "value is out of range"))
    }
}

fn as_f64_checked(value: &ColValue) -> anyhow::Result<f64> {
    let converted = match value {
        ColValue::Float(value) => *value as f64,
        ColValue::Double(value) => *value,
        ColValue::Decimal(value) => {
            decimal_literal_shape(value)?;
            value.parse::<f64>()?
        }
        value if value.is_integer() => value.convert_into_integer_128()? as f64,
        _ => return Err(invalid_value(value, "float", "incompatible value type")),
    };

    if converted.is_finite() {
        Ok(converted)
    } else {
        Err(invalid_value(value, "float", "value must be finite"))
    }
}

fn as_big_decimal(value: &ColValue) -> anyhow::Result<BigDecimal> {
    match value {
        ColValue::Decimal(value) => parse_decimal(value),
        value if value.is_integer() => {
            parse_decimal(&value.convert_into_integer_128()?.to_string())
        }
        _ => Err(invalid_value(value, "decimal", "incompatible value type")),
    }
}

fn as_money_decimal(value: &ColValue) -> anyhow::Result<BigDecimal> {
    match value {
        ColValue::Float(value) if value.is_finite() => parse_decimal(&value.to_string()),
        ColValue::Double(value) if value.is_finite() => parse_decimal(&value.to_string()),
        ColValue::Float(_) | ColValue::Double(_) => {
            Err(invalid_value(value, "money", "value must be finite"))
        }
        _ => as_big_decimal(value),
    }
}

fn parse_decimal(value: &str) -> anyhow::Result<BigDecimal> {
    validate_decimal_literal(value)?;
    BigDecimal::from_str(value).context(DtError::DatabaseStatementFailed(
        DbType::Mssql,
        "invalid decimal value".to_string(),
    ))
}

fn validate_decimal_literal(value: &str) -> anyhow::Result<()> {
    let (precision, scale) = decimal_literal_shape(value)?;
    if precision > 38 {
        bail!(DtError::DatabaseStatementFailed(
            DbType::Mssql,
            "decimal value exceeds SQL Server precision 38".to_string(),
        ));
    }
    // Tiberius 0.12.3 Numeric::new_with_scale asserts that scale is below 38.
    if scale >= 38 {
        bail!(DtError::DatabaseStatementFailed(
            DbType::Mssql,
            "Tiberius 0.12.3 cannot bind decimal values with scale 38".to_string(),
        ));
    }
    Ok(())
}

fn decimal_literal_shape(value: &str) -> anyhow::Result<(usize, usize)> {
    let unsigned = value
        .strip_prefix('+')
        .or_else(|| value.strip_prefix('-'))
        .unwrap_or(value);
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();

    if value.is_empty()
        || unsigned.is_empty()
        || (integer.is_empty() && fraction.is_empty())
        || parts.next().is_some()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!(DtError::DatabaseStatementFailed(
            DbType::Mssql,
            "decimal value must use plain base-10 notation".to_string(),
        ));
    }

    let precision = integer
        .bytes()
        .chain(fraction.bytes())
        .skip_while(|byte| *byte == b'0')
        .count()
        .max(1);
    Ok((precision, fraction.len()))
}

fn as_text<'a>(value: &'a ColValue) -> anyhow::Result<Cow<'a, str>> {
    let value: Cow<'a, str> = match value {
        ColValue::Bool(value) => Cow::Owned(value.to_string()),
        ColValue::Tiny(value) => Cow::Owned(value.to_string()),
        ColValue::UnsignedTiny(value) => Cow::Owned(value.to_string()),
        ColValue::Short(value) => Cow::Owned(value.to_string()),
        ColValue::UnsignedShort(value) => Cow::Owned(value.to_string()),
        ColValue::Long(value) => Cow::Owned(value.to_string()),
        ColValue::UnsignedLong(value) => Cow::Owned(value.to_string()),
        ColValue::LongLong(value) => Cow::Owned(value.to_string()),
        ColValue::UnsignedLongLong(value) => Cow::Owned(value.to_string()),
        ColValue::Float(value) if value.is_finite() => Cow::Owned(value.to_string()),
        ColValue::Double(value) if value.is_finite() => Cow::Owned(value.to_string()),
        ColValue::Float(_) | ColValue::Double(_) => {
            return Err(invalid_value(value, "text", "float value must be finite"));
        }
        ColValue::Decimal(value)
        | ColValue::Time(value)
        | ColValue::Date(value)
        | ColValue::DateTime(value)
        | ColValue::Timestamp(value)
        | ColValue::String(value)
        | ColValue::Set2(value)
        | ColValue::Enum2(value)
        | ColValue::Json2(value) => Cow::Borrowed(value),
        ColValue::Year(value) => Cow::Owned(value.to_string()),
        ColValue::RawString(value) | ColValue::Json(value) => {
            Cow::Borrowed(std::str::from_utf8(value)?)
        }
        ColValue::Bit(value) => Cow::Owned(value.to_string()),
        ColValue::Set(value) => Cow::Owned(value.to_string()),
        ColValue::Enum(value) => Cow::Owned(value.to_string()),
        ColValue::Json3(value) => Cow::Owned(value.to_string()),
        ColValue::None
        | ColValue::UnchangedToast
        | ColValue::Blob(_)
        | ColValue::MongoDoc(_)
        | ColValue::MongoRawDoc(_) => {
            return Err(invalid_value(value, "text", "incompatible value type"));
        }
    };
    Ok(value)
}

fn as_binary(value: &ColValue) -> anyhow::Result<Cow<'_, [u8]>> {
    match value {
        ColValue::Blob(value) | ColValue::RawString(value) => Ok(Cow::Borrowed(value.as_slice())),
        _ => Err(invalid_value(value, "binary", "incompatible value type")),
    }
}

fn parse_uuid(value: &ColValue) -> anyhow::Result<Uuid> {
    Ok(Uuid::parse_str(as_utf8_text(value, "uniqueidentifier")?)?)
}

fn parse_xml(value: &ColValue) -> anyhow::Result<XmlData> {
    Ok(XmlData::new(as_utf8_text(value, "xml")?))
}

fn parse_date(value: &ColValue) -> anyhow::Result<NaiveDate> {
    let value = match value {
        ColValue::Date(value) | ColValue::String(value) => value,
        _ => return Err(invalid_value(value, "date", "incompatible value type")),
    };
    Ok(NaiveDate::parse_from_str(value, "%Y-%m-%d")?)
}

fn parse_time(value: &ColValue) -> anyhow::Result<NaiveTime> {
    let value = match value {
        ColValue::Time(value) | ColValue::String(value) => value,
        _ => return Err(invalid_value(value, "time", "incompatible value type")),
    };
    Ok(NaiveTime::parse_from_str(value, "%H:%M:%S%.f")?)
}

fn parse_datetime(value: &ColValue) -> anyhow::Result<NaiveDateTime> {
    let value = match value {
        ColValue::DateTime(value) | ColValue::Timestamp(value) | ColValue::String(value) => value,
        _ => {
            return Err(invalid_value(value, "datetime", "incompatible value type"));
        }
    };
    Ok(NaiveDateTime::parse_from_str(
        value,
        "%Y-%m-%d %H:%M:%S%.f",
    )?)
}

fn parse_datetime_offset(value: &ColValue) -> anyhow::Result<DateTime<FixedOffset>> {
    let value = match value {
        ColValue::Timestamp(value) | ColValue::String(value) => value,
        _ => {
            return Err(invalid_value(
                value,
                "datetimeoffset",
                "incompatible value type",
            ));
        }
    };
    Ok(DateTime::parse_from_rfc3339(value)?)
}

fn as_utf8_text<'a>(value: &'a ColValue, target: &str) -> anyhow::Result<&'a str> {
    match value {
        ColValue::String(value) => Ok(value),
        ColValue::RawString(value) => Ok(std::str::from_utf8(value)?),
        _ => Err(invalid_value(value, target, "incompatible value type")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::mssql::mssql_col_type::parse_mssql_col_type;

    fn col_type(type_name: &str) -> MssqlColType {
        parse_mssql_col_type(type_name).unwrap()
    }

    #[test]
    fn classifies_supported_bind_types() {
        let cases = [
            ("bit", MssqlBindKind::Bool),
            ("tinyint", MssqlBindKind::U8),
            ("smallint", MssqlBindKind::I16),
            ("int", MssqlBindKind::I32),
            ("bigint", MssqlBindKind::I64),
            ("real", MssqlBindKind::F32),
            ("float", MssqlBindKind::F64),
            ("money", MssqlBindKind::Money),
            ("smallmoney", MssqlBindKind::Money),
            ("decimal", MssqlBindKind::Decimal),
            ("numeric", MssqlBindKind::Decimal),
            ("varchar", MssqlBindKind::Text),
            ("char", MssqlBindKind::Text),
            ("nvarchar", MssqlBindKind::Text),
            ("nchar", MssqlBindKind::Text),
            ("text", MssqlBindKind::Text),
            ("ntext", MssqlBindKind::Text),
            ("varbinary", MssqlBindKind::Binary),
            ("binary", MssqlBindKind::Binary),
            ("image", MssqlBindKind::Binary),
            ("rowversion", MssqlBindKind::Binary),
            ("timestamp", MssqlBindKind::Binary),
            ("uniqueidentifier", MssqlBindKind::Uuid),
            ("xml", MssqlBindKind::Xml),
            ("date", MssqlBindKind::Date),
            ("time", MssqlBindKind::Time),
            ("smalldatetime", MssqlBindKind::NaiveDateTime),
            ("datetime", MssqlBindKind::NaiveDateTime),
            ("datetime2", MssqlBindKind::NaiveDateTime),
            ("datetimeoffset", MssqlBindKind::DateTimeOffset),
        ];

        for (type_name, expected) in cases {
            assert_eq!(bind_kind(&col_type(type_name)).unwrap(), expected);
        }
        assert_eq!(
            bind_kind(&col_type(" NVARCHAR ")).unwrap(),
            MssqlBindKind::Text
        );
        assert_eq!(bind_kind(&MssqlColType::Bit).unwrap(), MssqlBindKind::Bool);
        assert_eq!(
            bind_kind(&MssqlColType::Datetimen).unwrap(),
            MssqlBindKind::NaiveDateTime
        );
    }

    #[test]
    fn rejects_unsupported_bind_types() {
        assert!(bind_kind(&col_type("sql_variant")).is_err());

        for col_type in [
            MssqlColType::Null,
            MssqlColType::Intn,
            MssqlColType::Floatn,
            MssqlColType::Udt,
            MssqlColType::SSVariant,
        ] {
            assert!(bind_kind(&col_type).is_err());
        }
    }

    #[test]
    fn converts_integers_without_truncation() {
        assert_eq!(
            as_u8_checked(&ColValue::UnsignedTiny(u8::MAX)).unwrap(),
            u8::MAX
        );
        assert!(as_u8_checked(&ColValue::Long(-1)).is_err());
        assert!(as_u8_checked(&ColValue::UnsignedShort(256)).is_err());

        assert_eq!(
            as_i16_checked(&ColValue::Short(i16::MIN)).unwrap(),
            i16::MIN
        );
        assert!(as_i16_checked(&ColValue::Long(i16::MAX as i32 + 1)).is_err());
        assert!(as_i32_checked(&ColValue::UnsignedLong(i32::MAX as u32 + 1)).is_err());
        assert!(as_i64_checked(&ColValue::UnsignedLongLong(u64::MAX)).is_err());
    }

    #[test]
    fn converts_boolean_values_strictly() {
        assert!(as_bool_checked(&ColValue::Bool(true)).unwrap());
        assert!(!as_bool_checked(&ColValue::Long(0)).unwrap());
        assert!(as_bool_checked(&ColValue::String("TRUE".to_string())).unwrap());
        assert!(!as_bool_checked(&ColValue::RawString(b"false".to_vec())).unwrap());
        assert!(as_bool_checked(&ColValue::Long(2)).is_err());
        assert!(as_bool_checked(&ColValue::String("yes".to_string())).is_err());
        assert!(as_bool_checked(&ColValue::RawString(vec![0xff])).is_err());
    }

    #[test]
    fn validates_float_and_decimal_values() {
        assert_eq!(
            as_f64_checked(&ColValue::Decimal("12.5".to_string())).unwrap(),
            12.5
        );
        assert!(as_f64_checked(&ColValue::Decimal("9".repeat(39))).is_ok());
        assert!(as_f64_checked(&ColValue::Decimal(format!("0.{}1", "0".repeat(37)))).is_ok());
        assert!(as_f64_checked(&ColValue::Double(f64::INFINITY)).is_err());
        assert!(as_f32_checked(&ColValue::Double(f64::MAX)).is_err());

        let max_precision = "9".repeat(38);
        assert_eq!(
            parse_decimal(&max_precision).unwrap().to_string(),
            max_precision
        );
        assert_eq!(parse_decimal("-12.3400").unwrap().to_string(), "-12.3400");
        assert!(parse_decimal(&"9".repeat(39)).is_err());
        assert!(parse_decimal(&format!("0.{}1", "0".repeat(37))).is_err());
        assert!(parse_decimal(&format!("0.{}1", "0".repeat(38))).is_err());
        assert!(parse_decimal("1e2").is_err());
        assert!(parse_decimal("1.2.3").is_err());

        assert_eq!(
            as_money_decimal(&ColValue::Double(12.34))
                .unwrap()
                .to_string(),
            "12.34"
        );
        assert!(as_money_decimal(&ColValue::Double(f64::NAN)).is_err());
    }

    #[test]
    fn converts_text_and_binary_values() {
        assert_eq!(
            as_text(&ColValue::String("text".to_string())).unwrap(),
            "text"
        );
        assert_eq!(
            as_text(&ColValue::Json3(serde_json::json!({"a": 1}))).unwrap(),
            r#"{"a":1}"#
        );
        assert!(as_text(&ColValue::RawString(vec![0xff])).is_err());
        assert!(as_text(&ColValue::Blob(vec![1])).is_err());

        assert_eq!(
            as_binary(&ColValue::Blob(vec![1, 2])).unwrap().as_ref(),
            &[1, 2]
        );
        assert!(as_binary(&ColValue::String("not binary".to_string())).is_err());
    }

    #[test]
    fn parses_uuid_xml_and_temporal_values() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            parse_uuid(&ColValue::String(uuid.to_string()))
                .unwrap()
                .to_string(),
            uuid
        );
        assert!(parse_uuid(&ColValue::String("invalid".to_string())).is_err());
        assert_eq!(
            parse_xml(&ColValue::String("<root/>".to_string()))
                .unwrap()
                .to_string(),
            "<root/>"
        );

        assert_eq!(
            parse_date(&ColValue::Date("2026-08-11".to_string()))
                .unwrap()
                .to_string(),
            "2026-08-11"
        );
        assert!(parse_date(&ColValue::Date("2026-02-30".to_string())).is_err());
        assert_eq!(
            parse_time(&ColValue::Time("12:34:56.1234567".to_string()))
                .unwrap()
                .format("%H:%M:%S%.f")
                .to_string(),
            "12:34:56.123456700"
        );
        assert!(parse_datetime(&ColValue::Timestamp(
            "2026-08-11T12:34:56+08:00".to_string()
        ))
        .is_err());
        assert_eq!(
            parse_datetime_offset(&ColValue::Timestamp(
                "2026-08-11T12:34:56+08:00".to_string()
            ))
            .unwrap()
            .offset()
            .local_minus_utc(),
            8 * 60 * 60
        );
    }

    #[test]
    fn parses_checkpoint_strings_by_column_type() {
        assert_eq!(
            MssqlColValueConvertor::from_str(&col_type("int"), "-42").unwrap(),
            ColValue::Long(-42)
        );
        assert_eq!(
            MssqlColValueConvertor::from_str(&col_type("money"), "922337203685477.5807").unwrap(),
            ColValue::Decimal("922337203685477.5807".to_string())
        );
        assert_eq!(
            MssqlColValueConvertor::from_str(&col_type("varbinary"), "00ff10").unwrap(),
            ColValue::Blob(vec![0, 255, 16])
        );
        assert!(MssqlColValueConvertor::from_str(&col_type("bit"), "2").is_err());
        assert!(MssqlColValueConvertor::from_str(&col_type("real"), "NaN").is_err());
        assert!(MssqlColValueConvertor::from_str(&col_type("float"), "inf").is_err());
    }

    #[test]
    fn binds_typed_nulls_and_rejects_invalid_values_before_into_sql() {
        let null = ColValue::None;
        for type_name in [
            "bit",
            "tinyint",
            "smallint",
            "int",
            "bigint",
            "real",
            "float",
            "money",
            "smallmoney",
            "decimal",
            "numeric",
            "varchar",
            "char",
            "nvarchar",
            "nchar",
            "text",
            "ntext",
            "varbinary",
            "binary",
            "image",
            "rowversion",
            "timestamp",
            "uniqueidentifier",
            "xml",
            "date",
            "time",
            "smalldatetime",
            "datetime",
            "datetime2",
            "datetimeoffset",
        ] {
            let mut query = Query::new("SELECT @P1");
            MssqlColValueConvertor::bind(&mut query, &null, &col_type(type_name)).unwrap();
        }

        let mut query = Query::new("SELECT @P1");
        assert!(MssqlColValueConvertor::bind(
            &mut query,
            &ColValue::UnchangedToast,
            &col_type("int")
        )
        .is_err());
    }
}
