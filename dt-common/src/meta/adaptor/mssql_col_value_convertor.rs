use std::str::FromStr;

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
enum MssqlColValueKind {
    Bool,
    UnsignedTiny,
    Short,
    Long,
    LongLong,
    Float,
    Double,
    Decimal,
    String,
    Blob,
    Date,
    Time,
    DateTime,
    Timestamp,
}

impl MssqlColValueKind {
    fn type_name(self) -> &'static str {
        match self {
            Self::Bool => "Bool",
            Self::UnsignedTiny => "UnsignedTiny",
            Self::Short => "Short",
            Self::Long => "Long",
            Self::LongLong => "LongLong",
            Self::Float => "Float",
            Self::Double => "Double",
            Self::Decimal => "Decimal",
            Self::String => "String",
            Self::Blob => "Blob",
            Self::Date => "Date",
            Self::Time => "Time",
            Self::DateTime => "DateTime",
            Self::Timestamp => "Timestamp",
        }
    }

    fn matches(self, value: &ColValue) -> bool {
        matches!(
            (self, value),
            (Self::Bool, ColValue::Bool(_))
                | (Self::UnsignedTiny, ColValue::UnsignedTiny(_))
                | (Self::Short, ColValue::Short(_))
                | (Self::Long, ColValue::Long(_))
                | (Self::LongLong, ColValue::LongLong(_))
                | (Self::Float, ColValue::Float(_))
                | (Self::Double, ColValue::Double(_))
                | (Self::Decimal, ColValue::Decimal(_))
                | (Self::String, ColValue::String(_))
                | (Self::Blob, ColValue::Blob(_))
                | (Self::Date, ColValue::Date(_))
                | (Self::Time, ColValue::Time(_))
                | (Self::DateTime, ColValue::DateTime(_))
                | (Self::Timestamp, ColValue::Timestamp(_))
        )
    }
}

pub struct MssqlColValueConvertor;

impl MssqlColValueConvertor {
    pub fn from_query(row: &Row, col: &str, col_type: &MssqlColType) -> anyhow::Result<ColValue> {
        let index = Self::query_col_index(row, col)?;
        Self::from_query_at(row, index, col_type)
    }

    fn from_query_by_result_type(row: &Row, col: &str) -> anyhow::Result<ColValue> {
        let index = Self::query_col_index(row, col)?;
        let col_type = MssqlColType::try_from(row.columns()[index].column_type())?;
        Self::from_query_at(row, index, &col_type)
    }

    fn query_col_index(row: &Row, col: &str) -> anyhow::Result<usize> {
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

        Ok(index)
    }

    pub fn from_query_required_string(row: &Row, col: &str) -> anyhow::Result<String> {
        Self::from_query_required(row, col, "String", |value| match value {
            ColValue::String(value) => Some(value),
            _ => None,
        })
    }

    pub fn from_query_optional_string(row: &Row, col: &str) -> anyhow::Result<Option<String>> {
        let value = Self::from_query_by_result_type(row, col)?;
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
        let value = Self::from_query_by_result_type(row, col)?;
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

    fn from_query_at(row: &Row, index: usize, col_type: &MssqlColType) -> anyhow::Result<ColValue> {
        match col_value_kind(col_type) {
            MssqlColValueKind::Bool => Self::try_get_as::<bool>(row, index, ColValue::Bool),
            MssqlColValueKind::UnsignedTiny => {
                Self::try_get_as::<u8>(row, index, ColValue::UnsignedTiny)
            }
            MssqlColValueKind::Short => Self::try_get_as::<i16>(row, index, ColValue::Short),
            MssqlColValueKind::Long => Self::try_get_as::<i32>(row, index, ColValue::Long),
            MssqlColValueKind::LongLong => Self::try_get_as::<i64>(row, index, ColValue::LongLong),
            MssqlColValueKind::Float => Self::try_get_as::<f32>(row, index, ColValue::Float),
            MssqlColValueKind::Double => Self::try_get_as::<f64>(row, index, ColValue::Double),
            MssqlColValueKind::Decimal => Self::try_get_as::<BigDecimal>(row, index, |value| {
                ColValue::Decimal(value.to_string())
            }),
            MssqlColValueKind::String => match col_type {
                MssqlColType::Guid => Self::try_get_as::<Uuid>(row, index, |value| {
                    ColValue::String(value.to_string())
                }),
                MssqlColType::Xml => Self::try_get_as::<&XmlData>(row, index, |value| {
                    ColValue::String(value.to_string())
                }),
                _ => {
                    Self::try_get_as::<&str>(row, index, |value| ColValue::String(value.to_owned()))
                }
            },
            MssqlColValueKind::Blob => {
                Self::try_get_as::<&[u8]>(row, index, |value| ColValue::Blob(value.to_vec()))
            }
            MssqlColValueKind::DateTime => Self::try_get_as::<NaiveDateTime>(row, index, |value| {
                ColValue::DateTime(value.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            }),
            MssqlColValueKind::Date => Self::try_get_as::<NaiveDate>(row, index, |value| {
                ColValue::Date(value.format("%Y-%m-%d").to_string())
            }),
            MssqlColValueKind::Time => Self::try_get_as::<NaiveTime>(row, index, |value| {
                ColValue::Time(value.format("%H:%M:%S%.f").to_string())
            }),
            MssqlColValueKind::Timestamp => {
                Self::try_get_as::<DateTime<FixedOffset>>(row, index, |value| {
                    ColValue::Timestamp(value.to_rfc3339())
                })
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
        let parsed = match col_value_kind(col_type) {
            MssqlColValueKind::Bool => ColValue::Bool(parse_bool(value).ok_or_else(|| {
                DtError::DatabaseStatementFailed(
                    DbType::Mssql,
                    format!("invalid bit value: {value}"),
                )
            })?),
            MssqlColValueKind::UnsignedTiny => ColValue::UnsignedTiny(value.parse()?),
            MssqlColValueKind::Short => ColValue::Short(value.parse()?),
            MssqlColValueKind::Long => ColValue::Long(value.parse()?),
            MssqlColValueKind::LongLong => ColValue::LongLong(value.parse()?),
            MssqlColValueKind::Float => ColValue::Float(parse_finite_f32(value)?),
            MssqlColValueKind::Double => ColValue::Double(parse_finite_f64(value)?),
            MssqlColValueKind::Decimal => ColValue::Decimal(parse_decimal(value)?.to_string()),
            MssqlColValueKind::String => match col_type {
                MssqlColType::Guid => ColValue::String(Uuid::parse_str(value)?.to_string()),
                MssqlColType::Xml => ColValue::String(XmlData::new(value).to_string()),
                _ => ColValue::String(value.to_string()),
            },
            MssqlColValueKind::Blob => ColValue::Blob(hex::decode(value)?),
            MssqlColValueKind::Date => ColValue::Date(
                NaiveDate::parse_from_str(value, "%Y-%m-%d")?
                    .format("%Y-%m-%d")
                    .to_string(),
            ),
            MssqlColValueKind::Time => ColValue::Time(
                NaiveTime::parse_from_str(value, "%H:%M:%S%.f")?
                    .format("%H:%M:%S%.f")
                    .to_string(),
            ),
            MssqlColValueKind::DateTime => ColValue::DateTime(
                NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")?
                    .format("%Y-%m-%d %H:%M:%S%.f")
                    .to_string(),
            ),
            MssqlColValueKind::Timestamp => {
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
        let kind = col_value_kind(col_type);
        if !matches!(value, ColValue::None | ColValue::UnchangedToast) && !kind.matches(value) {
            bail!(invalid_value(
                value,
                &format!("MSSQL {col_type:?}"),
                format!("expected ColValue::{}", kind.type_name()),
            ));
        }

        let result = match kind {
            MssqlColValueKind::Bool => Self::bind_as(query, value, as_bool_checked),
            MssqlColValueKind::UnsignedTiny => Self::bind_as(query, value, as_u8_checked),
            MssqlColValueKind::Short => Self::bind_as(query, value, as_i16_checked),
            MssqlColValueKind::Long => Self::bind_as(query, value, as_i32_checked),
            MssqlColValueKind::LongLong => Self::bind_as(query, value, as_i64_checked),
            MssqlColValueKind::Float => Self::bind_as(query, value, as_f32_checked),
            MssqlColValueKind::Double => Self::bind_as(query, value, as_f64_checked),
            MssqlColValueKind::Decimal => Self::bind_as(query, value, as_big_decimal),
            MssqlColValueKind::String => match col_type {
                MssqlColType::Guid => Self::bind_as(query, value, parse_uuid),
                MssqlColType::Xml => Self::bind_as(query, value, parse_xml),
                _ => Self::bind_as(query, value, as_text),
            },
            MssqlColValueKind::Blob => Self::bind_as(query, value, as_binary),
            MssqlColValueKind::Date => Self::bind_as(query, value, parse_date),
            MssqlColValueKind::Time => Self::bind_as(query, value, parse_time),
            MssqlColValueKind::DateTime => Self::bind_as(query, value, parse_datetime),
            MssqlColValueKind::Timestamp => Self::bind_as(query, value, parse_datetime_offset),
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

fn col_value_kind(col_type: &MssqlColType) -> MssqlColValueKind {
    match col_type {
        MssqlColType::Bit | MssqlColType::Bitn => MssqlColValueKind::Bool,
        MssqlColType::Int1 => MssqlColValueKind::UnsignedTiny,
        MssqlColType::Int2 => MssqlColValueKind::Short,
        MssqlColType::Int4 => MssqlColValueKind::Long,
        MssqlColType::Int8 => MssqlColValueKind::LongLong,
        MssqlColType::Float4 => MssqlColValueKind::Float,
        MssqlColType::Float8 => MssqlColValueKind::Double,
        MssqlColType::Money | MssqlColType::Money4 => MssqlColValueKind::Double,
        MssqlColType::Decimaln | MssqlColType::Numericn => MssqlColValueKind::Decimal,
        MssqlColType::BigVarChar
        | MssqlColType::BigChar
        | MssqlColType::NVarchar
        | MssqlColType::NChar
        | MssqlColType::Text
        | MssqlColType::NText
        | MssqlColType::Guid
        | MssqlColType::Xml => MssqlColValueKind::String,
        MssqlColType::BigVarBin | MssqlColType::BigBinary | MssqlColType::Image => {
            MssqlColValueKind::Blob
        }
        MssqlColType::Daten => MssqlColValueKind::Date,
        MssqlColType::Timen => MssqlColValueKind::Time,
        MssqlColType::Datetime4
        | MssqlColType::Datetime
        | MssqlColType::Datetimen
        | MssqlColType::Datetime2 => MssqlColValueKind::DateTime,
        MssqlColType::DatetimeOffsetn => MssqlColValueKind::Timestamp,
    }
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
    match value {
        ColValue::UnsignedTiny(value) => Ok(*value),
        _ => Err(invalid_value(value, "tinyint", "incompatible value type")),
    }
}

fn as_i16_checked(value: &ColValue) -> anyhow::Result<i16> {
    match value {
        ColValue::Short(value) => Ok(*value),
        _ => Err(invalid_value(value, "smallint", "incompatible value type")),
    }
}

fn as_i32_checked(value: &ColValue) -> anyhow::Result<i32> {
    match value {
        ColValue::Long(value) => Ok(*value),
        _ => Err(invalid_value(value, "int", "incompatible value type")),
    }
}

fn as_i64_checked(value: &ColValue) -> anyhow::Result<i64> {
    match value {
        ColValue::LongLong(value) => Ok(*value),
        _ => Err(invalid_value(value, "bigint", "incompatible value type")),
    }
}

fn as_f32_checked(value: &ColValue) -> anyhow::Result<f32> {
    match value {
        ColValue::Float(value) if value.is_finite() => Ok(*value),
        ColValue::Float(_) => Err(invalid_value(value, "real", "value must be finite")),
        _ => Err(invalid_value(value, "real", "incompatible value type")),
    }
}

fn as_f64_checked(value: &ColValue) -> anyhow::Result<f64> {
    match value {
        ColValue::Double(value) if value.is_finite() => Ok(*value),
        ColValue::Double(_) => Err(invalid_value(value, "float", "value must be finite")),
        _ => Err(invalid_value(value, "float", "incompatible value type")),
    }
}

fn as_big_decimal(value: &ColValue) -> anyhow::Result<BigDecimal> {
    match value {
        ColValue::Decimal(value) => parse_decimal(value),
        _ => Err(invalid_value(value, "decimal", "incompatible value type")),
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

fn as_text(value: &ColValue) -> anyhow::Result<&str> {
    match value {
        ColValue::String(value) => Ok(value),
        _ => Err(invalid_value(value, "text", "incompatible value type")),
    }
}

fn as_binary(value: &ColValue) -> anyhow::Result<&[u8]> {
    match value {
        ColValue::Blob(value) => Ok(value),
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
        ColValue::Date(value) => value,
        _ => return Err(invalid_value(value, "date", "incompatible value type")),
    };
    Ok(NaiveDate::parse_from_str(value, "%Y-%m-%d")?)
}

fn parse_time(value: &ColValue) -> anyhow::Result<NaiveTime> {
    let value = match value {
        ColValue::Time(value) => value,
        _ => return Err(invalid_value(value, "time", "incompatible value type")),
    };
    Ok(NaiveTime::parse_from_str(value, "%H:%M:%S%.f")?)
}

fn parse_datetime(value: &ColValue) -> anyhow::Result<NaiveDateTime> {
    let value = match value {
        ColValue::DateTime(value) => value,
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
        ColValue::Timestamp(value) => value,
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
    fn classifies_supported_col_value_types() {
        let cases = [
            ("bit", MssqlColValueKind::Bool),
            ("tinyint", MssqlColValueKind::UnsignedTiny),
            ("smallint", MssqlColValueKind::Short),
            ("int", MssqlColValueKind::Long),
            ("bigint", MssqlColValueKind::LongLong),
            ("real", MssqlColValueKind::Float),
            ("float", MssqlColValueKind::Double),
            ("money", MssqlColValueKind::Double),
            ("smallmoney", MssqlColValueKind::Double),
            ("decimal", MssqlColValueKind::Decimal),
            ("numeric", MssqlColValueKind::Decimal),
            ("varchar", MssqlColValueKind::String),
            ("char", MssqlColValueKind::String),
            ("nvarchar", MssqlColValueKind::String),
            ("nchar", MssqlColValueKind::String),
            ("text", MssqlColValueKind::String),
            ("ntext", MssqlColValueKind::String),
            ("varbinary", MssqlColValueKind::Blob),
            ("binary", MssqlColValueKind::Blob),
            ("image", MssqlColValueKind::Blob),
            ("rowversion", MssqlColValueKind::Blob),
            ("timestamp", MssqlColValueKind::Blob),
            ("uniqueidentifier", MssqlColValueKind::String),
            ("xml", MssqlColValueKind::String),
            ("date", MssqlColValueKind::Date),
            ("time", MssqlColValueKind::Time),
            ("smalldatetime", MssqlColValueKind::DateTime),
            ("datetime", MssqlColValueKind::DateTime),
            ("datetime2", MssqlColValueKind::DateTime),
            ("datetimeoffset", MssqlColValueKind::Timestamp),
        ];

        for (type_name, expected) in cases {
            assert_eq!(col_value_kind(&col_type(type_name)), expected);
        }
        assert_eq!(
            col_value_kind(&col_type(" NVARCHAR ")),
            MssqlColValueKind::String
        );
        assert_eq!(col_value_kind(&MssqlColType::Bit), MssqlColValueKind::Bool);
        assert_eq!(
            col_value_kind(&MssqlColType::Datetimen),
            MssqlColValueKind::DateTime
        );
    }

    #[test]
    fn rejects_unsupported_bind_types() {
        for col_type in [
            tiberius::ColumnType::Null,
            tiberius::ColumnType::Intn,
            tiberius::ColumnType::Floatn,
            tiberius::ColumnType::Udt,
            tiberius::ColumnType::SSVariant,
        ] {
            assert!(MssqlColType::try_from(col_type).is_err());
        }
        assert!(parse_mssql_col_type("sql_variant").is_err());
    }

    #[test]
    fn accepts_only_the_mapped_integer_variant() {
        assert_eq!(
            as_u8_checked(&ColValue::UnsignedTiny(u8::MAX)).unwrap(),
            u8::MAX
        );
        assert!(as_u8_checked(&ColValue::Long(-1)).is_err());
        assert!(as_u8_checked(&ColValue::UnsignedShort(255)).is_err());

        assert_eq!(
            as_i16_checked(&ColValue::Short(i16::MIN)).unwrap(),
            i16::MIN
        );
        assert!(as_i16_checked(&ColValue::Long(i16::MAX as i32)).is_err());
        assert!(as_i32_checked(&ColValue::Short(1)).is_err());
        assert!(as_i64_checked(&ColValue::Long(1)).is_err());
    }

    #[test]
    fn converts_boolean_values_strictly() {
        assert!(as_bool_checked(&ColValue::Bool(true)).unwrap());
        assert!(!as_bool_checked(&ColValue::Bool(false)).unwrap());
        assert!(as_bool_checked(&ColValue::Long(1)).is_err());
        assert!(as_bool_checked(&ColValue::String("true".to_string())).is_err());
    }

    #[test]
    fn validates_float_and_decimal_values() {
        assert_eq!(as_f64_checked(&ColValue::Double(12.5)).unwrap(), 12.5);
        assert!(as_f64_checked(&ColValue::Float(12.5)).is_err());
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
            as_big_decimal(&ColValue::Decimal("12.3400".to_string()))
                .unwrap()
                .to_string(),
            "12.3400"
        );
        assert!(as_big_decimal(&ColValue::Double(12.34)).is_err());
    }

    #[test]
    fn converts_text_and_binary_values() {
        assert_eq!(
            as_text(&ColValue::String("text".to_string())).unwrap(),
            "text"
        );
        assert!(as_text(&ColValue::Json3(serde_json::json!({"a": 1}))).is_err());
        assert!(as_text(&ColValue::RawString(vec![0xff])).is_err());
        assert!(as_text(&ColValue::Blob(vec![1])).is_err());

        assert_eq!(as_binary(&ColValue::Blob(vec![1, 2])).unwrap(), &[1, 2]);
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
            MssqlColValueConvertor::from_str(&col_type("money"), "12.3400").unwrap(),
            ColValue::Double(12.34)
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

        let wrong_money_value = ColValue::Decimal("12.3400".to_string());
        let mut query = Query::new("SELECT @P1");
        assert!(
            MssqlColValueConvertor::bind(&mut query, &wrong_money_value, &col_type("money"))
                .is_err()
        );

        let money_value = ColValue::Double(12.34);
        let mut query = Query::new("SELECT @P1");
        MssqlColValueConvertor::bind(&mut query, &money_value, &col_type("money")).unwrap();
    }
}
