use anyhow::{bail, Context};
use tiberius::Query;

use super::mssql_col_value_convertor::{
    as_big_decimal, as_binary, as_bool_checked, as_f32_checked, as_f64_checked, as_i16_checked,
    as_i32_checked, as_i64_checked, as_text, as_u8_checked, col_value_kind, invalid_value,
    parse_date, parse_datetime, parse_datetime_offset, parse_time, parse_uuid, parse_xml,
    MssqlColValueConvertor, MssqlColValueKind,
};
use crate::meta::{col_value::ColValue, mssql::mssql_col_type::MssqlColType};

pub trait TiberiusExt<'q> {
    fn bind_col_value(
        &mut self,
        value: &'q ColValue,
        col_type: &MssqlColType,
    ) -> anyhow::Result<()>;
}

impl<'q> TiberiusExt<'q> for Query<'q> {
    fn bind_col_value(
        &mut self,
        value: &'q ColValue,
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
            MssqlColValueKind::Bool => {
                MssqlColValueConvertor::bind_as(self, value, as_bool_checked)
            }
            MssqlColValueKind::UnsignedTiny => {
                MssqlColValueConvertor::bind_as(self, value, as_u8_checked)
            }
            MssqlColValueKind::Short => {
                MssqlColValueConvertor::bind_as(self, value, as_i16_checked)
            }
            MssqlColValueKind::Long => MssqlColValueConvertor::bind_as(self, value, as_i32_checked),
            MssqlColValueKind::LongLong => {
                MssqlColValueConvertor::bind_as(self, value, as_i64_checked)
            }
            MssqlColValueKind::Float => {
                MssqlColValueConvertor::bind_as(self, value, as_f32_checked)
            }
            MssqlColValueKind::Double => {
                MssqlColValueConvertor::bind_as(self, value, as_f64_checked)
            }
            MssqlColValueKind::Decimal => {
                MssqlColValueConvertor::bind_as(self, value, as_big_decimal)
            }
            MssqlColValueKind::String => match col_type {
                MssqlColType::Guid => MssqlColValueConvertor::bind_as(self, value, parse_uuid),
                MssqlColType::Xml => MssqlColValueConvertor::bind_as(self, value, parse_xml),
                _ => MssqlColValueConvertor::bind_as(self, value, as_text),
            },
            MssqlColValueKind::Blob => MssqlColValueConvertor::bind_as(self, value, as_binary),
            MssqlColValueKind::Date => MssqlColValueConvertor::bind_as(self, value, parse_date),
            MssqlColValueKind::Time => MssqlColValueConvertor::bind_as(self, value, parse_time),
            MssqlColValueKind::DateTime => {
                MssqlColValueConvertor::bind_as(self, value, parse_datetime)
            }
            MssqlColValueKind::Timestamp => {
                MssqlColValueConvertor::bind_as(self, value, parse_datetime_offset)
            }
        };

        result.with_context(|| {
            format!(
                "failed to bind ColValue::{} as MSSQL {col_type:?}",
                value.type_name()
            )
        })
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
            query.bind_col_value(&null, &col_type(type_name)).unwrap();
        }

        let mut query = Query::new("SELECT @P1");
        assert!(query
            .bind_col_value(&ColValue::UnchangedToast, &col_type("int"))
            .is_err());

        let wrong_money_value = ColValue::Decimal("12.3400".to_string());
        let mut query = Query::new("SELECT @P1");
        assert!(query
            .bind_col_value(&wrong_money_value, &col_type("money"))
            .is_err());

        let money_value = ColValue::Double(12.34);
        let mut query = Query::new("SELECT @P1");
        query
            .bind_col_value(&money_value, &col_type("money"))
            .unwrap();
    }
}
