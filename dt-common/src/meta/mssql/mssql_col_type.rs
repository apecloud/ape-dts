use tiberius::ColumnType;

use crate::{config::config_enums::DbType, error::DtError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlColType {
    Bit,
    Int1,
    Int2,
    Int4,
    Int8,
    Datetime4,
    Float4,
    Float8,
    Money,
    Datetime,
    Money4,
    Guid,
    Bitn,
    Decimaln,
    Numericn,
    Datetimen,
    Daten,
    Timen,
    Datetime2,
    DatetimeOffsetn,
    BigVarBin,
    BigVarChar,
    BigBinary,
    BigChar,
    NVarchar,
    NChar,
    Xml,
    Text,
    Image,
    NText,
    Geometry,
    Geography,
    HierarchyId,
    AssemblyUdt,
    // TODO: Add SqlVariant after Tiberius can decode and encode SSVariant values.
}

impl MssqlColType {
    pub fn order_key_weight(&self) -> Option<u32> {
        Some(match self {
            Self::Int1 | Self::Int2 | Self::Int4 | Self::Int8 => 1,
            Self::Datetime4
            | Self::Datetime
            | Self::Datetimen
            | Self::Daten
            | Self::Timen
            | Self::Datetime2
            | Self::DatetimeOffsetn => 2,
            Self::Money | Self::Money4 | Self::Decimaln | Self::Numericn => 3,
            Self::Guid | Self::Bit | Self::Bitn => 4,
            Self::BigVarBin | Self::BigBinary => 6,
            Self::BigVarChar | Self::BigChar | Self::NVarchar | Self::NChar => 8,
            Self::Float4 | Self::Float8 => 12,
            Self::Xml
            | Self::Text
            | Self::Image
            | Self::NText
            | Self::Geometry
            | Self::Geography
            | Self::HierarchyId
            | Self::AssemblyUdt => return None,
        })
    }

    pub fn can_be_splitted(&self) -> bool {
        matches!(
            self,
            Self::Int1
                | Self::Int2
                | Self::Int4
                | Self::Int8
                | Self::Float4
                | Self::Float8
                | Self::Money
                | Self::Money4
                | Self::Decimaln
                | Self::Numericn
                | Self::Datetime4
                | Self::Datetime
                | Self::Datetimen
                | Self::Daten
                | Self::Timen
                | Self::Datetime2
                | Self::DatetimeOffsetn
                | Self::Guid
                | Self::BigVarChar
                | Self::BigChar
                | Self::NVarchar
                | Self::NChar
                | Self::BigVarBin
                | Self::BigBinary
        )
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Int1 | Self::Int2 | Self::Int4 | Self::Int8)
    }

    pub fn is_string(&self) -> bool {
        matches!(
            self,
            Self::BigVarChar
                | Self::BigChar
                | Self::NVarchar
                | Self::NChar
                | Self::Text
                | Self::NText
        )
    }

    pub fn is_binary(&self) -> bool {
        matches!(self, Self::BigVarBin | Self::BigBinary | Self::Image)
    }

    pub fn requires_special_transfer(&self) -> bool {
        matches!(
            self,
            Self::Geometry | Self::Geography | Self::HierarchyId | Self::AssemblyUdt
        )
    }

    pub fn bind_parameter_count(&self) -> usize {
        match self {
            Self::Geometry | Self::Geography => 2,
            _ => 1,
        }
    }

    pub fn wire_type(&self) -> ColumnType {
        match self {
            Self::Bit => ColumnType::Bit,
            Self::Int1 => ColumnType::Int1,
            Self::Int2 => ColumnType::Int2,
            Self::Int4 => ColumnType::Int4,
            Self::Int8 => ColumnType::Int8,
            Self::Datetime4 => ColumnType::Datetime4,
            Self::Float4 => ColumnType::Float4,
            Self::Float8 => ColumnType::Float8,
            Self::Money => ColumnType::Money,
            Self::Datetime => ColumnType::Datetime,
            Self::Money4 => ColumnType::Money4,
            Self::Guid => ColumnType::Guid,
            Self::Bitn => ColumnType::Bitn,
            Self::Decimaln => ColumnType::Decimaln,
            Self::Numericn => ColumnType::Numericn,
            Self::Datetimen => ColumnType::Datetimen,
            Self::Daten => ColumnType::Daten,
            Self::Timen => ColumnType::Timen,
            Self::Datetime2 => ColumnType::Datetime2,
            Self::DatetimeOffsetn => ColumnType::DatetimeOffsetn,
            Self::BigVarBin | Self::AssemblyUdt => ColumnType::BigVarBin,
            Self::BigVarChar => ColumnType::BigVarChar,
            Self::BigBinary => ColumnType::BigBinary,
            Self::BigChar => ColumnType::BigChar,
            Self::NVarchar | Self::Geometry | Self::Geography | Self::HierarchyId => {
                ColumnType::NVarchar
            }
            Self::NChar => ColumnType::NChar,
            Self::Xml => ColumnType::Xml,
            Self::Text => ColumnType::Text,
            Self::Image => ColumnType::Image,
            Self::NText => ColumnType::NText,
        }
    }
}

impl TryFrom<ColumnType> for MssqlColType {
    type Error = DtError;

    fn try_from(value: ColumnType) -> Result<Self, Self::Error> {
        let col_type = match value {
            ColumnType::Bit => Self::Bit,
            ColumnType::Int1 => Self::Int1,
            ColumnType::Int2 => Self::Int2,
            ColumnType::Int4 => Self::Int4,
            ColumnType::Int8 => Self::Int8,
            ColumnType::Datetime4 => Self::Datetime4,
            ColumnType::Float4 => Self::Float4,
            ColumnType::Float8 => Self::Float8,
            ColumnType::Money => Self::Money,
            ColumnType::Datetime => Self::Datetime,
            ColumnType::Money4 => Self::Money4,
            ColumnType::Guid => Self::Guid,
            ColumnType::Bitn => Self::Bitn,
            ColumnType::Decimaln => Self::Decimaln,
            ColumnType::Numericn => Self::Numericn,
            ColumnType::Datetimen => Self::Datetimen,
            ColumnType::Daten => Self::Daten,
            ColumnType::Timen => Self::Timen,
            ColumnType::Datetime2 => Self::Datetime2,
            ColumnType::DatetimeOffsetn => Self::DatetimeOffsetn,
            ColumnType::BigVarBin => Self::BigVarBin,
            ColumnType::BigVarChar => Self::BigVarChar,
            ColumnType::BigBinary => Self::BigBinary,
            ColumnType::BigChar => Self::BigChar,
            ColumnType::NVarchar => Self::NVarchar,
            ColumnType::NChar => Self::NChar,
            ColumnType::Xml => Self::Xml,
            ColumnType::Text => Self::Text,
            ColumnType::Image => Self::Image,
            ColumnType::NText => Self::NText,
            ColumnType::Null
            | ColumnType::Intn
            | ColumnType::Floatn
            | ColumnType::Udt
            | ColumnType::SSVariant => {
                return Err(DtError::DatabaseUnsupportedTableStructure(
                    DbType::Mssql,
                    format!("MSSQL column type {value:?} is not supported"),
                ));
            }
        };
        Ok(col_type)
    }
}

pub fn parse_mssql_col_type(type_name: &str) -> anyhow::Result<MssqlColType> {
    parse_mssql_col_type_with_length(type_name, 0)
}

pub fn parse_mssql_col_type_with_length(
    type_name: &str,
    max_length: i16,
) -> anyhow::Result<MssqlColType> {
    let col_type = match type_name.trim().to_ascii_lowercase().as_str() {
        "bit" => MssqlColType::Bitn,
        "tinyint" => MssqlColType::Int1,
        "smallint" => MssqlColType::Int2,
        "int" => MssqlColType::Int4,
        "bigint" => MssqlColType::Int8,
        "smalldatetime" => MssqlColType::Datetime4,
        "real" => MssqlColType::Float4,
        "float" if max_length == 4 => MssqlColType::Float4,
        "float" => MssqlColType::Float8,
        "money" => MssqlColType::Money,
        "datetime" => MssqlColType::Datetime,
        "smallmoney" => MssqlColType::Money4,
        "uniqueidentifier" => MssqlColType::Guid,
        "decimal" => MssqlColType::Decimaln,
        "numeric" => MssqlColType::Numericn,
        "date" => MssqlColType::Daten,
        "time" => MssqlColType::Timen,
        "datetime2" => MssqlColType::Datetime2,
        "datetimeoffset" => MssqlColType::DatetimeOffsetn,
        "varbinary" | "timestamp" | "rowversion" => MssqlColType::BigVarBin,
        "varchar" => MssqlColType::BigVarChar,
        "binary" => MssqlColType::BigBinary,
        "char" => MssqlColType::BigChar,
        "nvarchar" => MssqlColType::NVarchar,
        "nchar" => MssqlColType::NChar,
        "xml" => MssqlColType::Xml,
        "text" => MssqlColType::Text,
        "image" => MssqlColType::Image,
        "ntext" => MssqlColType::NText,
        // TODO: Support sql_variant after the Tiberius SSVariant codec is implemented.
        "sql_variant" => anyhow::bail!("unsupported MSSQL column type {type_name}"),
        _ => anyhow::bail!("unsupported MSSQL column type {type_name}"),
    };
    Ok(col_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUPPORTED_TYPES: [MssqlColType; 30] = [
        MssqlColType::Bit,
        MssqlColType::Int1,
        MssqlColType::Int2,
        MssqlColType::Int4,
        MssqlColType::Int8,
        MssqlColType::Datetime4,
        MssqlColType::Float4,
        MssqlColType::Float8,
        MssqlColType::Money,
        MssqlColType::Datetime,
        MssqlColType::Money4,
        MssqlColType::Guid,
        MssqlColType::Bitn,
        MssqlColType::Decimaln,
        MssqlColType::Numericn,
        MssqlColType::Datetimen,
        MssqlColType::Daten,
        MssqlColType::Timen,
        MssqlColType::Datetime2,
        MssqlColType::DatetimeOffsetn,
        MssqlColType::BigVarBin,
        MssqlColType::BigVarChar,
        MssqlColType::BigBinary,
        MssqlColType::BigChar,
        MssqlColType::NVarchar,
        MssqlColType::NChar,
        MssqlColType::Xml,
        MssqlColType::Text,
        MssqlColType::Image,
        MssqlColType::NText,
    ];

    #[test]
    fn converts_supported_tiberius_types_bidirectionally() {
        for col_type in SUPPORTED_TYPES {
            let tiberius_type = col_type.wire_type();
            assert_eq!(MssqlColType::try_from(tiberius_type).unwrap(), col_type);
        }
    }

    #[test]
    fn exposes_special_transfer_wire_types() {
        assert_eq!(MssqlColType::Geometry.wire_type(), ColumnType::NVarchar);
        assert_eq!(MssqlColType::Geography.wire_type(), ColumnType::NVarchar);
        assert_eq!(MssqlColType::HierarchyId.wire_type(), ColumnType::NVarchar);
        assert_eq!(MssqlColType::Geometry.bind_parameter_count(), 2);
        assert_eq!(MssqlColType::Geography.bind_parameter_count(), 2);
        assert_eq!(MssqlColType::HierarchyId.bind_parameter_count(), 1);

        let udt = MssqlColType::AssemblyUdt;
        assert_eq!(udt.wire_type(), ColumnType::BigVarBin);
        assert!(udt.requires_special_transfer());
    }

    #[test]
    fn rejects_unsupported_tiberius_types() {
        for col_type in [
            ColumnType::Null,
            ColumnType::Intn,
            ColumnType::Floatn,
            ColumnType::Udt,
            ColumnType::SSVariant,
        ] {
            assert!(MssqlColType::try_from(col_type).is_err());
        }
    }

    #[test]
    fn classifies_integer_and_splittable_types() {
        for col_type in [
            MssqlColType::Int1,
            MssqlColType::Int2,
            MssqlColType::Int4,
            MssqlColType::Int8,
        ] {
            assert!(col_type.is_integer());
            assert!(col_type.can_be_splitted());
        }

        assert!(MssqlColType::NVarchar.can_be_splitted());
        assert!(MssqlColType::DatetimeOffsetn.can_be_splitted());
        assert!(!MssqlColType::Bitn.can_be_splitted());
        assert!(!MssqlColType::Xml.can_be_splitted());
    }

    #[test]
    fn classifies_string_and_binary_types() {
        assert!(MssqlColType::NVarchar.is_string());
        assert!(MssqlColType::Text.is_string());
        assert!(!MssqlColType::Xml.is_string());

        assert!(MssqlColType::BigVarBin.is_binary());
        assert!(MssqlColType::Image.is_binary());
        assert!(!MssqlColType::BigVarChar.is_binary());
    }

    #[test]
    fn parses_sql_server_type_names() {
        assert_eq!(parse_mssql_col_type("INT").unwrap(), MssqlColType::Int4);
        assert_eq!(
            parse_mssql_col_type(" rowversion ").unwrap(),
            MssqlColType::BigVarBin
        );
        assert_eq!(
            parse_mssql_col_type_with_length("float", 4).unwrap(),
            MssqlColType::Float4
        );
        for unsupported in ["sql_variant", "geometry", "geography", "hierarchyid"] {
            assert!(parse_mssql_col_type(unsupported).is_err());
        }
        assert!(parse_mssql_col_type("unknown_type").is_err());
    }
}
