pub type MssqlColType = tiberius::ColumnType;

pub trait MssqlColTypeExt {
    fn can_be_splitted(&self) -> bool;
    fn is_integer(&self) -> bool;
    fn is_string(&self) -> bool;
    fn is_binary(&self) -> bool;
}

#[derive(Clone, Copy)]
enum MssqlColTypeKind {
    Null,
    Bool,
    Integer,
    Float,
    Money,
    Decimal,
    Temporal,
    Guid,
    String,
    LegacyText,
    Binary,
    LegacyBinary,
    Xml,
    Udt,
    Variant,
}

impl MssqlColTypeExt for MssqlColType {
    fn can_be_splitted(&self) -> bool {
        matches!(
            col_type_kind(self),
            MssqlColTypeKind::Integer
                | MssqlColTypeKind::Float
                | MssqlColTypeKind::Money
                | MssqlColTypeKind::Decimal
                | MssqlColTypeKind::Temporal
                | MssqlColTypeKind::Guid
                | MssqlColTypeKind::String
                | MssqlColTypeKind::Binary
        )
    }

    fn is_integer(&self) -> bool {
        matches!(col_type_kind(self), MssqlColTypeKind::Integer)
    }

    fn is_string(&self) -> bool {
        matches!(
            col_type_kind(self),
            MssqlColTypeKind::String | MssqlColTypeKind::LegacyText
        )
    }

    fn is_binary(&self) -> bool {
        matches!(
            col_type_kind(self),
            MssqlColTypeKind::Binary | MssqlColTypeKind::LegacyBinary
        )
    }
}

fn col_type_kind(col_type: &MssqlColType) -> MssqlColTypeKind {
    match col_type {
        MssqlColType::Null => MssqlColTypeKind::Null,
        MssqlColType::Bit | MssqlColType::Bitn => MssqlColTypeKind::Bool,
        MssqlColType::Int1
        | MssqlColType::Int2
        | MssqlColType::Int4
        | MssqlColType::Int8
        | MssqlColType::Intn => MssqlColTypeKind::Integer,
        MssqlColType::Float4 | MssqlColType::Float8 | MssqlColType::Floatn => {
            MssqlColTypeKind::Float
        }
        MssqlColType::Money | MssqlColType::Money4 => MssqlColTypeKind::Money,
        MssqlColType::Decimaln | MssqlColType::Numericn => MssqlColTypeKind::Decimal,
        MssqlColType::Datetime4
        | MssqlColType::Datetime
        | MssqlColType::Datetimen
        | MssqlColType::Daten
        | MssqlColType::Timen
        | MssqlColType::Datetime2
        | MssqlColType::DatetimeOffsetn => MssqlColTypeKind::Temporal,
        MssqlColType::Guid => MssqlColTypeKind::Guid,
        MssqlColType::BigVarChar
        | MssqlColType::BigChar
        | MssqlColType::NVarchar
        | MssqlColType::NChar => MssqlColTypeKind::String,
        MssqlColType::Text | MssqlColType::NText => MssqlColTypeKind::LegacyText,
        MssqlColType::BigVarBin | MssqlColType::BigBinary => MssqlColTypeKind::Binary,
        MssqlColType::Image => MssqlColTypeKind::LegacyBinary,
        MssqlColType::Xml => MssqlColTypeKind::Xml,
        MssqlColType::Udt => MssqlColTypeKind::Udt,
        MssqlColType::SSVariant => MssqlColTypeKind::Variant,
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
        "sql_variant" => MssqlColType::SSVariant,
        "geometry" | "geography" | "hierarchyid" => MssqlColType::Udt,
        _ => anyhow::bail!("unsupported MSSQL column type {type_name}"),
    };
    Ok(col_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_integer_and_splittable_types() {
        for col_type in [
            MssqlColType::Int1,
            MssqlColType::Int2,
            MssqlColType::Int4,
            MssqlColType::Int8,
            MssqlColType::Intn,
        ] {
            assert!(col_type.is_integer());
            assert!(col_type.can_be_splitted());
        }

        assert!(MssqlColType::NVarchar.can_be_splitted());
        assert!(MssqlColType::DatetimeOffsetn.can_be_splitted());
        assert!(!MssqlColType::Bitn.can_be_splitted());
        assert!(!MssqlColType::Xml.can_be_splitted());
        assert!(!MssqlColType::SSVariant.can_be_splitted());
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
        assert_eq!(
            parse_mssql_col_type("geography").unwrap(),
            MssqlColType::Udt
        );
        assert!(parse_mssql_col_type("unknown_type").is_err());
    }
}
