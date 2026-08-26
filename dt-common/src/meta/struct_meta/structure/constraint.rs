use crate::config::config_enums::DbType;

#[derive(Debug, Clone)]
pub struct Constraint {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub constraint_name: String,
    pub constraint_type: ConstraintType,
    pub definition: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConstraintType {
    Primary,
    Unique,
    Check,
    Foreign,
    Unknown,
}

impl ConstraintType {
    pub fn from_str(str: &str, db_type: DbType) -> Self {
        match db_type {
            DbType::Mysql => match str {
                "PRIMARY KEY" => Self::Primary,
                "UNIQUE" => Self::Unique,
                "CHECK" => Self::Check,
                "FOREIGN KEY" => Self::Foreign,
                _ => Self::Unknown,
            },

            DbType::Pg => match str {
                "p" | "112" => Self::Primary,
                "u" | "117" => Self::Unique,
                "c" | "99" => Self::Check,
                "f" | "102" => Self::Foreign,
                _ => Self::Unknown,
            },

            _ => Self::Unknown,
        }
    }

    pub fn to_str(&self, db_type: DbType) -> &str {
        match db_type {
            DbType::Mysql => match self {
                Self::Primary => "PRIMARY KEY",
                Self::Unique => "UNIQUE",
                Self::Foreign => "FOREIGN KEY",
                Self::Check => "CHECK",
                Self::Unknown => "unknown",
            },

            DbType::Pg => match self {
                Self::Primary => "p",
                Self::Unique => "u",
                Self::Foreign => "f",
                Self::Check => "c",
                Self::Unknown => "unknown",
            },

            _ => "unknown",
        }
    }
}
