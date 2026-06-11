use dt_common::config::config_enums::DbType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbVersion {
    pub raw: String,
    pub major: u16,
    pub minor: Option<u16>,
    pub patch: Option<u16>,
}

impl DbVersion {
    pub fn parse(raw: &str) -> Self {
        let mut parts = raw
            .split(|c: char| !c.is_ascii_digit() && c != '.')
            .find(|s| !s.is_empty())
            .unwrap_or_default()
            .split('.');

        let major = parts
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();
        let minor = parts.next().and_then(|s| s.parse().ok());
        let patch = parts.next().and_then(|s| s.parse().ok());

        Self {
            raw: raw.to_string(),
            major,
            minor,
            patch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockDbContext {
    pub db_type: DbType,
    pub version: DbVersion,
}

impl MockDbContext {
    pub fn new(db_type: DbType, raw_version: &str) -> Self {
        Self {
            db_type,
            version: DbVersion::parse(raw_version),
        }
    }
}
