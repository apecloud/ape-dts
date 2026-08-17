use anyhow::Context;
use mongodb::{
    bson::{doc, Document},
    Client,
};

use crate::{
    config::config_enums::DbType,
    error::{DtError, DtOptionExt, DtResultExt, ErrorCode},
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MongoServerVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl MongoServerVersion {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    pub fn parse(version: &str) -> anyhow::Result<Self> {
        let mut parts = version.split(['.', '-', '+']);
        let major = parse_version_part(parts.next(), version, "major")?;
        let minor = match parts.next() {
            Some(part) => parse_version_part(Some(part), version, "minor")?,
            None => 0,
        };
        let patch = match parts.next() {
            Some(part) => parse_version_part(Some(part), version, "patch")?,
            None => 0,
        };
        Ok(Self::new(major, minor, patch))
    }
}

impl std::fmt::Display for MongoServerVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

pub async fn get_server_version(client: &Client) -> anyhow::Result<MongoServerVersion> {
    let build_info: Document = client
        .default_database()
        .unwrap_or_else(|| client.database("admin"))
        .run_command(doc! { "buildInfo": 1 })
        .await
        .code(ErrorCode::MetadataReadFailed)?;
    let version = build_info
        .get_str("version")
        .context(DtError::UnsupportedDatabaseVersion(
            DbType::Mongo,
            "MongoDB buildInfo response is missing a valid version".to_string(),
        ))?;
    MongoServerVersion::parse(version)
}

fn parse_version_part(part: Option<&str>, original: &str, field: &str) -> anyhow::Result<u32> {
    let part = part.or_dt_error(DtError::UnsupportedDatabaseVersion(
        DbType::Mongo,
        format!("MongoDB version is missing {field}: {original}"),
    ))?;
    let digits: String = part.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return Err(DtError::UnsupportedDatabaseVersion(
            DbType::Mongo,
            format!("invalid MongoDB version {field}: {original}"),
        )
        .into());
    }
    digits.parse().context(DtError::UnsupportedDatabaseVersion(
        DbType::Mongo,
        format!("invalid MongoDB version {field}: {original}"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_stable_version() {
        assert_eq!(
            MongoServerVersion::parse("6.0.14").unwrap(),
            MongoServerVersion::new(6, 0, 14)
        );
    }

    #[test]
    fn parse_prerelease_version() {
        assert_eq!(
            MongoServerVersion::parse("7.0.0-rc0").unwrap(),
            MongoServerVersion::new(7, 0, 0)
        );
    }

    #[test]
    fn parse_development_version() {
        assert_eq!(
            MongoServerVersion::parse("8.1.0-alpha-123-gabcdef").unwrap(),
            MongoServerVersion::new(8, 1, 0)
        );
    }
}
