use mongodb::{
    bson::{doc, Document},
    Client,
};

use crate::{
    error::{DtError, DtErrorContextExt, ErrorCode, OriginError},
    error_boundary::metadata::{mongodb_provider, mongodb_version_source},
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
        .map_err(|error| mongodb_provider(error, ErrorCode::MetadataReadFailed))?;
    let version = build_info.get_str("version").map_err(|error| {
        mongodb_version_source(
            "MongoDB buildInfo response is missing a valid version",
            error,
        )
    })?;
    MongoServerVersion::parse(version)
}

fn parse_version_part(part: Option<&str>, original: &str, field: &str) -> anyhow::Result<u32> {
    let part = part.ok_or_else(|| {
        DtError::UnsupportedDatabaseVersion(format!(
            "MongoDB version is missing {field}: {original}"
        ))
        .with_origin(OriginError::new("mongodb", None::<String>))
    })?;
    let digits: String = part.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return Err(DtError::UnsupportedDatabaseVersion(format!(
            "invalid MongoDB version {field}: {original}"
        ))
        .with_origin(OriginError::new("mongodb", None::<String>)));
    }
    digits.parse().map_err(|error| {
        mongodb_version_source(
            format!("invalid MongoDB version {field}: {original}"),
            error,
        )
    })
}

#[cfg(test)]
mod tests {
    use crate::error::AnyhowErrorExt;

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

    #[test]
    fn invalid_version_is_classified_as_unsupported() {
        let error = MongoServerVersion::parse("6.invalid").unwrap_err();
        assert_eq!(
            error.error_code(),
            Some(ErrorCode::UnsupportedDatabaseVersion)
        );
    }
}
