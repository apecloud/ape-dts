use std::fmt;

use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorCode {
    MissingConfig,
    MissingConfigItem,
    InvalidConfig,
    ConnectionFailed,
    ConnectionTimeout,
    TlsFailed,
    AuthenticationFailed,
    PermissionDenied,
    PrerequisiteNotMet,
    UnsupportedDatabaseVersion,
    CdcNotEnabled,
    ReplicationCapacityExhausted,
    UnsupportedTableStructure,
    ObjectNotFound,
    DatabaseNotFound,
    MetadataFailed,
    StatementFailed,
    IntegrityViolation,
    CheckpointReadFailed,
    IoFailed,
    WorkerFailed,
    InvariantViolated,
    Unclassified,
}

impl ErrorCode {
    pub const ALL: [Self; 23] = [
        Self::MissingConfig,
        Self::MissingConfigItem,
        Self::InvalidConfig,
        Self::ConnectionFailed,
        Self::ConnectionTimeout,
        Self::TlsFailed,
        Self::AuthenticationFailed,
        Self::PermissionDenied,
        Self::PrerequisiteNotMet,
        Self::UnsupportedDatabaseVersion,
        Self::CdcNotEnabled,
        Self::ReplicationCapacityExhausted,
        Self::UnsupportedTableStructure,
        Self::ObjectNotFound,
        Self::DatabaseNotFound,
        Self::MetadataFailed,
        Self::StatementFailed,
        Self::IntegrityViolation,
        Self::CheckpointReadFailed,
        Self::IoFailed,
        Self::WorkerFailed,
        Self::InvariantViolated,
        Self::Unclassified,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingConfig => "CF001",
            Self::MissingConfigItem => "CF003",
            Self::InvalidConfig => "CF002",
            Self::ConnectionFailed => "CN001",
            Self::ConnectionTimeout => "CN002",
            Self::TlsFailed => "CN003",
            Self::AuthenticationFailed => "AU001",
            Self::PermissionDenied => "AU002",
            Self::PrerequisiteNotMet => "PR001",
            Self::UnsupportedDatabaseVersion => "PR002",
            Self::CdcNotEnabled => "PR003",
            Self::ReplicationCapacityExhausted => "PR004",
            Self::UnsupportedTableStructure => "PR005",
            Self::ObjectNotFound => "MD001",
            Self::DatabaseNotFound => "MD002",
            Self::MetadataFailed => "MD099",
            Self::StatementFailed => "DB001",
            Self::IntegrityViolation => "IC001",
            Self::CheckpointReadFailed => "ST001",
            Self::IoFailed => "IO001",
            Self::WorkerFailed => "RT001",
            Self::InvariantViolated => "IN001",
            Self::Unclassified => "IN999",
        }
    }

    pub const fn default_message(self) -> &'static str {
        match self {
            Self::MissingConfig => "The task configuration could not be found",
            Self::MissingConfigItem => "A required configuration item is missing",
            Self::InvalidConfig => "The task configuration is invalid",
            Self::ConnectionFailed => "The configured endpoint could not be reached",
            Self::ConnectionTimeout => "The connection to the configured endpoint timed out",
            Self::TlsFailed => {
                "A secure connection to the configured endpoint could not be established"
            }
            Self::AuthenticationFailed => "The configured endpoint rejected the credentials",
            Self::PermissionDenied => {
                "The configured account does not have the required permission"
            }
            Self::PrerequisiteNotMet => "A migration prerequisite is not met",
            Self::UnsupportedDatabaseVersion => "The database version is not supported",
            Self::CdcNotEnabled => "The source database is not configured for CDC",
            Self::ReplicationCapacityExhausted => {
                "The source database has no available replication capacity"
            }
            Self::UnsupportedTableStructure => {
                "A table structure is not supported by this migration"
            }
            Self::ObjectNotFound => "A required source or destination object was not found",
            Self::DatabaseNotFound => "The configured database was not found",
            Self::MetadataFailed => "Source or destination metadata could not be read",
            Self::StatementFailed => "A source or destination operation failed",
            Self::IntegrityViolation => "Data violates a destination constraint",
            Self::CheckpointReadFailed => "Saved task progress could not be restored",
            Self::IoFailed => "A required file or I/O operation failed",
            Self::WorkerFailed => "The task stopped unexpectedly",
            Self::InvariantViolated => "The task encountered an internal error",
            Self::Unclassified => "The task failed unexpectedly",
        }
    }

    pub const fn default_hint(self) -> &'static str {
        match self {
            Self::MissingConfig => {
                "Provide an existing task config with --config <CONFIG> or as a positional path."
            }
            Self::MissingConfigItem => {
                "Add the reported configuration item and start the task again."
            }
            Self::InvalidConfig => {
                "Correct the reported configuration value and start the task again."
            }
            Self::ConnectionFailed => {
                "Check the endpoint address, service status, network, firewall, and security group."
            }
            Self::ConnectionTimeout => {
                "Check endpoint reachability and connection load, then retry the task."
            }
            Self::TlsFailed => {
                "Check the TLS mode, CA certificate, client certificate, and endpoint hostname."
            }
            Self::AuthenticationFailed => {
                "Check the configured username, password, and endpoint authentication settings."
            }
            Self::PermissionDenied => {
                "Grant the required source or destination permissions to the configured account."
            }
            Self::PrerequisiteNotMet => {
                "Resolve the reported precheck requirement before starting the migration."
            }
            Self::UnsupportedDatabaseVersion => {
                "Use a supported database version or a compatible Ape-DTS release."
            }
            Self::CdcNotEnabled => {
                "Enable the required database change-log settings and run precheck again."
            }
            Self::ReplicationCapacityExhausted => {
                "Increase replication capacity or remove an unused replication slot or sender."
            }
            Self::UnsupportedTableStructure => {
                "Adjust the reported table structure or exclude the table from the migration."
            }
            Self::ObjectNotFound => {
                "Check object routing and create the required object or enable structure initialization."
            }
            Self::DatabaseNotFound => {
                "Check the database name and create the database if it is required."
            }
            Self::MetadataFailed => {
                "Check metadata permissions and endpoint availability, then retry."
            }
            Self::StatementFailed => {
                "Check the affected object and source or destination logs for the rejected operation."
            }
            Self::IntegrityViolation => {
                "Check duplicate keys, constraints, and the configured conflict policy."
            }
            Self::CheckpointReadFailed => {
                "Check the metadata endpoint, checkpoint object, and account permissions."
            }
            Self::IoFailed => "Check the reported path, file permissions, and available disk space.",
            Self::WorkerFailed | Self::InvariantViolated | Self::Unclassified => {
                "Retry once; if the error repeats, contact support with the task ID and error code."
            }
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for ErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn registry_has_stable_valid_codes() {
        let mut codes = HashSet::new();
        for code in ErrorCode::ALL {
            let value = code.as_str();
            assert_eq!(value.len(), 5);
            assert!(value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()));
            assert!(!value.ends_with("000"));
            assert!(!code.default_message().is_empty());
            assert!(!code.default_hint().is_empty());
            assert!(codes.insert(value), "duplicate error code: {value}");
        }
    }

    #[test]
    fn serializes_as_wire_code() {
        assert_eq!(
            serde_json::to_string(&ErrorCode::ObjectNotFound).unwrap(),
            "\"MD001\""
        );
    }
}
