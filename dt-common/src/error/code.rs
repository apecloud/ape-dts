use std::fmt;

use serde::Serialize;

macro_rules! define_error_codes {
    (
        $(
            $variant:ident {
                code: $code:literal,
                message: $message:literal,
                hint: $hint:literal,
            }
        )+
    ) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        pub enum ErrorCode {
            $($variant,)+
        }

        impl ErrorCode {
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $code,)+
                }
            }

            pub const fn default_message(self) -> &'static str {
                match self {
                    $(Self::$variant => $message,)+
                }
            }

            pub const fn default_hint(self) -> &'static str {
                match self {
                    $(Self::$variant => $hint,)+
                }
            }
        }
    };
}

define_error_codes! {
    MissingConfig {
        code: "CF001",
        message: "The task configuration could not be found",
        hint: "Provide an existing task config with --config <CONFIG> or as a positional path.",
    }
    MissingConfigItem {
        code: "CF003",
        message: "A required configuration item is missing",
        hint: "Add the reported configuration item and start the task again.",
    }
    InvalidConfig {
        code: "CF002",
        message: "The task configuration is invalid",
        hint: "Correct the reported configuration value and start the task again.",
    }
    ConnectionFailed {
        code: "CN001",
        message: "The configured endpoint could not be reached",
        hint: "Check the endpoint address, service status, network, firewall, and security group.",
    }
    ConnectionTimeout {
        code: "CN002",
        message: "The connection to the configured endpoint timed out",
        hint: "Check endpoint reachability and connection load, then retry the task.",
    }
    TlsFailed {
        code: "CN003",
        message: "A secure connection to the configured endpoint could not be established",
        hint: "Check the TLS mode, CA certificate, client certificate, and endpoint hostname.",
    }
    AuthenticationFailed {
        code: "AU001",
        message: "The configured endpoint rejected the credentials",
        hint: "Check the configured username, password, and endpoint authentication settings.",
    }
    PermissionDenied {
        code: "AU002",
        message: "The configured account does not have the required permission",
        hint: "Grant the required source or destination permissions to the configured account.",
    }
    PrerequisiteNotMet {
        code: "PR001",
        message: "A migration prerequisite is not met",
        hint: "Resolve the reported precheck requirement before starting the migration.",
    }
    UnsupportedDatabaseVersion {
        code: "PR002",
        message: "The database version is not supported",
        hint: "Use a supported database version or a compatible Ape-DTS release.",
    }
    CdcNotEnabled {
        code: "PR003",
        message: "The source database is not configured for CDC",
        hint: "Enable the required database change-log settings and run precheck again.",
    }
    ReplicationCapacityExhausted {
        code: "PR004",
        message: "The source database has no available replication capacity",
        hint: "Increase replication capacity or remove an unused replication slot or sender.",
    }
    UnsupportedTableStructure {
        code: "PR005",
        message: "A table structure is not supported by this migration",
        hint: "Adjust the reported table structure or exclude the table from the migration.",
    }
    ObjectNotFound {
        code: "MD001",
        message: "A required source or destination object was not found",
        hint: "Check object routing and create the required object or enable structure initialization.",
    }
    DatabaseNotFound {
        code: "MD002",
        message: "The configured database was not found",
        hint: "Check the database name and create the database if it is required.",
    }
    MetadataFailed {
        code: "MD099",
        message: "Source or destination metadata could not be read",
        hint: "Check metadata permissions and endpoint availability, then retry.",
    }
    StatementFailed {
        code: "DB001",
        message: "A source or destination operation failed",
        hint: "Check the affected object and source or destination logs for the rejected operation.",
    }
    IntegrityViolation {
        code: "IC001",
        message: "Data violates a destination constraint",
        hint: "Check duplicate keys, constraints, and the configured conflict policy.",
    }
    CheckpointReadFailed {
        code: "ST001",
        message: "Saved task progress could not be restored",
        hint: "Check the metadata endpoint, checkpoint object, and account permissions.",
    }
    IoFailed {
        code: "IO001",
        message: "A required file or I/O operation failed",
        hint: "Check the reported path, file permissions, and available disk space.",
    }
    WorkerFailed {
        code: "RT001",
        message: "The task stopped unexpectedly",
        hint: "Retry once; if the error repeats, contact support with the task ID and error code.",
    }
    InvariantViolated {
        code: "IN001",
        message: "The task encountered an internal error",
        hint: "Retry once; if the error repeats, contact support with the task ID and error code.",
    }
    Unclassified {
        code: "IN999",
        message: "The task failed unexpectedly",
        hint: "Retry once; if the error repeats, contact support with the task ID and error code.",
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
        for &code in ErrorCode::ALL {
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
