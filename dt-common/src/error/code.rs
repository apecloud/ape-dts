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
    UnsupportedStatement {
        code: "PR006",
        message: "A source statement is not supported by this migration",
        hint: "Execute the reported statement manually on the destination or exclude it from structure migration.",
    }
    ResourceExhausted {
        code: "RS001",
        message: "A source or destination resource limit was reached",
        hint: "Check endpoint capacity, quotas, connection limits, memory, disk space, and request load.",
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
    DatabaseOperationFailed {
        code: "DB001",
        message: "A database operation failed",
        hint: "Check the affected object and database logs for the rejected operation.",
    }
    DatabaseOperationTimeout {
        code: "DB002",
        message: "A database operation timed out",
        hint: "Check database load, locks, and operation timeout settings, then retry the operation.",
    }
    DatabaseOperationConflict {
        code: "DB003",
        message: "A database operation conflicts with concurrent or existing database state",
        hint: "Retry after the conflicting operation completes, or resolve the reported database state.",
    }
    DataDecodeFailed {
        code: "DT001",
        message: "Migration data could not be decoded",
        hint: "Check the reported source payload or persisted data format and ensure it is compatible with this Ape-DTS version.",
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
        hint: "Check the reported path or endpoint storage, permissions, availability, and disk space.",
    }
    WorkerFailed {
        code: "RT001",
        message: "The task stopped unexpectedly",
        hint: "Retry once; if the error repeats, contact support with the task ID and error code.",
    }
    OperationInterrupted {
        code: "RT002",
        message: "The requested operation was interrupted",
        hint: "Retry if the interruption was transient; otherwise check task cancellation, shutdown, and provider timeout settings.",
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
