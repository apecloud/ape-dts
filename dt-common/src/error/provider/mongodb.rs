use std::io::ErrorKind;

use mongodb::error::{
    Error as MongoError, ErrorKind as MongoErrorKind, GridFsErrorKind, WriteFailure,
    SYSTEM_OVERLOADED_ERROR,
};

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode},
    classification::{provider_context, provider_detail},
};

impl ClassifyError for MongoError {
    fn classify(&self) -> DtErrorContext {
        let provider_codes = mongodb_error_codes(&self.kind);
        let provider_code = (!provider_codes.is_empty()).then(|| {
            provider_codes
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        });
        let code = if self.contains_label(SYSTEM_OVERLOADED_ERROR) {
            Some(ErrorCode::ResourceExhausted)
        } else {
            classify_mongodb_kind(&self.kind)
                .or_else(|| classify_mongodb_codes(provider_codes.as_slice()))
        };
        provider_context(
            Some(code.unwrap_or(ErrorCode::DatabaseOperationFailed)),
            provider_detail("mongodb", provider_code, self),
        )
    }
}

fn classify_mongodb_kind(kind: &MongoErrorKind) -> Option<ErrorCode> {
    match kind {
        MongoErrorKind::InvalidArgument { .. } => Some(ErrorCode::InvalidConfig),
        MongoErrorKind::Authentication { .. } => Some(ErrorCode::AuthenticationFailed),
        MongoErrorKind::InvalidTlsConfig { .. } => Some(ErrorCode::TlsFailed),
        MongoErrorKind::Io(error) if is_timeout_kind(error.kind()) => {
            Some(ErrorCode::ConnectionTimeout)
        }
        MongoErrorKind::Io(_)
        | MongoErrorKind::DnsResolve { .. }
        | MongoErrorKind::ConnectionPoolCleared { .. }
        | MongoErrorKind::InvalidResponse { .. }
        | MongoErrorKind::ServerSelection { .. }
        | MongoErrorKind::Shutdown => Some(ErrorCode::ConnectionFailed),
        MongoErrorKind::SessionsNotSupported | MongoErrorKind::IncompatibleServer { .. } => {
            Some(ErrorCode::PrerequisiteNotMet)
        }
        MongoErrorKind::MissingResumeToken => Some(ErrorCode::CheckpointReadFailed),
        MongoErrorKind::GridFs(
            GridFsErrorKind::FileNotFound { .. }
            | GridFsErrorKind::RevisionNotFound { .. }
            | GridFsErrorKind::MissingChunk { .. },
        ) => Some(ErrorCode::ObjectNotFound),
        _ => None,
    }
}

fn classify_mongodb_command_code(code: i32) -> Option<ErrorCode> {
    match code {
        13 => Some(ErrorCode::PermissionDenied),
        18 | 391 => Some(ErrorCode::AuthenticationFailed),
        26 | 27 => Some(ErrorCode::ObjectNotFound),
        50 | 262 => Some(ErrorCode::DatabaseOperationTimeout),
        112 | 244 | 251 => Some(ErrorCode::DatabaseOperationConflict),
        121 | 11000 | 11001 | 12582 => Some(ErrorCode::IntegrityViolation),
        6 | 7 | 89 | 91 | 189 | 9001 | 10058 | 10107 | 11600 | 11602 | 13435 | 13436 => {
            Some(ErrorCode::ConnectionFailed)
        }
        _ => None,
    }
}

fn classify_mongodb_codes(codes: &[i32]) -> Option<ErrorCode> {
    codes
        .iter()
        .find_map(|code| classify_mongodb_command_code(*code))
}

fn mongodb_error_codes(kind: &MongoErrorKind) -> Vec<i32> {
    let codes = match kind {
        MongoErrorKind::Command(error) => vec![error.code],
        MongoErrorKind::InsertMany(error) => {
            let mut codes = error
                .write_concern_error
                .iter()
                .map(|error| error.code)
                .collect::<Vec<_>>();
            if let Some(write_errors) = &error.write_errors {
                codes.extend(write_errors.iter().map(|error| error.code));
            }
            codes
        }
        MongoErrorKind::BulkWrite(error) => {
            let mut codes = error
                .write_concern_errors
                .iter()
                .map(|error| error.code)
                .collect::<Vec<_>>();
            let mut write_errors = error.write_errors.iter().collect::<Vec<_>>();
            write_errors.sort_unstable_by_key(|(index, _)| *index);
            codes.extend(write_errors.into_iter().map(|(_, error)| error.code));
            codes
        }
        MongoErrorKind::Write(WriteFailure::WriteConcernError(error)) => vec![error.code],
        MongoErrorKind::Write(WriteFailure::WriteError(error)) => vec![error.code],
        _ => Vec::new(),
    };
    let mut unique_codes = Vec::with_capacity(codes.len());
    for code in codes {
        if !unique_codes.contains(&code) {
            unique_codes.push(code);
        }
    }
    unique_codes
}

fn is_timeout_kind(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::TimedOut | ErrorKind::WouldBlock)
}
