use sqlx::error::ErrorKind;

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode, ErrorObject, OriginError},
    classification::{classify_mysql_code, classify_postgres_code, provider_context},
};

pub fn classify_sqlx_error(error: &sqlx::Error) -> DtErrorContext {
    let mut system = "sqlx";
    let mut code = None;
    let mut origin_code = None;
    let mut object = None;

    match error {
        sqlx::Error::Database(database_error) => {
            (system, origin_code, code) = classify_database_error(database_error.as_ref());
            let mut error_object = ErrorObject {
                table: database_error.table().map(str::to_string),
                constraint: database_error.constraint().map(str::to_string),
                ..Default::default()
            };
            if let Some(pg_error) =
                database_error.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            {
                error_object.schema = pg_error.schema().map(str::to_string);
                error_object.column = pg_error.column().map(str::to_string);
            }
            if error_object != ErrorObject::default() {
                object = Some(error_object);
            }
        }
        sqlx::Error::PoolTimedOut => code = Some(ErrorCode::ConnectionTimeout),
        sqlx::Error::Configuration(_) => code = Some(ErrorCode::InvalidConfig),
        sqlx::Error::Tls(_) => code = Some(ErrorCode::TlsFailed),
        sqlx::Error::Io(_) | sqlx::Error::Protocol(_) | sqlx::Error::PoolClosed => {
            code = Some(ErrorCode::ConnectionFailed)
        }
        sqlx::Error::WorkerCrashed => code = Some(ErrorCode::WorkerFailed),
        _ => {}
    }

    let context = provider_context(code, OriginError::new(system, origin_code));
    match object {
        Some(object) => context.with_object(object),
        None => context,
    }
}

impl ClassifyError for sqlx::Error {
    fn classify(&self) -> DtErrorContext {
        classify_sqlx_error(self)
    }
}

fn classify_database_error(
    error: &(dyn sqlx::error::DatabaseError + 'static),
) -> (&'static str, Option<String>, Option<ErrorCode>) {
    let (system, provider_code, classified_code) =
        if let Some(mysql_error) = error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>() {
            let provider_code = mysql_error.number().to_string();
            let classified_code = classify_mysql_code(&provider_code);
            ("mysql", Some(provider_code), classified_code)
        } else if error
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            .is_some()
        {
            let provider_code = error.code().map(|code| code.into_owned());
            let classified_code = provider_code.as_deref().and_then(classify_postgres_code);
            ("postgres", provider_code, classified_code)
        } else {
            ("sqlx", error.code().map(|code| code.into_owned()), None)
        };
    let code = if matches!(error.kind(), ErrorKind::Other) {
        classified_code
    } else {
        Some(ErrorCode::IntegrityViolation)
    };
    (system, provider_code, code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_sqlx_transport_errors() {
        for (error, expected) in [
            (
                sqlx::Error::PoolTimedOut,
                Some(ErrorCode::ConnectionTimeout),
            ),
            (sqlx::Error::PoolClosed, Some(ErrorCode::ConnectionFailed)),
            (
                sqlx::Error::Tls(Box::new(std::io::Error::other("invalid certificate"))),
                Some(ErrorCode::TlsFailed),
            ),
            (sqlx::Error::WorkerCrashed, Some(ErrorCode::WorkerFailed)),
            (
                sqlx::Error::Configuration(Box::new(std::io::Error::other("invalid database URL"))),
                Some(ErrorCode::InvalidConfig),
            ),
            (sqlx::Error::RowNotFound, None),
        ] {
            assert_eq!(classify_sqlx_error(&error).error_code(), expected);
        }
    }
}
