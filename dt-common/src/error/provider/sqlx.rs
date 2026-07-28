use sqlx::error::ErrorKind;

use super::{
    super::{ClassifyError, DtErrorContext, ErrorCode, ErrorObject},
    classification::{
        classify_mysql_code, classify_postgres_code, provider_context, provider_detail,
    },
};

pub fn classify_sqlx_error(error: &sqlx::Error) -> DtErrorContext {
    let mut code = None;
    let mut object = None;

    match error {
        sqlx::Error::Database(database_error) => {
            code = classify_database_error(database_error.as_ref());
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

    let context = provider_context(code, sqlx_detail(error));
    match object {
        Some(object) => context.with_object(object),
        None => context,
    }
}

fn sqlx_detail(error: &sqlx::Error) -> String {
    let sqlx::Error::Database(database_error) = error else {
        return provider_detail("sqlx", None, error);
    };
    if database_error
        .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
        .is_some()
    {
        provider_detail(
            "postgres",
            database_error.code().map(|code| code.into_owned()),
            error,
        )
    } else if let Some(mysql_error) =
        database_error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
    {
        provider_detail("mysql", Some(mysql_error.number().to_string()), error)
    } else {
        provider_detail(
            "sqlx",
            database_error.code().map(|code| code.into_owned()),
            error,
        )
    }
}

impl ClassifyError for sqlx::Error {
    fn classify(&self) -> DtErrorContext {
        classify_sqlx_error(self)
    }
}

fn classify_database_error(
    error: &(dyn sqlx::error::DatabaseError + 'static),
) -> Option<ErrorCode> {
    let classified_code =
        if let Some(mysql_error) = error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>() {
            classify_mysql_code(&mysql_error.number().to_string())
        } else if error
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            .is_some()
        {
            error.code().as_deref().and_then(classify_postgres_code)
        } else {
            None
        };
    if matches!(error.kind(), ErrorKind::Other) {
        classified_code
    } else {
        Some(ErrorCode::IntegrityViolation)
    }
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
