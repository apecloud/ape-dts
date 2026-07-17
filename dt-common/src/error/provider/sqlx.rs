use sqlx::error::ErrorKind;

use super::{
    super::{DtError, ErrorCode, ErrorObject, OriginError},
    classification::{classify_mysql_code, classify_postgres_code},
    ProviderErrorClassification,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlxProvider {
    MySql,
    Postgres,
    Unknown,
}

impl SqlxProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MySql => "mysql",
            Self::Postgres => "postgres",
            Self::Unknown => "sqlx",
        }
    }
}

pub fn classify_sqlx_error(
    error: &sqlx::Error,
    provider: SqlxProvider,
    fallback: ErrorCode,
) -> ProviderErrorClassification {
    let provider = infer_provider(error, provider);
    let mut code = fallback;
    let mut origin_code = None;
    let mut object = None;

    match error {
        sqlx::Error::Database(database_error) => {
            origin_code = database_code(database_error.as_ref(), provider);
            code = classify_database_error(
                provider,
                origin_code.as_deref(),
                &database_error.kind(),
                fallback,
            );
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
        sqlx::Error::PoolTimedOut => code = ErrorCode::ConnectionTimeout,
        sqlx::Error::Configuration(_) => code = ErrorCode::InvalidConfig,
        sqlx::Error::Tls(_) => code = ErrorCode::TlsFailed,
        sqlx::Error::Io(_) | sqlx::Error::Protocol(_) | sqlx::Error::PoolClosed => {
            code = ErrorCode::ConnectionFailed
        }
        sqlx::Error::WorkerCrashed => code = ErrorCode::WorkerFailed,
        _ => {}
    }

    ProviderErrorClassification::new(code, OriginError::new(provider.as_str(), origin_code))
        .object(object)
}

#[track_caller]
pub fn try_dt_error_from_anyhow_sqlx(
    error: anyhow::Error,
    provider: SqlxProvider,
    fallback: ErrorCode,
) -> Result<DtError, anyhow::Error> {
    if error.downcast_ref::<DtError>().is_some() {
        return Err(error);
    }
    let Some(sqlx_error) = error.downcast_ref::<sqlx::Error>() else {
        return Err(error);
    };
    let classification = classify_sqlx_error(sqlx_error, provider, fallback);
    let mut dt_error = DtError::new(classification.code)
        .origin(classification.origin)
        .source(error.into_boxed_dyn_error());
    if let Some(object) = classification.object {
        dt_error = dt_error.object(object);
    }
    Ok(dt_error)
}

#[track_caller]
pub fn dt_error_from_sqlx(
    error: sqlx::Error,
    provider: SqlxProvider,
    fallback: ErrorCode,
) -> DtError {
    let classification = classify_sqlx_error(&error, provider, fallback);
    let mut dt_error = DtError::new(classification.code)
        .origin(classification.origin)
        .source(error);
    if let Some(object) = classification.object {
        dt_error = dt_error.object(object);
    }
    dt_error
}

fn infer_provider(error: &sqlx::Error, provider: SqlxProvider) -> SqlxProvider {
    if provider != SqlxProvider::Unknown {
        return provider;
    }
    let sqlx::Error::Database(database_error) = error else {
        return provider;
    };
    if database_error
        .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
        .is_some()
    {
        SqlxProvider::MySql
    } else if database_error
        .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
        .is_some()
    {
        SqlxProvider::Postgres
    } else {
        provider
    }
}

fn database_code(
    error: &(dyn sqlx::error::DatabaseError + 'static),
    provider: SqlxProvider,
) -> Option<String> {
    if provider == SqlxProvider::MySql {
        if let Some(mysql_error) = error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>() {
            return Some(mysql_error.number().to_string());
        }
    }
    error.code().map(|code| code.into_owned())
}

fn classify_database_error(
    provider: SqlxProvider,
    provider_code: Option<&str>,
    kind: &ErrorKind,
    fallback: ErrorCode,
) -> ErrorCode {
    if !matches!(kind, ErrorKind::Other) {
        return ErrorCode::IntegrityViolation;
    }

    match (provider, provider_code) {
        (SqlxProvider::Postgres, Some(code)) => classify_postgres_code(code, fallback),
        (SqlxProvider::MySql, Some(code)) => classify_mysql_code(code, fallback),
        _ => fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(provider: SqlxProvider, code: &str) -> ErrorCode {
        classify_database_error(
            provider,
            Some(code),
            &ErrorKind::Other,
            ErrorCode::StatementFailed,
        )
    }

    #[test]
    fn classifies_provider_codes() {
        assert_eq!(
            classify(SqlxProvider::Postgres, "42P01"),
            ErrorCode::ObjectNotFound
        );
        assert_eq!(
            classify(SqlxProvider::MySql, "1146"),
            ErrorCode::ObjectNotFound
        );
        assert_eq!(
            classify(SqlxProvider::Postgres, "3D000"),
            ErrorCode::DatabaseNotFound
        );
        assert_eq!(
            classify(SqlxProvider::MySql, "1049"),
            ErrorCode::DatabaseNotFound
        );
        assert_eq!(
            classify(SqlxProvider::Postgres, "28P01"),
            ErrorCode::AuthenticationFailed
        );
        assert_eq!(
            classify(SqlxProvider::MySql, "1045"),
            ErrorCode::AuthenticationFailed
        );
        assert_eq!(
            classify(SqlxProvider::Postgres, "42501"),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            classify(SqlxProvider::MySql, "1142"),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            classify(SqlxProvider::Postgres, "08006"),
            ErrorCode::ConnectionFailed
        );
        assert_eq!(
            classify(SqlxProvider::Postgres, "42703"),
            ErrorCode::ObjectNotFound
        );
        assert_eq!(
            classify(SqlxProvider::MySql, "2003"),
            ErrorCode::ConnectionFailed
        );
        assert_eq!(
            classify(SqlxProvider::MySql, "1227"),
            ErrorCode::PermissionDenied
        );
    }

    #[test]
    fn classifies_integrity_and_fallback() {
        assert_eq!(
            classify_database_error(
                SqlxProvider::Postgres,
                Some("23505"),
                &ErrorKind::UniqueViolation,
                ErrorCode::StatementFailed,
            ),
            ErrorCode::IntegrityViolation
        );
        assert_eq!(
            classify(SqlxProvider::Postgres, "XX000"),
            ErrorCode::StatementFailed
        );
    }

    #[test]
    fn classifies_sqlx_transport_errors() {
        assert_eq!(
            classify_sqlx_error(
                &sqlx::Error::PoolTimedOut,
                SqlxProvider::Postgres,
                ErrorCode::StatementFailed,
            )
            .code,
            ErrorCode::ConnectionTimeout
        );
        assert_eq!(
            classify_sqlx_error(
                &sqlx::Error::PoolClosed,
                SqlxProvider::Postgres,
                ErrorCode::StatementFailed,
            )
            .code,
            ErrorCode::ConnectionFailed
        );
        assert_eq!(
            classify_sqlx_error(
                &sqlx::Error::Tls(Box::new(std::io::Error::other("invalid certificate"))),
                SqlxProvider::Postgres,
                ErrorCode::StatementFailed,
            )
            .code,
            ErrorCode::TlsFailed
        );
        assert_eq!(
            classify_sqlx_error(
                &sqlx::Error::WorkerCrashed,
                SqlxProvider::Postgres,
                ErrorCode::StatementFailed,
            )
            .code,
            ErrorCode::WorkerFailed
        );

        let configuration =
            sqlx::Error::Configuration(Box::new(std::io::Error::other("invalid database URL")));
        assert_eq!(
            classify_sqlx_error(
                &configuration,
                SqlxProvider::Postgres,
                ErrorCode::ConnectionFailed,
            )
            .code,
            ErrorCode::InvalidConfig
        );
    }

    #[test]
    fn wraps_sqlx_from_anyhow_but_preserves_existing_dt_error() {
        let sqlx_error =
            anyhow::Error::new(sqlx::Error::PoolTimedOut).context("opening sink connection");
        let dt_error = try_dt_error_from_anyhow_sqlx(
            sqlx_error,
            SqlxProvider::Postgres,
            ErrorCode::StatementFailed,
        )
        .unwrap();
        assert_eq!(dt_error.code(), ErrorCode::ConnectionTimeout);
        assert_eq!(dt_error.origin_error().unwrap().system, "postgres");

        let existing = anyhow::Error::new(DtError::new(ErrorCode::InvariantViolated));
        assert!(try_dt_error_from_anyhow_sqlx(
            existing,
            SqlxProvider::Postgres,
            ErrorCode::StatementFailed,
        )
        .is_err());
    }
}
