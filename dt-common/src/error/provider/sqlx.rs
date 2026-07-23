use sqlx::error::ErrorKind;

use super::{
    super::{
        ClassifyError, DtErrorContext, DtErrorContextExt, ErrorCode, ErrorObject, OriginError,
    },
    classification::{classify_mysql_code, classify_postgres_code, provider_context},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlxProvider {
    MySql,
    Postgres,
    Unknown,
}

pub trait SqlxErrorExt {
    fn with_sqlx_provider(self, provider: SqlxProvider) -> anyhow::Error;
}

impl SqlxErrorExt for sqlx::Error {
    #[inline(always)]
    fn with_sqlx_provider(self, provider: SqlxProvider) -> anyhow::Error {
        let context = classify_sqlx_error(&self, provider);
        context.attach(self)
    }
}

impl SqlxErrorExt for anyhow::Error {
    #[inline(always)]
    fn with_sqlx_provider(self, provider: SqlxProvider) -> anyhow::Error {
        let context = self
            .downcast_ref::<sqlx::Error>()
            .map(|error| classify_sqlx_error(error, provider));
        match context {
            Some(context) => self.with_context(context),
            None => self,
        }
    }
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

pub fn classify_sqlx_error(error: &sqlx::Error, provider: SqlxProvider) -> DtErrorContext {
    let provider = infer_provider(error, provider);
    let mut code = None;
    let mut origin_code = None;
    let mut object = None;

    match error {
        sqlx::Error::Database(database_error) => {
            origin_code = database_code(database_error.as_ref(), provider);
            code =
                classify_database_error(provider, origin_code.as_deref(), &database_error.kind());
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

    let context = provider_context(code, OriginError::new(provider.as_str(), origin_code));
    match object {
        Some(object) => context.object(object),
        None => context,
    }
}

impl ClassifyError for sqlx::Error {
    fn classify(&self) -> DtErrorContext {
        classify_sqlx_error(self, SqlxProvider::Unknown)
    }
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
) -> Option<ErrorCode> {
    if !matches!(kind, ErrorKind::Other) {
        return Some(ErrorCode::IntegrityViolation);
    }

    match (provider, provider_code) {
        (SqlxProvider::Postgres, Some(code)) => classify_postgres_code(code),
        (SqlxProvider::MySql, Some(code)) => classify_mysql_code(code),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(provider: SqlxProvider, code: &str) -> Option<ErrorCode> {
        classify_database_error(provider, Some(code), &ErrorKind::Other)
    }

    #[test]
    fn classifies_database_errors() {
        for (provider, provider_code, expected) in [
            (SqlxProvider::Postgres, "42P01", ErrorCode::ObjectNotFound),
            (SqlxProvider::MySql, "1146", ErrorCode::ObjectNotFound),
            (SqlxProvider::Postgres, "3D000", ErrorCode::DatabaseNotFound),
            (SqlxProvider::MySql, "1049", ErrorCode::DatabaseNotFound),
            (
                SqlxProvider::Postgres,
                "28P01",
                ErrorCode::AuthenticationFailed,
            ),
            (SqlxProvider::MySql, "1045", ErrorCode::AuthenticationFailed),
            (SqlxProvider::Postgres, "42501", ErrorCode::PermissionDenied),
            (SqlxProvider::MySql, "1142", ErrorCode::PermissionDenied),
            (SqlxProvider::Postgres, "08006", ErrorCode::ConnectionFailed),
            (SqlxProvider::Postgres, "42703", ErrorCode::ObjectNotFound),
            (SqlxProvider::MySql, "2003", ErrorCode::ConnectionFailed),
            (SqlxProvider::MySql, "1227", ErrorCode::PermissionDenied),
        ] {
            assert_eq!(classify(provider, provider_code), Some(expected));
        }

        assert_eq!(
            classify_database_error(
                SqlxProvider::Postgres,
                Some("23505"),
                &ErrorKind::UniqueViolation,
            ),
            Some(ErrorCode::IntegrityViolation)
        );
        assert_eq!(classify(SqlxProvider::Postgres, "XX000"), None);
    }

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
            assert_eq!(
                classify_sqlx_error(&error, SqlxProvider::Postgres).error_code(),
                expected
            );
        }
    }
}
