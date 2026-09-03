use dt_common::error::Error;

// Keep these mappings aligned with dt-common/src/error/provider/classification.rs on main.
fn is_missing_mysql_code(code: u16) -> bool {
    matches!(code, 1049 | 1051 | 1054 | 1091 | 1109 | 1146 | 1305)
}

fn is_missing_postgres_code(code: &str) -> bool {
    matches!(
        code,
        "3D000" | "3F000" | "42P01" | "42703" | "42704" | "42883" | "57P04"
    )
}

pub fn is_missing_target(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(Error::MetadataError(message)) = cause.downcast_ref::<Error>() {
            return message.starts_with("failed to get table metadata for:")
                || message.starts_with("failed to get oid for:");
        }

        let Some(sqlx_error) = cause.downcast_ref::<sqlx::Error>() else {
            return false;
        };
        match sqlx_error {
            sqlx::Error::TypeNotFound { .. } => true,
            sqlx::Error::Database(database_error) => {
                if let Some(mysql_error) =
                    database_error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                {
                    is_missing_mysql_code(mysql_error.number())
                } else if let Some(pg_error) =
                    database_error.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                {
                    is_missing_postgres_code(pg_error.code())
                } else {
                    false
                }
            }
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_target_provider_codes_are_recognized() {
        for code in [1049, 1051, 1054, 1091, 1109, 1146, 1305] {
            assert!(is_missing_mysql_code(code));
        }
        assert!(!is_missing_mysql_code(1045));

        for code in [
            "3D000", "3F000", "42P01", "42703", "42704", "42883", "57P04",
        ] {
            assert!(is_missing_postgres_code(code));
        }
        assert!(!is_missing_postgres_code("42501"));
    }

    #[test]
    fn missing_target_error_chain_is_recognized() {
        let wrapped_sqlx_error = anyhow::Error::new(Error::SqlxError(sqlx::Error::TypeNotFound {
            type_name: "missing_type".to_string(),
        }))
        .context("failed to load target metadata");
        assert!(is_missing_target(&wrapped_sqlx_error));

        let mysql_metadata_error = anyhow::Error::new(Error::MetadataError(
            "failed to get table metadata for: `test_db`.`test_tb`".to_string(),
        ));
        assert!(is_missing_target(&mysql_metadata_error));

        let unrelated_error = anyhow::Error::new(sqlx::Error::RowNotFound);
        assert!(!is_missing_target(&unrelated_error));
    }
}
