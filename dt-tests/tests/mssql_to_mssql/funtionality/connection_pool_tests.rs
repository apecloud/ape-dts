#[cfg(test)]
mod test {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use anyhow::Context;
    use dt_common::{
        config::{
            connection_auth_config::ConnectionAuthConfig,
            ssl_config::{SslConfig, SslMode},
        },
        error::{ErrorCode, ErrorReport},
        meta::{
            mssql::{
                mssql_connection_pool::{MssqlClient, MssqlConnectionPool, MssqlPooledConnection},
                mssql_tb_meta::MssqlTbMeta,
            },
            rdb_tb_meta::RdbTbMeta,
        },
    };
    use serial_test::serial;
    use tiberius::Row;
    use tokio::sync::Barrier;
    use url::Url;

    use super::super::{JDBC_TASK_CONFIG_FILE, TASK_CONFIG_FILE};
    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    const TEST_DATABASE: &str = "ape_dts";
    const CROSS_TASK_TABLE: &str = "[ape_dts].[dbo].[ape_dts_pool_cross_task_test]";
    const TRANSACTION_TABLE: &str = "[ape_dts].[dbo].[ape_dts_pool_transaction_test]";
    const POISONED_TABLE: &str = "[ape_dts].[dbo].[ape_dts_pool_poisoned_test]";
    const TABLE_SINK_IDENTITY_TABLE: &str = "ape_dts_pool_table_sink_identity_test";
    const TABLE_SINK_REGULAR_TABLE: &str = "ape_dts_pool_table_sink_regular_test";

    fn load_endpoint(config_endpoint: TaskConfigEndpoint) -> anyhow::Result<MssqlTestEndpoint> {
        MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, config_endpoint)
    }

    async fn create_pool_for_endpoint(
        endpoint: &MssqlTestEndpoint,
        auth: &ConnectionAuthConfig,
        max_connections: u32,
        connection_timeout_secs: u64,
    ) -> anyhow::Result<MssqlConnectionPool> {
        endpoint.ensure_database(TEST_DATABASE).await?;

        let pool = MssqlConnectionPool::from_config(
            endpoint.connection_string(),
            auth,
            None,
            max_connections,
            connection_timeout_secs,
        )
        .await?;
        pool.check_connection().await?;
        Ok(pool)
    }

    async fn create_source_pool(max_connections: u32) -> anyhow::Result<MssqlConnectionPool> {
        let endpoint = load_endpoint(TaskConfigEndpoint::Extractor)?;
        create_pool_for_endpoint(&endpoint, endpoint.connection_auth(), max_connections, 15).await
    }

    async fn execute_batch(
        connection: &mut MssqlPooledConnection<'_>,
        sql: &str,
    ) -> anyhow::Result<()> {
        connection
            .client_mut()
            .simple_query(sql)
            .await?
            .into_results()
            .await?;
        Ok(())
    }

    async fn query_rows(
        connection: &mut MssqlPooledConnection<'_>,
        sql: &str,
    ) -> anyhow::Result<Vec<Row>> {
        Ok(connection
            .client_mut()
            .query(sql, &[])
            .await?
            .into_first_result()
            .await?)
    }

    async fn query_i32(
        connection: &mut MssqlPooledConnection<'_>,
        sql: &str,
    ) -> anyhow::Result<i32> {
        let row = query_rows(connection, sql)
            .await?
            .into_iter()
            .next()
            .context("MSSQL scalar query returned no rows")?;
        row.try_get::<i32, _>(0)?
            .context("MSSQL scalar query returned NULL")
    }

    async fn query_i32_from_client(client: &mut MssqlClient, sql: &str) -> anyhow::Result<i32> {
        let row = client
            .query(sql, &[])
            .await?
            .into_row()
            .await?
            .context("MSSQL scalar query returned no rows")?;
        row.try_get::<i32, _>(0)?
            .context("MSSQL scalar query returned NULL")
    }

    async fn query_string(
        connection: &mut MssqlPooledConnection<'_>,
        sql: &str,
    ) -> anyhow::Result<String> {
        let row = query_rows(connection, sql)
            .await?
            .into_iter()
            .next()
            .context("MSSQL scalar query returned no rows")?;
        row.try_get::<&str, _>(0)?
            .map(str::to_owned)
            .context("MSSQL scalar query returned NULL")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn pool_uses_ini_endpoints_and_configured_limits() -> anyhow::Result<()> {
        let configurations = [
            (TaskConfigEndpoint::Extractor, 1, 2),
            (TaskConfigEndpoint::Sinker, 3, 4),
        ];

        for (endpoint_section, max_connections, connection_timeout_secs) in configurations {
            let endpoint = load_endpoint(endpoint_section)?;
            let pool = create_pool_for_endpoint(
                &endpoint,
                endpoint.connection_auth(),
                max_connections,
                connection_timeout_secs,
            )
            .await?;

            assert_eq!(pool.max_size(), max_connections);
            assert_eq!(
                pool.connection_timeout(),
                Duration::from_secs(connection_timeout_secs)
            );
            let mut connection = pool.get().await?;
            assert_eq!(query_i32(&mut connection, "SELECT 1").await?, 1);
            drop(connection);
        }
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn ordinary_queries_reuse_connections_without_marking() -> anyhow::Result<()> {
        let pool = create_source_pool(1).await?;

        let first_session_id = {
            let mut connection = pool.get().await?;
            assert!(!connection.will_discard());
            query_i32(&mut connection, "SELECT CAST(@@SPID AS INT)").await?
        };
        let second_session_id = {
            let mut connection = pool.get().await?;
            query_i32(&mut connection, "SELECT CAST(@@SPID AS INT)").await?
        };

        assert_eq!(first_session_id, second_session_id);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn managed_connection_exposes_discard_state() -> anyhow::Result<()> {
        let pool = create_source_pool(1).await?;
        let mut connection = pool.get().await?;

        assert!(!connection.will_discard());
        connection.mark_for_discard();
        assert!(connection.will_discard());
        connection.clear_discard_mark();
        assert!(!connection.will_discard());
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn pool_rejects_invalid_configuration_parameters() -> anyhow::Result<()> {
        struct InvalidConfigCase {
            name: &'static str,
            connection_string: String,
            auth: ConnectionAuthConfig,
            max_connections: u32,
            connection_timeout_secs: u64,
        }

        let endpoint = load_endpoint(TaskConfigEndpoint::Extractor)?;
        let valid_auth = endpoint.connection_auth().clone();
        let username = endpoint.username()?.to_string();
        let password = endpoint.password()?.to_string();

        let cases = vec![
            InvalidConfigCase {
                name: "malformed connection string",
                connection_string: "not-a-mssql-connection-string".to_string(),
                auth: valid_auth.clone(),
                max_connections: 1,
                connection_timeout_secs: 15,
            },
            InvalidConfigCase {
                name: "missing authentication",
                connection_string: endpoint.connection_string().to_string(),
                auth: ConnectionAuthConfig::NoAuth,
                max_connections: 1,
                connection_timeout_secs: 15,
            },
            InvalidConfigCase {
                name: "empty username",
                connection_string: endpoint.connection_string().to_string(),
                auth: ConnectionAuthConfig::Basic {
                    username: String::new(),
                    password: Some(password.clone()),
                },
                max_connections: 1,
                connection_timeout_secs: 15,
            },
            InvalidConfigCase {
                name: "missing password",
                connection_string: endpoint.connection_string().to_string(),
                auth: ConnectionAuthConfig::Basic {
                    username: username.clone(),
                    password: None,
                },
                max_connections: 1,
                connection_timeout_secs: 15,
            },
            InvalidConfigCase {
                name: "unsupported verify_ca TLS mode",
                connection_string: endpoint.connection_string().to_string(),
                auth: ConnectionAuthConfig::BasicSsl {
                    username: Some(username.clone()),
                    password: Some(password.clone()),
                    ssl_config: SslConfig {
                        ssl_mode: SslMode::VerifyCa,
                        ssl_ca_path: String::new(),
                    },
                },
                max_connections: 1,
                connection_timeout_secs: 15,
            },
            InvalidConfigCase {
                name: "verify_full TLS mode without CA",
                connection_string: endpoint.connection_string().to_string(),
                auth: ConnectionAuthConfig::BasicSsl {
                    username: Some(username.clone()),
                    password: Some(password.clone()),
                    ssl_config: SslConfig {
                        ssl_mode: SslMode::VerifyFull,
                        ssl_ca_path: String::new(),
                    },
                },
                max_connections: 1,
                connection_timeout_secs: 15,
            },
            InvalidConfigCase {
                name: "zero max connections",
                connection_string: endpoint.connection_string().to_string(),
                auth: valid_auth,
                max_connections: 0,
                connection_timeout_secs: 15,
            },
            InvalidConfigCase {
                name: "zero connection timeout",
                connection_string: endpoint.connection_string().to_string(),
                auth: endpoint.connection_auth().clone(),
                max_connections: 1,
                connection_timeout_secs: 0,
            },
        ];

        for case in cases {
            let result = MssqlConnectionPool::from_config(
                &case.connection_string,
                &case.auth,
                None,
                case.max_connections,
                case.connection_timeout_secs,
            )
            .await;
            assert!(result.is_err(), "{} should be rejected", case.name);
        }
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn tiberius_provider_classifies_sql_server_authentication_errors() -> anyhow::Result<()> {
        let endpoint = load_endpoint(TaskConfigEndpoint::Extractor)?;
        let invalid_auth = ConnectionAuthConfig::BasicSsl {
            username: Some(endpoint.username()?.to_string()),
            password: Some("invalid-password".to_string()),
            ssl_config: SslConfig {
                ssl_mode: SslMode::Disable,
                ssl_ca_path: String::new(),
            },
        };
        let invalid_endpoint = MssqlTestEndpoint::from_connection_string_and_auth(
            endpoint.connection_string(),
            invalid_auth,
        )?;
        let result = invalid_endpoint.check_connection().await;
        let error = match result {
            Ok(_) => panic!("invalid MSSQL credentials should be rejected"),
            Err(error) => error,
        };
        let report = ErrorReport::from_anyhow(&error);

        assert_eq!(report.code, ErrorCode::AuthenticationFailed);
        assert!(report
            .details
            .iter()
            .any(|detail| detail.starts_with("mssql/18456:")));
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn pool_accepts_url_ado_and_jdbc_strings_with_task_overrides() -> anyhow::Result<()> {
        let endpoint = load_endpoint(TaskConfigEndpoint::Extractor)?;
        endpoint.ensure_database(TEST_DATABASE).await?;
        let auth = endpoint.connection_auth();

        let mut url_only_connection_string = Url::parse(endpoint.connection_string())?;
        url_only_connection_string
            .set_username(endpoint.username()?)
            .map_err(|_| anyhow::anyhow!("MSSQL test URL should accept a username"))?;
        url_only_connection_string
            .set_password(Some(endpoint.password()?))
            .map_err(|_| anyhow::anyhow!("MSSQL test URL should accept a password"))?;
        url_only_connection_string
            .query_pairs_mut()
            .append_pair("encrypt", "disable")
            .append_pair("app name", "from-url-only");
        let pool = MssqlConnectionPool::from_config(
            url_only_connection_string.as_str(),
            &ConnectionAuthConfig::NoAuth,
            None,
            1,
            15,
        )
        .await?;
        let mut connection = pool.get().await?;
        assert_eq!(
            query_string(&mut connection, "SELECT APP_NAME()").await?,
            "from-url-only"
        );
        assert_eq!(
            query_string(&mut connection, "SELECT DB_NAME()").await?,
            TEST_DATABASE
        );
        drop(connection);
        drop(pool);

        let endpoint_url = Url::parse(endpoint.connection_string())?;
        let host = endpoint_url
            .host_str()
            .context("MSSQL test URL should contain a host")?;
        let port = endpoint_url.port().unwrap_or(1433);
        let ado_base = format!("server=tcp:{host},{port};database={TEST_DATABASE}");
        let ado_only_connection_string = format!(
            "{};User ID={};Password={};Encrypt=DANGER_PLAINTEXT;Application Name=from-ado-only",
            ado_base,
            endpoint.username()?,
            endpoint.password()?
        );
        let pool = MssqlConnectionPool::from_config(
            &ado_only_connection_string,
            &ConnectionAuthConfig::NoAuth,
            None,
            1,
            15,
        )
        .await?;
        let mut connection = pool.get().await?;
        assert_eq!(
            query_string(&mut connection, "SELECT APP_NAME()").await?,
            "from-ado-only"
        );
        drop(connection);
        drop(pool);

        let ado_connection_string = format!(
            "{};User ID=invalid;Password=invalid;Encrypt=true;Application Name=from-ado",
            ado_base
        );
        let jdbc_endpoint = MssqlTestEndpoint::from_config_file(
            JDBC_TASK_CONFIG_FILE,
            TaskConfigEndpoint::Extractor,
        )?;
        let configurations = [
            (ado_connection_string.as_str(), "ape-dts-ado-test"),
            (jdbc_endpoint.connection_string(), "ape-dts-jdbc-test"),
        ];

        for (connection_string, application_name) in configurations {
            let pool = MssqlConnectionPool::from_config(
                connection_string,
                auth,
                Some(application_name),
                1,
                15,
            )
            .await?;
            let mut connection = pool.get().await?;
            assert_eq!(
                query_string(&mut connection, "SELECT APP_NAME()").await?,
                application_name
            );
            assert_eq!(
                query_string(&mut connection, "SELECT DB_NAME()").await?,
                TEST_DATABASE
            );
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn pool_can_be_used_from_multiple_tokio_tasks() -> anyhow::Result<()> {
        const MAX_CONNECTIONS: u32 = 2;
        const TASK_COUNT: usize = 8;

        let pool = create_source_pool(MAX_CONNECTIONS).await?;
        assert_eq!(pool.max_size(), MAX_CONNECTIONS);
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {CROSS_TASK_TABLE};\
                 CREATE TABLE {CROSS_TASK_TABLE} (\
                     task_id INT NOT NULL PRIMARY KEY,\
                     session_id INT NOT NULL\
                 )"
            ),
        )
        .await?;

        let start = Arc::new(Barrier::new(TASK_COUNT));
        let mut tasks = Vec::with_capacity(TASK_COUNT);
        for task_id in 0..TASK_COUNT {
            let task_pool = pool.clone();
            let task_start = start.clone();
            tasks.push(tokio::spawn(async move {
                task_start.wait().await;
                let mut connection = task_pool.get().await?;
                let session_id = query_i32(&mut connection, "SELECT CAST(@@SPID AS INT)").await?;
                connection
                    .client_mut()
                    .execute(
                        &format!(
                            "INSERT INTO {CROSS_TASK_TABLE} (task_id, session_id) \
                             VALUES (@P1, @P2)"
                        ),
                        &[&(task_id as i32), &session_id],
                    )
                    .await?;
                execute_batch(&mut connection, "WAITFOR DELAY '00:00:00.050'").await?;
                anyhow::Ok(session_id)
            }));
        }

        let mut task_session_ids = Vec::with_capacity(TASK_COUNT);
        for task in tasks {
            task_session_ids.push(task.await.context("MSSQL pool worker task panicked")??);
        }

        let distinct_sessions = task_session_ids.into_iter().collect::<HashSet<_>>();
        assert!(!distinct_sessions.is_empty());
        assert!(distinct_sessions.len() <= MAX_CONNECTIONS as usize);

        let mut connection = pool.get().await?;
        let rows = query_rows(
            &mut connection,
            &format!("SELECT task_id, session_id FROM {CROSS_TASK_TABLE} ORDER BY task_id"),
        )
        .await?;
        drop(connection);

        assert_eq!(rows.len(), TASK_COUNT);
        for (expected_task_id, row) in rows.iter().enumerate() {
            assert_eq!(row.get::<i32, _>("task_id"), Some(expected_task_id as i32));
            let session_id = row
                .get::<i32, _>("session_id")
                .context("session_id should not be NULL")?;
            assert!(distinct_sessions.contains(&session_id));
        }

        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!("DROP TABLE IF EXISTS {CROSS_TASK_TABLE}"),
        )
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn pooled_connection_supports_commit_and_rollback() -> anyhow::Result<()> {
        let pool = create_source_pool(1).await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {TRANSACTION_TABLE};\
                 CREATE TABLE {TRANSACTION_TABLE} (id INT NOT NULL PRIMARY KEY)"
            ),
        )
        .await?;

        let mut transaction = pool.begin().await?;
        assert_eq!(
            query_i32_from_client(transaction.client_mut(), "SELECT @@TRANCOUNT").await?,
            1
        );
        let committed_session_id =
            query_i32_from_client(transaction.client_mut(), "SELECT CAST(@@SPID AS INT)").await?;
        transaction
            .client_mut()
            .execute(
                &format!("INSERT INTO {TRANSACTION_TABLE} (id) VALUES (@P1)"),
                &[&1i32],
            )
            .await?;
        transaction.commit().await?;

        let mut transaction = pool.begin().await?;
        let rolled_back_session_id =
            query_i32_from_client(transaction.client_mut(), "SELECT CAST(@@SPID AS INT)").await?;
        assert_eq!(committed_session_id, rolled_back_session_id);
        transaction
            .client_mut()
            .execute(
                &format!("INSERT INTO {TRANSACTION_TABLE} (id) VALUES (@P1)"),
                &[&2i32],
            )
            .await?;
        transaction.rollback().await?;

        let mut connection = pool.get().await?;
        assert_eq!(query_i32(&mut connection, "SELECT @@TRANCOUNT").await?, 0);
        let rows = query_rows(
            &mut connection,
            &format!("SELECT id FROM {TRANSACTION_TABLE} ORDER BY id"),
        )
        .await?;
        drop(connection);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<i32, _>("id"), Some(1));

        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!("DROP TABLE IF EXISTS {TRANSACTION_TABLE}"),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn dropping_an_active_transaction_rolls_back_by_discarding_its_session(
    ) -> anyhow::Result<()> {
        let pool = create_source_pool(1).await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {POISONED_TABLE};\
                 CREATE TABLE {POISONED_TABLE} (id INT NOT NULL PRIMARY KEY)"
            ),
        )
        .await?;

        {
            let mut transaction = pool.begin().await?;
            transaction
                .client_mut()
                .execute(
                    &format!("INSERT INTO {POISONED_TABLE} (id) VALUES (@P1)"),
                    &[&1i32],
                )
                .await?;
        }

        let mut replacement = pool.get().await?;
        let transaction_count = query_i32(&mut replacement, "SELECT @@TRANCOUNT").await?;
        let row_count = query_i32(
            &mut replacement,
            &format!("SELECT COUNT(*) FROM {POISONED_TABLE}"),
        )
        .await?;
        drop(replacement);

        assert_eq!(transaction_count, 0);
        assert_eq!(row_count, 0, "the uncommitted insert should be rolled back");

        MssqlTestEndpoint::execute_batch(&pool, &format!("DROP TABLE IF EXISTS {POISONED_TABLE}"))
            .await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn table_sink_session_uses_table_meta_to_control_identity_insert() -> anyhow::Result<()> {
        let pool = create_source_pool(1).await?;
        let identity_table = format!("[{TEST_DATABASE}].[dbo].[{TABLE_SINK_IDENTITY_TABLE}]");
        let regular_table = format!("[{TEST_DATABASE}].[dbo].[{TABLE_SINK_REGULAR_TABLE}]");
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {identity_table};
                 DROP TABLE IF EXISTS {regular_table};
                 CREATE TABLE {identity_table} (
                    id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY
                 );
                 CREATE TABLE {regular_table} (
                    id INT NOT NULL PRIMARY KEY
                 );"
            ),
        )
        .await?;

        let build_tb_meta = |table: &str, identity_col: Option<&str>| MssqlTbMeta {
            basic: RdbTbMeta {
                db: TEST_DATABASE.to_string(),
                schema: "dbo".to_string(),
                tb: table.to_string(),
                ..Default::default()
            },
            identity_col: identity_col.map(str::to_string),
            ..Default::default()
        };
        let identity_meta = build_tb_meta(TABLE_SINK_IDENTITY_TABLE, Some("id"));
        let regular_meta = build_tb_meta(TABLE_SINK_REGULAR_TABLE, None);

        {
            let mut session = pool.get_table_sink_session(&identity_meta).await?;
            session.begin().await?;
            session
                .client_mut()
                .simple_query(&format!("INSERT INTO {identity_table} (id) VALUES (10)"))
                .await?
                .into_results()
                .await?;
            session.commit().await?;
            session.post().await?;
        }

        {
            // SQL Server rejects IDENTITY_INSERT for this table. Successful
            // construction proves that metadata without an identity column
            // skips both the pre and post identity statements.
            let mut session = pool.get_table_sink_session(&regular_meta).await?;
            session.begin().await?;
            session
                .client_mut()
                .simple_query(&format!("INSERT INTO {regular_table} (id) VALUES (20)"))
                .await?
                .into_results()
                .await?;
            session.commit().await?;
            session.post().await?;
        }

        let mut connection = pool.get().await?;
        assert_eq!(
            query_i32(
                &mut connection,
                &format!("SELECT COUNT(*) FROM {identity_table} WHERE id = 10"),
            )
            .await?,
            1
        );
        assert_eq!(
            query_i32(
                &mut connection,
                &format!("SELECT COUNT(*) FROM {regular_table} WHERE id = 20"),
            )
            .await?,
            1
        );
        drop(connection);

        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {identity_table};
                 DROP TABLE IF EXISTS {regular_table};"
            ),
        )
        .await?;
        Ok(())
    }
}
