#[cfg(test)]
mod test {
    use std::{collections::HashSet, env, sync::Arc};

    use anyhow::Context;
    use dt_common::{
        config::{
            connection_auth_config::ConnectionAuthConfig,
            ssl_config::{SslConfig, SslMode},
        },
        meta::mssql::mssql_connection_pool::{MssqlConnectionPool, MssqlPooledConnection},
    };
    use serial_test::serial;
    use tiberius::Row;
    use tokio::sync::Barrier;
    use url::Url;

    use crate::{
        test_config_util::TestConfigUtil, test_runner::mssql_test_client::MssqlTestClient,
    };

    const CROSS_TASK_TABLE: &str = "[dbo].[ape_dts_pool_cross_task_test]";
    const TRANSACTION_TABLE: &str = "[dbo].[ape_dts_pool_transaction_test]";
    const POISONED_TABLE: &str = "[dbo].[ape_dts_pool_poisoned_test]";

    #[derive(Clone)]
    struct TestEndpoint {
        url: String,
        username: String,
        password: String,
    }

    impl TestEndpoint {
        fn from_env(prefix: &str) -> anyhow::Result<Self> {
            load_test_env()?;
            Ok(Self {
                url: required_env(&format!("{prefix}_without_auth_url"))?,
                username: required_env(&format!("{prefix}_username"))?,
                password: required_env(&format!("{prefix}_password"))?,
            })
        }

        fn disabled_tls_auth(&self) -> ConnectionAuthConfig {
            ConnectionAuthConfig::BasicSsl {
                username: Some(self.username.clone()),
                password: Some(self.password.clone()),
                ssl_config: SslConfig {
                    ssl_mode: SslMode::Disable,
                    ssl_ca_path: String::new(),
                },
            }
        }

        async fn ensure_database(&self) -> anyhow::Result<()> {
            let client = MssqlTestClient::from_url_and_auth(&self.url, self.disabled_tls_auth())?;
            client.ensure_database(client.database()).await
        }
    }

    fn load_test_env() -> anyhow::Result<()> {
        let default_env = TestConfigUtil::get_absolute_path(".env");
        dotenv::from_path(&default_env).with_context(|| format!("failed to load {default_env}"))?;
        Ok(())
    }

    fn required_env(key: &str) -> anyhow::Result<String> {
        env::var(key).with_context(|| format!("required MSSQL test environment variable {key}"))
    }

    async fn create_pool_for_endpoint(
        endpoint: &TestEndpoint,
        auth: &ConnectionAuthConfig,
        max_connections: u32,
    ) -> anyhow::Result<MssqlConnectionPool> {
        endpoint.ensure_database().await?;

        let pool = MssqlConnectionPool::from_config(&endpoint.url, auth, max_connections).await?;
        pool.check_connection().await?;
        Ok(pool)
    }

    async fn create_source_pool(max_connections: u32) -> anyhow::Result<MssqlConnectionPool> {
        let endpoint = TestEndpoint::from_env("mssql_extractor")?;
        create_pool_for_endpoint(&endpoint, &endpoint.disabled_tls_auth(), max_connections).await
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

    async fn execute_clean_batch(pool: &MssqlConnectionPool, sql: &str) -> anyhow::Result<()> {
        let mut connection = pool.get().await?;
        execute_batch(&mut connection, sql).await?;
        connection.mark_reusable()?;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn pool_uses_environment_endpoints_and_max_connections() -> anyhow::Result<()> {
        let configurations = [("mssql_extractor", 1), ("mssql_sinker", 3)];

        for (endpoint_prefix, max_connections) in configurations {
            let endpoint = TestEndpoint::from_env(endpoint_prefix)?;
            let pool =
                create_pool_for_endpoint(&endpoint, &endpoint.disabled_tls_auth(), max_connections)
                    .await?;

            assert_eq!(pool.max_size(), max_connections);
            let mut connection = pool.get().await?;
            assert_eq!(query_i32(&mut connection, "SELECT 1").await?, 1);
            connection.mark_reusable()?;
            drop(connection);

            pool.close().await?;
            assert!(pool.get().await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn pool_rejects_invalid_configuration_parameters() -> anyhow::Result<()> {
        struct InvalidConfigCase {
            name: &'static str,
            url: String,
            auth: ConnectionAuthConfig,
            max_connections: u32,
        }

        let endpoint = TestEndpoint::from_env("mssql_extractor")?;
        let valid_auth = endpoint.disabled_tls_auth();
        let mut url_without_database = Url::parse(&endpoint.url)?;
        url_without_database.set_path("");
        let mut wrong_scheme_url = Url::parse(&endpoint.url)?;
        wrong_scheme_url
            .set_scheme("postgres")
            .expect("postgres should be a valid URL scheme");

        let cases = vec![
            InvalidConfigCase {
                name: "malformed URL",
                url: "not-a-mssql-url".to_string(),
                auth: valid_auth.clone(),
                max_connections: 1,
            },
            InvalidConfigCase {
                name: "non-MSSQL URL scheme",
                url: wrong_scheme_url.to_string(),
                auth: valid_auth.clone(),
                max_connections: 1,
            },
            InvalidConfigCase {
                name: "URL without database",
                url: url_without_database.to_string(),
                auth: valid_auth.clone(),
                max_connections: 1,
            },
            InvalidConfigCase {
                name: "missing authentication",
                url: endpoint.url.clone(),
                auth: ConnectionAuthConfig::NoAuth,
                max_connections: 1,
            },
            InvalidConfigCase {
                name: "empty username",
                url: endpoint.url.clone(),
                auth: ConnectionAuthConfig::Basic {
                    username: String::new(),
                    password: Some(endpoint.password.clone()),
                },
                max_connections: 1,
            },
            InvalidConfigCase {
                name: "missing password",
                url: endpoint.url.clone(),
                auth: ConnectionAuthConfig::Basic {
                    username: endpoint.username.clone(),
                    password: None,
                },
                max_connections: 1,
            },
            InvalidConfigCase {
                name: "unsupported verify_ca TLS mode",
                url: endpoint.url.clone(),
                auth: ConnectionAuthConfig::BasicSsl {
                    username: Some(endpoint.username.clone()),
                    password: Some(endpoint.password.clone()),
                    ssl_config: SslConfig {
                        ssl_mode: SslMode::VerifyCa,
                        ssl_ca_path: String::new(),
                    },
                },
                max_connections: 1,
            },
            InvalidConfigCase {
                name: "verify_full TLS mode without CA",
                url: endpoint.url.clone(),
                auth: ConnectionAuthConfig::BasicSsl {
                    username: Some(endpoint.username.clone()),
                    password: Some(endpoint.password.clone()),
                    ssl_config: SslConfig {
                        ssl_mode: SslMode::VerifyFull,
                        ssl_ca_path: String::new(),
                    },
                },
                max_connections: 1,
            },
            InvalidConfigCase {
                name: "zero max connections",
                url: endpoint.url.clone(),
                auth: valid_auth,
                max_connections: 0,
            },
        ];

        for case in cases {
            let result =
                MssqlConnectionPool::from_config(&case.url, &case.auth, case.max_connections).await;
            assert!(result.is_err(), "{} should be rejected", case.name);
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
        execute_clean_batch(
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
                connection.mark_reusable()?;
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
        connection.mark_reusable()?;
        drop(connection);

        assert_eq!(rows.len(), TASK_COUNT);
        for (expected_task_id, row) in rows.iter().enumerate() {
            assert_eq!(row.get::<i32, _>("task_id"), Some(expected_task_id as i32));
            let session_id = row
                .get::<i32, _>("session_id")
                .context("session_id should not be NULL")?;
            assert!(distinct_sessions.contains(&session_id));
        }

        execute_clean_batch(&pool, &format!("DROP TABLE IF EXISTS {CROSS_TASK_TABLE}")).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn pooled_connection_supports_commit_and_rollback() -> anyhow::Result<()> {
        let pool = create_source_pool(1).await?;
        execute_clean_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {TRANSACTION_TABLE};\
                 CREATE TABLE {TRANSACTION_TABLE} (id INT NOT NULL PRIMARY KEY)"
            ),
        )
        .await?;

        let mut connection = pool.get().await?;
        execute_batch(&mut connection, "BEGIN TRANSACTION").await?;
        assert_eq!(query_i32(&mut connection, "SELECT @@TRANCOUNT").await?, 1);
        connection
            .client_mut()
            .execute(
                &format!("INSERT INTO {TRANSACTION_TABLE} (id) VALUES (@P1)"),
                &[&1i32],
            )
            .await?;
        execute_batch(&mut connection, "COMMIT TRANSACTION").await?;
        assert_eq!(query_i32(&mut connection, "SELECT @@TRANCOUNT").await?, 0);
        connection.mark_reusable()?;
        drop(connection);

        let mut connection = pool.get().await?;
        execute_batch(&mut connection, "BEGIN TRANSACTION").await?;
        connection
            .client_mut()
            .execute(
                &format!("INSERT INTO {TRANSACTION_TABLE} (id) VALUES (@P1)"),
                &[&2i32],
            )
            .await?;
        execute_batch(&mut connection, "ROLLBACK TRANSACTION").await?;
        assert_eq!(query_i32(&mut connection, "SELECT @@TRANCOUNT").await?, 0);
        connection.mark_reusable()?;
        drop(connection);

        let mut connection = pool.get().await?;
        let rows = query_rows(
            &mut connection,
            &format!("SELECT id FROM {TRANSACTION_TABLE} ORDER BY id"),
        )
        .await?;
        connection.mark_reusable()?;
        drop(connection);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<i32, _>("id"), Some(1));

        execute_clean_batch(&pool, &format!("DROP TABLE IF EXISTS {TRANSACTION_TABLE}")).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn dropping_an_unclean_connection_discards_its_session() -> anyhow::Result<()> {
        let pool = create_source_pool(1).await?;
        execute_clean_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {POISONED_TABLE};\
                 CREATE TABLE {POISONED_TABLE} (id INT NOT NULL PRIMARY KEY)"
            ),
        )
        .await?;

        let mut connection = pool.get().await?;
        execute_batch(&mut connection, "BEGIN TRANSACTION").await?;
        connection
            .client_mut()
            .execute(
                &format!("INSERT INTO {POISONED_TABLE} (id) VALUES (@P1)"),
                &[&1i32],
            )
            .await?;
        drop(connection);

        let mut replacement = pool.get().await?;
        let transaction_count = query_i32(&mut replacement, "SELECT @@TRANCOUNT").await?;
        let row_count = query_i32(
            &mut replacement,
            &format!("SELECT COUNT(*) FROM {POISONED_TABLE}"),
        )
        .await?;
        replacement.mark_reusable()?;
        drop(replacement);

        assert_eq!(transaction_count, 0);
        assert_eq!(row_count, 0, "the uncommitted insert should be rolled back");

        execute_clean_batch(&pool, &format!("DROP TABLE IF EXISTS {POISONED_TABLE}")).await?;
        Ok(())
    }
}
