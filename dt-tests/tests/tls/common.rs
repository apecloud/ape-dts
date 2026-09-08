use std::{str::FromStr, time::Duration};

use dt_common::{
    config::{
        connection_auth_config::ConnectionAuthConfig,
        ssl_config::{SslConfig, SslMode},
    },
    meta::mssql::mssql_connection_pool::MssqlConnectionPool,
};
use mongodb::bson::doc;
use sqlx::{mysql::MySqlConnectOptions, postgres::PgConnectOptions, Connection};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::{
    test_config_util::TestConfigUtil,
    test_runner::{
        check_test_runner::CheckTestRunner, mongo_test_runner::MongoTestRunner,
        rdb_struct_test_runner::RdbStructTestRunner, rdb_test_runner::RdbTestRunner,
        redis_test_runner::RedisTestRunner,
    },
};

fn ssl_overrides(mode: SslMode) -> Vec<(String, String, String)> {
    ["extractor", "sinker"]
        .into_iter()
        .map(|section| (section.into(), "ssl_mode".into(), mode.to_string()))
        .collect()
}

pub(super) async fn run_tls_task_test(engine: &str, kind: &str, mode: SslMode) {
    match engine {
        "mongo" | "mongo_shard" => {
            run_mongo_tls_task_test(engine, kind, mode).await;
            return;
        }
        "redis_7_0" | "redis_8_0" | "redis_cluster_6_2" | "redis_cluster_7_0" => {
            run_redis_tls_task_test(engine, kind, mode).await;
            return;
        }
        _ => {}
    }
    let dir = format!("tls/{engine}/{kind}");
    let fixture = TestConfigUtil::load_task_config(&format!("{dir}/task_config.ini")).unwrap();
    let mut overrides = ssl_overrides(mode.clone());
    for (section, url, auth) in [
        (
            "extractor",
            &fixture.extractor_basic.url,
            &fixture.extractor_basic.connection_auth,
        ),
        (
            "sinker",
            &fixture.sinker_basic.url,
            &fixture.sinker_basic.connection_auth,
        ),
    ] {
        let mut url = url::Url::parse(url).unwrap();
        if mode != SslMode::Disable && engine != "mssql" {
            url.set_username(if engine == "mysql" && kind == "cdc" {
                "ape_cdc"
            } else {
                "ape_dts"
            })
            .unwrap();
            url.set_password(Some("123456")).unwrap();
        }
        let mut ssl = auth.ssl_config().unwrap().clone();
        ssl.ssl_mode = mode.clone();
        let encrypted = connect_and_check_encryption(engine, url.as_str(), &ssl)
            .await
            .unwrap();
        assert_eq!(
            encrypted,
            mode != SslMode::Disable,
            "{engine}/{kind}/{mode}/{section}"
        );
        overrides.push((section.into(), "url".into(), url.to_string()));
    }
    let runner = RdbTestRunner::new_with_config_overrides(&dir, &overrides)
        .await
        .unwrap();
    match kind {
        "snapshot" => runner.run_snapshot_test(true).await.unwrap(),
        "cdc" => runner.run_cdc_test(2000, 3000).await.unwrap(),
        "checker" => {
            let runner = CheckTestRunner::from_runner(runner).await.unwrap();
            runner.run_check_test().await.unwrap();
            runner.close().await.unwrap();
            return;
        }
        "struct" => {
            let mut runner = RdbStructTestRunner { base: runner };
            if engine == "pg" {
                runner.run_pg_struct_test().await.unwrap();
            } else {
                runner.run_struct_test_without_check().await.unwrap();
                let sql = "SHOW CREATE TABLE tls_test.accounts";
                let src: (String, String) = sqlx::query_as(sql)
                    .fetch_one(runner.base.src_conn_pool_mysql.as_ref().unwrap())
                    .await
                    .unwrap();
                let dst: (String, String) = sqlx::query_as(sql)
                    .fetch_one(runner.base.dst_conn_pool_mysql.as_ref().unwrap())
                    .await
                    .unwrap();
                assert_eq!(src, dst);
            }
            runner.close().await.unwrap();
            return;
        }
        _ => panic!("unsupported TLS test workflow: {kind}"),
    }
    runner.close().await.unwrap();
}

async fn run_mongo_tls_task_test(engine: &str, kind: &str, mode: SslMode) {
    let dir = format!("tls/{engine}/{kind}");
    let overrides = ssl_overrides(mode);
    let runner = MongoTestRunner::new_with_config_overrides(&dir, &overrides)
        .await
        .unwrap();
    let is_shard = engine == "mongo_shard";
    match kind {
        "snapshot" => runner.run_snapshot_test(true).await.unwrap(),
        "cdc" if is_shard => runner.run_cdc_in_order_test(3000, 5000).await.unwrap(),
        "cdc" => runner.run_cdc_test(2000, 2000).await.unwrap(),
        "struct" => {
            runner.run_struct_test().await.unwrap();
            let db = if is_shard {
                "mongo_tls_sharding_struct"
            } else {
                "mongo_tls_struct"
            };
            runner.assert_dst_collection_exists(db, "accounts").await;
            runner
                .assert_dst_index_exists(
                    db,
                    "accounts",
                    "tenant_account_idx",
                    doc! { "tenant_id": 1, "account_id": 1 },
                )
                .await;
            if !is_shard {
                runner
                    .assert_dst_collection_option_bool(db, "accounts", "capped", true)
                    .await;
            }
        }
        _ => panic!("unsupported MongoDB TLS test workflow: {kind}"),
    }
    if is_shard {
        runner
            .assert_dst_shard_collection(
                &format!("mongo_tls_sharding_{kind}.accounts"),
                doc! { "tenant_id": 1, "account_id": 1 },
                false,
            )
            .await;
    }
}

async fn run_redis_tls_task_test(engine: &str, kind: &str, mode: SslMode) {
    assert_eq!(kind, "snapshot_and_cdc");
    let dir = format!("tls/{engine}/{kind}");
    let overrides = ssl_overrides(mode);
    let mut runner = RedisTestRunner::new_with_config_overrides(&dir, &overrides)
        .await
        .unwrap();
    runner.run_cdc_test(2000, 3000).await.unwrap();
}

// Query the actual session encryption state, not merely its configured mode.
async fn connect_and_check_encryption(
    engine: &str,
    url: &str,
    ssl: &SslConfig,
) -> anyhow::Result<bool> {
    tokio::time::timeout(Duration::from_secs(10), async {
        match engine {
            "mysql" => {
                let options = ssl.apply_mysql(MySqlConnectOptions::from_str(url)?)?;
                let mut conn = sqlx::MySqlConnection::connect_with(&options).await?;
                let (_, cipher): (String, String) = sqlx::query_as("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
                    .fetch_one(&mut conn).await?;
                conn.close().await?;
                Ok(!cipher.is_empty())
            }
            "pg" => {
                let options = ssl.apply_pg(PgConnectOptions::from_str(url)?)?;
                let mut conn = sqlx::PgConnection::connect_with(&options).await?;
                let encrypted = sqlx::query_scalar("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()")
                    .fetch_one(&mut conn).await?;
                conn.close().await?;
                Ok(encrypted)
            }
            "mssql" => {
                let auth = ConnectionAuthConfig::BasicSsl {
                    username: None, password: None, ssl_config: ssl.clone(),
                };
                let config = MssqlConnectionPool::build_client_config(url, &auth, None)?;
                let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
                let mut conn = tiberius::Client::connect(config, tcp.compat_write()).await?;
                let row = conn.simple_query("SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id=@@SPID")
                    .await?.into_row().await?.ok_or_else(|| anyhow::anyhow!("missing MSSQL session"))?;
                Ok(row.get::<&str, _>(0) == Some("TRUE"))
            }
            _ => anyhow::bail!("unsupported TLS test engine: {engine}"),
        }
    }).await?
}
