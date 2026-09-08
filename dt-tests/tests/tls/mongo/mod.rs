use std::time::Duration;

use dt_common::config::{
    connection_auth_config::ConnectionAuthConfig,
    ssl_config::{SslConfig, SslMode},
};
use mongodb::bson::doc;

use crate::test_config_util::TestConfigUtil;

tls_task_tests!(require, "mongo", Require, false;
    structure => "struct", snapshot => "snapshot", cdc => "cdc");
tls_task_tests!(verify_full, "mongo", VerifyFull, false; snapshot => "snapshot");

pub(super) fn mutual_tls_url(section: &str) -> String {
    let key = format!("mongo_mtls_{section}_url");
    std::env::var(&key).unwrap_or_else(|_| panic!("missing {key}"))
}

async fn connect_and_ping(url: &str, ssl: &SslConfig) -> mongodb::error::Result<()> {
    // Invalid local options/PEM files must fail the test, not count as server rejection.
    let mut options = ssl
        .apply_mongo(mongodb::options::ClientOptions::parse(url).await.unwrap())
        .unwrap();
    options.server_selection_timeout = Some(Duration::from_millis(750));
    options.connect_timeout = Some(Duration::from_secs(2));
    let client = mongodb::Client::with_options(options).unwrap();
    let result = client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await;
    client.shutdown().await;
    result.map(|_| ())
}

#[tokio::test]
#[serial_test::serial]
async fn tls_connection_validation() {
    let config = TestConfigUtil::load_task_config("tls/mongo/snapshot/task_config.ini").unwrap();
    let url = ConnectionAuthConfig::merge_url_with_auth(
        &config.extractor_basic.url,
        &config.extractor_basic.connection_auth,
    )
    .unwrap();
    let mut wrong_host = url::Url::parse(&url).unwrap();
    wrong_host.set_host(Some("127.0.0.1")).unwrap();
    for mode in [SslMode::Require, SslMode::VerifyCa, SslMode::VerifyFull] {
        for allow in [false, true] {
            let mut ssl = config
                .extractor_basic
                .connection_auth
                .ssl_config()
                .unwrap()
                .clone();
            ssl.ssl_mode = mode.clone();
            ssl.ssl_allow_invalid_hostnames = allow;
            connect_and_ping(&url, &ssl).await.unwrap();
            // The rustls MongoDB backend has no independent hostname opt-out.
            assert_eq!(
                connect_and_ping(wrong_host.as_str(), &ssl).await.is_ok(),
                mode == SslMode::Require
            );
            ssl.ssl_ca_path = "./docker/tls/client/client-ca.crt".into();
            assert_eq!(
                connect_and_ping(&url, &ssl).await.is_ok(),
                mode == SslMode::Require
            );
        }
    }
}

#[tokio::test]
#[serial_test::serial]
async fn server_requires_trusted_client_certificate() {
    let config = TestConfigUtil::load_task_config("tls/mongo/snapshot/task_config.ini").unwrap();
    for (section, auth) in [
        ("extractor", &config.extractor_basic.connection_auth),
        ("sinker", &config.sinker_basic.connection_auth),
    ] {
        let url =
            ConnectionAuthConfig::merge_url_with_auth(&mutual_tls_url(section), auth).unwrap();
        let mut ssl = auth.ssl_config().unwrap().clone();
        ssl.ssl_mode = SslMode::VerifyFull;
        connect_and_ping(&url, &ssl).await.unwrap();

        // Inspect the server's effective policy as well as testing handshake behavior.
        let options = ssl
            .apply_mongo(mongodb::options::ClientOptions::parse(&url).await.unwrap())
            .unwrap();
        let client = mongodb::Client::with_options(options).unwrap();
        let options = client
            .database("admin")
            .run_command(doc! { "getCmdLineOpts": 1 })
            .await
            .unwrap();
        client.shutdown().await;
        let tls = options
            .get_document("parsed")
            .unwrap()
            .get_document("net")
            .unwrap()
            .get_document("tls")
            .unwrap();
        assert_eq!(tls.get_str("mode").unwrap(), "requireTLS", "{section}");
        assert!(
            tls.get_str("CAFile").is_ok(),
            "{section}: missing trusted CA"
        );
        // MongoDB omits unset flags; allowConnectionsWithoutCertificates defaults to false.
        assert_ne!(
            tls.get_bool("allowConnectionsWithoutCertificates").ok(),
            Some(true),
            "{section}: server allows missing client certificates"
        );
        assert_ne!(
            tls.get_bool("allowInvalidCertificates").ok(),
            Some(true),
            "{section}: server accepts invalid client certificates"
        );

        for (case, cert_path) in [
            ("missing certificate", ""),
            (
                "untrusted certificate",
                "./docker/tls/client/untrusted-client.pem",
            ),
        ] {
            let mut rejected = ssl.clone();
            rejected.ssl_client_cert_path = cert_path.into();
            rejected.ssl_client_key_path.clear();
            let error = connect_and_ping(&url, &rejected)
                .await
                .expect_err(&format!("{section}: server accepted {case}"));
            assert!(
                matches!(
                    *error.kind,
                    mongodb::error::ErrorKind::ServerSelection { .. }
                ),
                "{section}/{case}: expected handshake rejection, got {error}"
            );
            // A healthy endpoint and valid identity must still work after each rejection.
            connect_and_ping(&url, &ssl).await.unwrap();
        }
    }
}
