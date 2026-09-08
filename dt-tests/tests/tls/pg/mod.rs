use std::time::Duration;

use dt_common::config::{connection_auth_config::ConnectionAuthConfig, ssl_config::SslMode};

use crate::test_config_util::TestConfigUtil;

tls_task_tests!(disable, "pg", Disable, false;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(require, "pg", Require, false;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(verify_ca, "pg", VerifyCa, false;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(verify_full, "pg", VerifyFull, false;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(allow_invalid_hostnames, "pg", VerifyCa, true;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");

#[tokio::test]
#[serial_test::serial]
async fn tls_connection_validation() {
    super::common::tls_connection_validation("pg").await;
    // PG CDC uses OpenSSL, whose hostname opt-out works independently of SQLx.
    let fixture = TestConfigUtil::load_task_config("tls/pg/cdc/task_config.ini").unwrap();
    let mut url = url::Url::parse(&fixture.extractor_basic.url).unwrap();
    url.set_host(Some("127.0.0.1")).unwrap();
    for mode in [SslMode::VerifyCa, SslMode::VerifyFull] {
        for allow in [false, true] {
            let mut ssl = fixture
                .extractor_basic
                .connection_auth
                .ssl_config()
                .unwrap()
                .clone();
            ssl.ssl_mode = mode.clone();
            ssl.ssl_allow_invalid_hostnames = allow;
            let mut client = dt_connector::extractor::pg::pg_cdc_client::PgCdcClient {
                url: url.to_string(),
                connection_auth: ConnectionAuthConfig::BasicSsl {
                    username: None,
                    password: None,
                    ssl_config: ssl,
                },
                slot_name: "tls_hostname_probe".into(),
                pub_name: String::new(),
                start_lsn: String::new(),
                recreate_slot_if_exists: true,
            };
            let result = tokio::time::timeout(Duration::from_secs(10), client.connect())
                .await
                .unwrap();
            assert_eq!(
                result.is_ok(),
                allow,
                "PG CDC {mode}/allow={allow}: {:?}",
                result.err()
            );
        }
    }
}
