#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::{rdb_cycle_test_runner::RdbCycleTestRunner, test_base::TestBase};

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/basic_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_uk_changed_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/uk_changed_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_charset_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/charset_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_json_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/json_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_ddl_test() {
        TestBase::run_ddl_test("mysql_to_mysql/cdc/ddl_test", 3000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_ddl_route_test() {
        TestBase::run_ddl_test("mysql_to_mysql/cdc/ddl_route_test", 3000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    // Ignored for now: MySQL CDC meta center follows current schema invalidation/reload,
    // but it does not preserve historical schema snapshots for arbitrary binlog replay.
    // Keeping this test enabled can give a misleading signal about historical CDC correctness.
    #[ignore = "meta center does not validate historical-schema replay correctness yet"]
    async fn cdc_ddl_meta_center_test() {
        TestBase::run_ddl_meta_center_test("mysql_to_mysql/cdc/ddl_meta_center_test", 3000, 5000)
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_timezone_test() {
        println!("cdc_timezone_test can be covered by test: cdc_basic_test, table: one_pk_no_uk, field: f_13 timestamp(6), the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00 ")
    }

    #[tokio::test]
    #[serial]
    async fn cdc_special_character_in_name_test() {
        TestBase::run_cdc_test(
            "mysql_to_mysql/cdc/special_character_in_name_test",
            3000,
            2000,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_wildchar_filter_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/wildchar_filter_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cycle_basic_test() {
        let tx_check_data = vec![
            ("node1", "node2", "node1", "10"),
            ("node2", "node1", "node2", "10"),
        ];

        RdbCycleTestRunner::run_cycle_cdc_test(
            "mysql_to_mysql/cdc/cycle_basic_test",
            1000,
            2000,
            &tx_check_data,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    // Ignored for now: star-cycle convergence depends on multi-hop propagation timing and
    // data-marker counts.
    #[ignore = "star cycle integration case is timing-sensitive and flaky"]
    async fn cycle_star_test() {
        let tx_check_data = vec![
            ("node1", "node2", "node1", "10"),
            ("node1", "node2", "node3", "10"),
            ("node1", "node3", "node1", "10"),
            ("node1", "node3", "node2", "10"),
            ("node2", "node1", "node2", "10"),
            ("node3", "node1", "node3", "10"),
        ];

        RdbCycleTestRunner::run_cycle_cdc_test(
            "mysql_to_mysql/cdc/cycle_star_test",
            1000,
            5000,
            &tx_check_data,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cycle_net_test() {
        let tx_check_data = vec![
            ("node1", "node2", "node1", "10"),
            ("node1", "node3", "node1", "10"),
            ("node2", "node1", "node2", "10"),
            ("node2", "node3", "node2", "10"),
            ("node3", "node1", "node3", "10"),
            ("node3", "node2", "node3", "10"),
        ];

        RdbCycleTestRunner::run_cycle_cdc_test(
            "mysql_to_mysql/cdc/cycle_net_test",
            1000,
            5000,
            &tx_check_data,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_route_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/route_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_foreign_key_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/foreign_key_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_heartbeat_test() {
        TestBase::run_heartbeat_test("mysql_to_mysql/cdc/heartbeat_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_to_sql_test() {
        TestBase::run_cdc_to_sql_test("mysql_to_mysql/cdc/to_sql_test", false, 1000, 0).await;
    }

    /// need mysql config: binlog_row_image =FULL
    #[tokio::test]
    #[serial]
    async fn cdc_to_reverse_sql_test() {
        TestBase::run_cdc_to_sql_test("mysql_to_mysql/cdc/to_sql_test", true, 1000, 0).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_gtid_test() {
        TestBase::run_cdc_test("mysql_to_mysql/cdc/gtid_test", 3000, 2000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_dcl_test() {
        TestBase::run_dcl_test("mysql_to_mysql/cdc/dcl_test", 3000, 5000).await;
    }
}
