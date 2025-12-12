#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::{rdb_cycle_test_runner::RdbCycleTestRunner, test_base::TestBase};

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/basic_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_postgis_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/postgis_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_postgis_array_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/postgis_array_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_charset_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/charset_test", 3000, 5000).await;
    }

    // #[tokio::test]
    #[serial]
    async fn cdc_charset_euc_cn_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/charset_euc_cn_test", 3000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_timezone_test() {
        println!("cdc_timezone_test can be covered by test: cdc_basic_test, table: timezone_table, the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00")
    }

    #[tokio::test]
    #[serial]
    async fn cdc_special_character_in_name_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/special_character_in_name_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_wildchar_filter_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/wildchar_filter_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_route_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/route_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_foreign_key_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/foreign_key_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_ddl_test() {
        TestBase::run_ddl_test("pg_to_pg/cdc/ddl_test", 3000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cycle_basic_test() {
        let tx_check_data = vec![
            ("node1", "node2", "node1", "10"),
            ("node2", "node1", "node2", "10"),
        ];

        RdbCycleTestRunner::run_cycle_cdc_test(
            "pg_to_pg/cdc/cycle_basic_test",
            2000,
            4000,
            &tx_check_data,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
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
            "pg_to_pg/cdc/cycle_star_test",
            2000,
            4000,
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
            "pg_to_pg/cdc/cycle_net_test",
            2000,
            4000,
            &tx_check_data,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_heartbeat_test() {
        TestBase::run_heartbeat_test("pg_to_pg/cdc/heartbeat_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_to_sql_test() {
        TestBase::run_cdc_to_sql_test("pg_to_pg/cdc/to_sql_test", false, 3000, 4000).await;
    }

    /// need postgres tables: ALTER TABLE table_name REPLICA IDENTITY FULL;
    #[tokio::test]
    #[serial]
    async fn cdc_to_reverse_sql_test() {
        TestBase::run_cdc_to_sql_test("pg_to_pg/cdc/to_sql_test", true, 3000, 4000).await;
    }
}
