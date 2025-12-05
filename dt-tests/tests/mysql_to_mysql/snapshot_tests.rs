#[cfg(test)]
mod test {

    use std::collections::HashMap;

    use dt_common::config::config_enums::DbType;
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_on_duplicate_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/on_duplicate_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_wildchar_filter_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/wildchar_filter_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_charset_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/charset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_special_character_in_name_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/special_character_in_name_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_log_test() {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert("test_db_1.no_pk_no_uk", 9);
        // no_pk_one_uk has a uk with multiple cols, UNIQUE KEY uk_1 (f_1,f_2), resume_filter won't work
        dst_expected_counts.insert("test_db_1.no_pk_one_uk", 9);
        // resume_filter works
        dst_expected_counts.insert("test_db_1.one_pk_multi_uk", 4);
        dst_expected_counts.insert("test_db_1.one_pk_no_uk", 4);
        dst_expected_counts.insert("test_db_1.multi_pk", 1);
        dst_expected_counts.insert("test_db_1.nullable_composite_unique_key_table", 6);
        dst_expected_counts.insert("test_db_1.bytea_pk_gb2312_test", 2);
        dst_expected_counts.insert("test_db_1.bytea_pk_utf8_test", 2);
        // with special characters in db && tb && col names
        dst_expected_counts.insert("test_db_@.resume_table_*$4", 1);

        dst_expected_counts.insert("test_db_@.finished_table_*$1", 0);
        dst_expected_counts.insert("test_db_@.finished_table_*$2", 0);

        dst_expected_counts.insert("test_db_@.in_position_log_table_*$1", 1);

        dst_expected_counts.insert("test_db_@.in_finished_log_table_*$1", 0);
        dst_expected_counts.insert("test_db_@.in_finished_log_table_*$2", 0);

        TestBase::run_snapshot_test_and_check_dst_count(
            "mysql_to_mysql/snapshot/resume_log_test",
            &DbType::Mysql,
            dst_expected_counts,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_db_test() {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert("test_db_1.no_pk_no_uk", 9);
        // no_pk_one_uk has a uk with multiple cols, UNIQUE KEY uk_1 (f_1,f_2), resume_filter won't work
        dst_expected_counts.insert("test_db_1.no_pk_one_uk", 9);
        // resume_filter works
        dst_expected_counts.insert("test_db_1.one_pk_multi_uk", 4);
        dst_expected_counts.insert("test_db_1.one_pk_no_uk", 4);
        dst_expected_counts.insert("test_db_1.multi_pk", 1);
        dst_expected_counts.insert("test_db_1.nullable_composite_unique_key_table", 6);
        dst_expected_counts.insert("test_db_1.bytea_pk_gb2312_test", 2);
        dst_expected_counts.insert("test_db_1.bytea_pk_utf8_test", 2);
        // with special characters in db && tb && col names
        dst_expected_counts.insert("test_db_@.resume_table_*$4", 1);

        dst_expected_counts.insert("test_db_@.finished_table_*$1", 0);
        dst_expected_counts.insert("test_db_@.finished_table_*$2", 0);

        TestBase::run_snapshot_test_and_check_dst_count(
            "mysql_to_mysql/snapshot/resume_db_test",
            &DbType::Mysql,
            dst_expected_counts,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_json_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/json_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_route_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_timezone_test() {
        println!("snapshot_timezone_test can be covered by test: cdc_basic_test, table: one_pk_no_uk, field: f_13 timestamp(6), the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00 ")
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_parallel_test() {
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/parallel_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_tb_parallel_test() {
        // [runtime]
        // tb_parallel_size=3
        TestBase::run_snapshot_test("mysql_to_mysql/snapshot/tb_parallel_test").await;
    }
}
