#[cfg(test)]
mod test {

    use std::collections::HashMap;

    use dt_common::config::config_enums::DbType;
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/basic_test").await;
    }

    /// dst table already has records with same primary keys of src table,
    /// src data should be synced to dst table by "ON CONFLICT (pk) DO UPDATE SET"
    #[tokio::test]
    #[serial]
    async fn snapshot_on_duplicate_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/on_duplicate_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_wildchar_filter_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/wildchar_filter_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_postgis_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/postgis_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_postgis_array_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/postgis_array_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_charset_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/charset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_charset_euc_cn_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/charset_euc_cn_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_log_test() {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert("public.resume_table_1", 1);
        dst_expected_counts.insert("public.resume_table_2", 1);
        dst_expected_counts.insert("public.resume_table_3", 1);
        dst_expected_counts.insert("public.resume_table_*$4", 1);
        dst_expected_counts.insert("public.nullable_composite_unique_key_table", 6);
        dst_expected_counts.insert("public.bytea_pk_test", 2);
        dst_expected_counts.insert(r#""test_db_*.*"."resume_table_*$5""#, 1);
        dst_expected_counts.insert(r#""test_db_*.*"."finished_table_*$1""#, 0);
        dst_expected_counts.insert(r#""test_db_*.*"."finished_table_*$2""#, 0);
        dst_expected_counts.insert(r#""test_db_*.*"."in_position_log_table_*$1""#, 1);
        dst_expected_counts.insert(r#""test_db_*.*"."in_finished_log_table_*$1""#, 0);
        dst_expected_counts.insert(r#""test_db_*.*"."in_finished_log_table_*$2""#, 0);

        TestBase::run_snapshot_test_and_check_dst_count(
            "pg_to_pg/snapshot/resume_log_test",
            &DbType::Pg,
            dst_expected_counts,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_log_charset_test() {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert("public.bytea_pk_test", 2);

        TestBase::run_snapshot_test_and_check_dst_count(
            "pg_to_pg/snapshot/resume_log_test/resume_charset_test",
            &DbType::Pg,
            dst_expected_counts,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_db_test() {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert("public.resume_table_1", 1);
        dst_expected_counts.insert("public.resume_table_2", 1);
        dst_expected_counts.insert("public.resume_table_3", 1);
        dst_expected_counts.insert("public.resume_table_*$4", 1);
        dst_expected_counts.insert("public.nullable_composite_unique_key_table", 6);
        dst_expected_counts.insert("public.bytea_pk_test", 2);
        dst_expected_counts.insert(r#""test_db_*.*"."resume_table_*$5""#, 1);
        dst_expected_counts.insert(r#""test_db_*.*"."finished_table_*$1""#, 0);
        dst_expected_counts.insert(r#""test_db_*.*"."finished_table_*$2""#, 0);

        TestBase::run_snapshot_test_and_check_dst_count(
            "pg_to_pg/snapshot/resume_db_test",
            &DbType::Pg,
            dst_expected_counts,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_db_charset_test() {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert("public.bytea_pk_test", 2);

        TestBase::run_snapshot_test_and_check_dst_count(
            "pg_to_pg/snapshot/resume_db_test/resume_charset_test",
            &DbType::Pg,
            dst_expected_counts,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_special_character_in_name_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/special_character_in_name_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_timezone_test() {
        println!("snapshot_timezone_test can be covered by test: snapshot_basic_test, table: timezone_table, the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00")
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_route_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_parallel_test() {
        TestBase::run_snapshot_test("pg_to_pg/snapshot/parallel_test").await;
    }
}
