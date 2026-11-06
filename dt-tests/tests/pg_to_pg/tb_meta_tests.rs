#[cfg(test)]
mod test {
    use crate::test_runner::rdb_test_runner::RdbTestRunner;
    use dt_common::meta::pg::pg_meta_manager::PgMetaManager;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        let runner = RdbTestRunner::new("pg_to_pg/tb_meta/basic_test")
            .await
            .unwrap();
        runner.execute_prepare_sqls().await.unwrap();

        let mut meta_manager = PgMetaManager::new(runner.src_conn_pool_pg.unwrap())
            .await
            .unwrap();
        let tb_meta = meta_manager
            .get_tb_meta("tb_meta_test", "full_column_type")
            .await
            .unwrap();

        let expect_col_alias_map = vec![
            ("id", "int4"),
            ("char_col", "bpchar"),
            ("char_col_2", "bpchar"),
            ("character_col", "bpchar"),
            ("character_col_2", "bpchar"),
            ("varchar_col", "varchar"),
            ("varchar_col_2", "varchar"),
            ("character_varying_col", "varchar"),
            ("character_varying_col_2", "varchar"),
            ("bpchar_col", "bpchar"),
            ("bpchar_col_2", "bpchar"),
            ("text_col", "text"),
            ("real_col", "float4"),
            ("float4_col", "float4"),
            ("double_precision_col", "float8"),
            ("float8_col", "float8"),
            ("numeric_col", "numeric"),
            ("numeric_col_2", "numeric"),
            ("decimal_col", "numeric"),
            ("decimal_col_2", "numeric"),
            ("smallint_col", "int2"),
            ("int2_col", "int2"),
            ("smallserial_col", "int2"),
            ("serial2_col", "int2"),
            ("integer_col", "int4"),
            ("int_col", "int4"),
            ("int4_col", "int4"),
            ("serial_col", "int4"),
            ("serial4_col", "int4"),
            ("bigint_col", "int8"),
            ("int8_col", "int8"),
            ("bigserial_col", "int8"),
            ("serial8_col", "int8"),
            ("bit_col", "bit"),
            ("bit_col_2", "bit"),
            ("bit_varying_col", "varbit"),
            ("bit_varying_col_2", "varbit"),
            ("varbit_col", "varbit"),
            ("varbit_col_2", "varbit"),
            ("time_col", "time"),
            ("time_col_2", "time"),
            ("time_col_3", "time"),
            ("time_col_4", "time"),
            ("timez_col", "timetz"),
            ("timez_col_2", "timetz"),
            ("timez_col_3", "timetz"),
            ("timez_col_4", "timetz"),
            ("timestamp_col", "timestamp"),
            ("timestamp_col_2", "timestamp"),
            ("timestamp_col_3", "timestamp"),
            ("timestamp_col_4", "timestamp"),
            ("timestampz_col", "timestamptz"),
            ("timestampz_col_2", "timestamptz"),
            ("timestampz_col_3", "timestamptz"),
            ("timestampz_col_4", "timestamptz"),
            ("boolean_col", "bool"),
            ("bool_col", "bool"),
            ("box_col", "box"),
            ("bytea_col", "bytea"),
            ("cidr_col", "cidr"),
            ("circle_col", "circle"),
            ("date_col", "date"),
            ("inet_col", "inet"),
            ("interval_col", "interval"),
            ("interval_col_2", "interval"),
            ("json_col", "json"),
            ("jsonb_col", "jsonb"),
            ("line_col", "line"),
            ("lseg_col", "lseg"),
            ("macaddr_col", "macaddr"),
            ("macaddr8_col", "macaddr8"),
            ("money_col", "money"),
            ("path_col", "path"),
            ("pg_lsn_col", "pg_lsn"),
            ("pg_snapshot_col", "pg_snapshot"),
            ("polygon_col", "polygon"),
            ("point_col", "point"),
            ("tsquery_col", "tsquery"),
            ("tsvector_col", "tsvector"),
            ("txid_snapshot_col", "txid_snapshot"),
            ("uuid_col", "uuid"),
            ("xml_col", "xml"),
            ("array_float4_col", "_float4"),
            ("array_float8_col", "_float8"),
            ("array_int2_col", "_int2"),
            ("array_int4_col", "_int4"),
            ("array_int8_col", "_int8"),
            ("array_int8_col_2", "_int8"),
            ("array_text_col", "_text"),
            ("array_boolean_col", "_bool"),
            ("array_boolean_col_2", "_bool"),
            ("array_date_col", "_date"),
            ("array_timestamp_col", "_timestamp"),
            ("array_timestamp_col_2", "_timestamp"),
            ("array_timestamptz_col", "_timestamptz"),
            ("array_timestamptz_col_2", "_timestamptz"),
        ];

        assert_eq!(expect_col_alias_map.len(), tb_meta.col_type_map.len());

        for (col, expect_col_alias) in expect_col_alias_map {
            let col_type = tb_meta.col_type_map.get(col).unwrap();
            println!(
                "col: {}, alias: {}, oid: {}",
                col, col_type.alias, col_type.oid
            );
            assert_eq!(col_type.alias, col_type.name);
            assert_eq!(expect_col_alias, col_type.alias);
        }
    }
}
