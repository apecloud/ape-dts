#[cfg(test)]
mod test {
    use anyhow::Context;
    use dt_common::meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        mssql::mssql_col_type::MssqlColType,
    };
    use serial_test::serial;

    use crate::{
        mssql_to_mssql::functionality::mssql_component_test_context::{
            MssqlComponentTestContext, TestTable,
        },
        test_runner::mssql_test_endpoint::MssqlTestEndpoint,
    };

    #[tokio::test]
    #[serial]
    async fn reads_and_invalidates_real_mssql_catalog_metadata() -> anyhow::Result<()> {
        let tables = [TestTable::new(
            "ape_dts_meta_manager_component_test",
            "meta_manager_test",
            "catalog_types",
        )];
        let context = MssqlComponentTestContext::new("meta_manager_test", &tables, &[]).await?;
        let pool = context.source_pool.clone();
        let [table] = tables;
        let mut manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;

        let mut connection = pool.get().await?;
        let row = connection
            .client_mut()
            .query(
                "SELECT CAST(NULL AS nvarchar(10)) AS null_value, \
                        CAST(1 AS int) AS wrong_type",
                &[],
            )
            .await?
            .into_row()
            .await?
            .context("MSSQL required value test returned no row")?;
        assert!(MssqlColValueConvertor::from_query_required_string(&row, "null_value").is_err());
        assert!(MssqlColValueConvertor::from_query_required_string(&row, "wrong_type").is_err());
        drop(connection);

        assert!(manager
            .list_schemas(table.db)
            .await?
            .contains(&table.schema.to_string()));
        assert_eq!(
            manager.list_tables(table.db, table.schema).await?,
            vec![table.tb]
        );
        assert!(manager
            .list_schema_tables(table.db)
            .await?
            .contains(&(table.schema.to_string(), table.tb.to_string())));

        let meta = manager
            .get_tb_meta(table.db, table.schema, table.tb)
            .await?;
        assert_eq!(
            meta.basic.cols,
            [
                "tenant_id",
                "id",
                "optional_name",
                "score",
                "alias_name",
                "computed_value",
                "valid_from",
                "valid_to",
                "version"
            ]
        );
        assert_eq!(meta.basic.db, table.db);
        assert_eq!(meta.basic.schema, table.schema);
        assert_eq!(meta.basic.tb, table.tb);
        assert!(meta.basic.nullable_cols.contains("optional_name"));
        assert!(!meta.basic.nullable_cols.contains("score"));
        assert_eq!(
            meta.basic.col_origin_type_map.get("alias_name"),
            Some(&"sysname".to_string())
        );
        assert_eq!(
            meta.basic.key_map.get("primary"),
            Some(&vec!["tenant_id".to_string(), "id".to_string()])
        );
        assert_eq!(
            meta.basic.key_map.get("uq_ape_dts_meta_manager_name"),
            Some(&vec!["optional_name".to_string()])
        );
        assert_eq!(meta.basic.order_cols, ["tenant_id", "id"]);
        assert_eq!(meta.basic.partition_col, "tenant_id");
        assert_eq!(meta.basic.id_cols, ["tenant_id", "id"]);
        assert_eq!(meta.identity_col.as_deref(), Some("id"));
        assert_eq!(meta.computed_cols, ["computed_value".to_string()].into());
        assert_eq!(meta.generated_always_type_map.get("valid_from"), Some(&1));
        assert_eq!(meta.generated_always_type_map.get("valid_to"), Some(&2));
        assert_eq!(meta.generated_always_type_map.get("tenant_id"), Some(&0));
        assert_eq!(meta.rowversion_cols, ["version".to_string()].into());
        assert_eq!(meta.get_col_type("tenant_id")?, &MssqlColType::Int4);
        assert_eq!(meta.get_col_type("id")?, &MssqlColType::Int8);
        assert_eq!(meta.get_col_type("optional_name")?, &MssqlColType::NVarchar);
        assert_eq!(meta.get_col_type("score")?, &MssqlColType::Float4);
        assert_eq!(meta.get_col_type("alias_name")?, &MssqlColType::NVarchar);
        assert_eq!(meta.get_col_type("computed_value")?, &MssqlColType::Int4);
        assert_eq!(meta.get_col_type("valid_from")?, &MssqlColType::Datetime2);
        assert_eq!(meta.get_col_type("version")?, &MssqlColType::BigVarBin);

        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "ALTER TABLE {} ADD [added_later] int NULL;",
                table.quoted_name()
            ),
        )
        .await?;

        assert!(!manager
            .get_tb_meta(table.db, table.schema, table.tb)
            .await?
            .basic
            .cols
            .contains(&"added_later".to_string()));

        manager.invalidate_cache_for_table(table.db, table.schema, table.tb);
        assert!(manager
            .get_tb_meta(table.db, table.schema, table.tb)
            .await?
            .basic
            .cols
            .contains(&"added_later".to_string()));

        assert!(manager
            .get_tb_meta(table.db, table.schema, "table_does_not_exist")
            .await
            .is_err());
        Ok(())
    }
}
