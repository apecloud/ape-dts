USE [master];

IF DB_ID(N'ape_dts_resumer_component_test') IS NOT NULL
BEGIN
    ALTER DATABASE [ape_dts_resumer_component_test]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [ape_dts_resumer_component_test];
END;

CREATE DATABASE [ape_dts_resumer_component_test];
GO
