```
create database if not exists struct_it_mysql2mysql_0 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
use struct_it_mysql2mysql_0;

DROP PROCEDURE IF EXISTS SetupTestDatabases;
CREATE PROCEDURE SetupTestDatabases()
BEGIN
    -- Declare variables for the loop counter and database name.
    DECLARE i INT DEFAULT 1;
    DECLARE db_name VARCHAR(255);

    WHILE i <= 100 DO
        -- Construct the database name, e.g., 'struct_it_mysql2mysql_1'
        SET db_name = CONCAT('struct_it_mysql2mysql_', i);

        -- ====================================================================
        -- Use PREPARE and EXECUTE to run dynamic SQL statements.
        -- This is the standard way to execute dynamic DDL (like CREATE, DROP)
        -- in a stored procedure.
        -- ====================================================================

        -- 1. Dynamically Drop Database
        -- Build the SQL command into a string.
        SET @sql_command = CONCAT('DROP DATABASE IF EXISTS `', db_name, '`');
        -- Prepare the statement.
        PREPARE stmt FROM @sql_command;
        -- Execute the statement.
        EXECUTE stmt;
        -- Deallocate the prepared statement.
        DEALLOCATE PREPARE stmt;
        -- Increment the loop counter.
        SET i = i + 1;
    END WHILE;
END;

CALL SetupTestDatabases();
```