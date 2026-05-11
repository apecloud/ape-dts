DROP DATABASE IF EXISTS struct_ssl_it_mysql2mysql;

CREATE DATABASE struct_ssl_it_mysql2mysql;

```
CREATE TABLE struct_ssl_it_mysql2mysql.ssl_test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INT DEFAULT NULL
);
```
