DROP DATABASE IF EXISTS test_db_1;
CREATE DATABASE test_db_1;
USE test_db_1;

CREATE TABLE test_tb_1 (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    age INT,
    created_at DATETIME,
    updated_at TIMESTAMP,
    is_active BOOLEAN,
    salary DECIMAL(10, 2),
    description TEXT,
    data BLOB
);

CREATE TABLE test_tb_2 (
    id INT UNSIGNED PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    status ENUM('active', 'inactive', 'pending'),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    notes TEXT,
    metadata JSON
);

DROP DATABASE IF EXISTS test_db_2;
CREATE DATABASE test_db_2;
USE test_db_2;

CREATE TABLE test_tb_1 (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    content TEXT,
    published_at DATE,
    views INT DEFAULT 0,
    rating FLOAT,
    tags SET('news', 'tech', 'sports', 'entertainment')
);

CREATE TABLE router_test_2 (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    value DOUBLE,
    created_at TIMESTAMP
); 