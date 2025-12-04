create database if not exists test_db_1;
use test_db_1;
create table if not exists double_check_test(
  id int primary key,
  name varchar(50)
);
