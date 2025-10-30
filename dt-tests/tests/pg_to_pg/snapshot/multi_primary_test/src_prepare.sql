DROP SCHEMA IF EXISTS test_db_1 CASCADE;
CREATE SCHEMA test_db_1;

```
CREATE TABLE test_db_1.student_courses (
    student_id INT NOT NULL,
    course_id INT NOT NULL,
    grade VARCHAR(2),

    -- composite primary key
    PRIMARY KEY (student_id, course_id)
);
```