DROP SCHEMA IF EXISTS test_db_1 CASCADE;
CREATE SCHEMA test_db_1;

DROP TYPE IF EXISTS public.my_status;
DROP TYPE IF EXISTS test_db_1.my_status;

CREATE TYPE public.my_status AS ENUM ('open', 'closed');
CREATE TYPE test_db_1.my_status AS ENUM ('active', 'inactive');

```
CREATE TABLE test_db_1.user_orders (
    order_id    serial PRIMARY KEY,
    user_name   text NOT NULL,
    current_status test_db_1.my_status
);
```

-- test enum type only in test_db_1 (no public counterpart)
DROP TYPE IF EXISTS test_db_1.priority_level;
CREATE TYPE test_db_1.priority_level AS ENUM ('low', 'medium', 'high', 'critical');

```
CREATE TABLE test_db_1.task_list (
    task_id     serial PRIMARY KEY,
    title       text NOT NULL,
    priority    test_db_1.priority_level
);
```

-- test composite type
DROP TYPE IF EXISTS test_db_1.address_type;
```
CREATE TYPE test_db_1.address_type AS (
    street  text,
    city    text,
    zip     varchar(10),
    country text
);
```

DROP TYPE IF EXISTS test_db_1.full_name;
```
CREATE TYPE test_db_1.full_name AS (
    first_name  text,
    last_name   text
);
```

```
CREATE TABLE test_db_1.composite_type_table (
    pk          serial PRIMARY KEY,
    home_addr   test_db_1.address_type,
    work_addr   test_db_1.address_type,
    person_name test_db_1.full_name
);
```

-- test custom range type
DROP TYPE IF EXISTS test_db_1.float8_range;
```
CREATE TYPE test_db_1.float8_range AS RANGE (
    subtype = float8,
    subtype_diff = float8mi
);
```

```
CREATE TABLE test_db_1.custom_range_table (
    pk          serial PRIMARY KEY,
    price_range test_db_1.float8_range
);
```

-- test domain type
DROP DOMAIN IF EXISTS test_db_1.positive_int;
CREATE DOMAIN test_db_1.positive_int AS integer CHECK (VALUE > 0);

DROP DOMAIN IF EXISTS test_db_1.email_address;
CREATE DOMAIN test_db_1.email_address AS text CHECK (VALUE ~ '^[^@]+@[^@]+\.[^@]+$');

DROP DOMAIN IF EXISTS test_db_1.short_text;
CREATE DOMAIN test_db_1.short_text AS varchar(100) NOT NULL DEFAULT '';

```
CREATE TABLE test_db_1.domain_type_table (
    pk          serial PRIMARY KEY,
    quantity    test_db_1.positive_int,
    contact     test_db_1.email_address,
    label       test_db_1.short_text
);
```

-- test type names with special characters
DROP TYPE IF EXISTS test_db_1."type:colon";
CREATE TYPE test_db_1."type:colon" AS ENUM ('a', 'b', 'c');

DROP TYPE IF EXISTS test_db_1."type,comma";
CREATE TYPE test_db_1."type,comma" AS ENUM ('x', 'y');

DROP TYPE IF EXISTS test_db_1."type space";
```
CREATE TYPE test_db_1."type space" AS (
    val1 int,
    val2 text
);
```

DROP DOMAIN IF EXISTS test_db_1."domain-dash";
CREATE DOMAIN test_db_1."domain-dash" AS integer CHECK (VALUE >= 0);

```
CREATE TABLE test_db_1.special_type_name_table (
    pk              serial PRIMARY KEY,
    col_colon       test_db_1."type:colon",
    col_comma       test_db_1."type,comma",
    col_space       test_db_1."type space",
    col_domain_dash test_db_1."domain-dash"
);
```
