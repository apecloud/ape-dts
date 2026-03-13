-- prepare user-defined types for struct copy test
DROP SCHEMA IF EXISTS struct_it_pg2pg_1 CASCADE;
CREATE SCHEMA struct_it_pg2pg_1;

-- 1. basic enum type
CREATE TYPE struct_it_pg2pg_1.order_status AS ENUM ('pending', 'paid', 'cancelled');

-- 2. domain on builtin type
```
CREATE DOMAIN struct_it_pg2pg_1.non_empty_text AS text
    CHECK (length(VALUE) > 0);
```    

-- 3. domain depending on enum (paid_status depends on order_status)
```
CREATE DOMAIN struct_it_pg2pg_1.paid_status AS struct_it_pg2pg_1.order_status
    CHECK (VALUE IN ('paid'));
```    

-- 4. composite type depending on enum + domain
```
CREATE TYPE struct_it_pg2pg_1.order_info AS (
    id      int,
    status  struct_it_pg2pg_1.order_status,
    remark  struct_it_pg2pg_1.non_empty_text
);
```

-- 5. range type using builtin subtype and builtin subtype_diff
```
CREATE TYPE struct_it_pg2pg_1.discount_range AS RANGE (
    subtype      = float8,
    subtype_diff = float8mi
);
```
