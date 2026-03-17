-- prepare user-defined types for struct copy test
DROP SCHEMA IF EXISTS struct_it_pg2pg_1 CASCADE;
CREATE SCHEMA struct_it_pg2pg_1;

-- basic enum type
CREATE TYPE struct_it_pg2pg_1.order_status AS ENUM ('pending', 'paid', 'cancelled');

-- domain on builtin type
```
CREATE DOMAIN struct_it_pg2pg_1.non_empty_text AS text
    CHECK (length(VALUE) > 0);
```

-- domain depending on enum (paid_status depends on order_status)
```
CREATE DOMAIN struct_it_pg2pg_1.paid_status AS struct_it_pg2pg_1.order_status
    CHECK (VALUE IN ('paid'));
```

-- composite type depending on enum + domain
```
CREATE TYPE struct_it_pg2pg_1.order_info AS (
    id      int,
    status  struct_it_pg2pg_1.order_status,
    remark  struct_it_pg2pg_1.non_empty_text
);
```

-- range type using builtin subtype and builtin subtype_diff
```
CREATE TYPE struct_it_pg2pg_1.discount_range AS RANGE (
    subtype      = float8,
    subtype_diff = float8mi
);
```

-- additional enum type to test multiple enums and identifier quoting
CREATE TYPE struct_it_pg2pg_1.priority_level AS ENUM ('low', 'medium', 'high');

-- range type without canonical or subtype_diff (only subtype)
```
CREATE TYPE struct_it_pg2pg_1.date_range AS RANGE (
    subtype = date
);
```

-- composite type that uses both enum and range types
```
CREATE TYPE struct_it_pg2pg_1.promo_window AS (
    promo_id        int,
    priority        struct_it_pg2pg_1.priority_level,
    active_period   struct_it_pg2pg_1.date_range,
    discount_bounds struct_it_pg2pg_1.discount_range
);
```

-- domain on range type with NOT NULL and CHECK constraint
```
CREATE DOMAIN struct_it_pg2pg_1.valid_promo_period AS struct_it_pg2pg_1.date_range
    NOT NULL
    CHECK (upper(VALUE) > lower(VALUE));
```