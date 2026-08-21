#[derive(Clone, Hash)]
pub enum RouterConfig {
    Rdb {
        // User-facing db_map maps the database-specific top-level namespace.
        schema_map: String,
        tb_map: String,
        col_map: String,
        topic_map: String,
    },
}
