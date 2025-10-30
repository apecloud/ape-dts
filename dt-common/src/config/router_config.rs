#[derive(Clone, Hash)]
pub enum RouterConfig {
    Rdb {
        schema_map: String,
        tb_map: String,
        col_map: String,
        topic_map: String,
    },
}
