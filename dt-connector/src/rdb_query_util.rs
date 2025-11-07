pub struct RdbQueryUtil {}

impl RdbQueryUtil {
    pub fn build_limit_offset_clause(limit: usize, offset: usize) -> String {
        format!("LIMIT {} OFFSET {}", limit, offset)
    }
}
