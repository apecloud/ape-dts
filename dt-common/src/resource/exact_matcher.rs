use super::{RMatcher, RID};

#[derive(Debug, Clone, Copy, Default)]
pub struct ExactMatcher;

impl RMatcher for ExactMatcher {
    fn is_match(&self, input: &RID, pattern: &RID) -> bool {
        input == pattern
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rid;

    #[test]
    fn exact_matching_preserves_characters_boundaries_and_depth() {
        let matcher: &dyn RMatcher = &ExactMatcher;
        for id in [
            rid!("db", "table"),
            rid!("*", "r#.*#"),
            rid!("`db`", "订单"),
        ] {
            assert!(matcher.is_match(&id, &id));
        }
        for (input, pattern) in [
            (rid!("db", "table"), rid!("db", "Table")),
            (rid!("db", "table"), rid!("db", "*")),
            (rid!("db", "table"), rid!("db", "r#.*#")),
            (rid!("db", "table"), rid!("`db`", "table")),
            (rid!("a.b", "c"), rid!("a", "b.c")),
            (rid!("db", "table"), rid!("db")),
            (rid!("db"), rid!("db", "table")),
        ] {
            assert!(!matcher.is_match(&input, &pattern));
        }
    }
}
