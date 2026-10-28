use super::{RMatcher, RID};

#[derive(Debug, Clone, Copy, Default)]
pub struct ExactMatcher;

impl RMatcher for ExactMatcher {
    fn is_match(&self, input: &RID, pattern: &RID) -> bool {
        input == pattern
    }
}
