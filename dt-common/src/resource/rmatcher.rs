use super::RID;

/// Match equal-depth RIDs without crossing segment boundaries.
pub trait RMatcher {
    fn is_match(&self, input: &RID, pattern: &RID) -> bool;
}
