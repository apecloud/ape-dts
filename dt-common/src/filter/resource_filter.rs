use std::collections::HashSet;

use anyhow::{ensure, Result};

use crate::resource::{PatternMatcher, RMatcher, RID};

#[derive(Debug, Clone, Default)]
pub struct ResourceFilter {
    pub do_set: HashSet<RID>,
    pub ignore_set: HashSet<RID>,
}

impl ResourceFilter {
    pub fn new(do_set: HashSet<RID>, ignore_set: HashSet<RID>) -> Self {
        Self { do_set, ignore_set }
    }

    pub fn matches(rules: &HashSet<RID>, id: &RID, matcher: &PatternMatcher) -> bool {
        rules.iter().any(|pattern| matcher.is_match(id, pattern))
    }

    pub fn matches_prefix(rules: &HashSet<RID>, prefix: &RID, matcher: &PatternMatcher) -> bool {
        rules.iter().any(|pattern| {
            prefix.len() <= pattern.len()
                && matcher.is_match_segments(prefix.segments(), &pattern.segments()[..prefix.len()])
        })
    }

    pub fn covers_descendants(
        rules: &HashSet<RID>,
        prefix: &RID,
        matcher: &PatternMatcher,
    ) -> bool {
        rules.iter().any(|pattern| {
            prefix.len() < pattern.len()
                && pattern.segments()[prefix.len()..]
                    .iter()
                    .all(|segment| segment == "*")
                && matcher.is_match_segments(prefix.segments(), &pattern.segments()[..prefix.len()])
        })
    }

    pub fn validate(&self, depth: usize, matcher: &PatternMatcher) -> Result<()> {
        ensure!(
            self.do_set
                .iter()
                .chain(&self.ignore_set)
                .all(|id| id.len() == depth),
            "resource patterns require {depth} segments"
        );
        for pattern in self.do_set.iter().chain(&self.ignore_set) {
            matcher.validate(pattern)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::config_enums::DbType, rid, utils::sql_util::SqlUtil};

    #[test]
    fn wildcards_preserve_namespace_filter_behavior() {
        let matcher = PatternMatcher::new(SqlUtil::get_escape_pairs(&DbType::Mysql));
        let prefix = rid!("db");
        let all = HashSet::from([rid!("db", "*")]);
        assert!(ResourceFilter::covers_descendants(&all, &prefix, &matcher));
        for rule in ["`*`", "`^.*$`", "order*", "?"] {
            let rules = HashSet::from([RID::new(["db", rule]).unwrap()]);
            assert!(
                !ResourceFilter::covers_descendants(&rules, &prefix, &matcher),
                "{rule}"
            );
        }
        assert!(!ResourceFilter::covers_descendants(
            &all,
            &rid!("other"),
            &matcher
        ));
        assert!(!ResourceFilter::matches(
            &all,
            &rid!("db", "order", "id"),
            &matcher
        ));
    }
}
