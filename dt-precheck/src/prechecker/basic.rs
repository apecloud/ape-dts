use dt_common::{filter::DtFilter, resource::ResourceKind};

pub struct BasicPrechecker {}

impl BasicPrechecker {
    pub fn is_filter_pattern(filter: &DtFilter) -> bool {
        filter.resource_filters().iter().any(|(kind, rules)| {
            if *kind == ResourceKind::Column {
                return false;
            }
            rules.do_set.iter().chain(&rules.ignore_set).any(|id| {
                id.segments()
                    .iter()
                    .any(|segment| filter.matcher.is_pattern(segment))
            })
        })
    }
}
