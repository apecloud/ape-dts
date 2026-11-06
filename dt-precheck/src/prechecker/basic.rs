use dt_common::{
    filter::DtFilter,
    resource::{ResourceKind, WILDCARD},
};

pub struct BasicPrechecker {}

impl BasicPrechecker {
    pub fn is_filter_pattern(filter: &DtFilter) -> bool {
        filter.resource_filters().values().any(|rules| {
            [(&rules.include_set, false), (&rules.exclude_set, true)]
                .into_iter()
                .any(|(rules, exclude)| {
                    rules.iter().any(|id| {
                        let Ok(object) = ResourceKind::precheck_path(filter.db_type(), id) else {
                            // Invalid paths cannot use the exact-name precheck.
                            return true;
                        };
                        if exclude
                            && !id.segments()[object.len()..]
                                .iter()
                                .all(|part| part == WILDCARD)
                        {
                            return false;
                        }
                        object
                            .segments()
                            .iter()
                            .any(|segment| filter.matcher.is_pattern(segment))
                    })
                })
        })
    }
}
