use std::collections::HashMap;

use anyhow::{ensure, Result};

use crate::resource::RID;

/// Rules at one source resource depth. Names and targets are always concrete RIDs;
/// exact/prefix/default matching is represented by the rule, not wildcard RID segments.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BaseRouter {
    exact: HashMap<RID, RID>,
    prefixes: HashMap<RID, RID>,
    default: Option<RID>,
}

#[derive(Clone, Copy)]
pub(super) enum RouteOutput {
    PreserveSuffix,
    Replace,
}

impl BaseRouter {
    /// Returns whether a rule changed. Overwrite is reserved for legacy configuration.
    pub(super) fn insert(
        &mut self,
        source: Option<RID>,
        descendants: bool,
        target: RID,
        overwrite: bool,
    ) -> Result<bool> {
        ensure!(
            source.is_some() || !descendants,
            "default route cannot specify descendants"
        );
        if let Some(source) = source {
            let map = if descendants {
                &mut self.prefixes
            } else {
                &mut self.exact
            };
            if let Some(previous) = map.get(&source) {
                ensure!(
                    overwrite || previous == &target,
                    "conflicting routes for {source}: {previous} and {target}"
                );
                if previous == &target {
                    return Ok(false);
                }
            }
            map.insert(source, target);
        } else {
            if let Some(previous) = &self.default {
                ensure!(
                    overwrite || previous == &target,
                    "conflicting default routes: {previous} and {target}"
                );
                if previous == &target {
                    return Ok(false);
                }
            }
            self.default = Some(target);
        }
        Ok(true)
    }

    pub(super) fn prefixes(&self) -> impl Iterator<Item = (&RID, &RID)> {
        self.prefixes.iter()
    }

    /// Exact resource, longest original prefix, then default. Defaults live in slot zero.
    /// The caller validates engine kinds and decides how an unmatched resource is handled.
    pub(super) fn route(levels: &[Self], source: &RID, output: RouteOutput) -> Result<Option<RID>> {
        if let Some(target) = levels
            .get(source.len() - 1)
            .and_then(|level| level.exact.get(source))
        {
            return Ok(Some(target.clone()));
        }
        for depth in (1..=source.len().min(levels.len())).rev() {
            let prefix = source.prefix(depth).expect("validated prefix");
            if let Some(target) = levels[depth - 1].prefixes.get(&prefix) {
                return Ok(Some(match output {
                    RouteOutput::PreserveSuffix => source.replace_prefix(depth, target)?,
                    RouteOutput::Replace => target.clone(),
                }));
            }
        }
        Ok(levels.first().and_then(|level| level.default.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rid;

    #[test]
    fn the_same_prefix_rule_can_preserve_or_replace_the_remaining_path() {
        let mut levels = vec![BaseRouter::default(); 3];
        levels[0]
            .insert(Some(rid!("inventory")), true, rid!("archive.topic"), false)
            .unwrap();
        let source = rid!("inventory", "order");
        assert_eq!(
            BaseRouter::route(&levels, &source, RouteOutput::PreserveSuffix).unwrap(),
            Some(rid!("archive.topic", "order"))
        );
        assert_eq!(
            BaseRouter::route(&levels, &source, RouteOutput::Replace).unwrap(),
            Some(rid!("archive.topic"))
        );
        assert_eq!(
            BaseRouter::route(&levels, &rid!("unmapped", "order"), RouteOutput::Replace).unwrap(),
            None
        );
    }
}
