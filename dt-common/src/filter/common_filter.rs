use std::collections::HashSet;

use strum::{AsRefStr, EnumIter, EnumString};

use crate::resource::WILDCARD;

#[derive(Debug, Clone, Default)]
pub struct CommonFilter {
    pub include_set: HashSet<String>,
    pub exclude_set: HashSet<String>,
}

impl CommonFilter {
    pub fn new(include_set: HashSet<String>, exclude_set: HashSet<String>) -> Self {
        Self {
            include_set,
            exclude_set,
        }
    }

    /// Only `include={"*"}` selects all; exclude values are literal and take precedence.
    pub fn excludes_value(&self, value: &str) -> bool {
        let all = self.include_set.len() == 1 && self.include_set.contains(WILDCARD);
        self.exclude_set.contains(value) || (!all && !self.include_set.contains(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, EnumIter, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum CommonFilterKind {
    Event,
    Ddl,
    Dcl,
    Structure,
    Cmd,
}

impl CommonFilterKind {
    pub fn default_include(self) -> &'static str {
        match self {
            Self::Event | Self::Structure | Self::Cmd => WILDCARD,
            Self::Ddl | Self::Dcl => "",
        }
    }
}
