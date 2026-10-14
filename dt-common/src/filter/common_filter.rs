use std::collections::HashSet;

use strum::{AsRefStr, EnumIter, EnumString};

use crate::resource::WILDCARD;

#[derive(Debug, Clone, Default)]
pub struct CommonFilter {
    pub do_set: HashSet<String>,
    pub ignore_set: HashSet<String>,
}

impl CommonFilter {
    pub fn new(do_set: HashSet<String>, ignore_set: HashSet<String>) -> Self {
        Self { do_set, ignore_set }
    }

    /// Only `do={"*"}` selects all; ignore values are literal and take precedence.
    pub fn excludes_value(&self, value: &str) -> bool {
        let all = self.do_set.len() == 1 && self.do_set.contains(WILDCARD);
        self.ignore_set.contains(value) || (!all && !self.do_set.contains(value))
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
    pub fn default_do(self) -> &'static str {
        match self {
            Self::Event | Self::Structure | Self::Cmd => WILDCARD,
            Self::Ddl | Self::Dcl => "",
        }
    }
}
