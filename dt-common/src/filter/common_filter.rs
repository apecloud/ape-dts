use std::collections::HashSet;

use strum::{AsRefStr, EnumString};

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
        let all = self.do_set.len() == 1 && self.do_set.contains("*");
        self.ignore_set.contains(value) || (!all && !self.do_set.contains(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum CommonFilterKind {
    Event,
    Ddl,
    Dcl,
    Structure,
    Cmd,
}

impl CommonFilterKind {
    pub const ALL: [Self; 5] = [
        Self::Event,
        Self::Ddl,
        Self::Dcl,
        Self::Structure,
        Self::Cmd,
    ];

    pub fn default_do(self) -> &'static str {
        match self {
            Self::Event | Self::Structure | Self::Cmd => "*",
            Self::Ddl | Self::Dcl => "",
        }
    }
}
