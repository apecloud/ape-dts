mod config_parser;
mod exact_matcher;
mod legacy;
mod pattern_matcher;
mod resource_kind;
mod rid;
mod rmatcher;

pub use config_parser::{
    ResourceConfigParser, MAPPING_SEPARATOR, RESOURCE_DELIMITERS, RESOURCE_SEPARATOR,
    RULE_SEPARATOR,
};
pub use exact_matcher::ExactMatcher;
pub use pattern_matcher::PatternMatcher;
pub use pattern_matcher::{OPTIONAL_WILDCARD, REGEX_PREFIX, REGEX_SUFFIX, WILDCARD};
pub use resource_kind::ResourceKind;
pub use rid::RID;
pub use rmatcher::RMatcher;
