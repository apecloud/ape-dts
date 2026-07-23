use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Bootstrap,
    Precheck,
    Extractor,
    Parallelizer,
    Pipeline,
    Sinker,
    Checker,
    Resumer,
    Task,
    Unknown,
}

impl Stage {
    pub const fn diagnostic_name(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Precheck => "precheck",
            Self::Extractor => "extractor",
            Self::Parallelizer => "parallelizer",
            Self::Pipeline => "pipeline",
            Self::Sinker => "sinker",
            Self::Checker => "checker",
            Self::Resumer => "resumer",
            Self::Task => "task",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointRole {
    Source,
    Destination,
    Metadata,
}

impl EndpointRole {
    pub const fn user_description(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
            Self::Metadata => "metadata store",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ErrorObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<String>,
}

impl ErrorObject {
    pub(crate) fn fill_missing_from(&mut self, outer: &Self) {
        self.schema = self.schema.take().or_else(|| outer.schema.clone());
        self.table = self.table.take().or_else(|| outer.table.clone());
        self.column = self.column.take().or_else(|| outer.column.clone());
        self.constraint = self.constraint.take().or_else(|| outer.constraint.clone());
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OriginError {
    pub system: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

impl OriginError {
    pub fn new(system: impl Into<String>, code: Option<impl Into<String>>) -> Self {
        Self {
            system: system.into(),
            code: code.map(Into::into),
        }
    }
}
