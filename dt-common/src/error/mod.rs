mod code;
mod context;
mod dt_error;
mod error_context;
pub mod provider;
mod report;

pub use code::ErrorCode;
pub use context::{EndpointRole, ErrorObject, Stage};
pub use dt_error::DtError;
pub use error_context::{ClassifyError, DtErrorContext, DtErrorContextExt, DtResultExt};
pub use provider::classify_sqlx_error;
pub use report::{ErrorReport, ERROR_REPORT_SCHEMA_VERSION};
