use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("too short row count: {0}, expected at least {1}")]
    TooShortRowCountError(u64, u64),
    #[error("bad split column, min value:{0}, max value:{1}")]
    BadSplitColumnError(String, String),
    #[error("{0} out of distribution factor range [{1},{2}]")]
    OutOfDistributionFactorRangeError(f64, f64, f64),
}
