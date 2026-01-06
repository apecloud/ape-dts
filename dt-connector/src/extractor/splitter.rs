use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("bad split column, min value:{0}, max value:{1}")]
    BadSplitColumnError(String, String),
    #[error("{0} out of distribution factor range [{1},{2}]")]
    OutOfDistributionFactorRangeError(f64, f64, f64),
}
