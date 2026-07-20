use crate::error::{DtError, ErrorCode, Stage};
use crate::utils::time_util::TimeUtil;

#[derive(Clone)]
pub struct TimeFilter {
    // timestamp in UTC
    pub start_timestamp: u32,
    pub end_timestamp: u32,
    pub started: bool,
    pub ended: bool,
}

impl TimeFilter {
    pub fn new(start_time_utc: &str, end_time_utc: &str) -> anyhow::Result<Self> {
        let start_timestamp = if start_time_utc.is_empty() {
            0
        } else {
            TimeUtil::datetime_from_utc_str(start_time_utc)
                .map_err(|error| {
                    DtError::new(ErrorCode::InvalidConfig)
                        .detail("config [extractor].start_time_utc is invalid")
                        .stage(Stage::Bootstrap)
                        .operation("parse_start_time_filter")
                        .source(error)
                })?
                .timestamp() as u32
        };

        let end_timestamp = if end_time_utc.is_empty() {
            u32::MAX
        } else {
            TimeUtil::datetime_from_utc_str(end_time_utc)
                .map_err(|error| {
                    DtError::new(ErrorCode::InvalidConfig)
                        .detail("config [extractor].end_time_utc is invalid")
                        .stage(Stage::Bootstrap)
                        .operation("parse_end_time_filter")
                        .source(error)
                })?
                .timestamp() as u32
        };

        Ok(Self {
            start_timestamp,
            end_timestamp,
            started: start_time_utc.is_empty(),
            ended: false,
        })
    }
}

impl Default for TimeFilter {
    fn default() -> Self {
        Self {
            start_timestamp: 0,
            end_timestamp: u32::MAX,
            started: true,
            ended: false,
        }
    }
}
