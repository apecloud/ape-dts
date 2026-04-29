use anyhow::Context;

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
                .with_context(|| {
                    format!("time_filter, invalid start_time_utc: [{}]", start_time_utc)
                })?
                .timestamp() as u32
        };

        let end_timestamp = if end_time_utc.is_empty() {
            u32::MAX
        } else {
            TimeUtil::datetime_from_utc_str(end_time_utc)
                .with_context(|| format!("time_filter, invalid end_time_utc: [{}]", end_time_utc))?
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
