use anyhow::{bail, Context};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use tokio::{time::sleep, time::Duration};

use crate::error::{DtError, ErrorCode};

pub struct TimeUtil {}

const UTC_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%z";

impl TimeUtil {
    #[inline(always)]
    pub async fn sleep_millis(millis: u64) {
        sleep(Duration::from_millis(millis)).await;
    }

    #[inline(always)]
    pub fn date_from_str(str: &str) -> anyhow::Result<NaiveDate> {
        let date = NaiveDate::parse_from_str(str, "%Y-%m-%d")
            .with_context(|| format!("date_from_utc_str failed, input: [{}]", str))?;
        Ok(date)
    }

    #[inline(always)]
    pub fn datetime_from_utc_str(str: &str) -> anyhow::Result<DateTime<Utc>> {
        if let Ok(dt) = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f") {
            Ok(dt.and_utc())
        } else {
            let dt = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f%#z")
                .with_context(|| format!("datetime_from_utc_str failed, input: [{}]", str))?;
            Ok(dt.and_utc())
        }
    }

    #[inline(always)]
    pub fn timestamp_to_str(timestamp: u32) -> anyhow::Result<String> {
        if let Some(datetime) = DateTime::from_timestamp(timestamp as i64, 0) {
            Ok(datetime.format(UTC_FORMAT).to_string())
        } else {
            bail!(DtError::new(ErrorCode::InvalidConfig)
                .detail(format!(
                    "timestamp is outside the supported range: {timestamp}"
                ))
                .operation("format_timestamp"))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_datetime_from_utc_str() {
        let inputs = [
            "2024-04-17 12:34:56",
            "2024-05-28 01:12:13.123456",
            "2016-11-04 06:51:30.123456+00",
            "2016-11-04 06:51:30.123456+0000",
        ];
        let check_values = [
            "2024-04-17T12:34:56+0000",
            "2024-05-28T01:12:13.123456+0000",
            "2016-11-04T06:51:30.123456+0000",
            "2016-11-04T06:51:30.123456+0000",
        ];

        let datetime = TimeUtil::datetime_from_utc_str(inputs[0]).unwrap();
        assert_eq!(
            datetime.format(UTC_FORMAT).to_string(),
            check_values[0].to_owned()
        );

        for i in 1..inputs.len() {
            let datetime = TimeUtil::datetime_from_utc_str(inputs[i]).unwrap();
            assert_eq!(
                datetime.format("%Y-%m-%dT%H:%M:%S%.f%z").to_string(),
                check_values[i].to_owned()
            )
        }
    }
}
