use std::str::FromStr;

use crate::error::DtError;

#[derive(Default, PartialEq, Eq)]
pub struct DtNaiveTime {
    pub is_negative: bool,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: u32,
}

impl std::fmt::Display for DtNaiveTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut str = format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second);
        if self.microsecond > 0 {
            str = format!("{}.{:06}", str, self.microsecond);
        }
        if self.is_negative {
            str = format!("-{}", str);
        }
        write!(f, "{}", str)
    }
}

impl FromStr for DtNaiveTime {
    type Err = anyhow::Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let is_negative = str.starts_with('-');
        let time_str = if is_negative { &str[1..] } else { str };

        let mut time = DtNaiveTime {
            is_negative,
            ..Default::default()
        };
        let parse_error =
            || DtError::StatementFailed(format!("failed to parse str: [{}] to DtUtcTime", str));

        let parts: Vec<&str> = time_str.split('.').collect();
        if !parts.is_empty() {
            let part_0: Vec<&str> = parts[0].split(':').collect();
            let hour = part_0[0].parse::<u32>();
            let minute = part_0[1].parse::<u32>();
            let second = part_0[2].parse::<u32>();
            match (hour, minute, second) {
                (Ok(h), Ok(m), Ok(s)) => {
                    time.hour = h;
                    time.minute = m;
                    time.second = s;
                }
                _ => return Err(parse_error().into()),
            }
        }

        if parts.len() > 1 {
            let microsecond = format!("{:0<width$}", parts[1], width = 6);
            match microsecond.parse::<u32>() {
                Ok(ms) => time.microsecond = ms,
                _ => return Err(parse_error().into()),
            }
        }

        Ok(time)
    }
}

impl DtNaiveTime {
    pub fn timestamp_micros(&self) -> i64 {
        // based on 1970-01-01 00:00:00 UTC
        let value = (self.hour as i64 * 3600 + self.minute as i64 * 60 + self.second as i64)
            * 1_000_000
            + self.microsecond as i64;
        if self.is_negative {
            return -value;
        }
        value
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_from_str() {
        let strs = [
            "-838:59:59.000000",
            "-838:59:59.0",
            "-838:59:59",
            "00:00:00.000000",
            "0:0:0.0",
            "0:0:0",
            "01:02:03.01",
            "1:2:3.12345",
            "1:2:3.123456",
            "1:2:3.100000",
            "838:59:59.000000",
            "838:59:59.0",
            "838:59:59",
        ];

        let expected_strs = [
            "-838:59:59",
            "-838:59:59",
            "-838:59:59",
            "00:00:00",
            "00:00:00",
            "00:00:00",
            "01:02:03.010000",
            "01:02:03.123450",
            "01:02:03.123456",
            "01:02:03.100000",
            "838:59:59",
            "838:59:59",
            "838:59:59",
        ];

        for i in 0..strs.len() {
            println!("str: {}", strs[i]);
            let time = DtNaiveTime::from_str(strs[i]).unwrap();
            assert_eq!(time.to_string(), expected_strs[i]);
        }
    }
}
