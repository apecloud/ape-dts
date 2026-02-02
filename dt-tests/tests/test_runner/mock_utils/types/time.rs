use fake::{Fake, Faker};
use time::{Date, Duration, PrimitiveDateTime, Time};

use crate::test_runner::mock_utils::constants::ConstantValues;
use crate::test_runner::mock_utils::random::{Random, RandomValue};

// =============================================================================
// Date
// =============================================================================

/// PostgreSQL date: calendar date (year, month, day)
/// Format: "YYYY-MM-DD"
pub struct PgDate(pub Date);

impl PgDate {
    pub fn new(date: Date) -> Self {
        Self(date)
    }
}

impl std::fmt::Display for PgDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl RandomValue for PgDate {
    fn next_value(_random: &mut Random) -> String {
        let d: Date = Faker.fake();
        PgDate::new(d).to_string()
    }
}

impl ConstantValues for PgDate {
    fn next_values() -> Vec<String> {
        [
            // --- 1. Standard Boundaries ---
            r#"1970-01-01"#, // Unix Epoch Start
            r#"2000-01-01"#, // Y2K Boundary
            r#"2038-01-19"#, // 32-bit Unix Time Overflow (Relevant for older clients)
            // --- 2. Leap Year Logic ---
            r#"2024-02-29"#, // Standard Leap Year
            r#"2000-02-29"#, // Century Leap Year (Divisible by 400)
            // Note: 1900-02-29 would be invalid (Divisible by 100 but not 400)
            // --- 3. Postgres Special Constants ---
            // These are distinct from NULL. They represent mathematical infinity.
            r#"infinity"#,  // Later than any other date
            r#"-infinity"#, // Earlier than any other date
            r#"today"#,     // Dynamic value (evaluated at insertion time)
            // --- 4. Historical & Far Future ---
            r#"4713-01-01 BC"#, // Julian Date 0 (Minimum valid date in many contexts)
            r#"5874897-12-31"#, // Near the maximum allowed date in PG
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

// =============================================================================
// Time
// =============================================================================

/// PostgreSQL time: time of day (no date)
/// Format: "HH:MM:SS" or "HH:MM:SS.UUUUUU"
pub struct PgTime(pub Time);

impl PgTime {
    pub fn new(time: Time) -> Self {
        Self(time)
    }
}

impl std::fmt::Display for PgTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl RandomValue for PgTime {
    fn next_value(_random: &mut Random) -> String {
        let t: Time = Faker.fake();
        PgTime::new(t).to_string()
    }
}

impl ConstantValues for PgTime {
    fn next_values() -> Vec<String> {
        [
            // --- 1. Day Boundaries ---
            r#"00:00:00"#, // Midnight (Start of day)
            r#"12:00:00"#, // Noon
            r#"23:59:59"#, // Last second of the day
            // --- 2. Postgres Special Extensions ---
            r#"24:00:00"#, // Represents midnight of the NEXT day (Valid in PG)
            r#"allballs"#, // Slang for 00:00:00.00 (Legacy compatibility)
            // --- 3. Precision (Microseconds) ---
            // Postgres stores up to 6 decimal places for seconds.
            r#"12:34:56.123456"#, // Max precision
            r#"12:34:56.999999"#, // Rollover edge case
            // --- 4. Timezone Offset (TIMETZ) ---
            // Though 'time with time zone' is discouraged by the SQL standard, PG supports it.
            r#"04:05:06-08:00"#, // Time with fixed offset
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

// =============================================================================
// DateTime (Timestamp)
// =============================================================================

/// PostgreSQL timestamp: date and time (no timezone)
/// Format: "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD HH:MM:SS.UUUUUU"
pub struct PgDateTime(pub PrimitiveDateTime);

impl PgDateTime {
    pub fn new(datetime: PrimitiveDateTime) -> Self {
        Self(datetime)
    }
}

impl std::fmt::Display for PgDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl RandomValue for PgDateTime {
    fn next_value(_random: &mut Random) -> String {
        let dt: PrimitiveDateTime = Faker.fake();
        PgDateTime::new(dt).to_string()
    }
}

impl ConstantValues for PgDateTime {
    fn next_values() -> Vec<String> {
        [
            // --- 1. ISO 8601 Standards ---
            r#"2024-01-01 12:00:00"#,    // Local time (No timezone info)
            r#"2024-01-01T12:00:00Z"#,   // UTC / Zulu time (Standard for data exchange)
            r#"2024-01-01 12:00:00+08"#, // Specific Offset (e.g., Asia/Shanghai)
            // --- 2. Special Constants ---
            r#"infinity"#,  // Future infinity
            r#"-infinity"#, // Past infinity
            r#"now"#,       // Server current time (Evaluated at transaction time)
            r#"epoch"#,     // 1970-01-01 00:00:00 UTC
            // --- 3. Extreme Values ---
            r#"294276-01-01 00:00:00"#, // Far future year (Max supported year is ~294276)
            r#"1999-12-31 23:59:59.999999"#, // Microsecond precision boundary
            // --- 4. Compact Formats ---
            r#"20240101 120000"#, // Compact string (Valid in PG)
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

// =============================================================================
// Interval
// =============================================================================
pub struct Interval(pub Duration);

impl Interval {
    pub fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let d = self.0;
        let sign = if d.is_negative() { "-" } else { "" };
        let abs_d = d.abs();

        let days = abs_d.whole_days();
        let hours = abs_d.whole_hours() % 24;
        let minutes = abs_d.whole_minutes() % 60;
        let seconds = abs_d.whole_seconds() % 60;
        let micros = abs_d.subsec_microseconds();

        if micros == 0 {
            write!(
                f,
                "{}{} days {:02}:{:02}:{:02}",
                sign, days, hours, minutes, seconds
            )
        } else {
            write!(
                f,
                "{}{} days {:02}:{:02}:{:02}.{:06}",
                sign, days, hours, minutes, seconds, micros
            )
        }
    }
}

impl RandomValue for Interval {
    fn next_value(_random: &mut Random) -> String {
        let d: Duration = Faker.fake();
        Interval::new(d).to_string()
    }
}

impl ConstantValues for Interval {
    fn next_values() -> Vec<String> {
        [
            // --- 1. Standard Units ---
            r#"1 year"#,
            r#"1 month"#,
            r#"1 day"#,
            r#"0"#, // Zero interval
            // --- 2. Verbose / Mixed Format ---
            r#"1 year 2 months 3 days"#,
            r#"1 day 2 hours 30 minutes 5 seconds"#,
            r#"@ 1 year 2 mons"#, // Alternative syntax starting with '@'
            // --- 3. ISO 8601 Duration Format ---
            // P=Period, Y=Year, M=Month, D=Day, T=Time separator
            r#"P1Y2M3DT4H5M6S"#, // Full specifier
            r#"PT1M"#,           // 1 Minute only
            // --- 4. Negative & Arithmetic ---
            r#"-1 hour"#,          // Negative duration
            r#"-1 days +2 hours"#, // Mixed signs (Calculates to -22 hours)
            // --- 5. Magnitude ---
            r#"1000000 years"#, // Large duration
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_next_values() {
        let mut random = Random::new(None);

        for _ in 0..3 {
            println!("Date: {}", PgDate::next_value(&mut random));
        }
        for _ in 0..3 {
            println!("Time: {}", PgTime::next_value(&mut random));
        }
        for _ in 0..3 {
            println!("DateTime: {}", PgDateTime::next_value(&mut random));
        }
        for _ in 0..3 {
            println!("Interval: {}", Interval::next_value(&mut random));
        }
    }

    #[test]
    fn test_all_constant_values() {
        println!("Date constants: {:?}", PgDate::next_values());
        println!("Time constants: {:?}", PgTime::next_values());
        println!("DateTime constants: {:?}", PgDateTime::next_values());
        println!("Interval constants: {:?}", Interval::next_values());
    }

    #[test]
    fn test_interval_display() {
        // Positive interval
        let d = Duration::new(90061, 123456000); // 1 day 1 hour 1 min 1 sec 123456 micros
        let interval = Interval::new(d);
        println!("Interval: {}", interval);

        // Zero interval
        let zero = Interval::new(Duration::ZERO);
        assert_eq!(zero.to_string(), "0 days 00:00:00");

        // Negative interval
        let neg = Interval::new(-Duration::hours(25));
        println!("Negative interval: {}", neg);
        assert!(neg.to_string().starts_with('-'));
    }
}
