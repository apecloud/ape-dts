use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use anyhow::anyhow;
use log::Record;
use log4rs::{
    config::{Deserialize, Deserializers},
    filter::{Filter, Response},
};
use serde::Deserialize as SerdeDeserialize;

#[derive(Debug)]
pub struct SizeLimitFilter {
    limit: u64,
    written: AtomicU64,
}

// if the file size is bigger then limit(no such accuracy), the filter will reject all logs after that
// this filter read the file meta once, and record the written size in memory
impl SizeLimitFilter {
    pub fn new(path: impl AsRef<Path>, limit: u64) -> Self {
        let initial_size = fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
        Self {
            limit,
            written: AtomicU64::new(initial_size),
        }
    }
}

impl Filter for SizeLimitFilter {
    fn filter(&self, record: &Record) -> Response {
        let msg_len = record.args().to_string().len() as u64;

        if self
            .written
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                let updated = current.saturating_add(msg_len);
                (updated < self.limit).then_some(updated)
            })
            .is_ok()
        {
            Response::Neutral
        } else {
            Response::Reject
        }
    }
}

#[derive(Debug)]
pub struct RowLimitFilter {
    limit: usize,
    written: AtomicUsize,
}

impl RowLimitFilter {
    pub fn new(path: impl AsRef<Path>, limit: usize) -> Self {
        let written = File::open(path)
            .map(|file| BufReader::new(file).lines().count())
            .unwrap_or(0);
        Self {
            limit,
            written: AtomicUsize::new(written),
        }
    }
}

impl Filter for RowLimitFilter {
    fn filter(&self, _record: &Record) -> Response {
        if self
            .written
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current < self.limit).then_some(current + 1)
            })
            .is_ok()
        {
            Response::Neutral
        } else {
            Response::Reject
        }
    }
}

#[derive(Clone, Debug, SerdeDeserialize)]
pub struct RowLimitFilterConfig {
    pub path: String,
    pub limit: usize,
}

#[derive(Clone, Debug, Default)]
pub struct RowLimitFilterDeserializer;

impl Deserialize for RowLimitFilterDeserializer {
    type Trait = dyn Filter;
    type Config = RowLimitFilterConfig;

    fn deserialize(
        &self,
        config: RowLimitFilterConfig,
        _deserializers: &Deserializers,
    ) -> anyhow::Result<Box<dyn Filter>> {
        Ok(Box::new(RowLimitFilter::new(config.path, config.limit)))
    }
}

#[derive(Clone, Debug, SerdeDeserialize)]
pub struct SizeLimitFilterConfig {
    pub path: String,
    #[serde(deserialize_with = "deserialize_limit")]
    pub limit: u64,
}

#[derive(Clone, Debug, Default)]
pub struct SizeLimitFilterDeserializer;

impl Deserialize for SizeLimitFilterDeserializer {
    type Trait = dyn Filter;
    type Config = SizeLimitFilterConfig;

    fn deserialize(
        &self,
        config: SizeLimitFilterConfig,
        _deserializers: &Deserializers,
    ) -> anyhow::Result<Box<dyn Filter>> {
        Ok(Box::new(SizeLimitFilter::new(config.path, config.limit)))
    }
}

fn parse_size_limit_bytes(v: &str) -> Result<u64, String> {
    let (number, unit) = match v.find(|c: char| !c.is_ascii_digit()) {
        Some(n) => (v[..n].trim(), Some(v[n..].trim())),
        None => (v.trim(), None),
    };

    let number = number
        .parse::<u64>()
        .map_err(|_| format!("invalid size number: {number}"))?;

    let unit = match unit {
        Some(u) => u,
        None => return Ok(number),
    };

    let number = if unit.eq_ignore_ascii_case("b") {
        Some(number)
    } else if unit.eq_ignore_ascii_case("kb") || unit.eq_ignore_ascii_case("kib") {
        number.checked_mul(1024)
    } else if unit.eq_ignore_ascii_case("mb") || unit.eq_ignore_ascii_case("mib") {
        number.checked_mul(1024 * 1024)
    } else if unit.eq_ignore_ascii_case("gb") || unit.eq_ignore_ascii_case("gib") {
        number.checked_mul(1024 * 1024 * 1024)
    } else if unit.eq_ignore_ascii_case("tb") || unit.eq_ignore_ascii_case("tib") {
        number.checked_mul(1024 * 1024 * 1024 * 1024)
    } else {
        return Err(format!("invalid size unit: {unit}"));
    };

    number.ok_or_else(|| format!("size overflow: {v}"))
}

pub fn parse_size_limit(v: &str) -> anyhow::Result<u64> {
    parse_size_limit_bytes(v).map_err(|e| anyhow!(e))
}

fn deserialize_limit<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;

    impl serde::de::Visitor<'_> for V {
        type Value = u64;

        fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            fmt.write_str("a size")
        }

        fn visit_u64<E>(self, v: u64) -> Result<u64, E>
        where
            E: serde::de::Error,
        {
            Ok(v)
        }

        fn visit_i64<E>(self, v: i64) -> Result<u64, E>
        where
            E: serde::de::Error,
        {
            if v < 0 {
                return Err(E::invalid_value(
                    serde::de::Unexpected::Signed(v),
                    &"a non-negative number",
                ));
            }

            Ok(v as u64)
        }

        fn visit_str<E>(self, v: &str) -> Result<u64, E>
        where
            E: serde::de::Error,
        {
            parse_size_limit_bytes(v)
                .map_err(|_| E::invalid_value(serde::de::Unexpected::Str(v), &"a byte size"))
        }
    }

    d.deserialize_any(V)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use log::{Level, Record};
    use log4rs::filter::{Filter, Response};

    use super::RowLimitFilter;

    #[test]
    fn row_limit_filter_rejects_records_after_limit() {
        let filter = RowLimitFilter::new("/path/that/does/not/exist", 2);
        let record = Record::builder()
            .args(format_args!("test"))
            .level(Level::Info)
            .build();

        assert!(matches!(filter.filter(&record), Response::Neutral));
        assert!(matches!(filter.filter(&record), Response::Neutral));
        assert!(matches!(filter.filter(&record), Response::Reject));
    }

    #[test]
    fn row_limit_filter_counts_existing_newline_terminated_rows() {
        let path = PathBuf::from(format!(
            "/tmp/ape-dts-row-limit-filter-{}.log",
            std::process::id()
        ));
        fs::write(&path, "first\nsecond\n").unwrap();
        let filter = RowLimitFilter::new(&path, 3);
        fs::remove_file(path).unwrap();
        let record = Record::builder()
            .args(format_args!("third"))
            .level(Level::Info)
            .build();

        assert!(matches!(filter.filter(&record), Response::Neutral));
        assert!(matches!(filter.filter(&record), Response::Reject));
    }
}
