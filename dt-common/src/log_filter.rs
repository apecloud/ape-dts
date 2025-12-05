use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

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
            let (number, unit) = match v.find(|c: char| !c.is_ascii_digit()) {
                Some(n) => (v[..n].trim(), Some(v[n..].trim())),
                None => (v.trim(), None),
            };

            let number = match number.parse::<u64>() {
                Ok(n) => n,
                Err(_) => {
                    return Err(E::invalid_value(
                        serde::de::Unexpected::Str(number),
                        &"a number",
                    ))
                }
            };

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
                return Err(E::invalid_value(
                    serde::de::Unexpected::Str(unit),
                    &"a valid unit",
                ));
            };

            match number {
                Some(n) => Ok(n),
                None => Err(E::invalid_value(
                    serde::de::Unexpected::Str(v),
                    &"a byte size",
                )),
            }
        }
    }

    d.deserialize_any(V)
}
