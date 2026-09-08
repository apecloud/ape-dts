use dt_common::config::{connection_auth_config::ConnectionAuthConfig, ssl_config::SslMode};
use dt_common::{log_info, utils::sql_util::SqlUtil, utils::time_util::TimeUtil};
use futures::TryStreamExt;
use mysql_binlog_connector_rust::binlog_client::StartPosition;
use mysql_binlog_connector_rust::{binlog_client::BinlogClient, event::event_data::EventData};
use sqlx::{MySql, Pool};

pub struct BinlogUtil {}

impl BinlogUtil {
    pub fn build_client(
        url: &str,
        auth: &ConnectionAuthConfig,
        server_id: u64,
        position: StartPosition,
    ) -> anyhow::Result<BinlogClient> {
        let url = ConnectionAuthConfig::merge_url_with_auth(url, auth)?;
        let mut url = url::Url::parse(&url)?;
        if let Some(ssl) = auth.ssl_config() {
            if matches!(ssl.ssl_mode, SslMode::VerifyCa | SslMode::VerifyFull) {
                anyhow::bail!(dt_common::error::DtError::invalid_config(
                    "MySQL CDC currently supports only ssl_mode=disable or require; the binlog driver does not support certificate verification"
                ));
            }
            if ssl.ssl_mode != SslMode::Disable
                && (!ssl.ssl_client_cert_path.is_empty() || !ssl.ssl_client_key_path.is_empty())
            {
                anyhow::bail!(dt_common::error::DtError::invalid_config(
                    "MySQL CDC binlog driver does not support ssl_client_cert_path/ssl_client_key_path"
                ));
            }
            let pairs: Vec<_> = url
                .query_pairs()
                .filter(|(k, _)| k != "ssl-mode")
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
            url.query_pairs_mut()
                .clear()
                .extend_pairs(pairs)
                .append_pair(
                    "ssl-mode",
                    if ssl.ssl_mode == SslMode::Disable {
                        "disabled"
                    } else {
                        "required"
                    },
                );
        }
        Ok(BinlogClient::new(url.as_str(), server_id, position))
    }

    pub async fn find_last_binlog_before_timestamp(
        start_timestamp: u32,
        url: &str,
        auth: &ConnectionAuthConfig,
        server_id: u64,
        conn_pool: &Pool<MySql>,
    ) -> anyhow::Result<String> {
        let binlogs = Self::get_binary_logs(conn_pool).await?;
        if binlogs.is_empty() {
            log_info!("no binlogs found");
            return Ok(String::new());
        }

        log_info!(
            "finding the last binlog before start_time: {}",
            TimeUtil::timestamp_to_str(start_timestamp)?
        );

        let mut left = 0;
        let mut right = binlogs.len() - 1;
        while left <= right {
            let mid = left + (right - left) / 2;

            let binlog = &binlogs[mid];
            let binlog_start_timestamp =
                Self::get_binlog_start_timestamp(url, auth, server_id, binlog).await?;

            if binlog_start_timestamp == start_timestamp {
                // found the binlog whose binlog_start_timestamp == start_timestamp, which happens rarely
                log_info!(
                    "found binlog: {}, binlog_start_time: {}",
                    binlog,
                    TimeUtil::timestamp_to_str(binlog_start_timestamp)?
                );
                return Ok(binlog.to_owned());
            } else if binlog_start_timestamp < start_timestamp {
                left = mid + 1;
            } else {
                if mid < 1 {
                    break;
                }
                right = mid - 1;
            }
        }

        // binlogs[left] is the first one whose binlog_start_time > start_time
        if left == 0 {
            // start_time is earlier than binlog_start_time of the first binlog
            let binlog_start_timestamp =
                Self::get_binlog_start_timestamp(url, auth, server_id, &binlogs[0]).await?;
            log_info!(
                "start_time is earlier than the first binlog: {}, binlog_start_time: {}",
                &binlogs[0],
                TimeUtil::timestamp_to_str(binlog_start_timestamp)?
            );
            Ok(binlogs[0].clone())
        } else {
            let binlog = binlogs[left - 1].to_owned();
            let binlog_start_timestamp =
                Self::get_binlog_start_timestamp(url, auth, server_id, &binlog).await?;
            log_info!(
                "found binlog: {}, binlog_start_time: {}",
                binlog,
                TimeUtil::timestamp_to_str(binlog_start_timestamp)?
            );
            Ok(binlog)
        }
    }

    async fn get_binary_logs(conn_pool: &Pool<MySql>) -> anyhow::Result<Vec<String>> {
        let mut binlogs = Vec::new();
        let sql = "SHOW BINARY LOGS";

        let mut rows = sqlx::raw_sql(sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await? {
            let log_name = SqlUtil::try_get_mysql_string(&row, 0)?;
            binlogs.push(log_name)
        }
        Ok(binlogs)
    }

    async fn get_binlog_start_timestamp(
        url: &str,
        auth: &ConnectionAuthConfig,
        server_id: u64,
        binlog: &str,
    ) -> anyhow::Result<u32> {
        let timestamp;
        let mut client = Self::build_client(
            url,
            auth,
            server_id,
            StartPosition::BinlogPosition(binlog.into(), 0),
        )?;
        let mut stream = client.connect().await?;
        loop {
            let (header, data) = stream.read().await?;
            // when binlog_client connected, the first 2 events we get:
            // 1, RotateEvent (with no timestamp in header)
            // 2, FormatDescriptionEvent
            if let EventData::FormatDescription(..) = data {
                timestamp = header.timestamp;
                break;
            }
        }
        stream.close().await?;
        // the timestamp in binlog is since the epoch in UTC, no matter what @@global.time_zone in mysql
        Ok(timestamp)
    }
}

#[cfg(test)]
mod tests {
    use dt_common::config::ssl_config::SslConfig;

    use super::*;

    #[test]
    fn tls_config_override_table() {
        for (mode, cert, expected_mode) in [
            (SslMode::Disable, "", Some("disabled")),
            (SslMode::Require, "", Some("required")),
            (SslMode::VerifyCa, "", None),
            (SslMode::VerifyFull, "", None),
            (SslMode::Require, "client.crt", None),
        ] {
            let auth = ConnectionAuthConfig::BasicSsl {
                username: Some("task_user".into()),
                password: Some("task_password".into()),
                ssl_config: SslConfig {
                    ssl_mode: mode.clone(),
                    ssl_client_cert_path: cert.into(),
                    ..SslConfig::default()
                },
            };
            let result = BinlogUtil::build_client(
                "mysql://url_user:url_password@localhost?ssl-mode=disabled",
                &auth,
                1,
                StartPosition::Latest,
            );
            if let Some(expected) = expected_mode {
                let url = url::Url::parse(&result.unwrap().url).unwrap();
                assert_eq!(url.username(), "task_user");
                assert_eq!(
                    url.query_pairs().find(|(k, _)| k == "ssl-mode").unwrap().1,
                    expected
                );
            } else {
                assert!(result.is_err(), "{mode}/{cert} must not silently downgrade");
            }
        }
    }
}
