use dt_common::{log_info, utils::time_util::TimeUtil};
use futures::TryStreamExt;
use mysql_binlog_connector_rust::{binlog_client::BinlogClient, event::event_data::EventData};
use sqlx::{MySql, Pool, Row};

pub struct BinlogUtil {}

impl BinlogUtil {
    pub async fn find_last_binlog_before_timestamp(
        start_timestamp: u32,
        url: &str,
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
                Self::get_binlog_start_timestamp(url, server_id, binlog).await?;

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
                Self::get_binlog_start_timestamp(url, server_id, &binlogs[0]).await?;
            log_info!(
                "start_time is earlier than the first binlog: {}, binlog_start_time: {}",
                &binlogs[0],
                TimeUtil::timestamp_to_str(binlog_start_timestamp)?
            );
            Ok(binlogs[0].clone())
        } else {
            let binlog = binlogs[left - 1].to_owned();
            let binlog_start_timestamp =
                Self::get_binlog_start_timestamp(url, server_id, &binlog).await?;
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

        let mut rows = sqlx::query(sql).fetch(conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let log_name: String = row.try_get(0)?;
            binlogs.push(log_name)
        }
        Ok(binlogs)
    }

    async fn get_binlog_start_timestamp(
        url: &str,
        server_id: u64,
        binlog: &str,
    ) -> anyhow::Result<u32> {
        let timestamp;
        let mut client = BinlogClient {
            url: url.into(),
            binlog_filename: binlog.into(),
            binlog_position: 0,
            server_id,
            ..Default::default()
        };
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
