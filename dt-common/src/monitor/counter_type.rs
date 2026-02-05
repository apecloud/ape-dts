use strum::{Display, EnumString, IntoStaticStr};

#[derive(EnumString, IntoStaticStr, Display, PartialEq, Eq, Hash, Clone)]
pub enum CounterType {
    // time window counter, aggregate by: sum, avg/max/min by second
    #[strum(serialize = "batch_write_failures")]
    BatchWriteFailures,
    #[strum(serialize = "serial_writes")]
    SerialWrites,
    #[strum(serialize = "record_count")]
    RecordCount,
    #[strum(serialize = "data_bytes")]
    DataBytes,
    #[strum(serialize = "extracted_records")]
    ExtractedRecords,
    #[strum(serialize = "extracted_bytes")]
    ExtractedBytes,
    #[strum(serialize = "records_per_query")]
    RecordsPerQuery,
    #[strum(serialize = "rt_per_query")]
    RtPerQuery,
    #[strum(serialize = "buffer_size")]
    BufferSize,
    #[strum(serialize = "checker_miss_count")]
    CheckerMissCount,
    #[strum(serialize = "checker_diff_count")]
    CheckerDiffCount,
    #[strum(serialize = "checker_generate_sql_count")]
    CheckerGenerateSqlCount,
    #[strum(serialize = "checker_lag_seconds")]
    CheckerLagSeconds,

    // time window counter, aggregate by: avg by count
    #[strum(serialize = "record_size")]
    RecordSize,

    // no window counter
    #[strum(serialize = "plan_records")]
    PlanRecordTotal,
    #[strum(serialize = "queued_records")]
    QueuedRecordCurrent,
    #[strum(serialize = "queued_bytes")]
    QueuedByteCurrent,
    #[strum(serialize = "checker_pending")]
    CheckerPending,
    #[strum(serialize = "checker_async_drop_count")]
    CheckerAsyncDropCount,

    #[strum(serialize = "sinked_records")]
    SinkedRecordTotal,
    #[strum(serialize = "sinked_bytes")]
    SinkedByteTotal,
    #[strum(serialize = "ddl_records")]
    DDLRecordTotal,
    #[strum(serialize = "timestamp")]
    Timestamp,
}

#[derive(EnumString, IntoStaticStr, Display, PartialEq, Eq, Hash, Clone)]
pub enum AggregateType {
    #[strum(serialize = "latest")]
    Latest,
    #[strum(serialize = "avg_by_sec")]
    AvgBySec,
    #[strum(serialize = "max_by_sec")]
    MaxBySec,
    #[strum(serialize = "min_by_sec")]
    MinBySec,
    #[strum(serialize = "max")]
    MaxByCount,
    #[strum(serialize = "avg")]
    AvgByCount,
    #[strum(serialize = "min")]
    MinByCount,
    #[strum(serialize = "sum")]
    Sum,
    #[strum(serialize = "count")]
    Count,
}

pub enum WindowType {
    NoWindow,
    TimeWindow,
}

impl CounterType {
    pub fn get_window_type(&self) -> WindowType {
        match self {
            Self::BatchWriteFailures
            | Self::SerialWrites
            | Self::RecordCount
            | Self::CheckerMissCount
            | Self::CheckerDiffCount
            | Self::CheckerGenerateSqlCount
            | Self::CheckerLagSeconds
            | Self::RecordsPerQuery
            | Self::RtPerQuery
            | Self::BufferSize
            | Self::DataBytes
            | Self::RecordSize
            | Self::ExtractedRecords
            | Self::ExtractedBytes => WindowType::TimeWindow,

            Self::PlanRecordTotal
            | Self::SinkedRecordTotal
            | Self::SinkedByteTotal
            | Self::QueuedRecordCurrent
            | Self::QueuedByteCurrent
            | Self::CheckerPending
            | Self::CheckerAsyncDropCount
            | Self::DDLRecordTotal
            | Self::Timestamp => WindowType::NoWindow,
        }
    }

    pub fn get_aggregate_types(&self) -> Vec<AggregateType> {
        match self.get_window_type() {
            WindowType::NoWindow => vec![AggregateType::Latest],

            WindowType::TimeWindow => match self {
                Self::RecordsPerQuery | Self::RtPerQuery | Self::BufferSize => {
                    vec![
                        AggregateType::Sum,
                        AggregateType::AvgByCount,
                        AggregateType::MaxByCount,
                        AggregateType::MinByCount,
                    ]
                }

                Self::RecordSize | Self::CheckerLagSeconds => {
                    vec![AggregateType::AvgByCount]
                }

                Self::BatchWriteFailures
                | Self::SerialWrites
                | Self::RecordCount
                | Self::CheckerMissCount
                | Self::CheckerDiffCount
                | Self::CheckerGenerateSqlCount
                | Self::DataBytes
                | Self::ExtractedRecords
                | Self::ExtractedBytes => {
                    vec![
                        AggregateType::Sum,
                        AggregateType::AvgBySec,
                        AggregateType::MaxBySec,
                        AggregateType::MinBySec,
                    ]
                }

                _ => vec![],
            },
        }
    }
}
