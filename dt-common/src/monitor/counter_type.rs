use strum::{Display, EnumString, IntoStaticStr};

use super::task_metrics::{TaskMetricValue, TaskMetricsType};

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
    #[strum(serialize = "sinker_workers_per_drain")]
    SinkerWorkersPerDrain,
    #[strum(serialize = "checker_miss_count")]
    CheckerMissCount,
    #[strum(serialize = "checker_diff_count")]
    CheckerDiffCount,
    // time window counter, aggregate by: avg by count
    #[strum(serialize = "record_size")]
    RecordSize,

    // no window counter
    #[strum(serialize = "pipeline_sink_parallel_utilization")]
    PipelineSinkParallelUtilization,
    #[strum(serialize = "pipeline_sink_duration_seconds")]
    PipelineSinkDurationSeconds,
    #[strum(serialize = "partitioner_duration_seconds")]
    PartitionerDurationSeconds,
    #[strum(serialize = "pipeline_sink_operations_total")]
    PipelineSinkOperationsTotal,
    #[strum(serialize = "plan_records")]
    PlanRecordTotal,
    #[strum(serialize = "queued_records")]
    QueuedRecordCurrent,
    #[strum(serialize = "queued_bytes")]
    QueuedByteCurrent,
    #[strum(serialize = "checker_pending")]
    CheckerPending,
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
    pub fn initial_value(&self) -> TaskMetricValue {
        match self {
            Self::PipelineSinkParallelUtilization
            | Self::PipelineSinkDurationSeconds
            | Self::PartitionerDurationSeconds => TaskMetricValue::Float(0.0),
            _ => TaskMetricValue::Integer(0),
        }
    }

    pub(crate) fn task_metrics(&self) -> Vec<(AggregateType, TaskMetricsType)> {
        use AggregateType::*;
        use TaskMetricsType::*;
        match self {
            Self::PipelineSinkOperationsTotal => vec![(Latest, PipelineSinkOperationsTotal)],
            Self::PipelineSinkParallelUtilization => vec![
                (AvgByCount, PipelineSinkParallelUtilizationAvg),
                (MinByCount, PipelineSinkParallelUtilizationMin),
                (MaxByCount, PipelineSinkParallelUtilizationMax),
            ],
            Self::PipelineSinkDurationSeconds => vec![
                (Sum, PipelineSinkDurationSecondsSum),
                (AvgByCount, PipelineSinkDurationSecondsAvg),
                (MinByCount, PipelineSinkDurationSecondsMin),
                (MaxByCount, PipelineSinkDurationSecondsMax),
            ],
            Self::PartitionerDurationSeconds => vec![
                (Sum, PartitionerDurationSecondsSum),
                (AvgByCount, PartitionerDurationSecondsAvg),
                (MinByCount, PartitionerDurationSecondsMin),
                (MaxByCount, PartitionerDurationSecondsMax),
            ],
            _ => vec![],
        }
    }

    pub fn get_window_type(&self) -> WindowType {
        match self {
            Self::BatchWriteFailures
            | Self::SerialWrites
            | Self::RecordCount
            | Self::CheckerMissCount
            | Self::CheckerDiffCount
            | Self::RecordsPerQuery
            | Self::RtPerQuery
            | Self::BufferSize
            | Self::SinkerWorkersPerDrain
            | Self::DataBytes
            | Self::RecordSize
            | Self::ExtractedRecords
            | Self::ExtractedBytes => WindowType::TimeWindow,

            Self::PipelineSinkParallelUtilization
            | Self::PipelineSinkDurationSeconds
            | Self::PartitionerDurationSeconds
            | Self::PipelineSinkOperationsTotal
            | Self::PlanRecordTotal
            | Self::SinkedRecordTotal
            | Self::SinkedByteTotal
            | Self::QueuedRecordCurrent
            | Self::QueuedByteCurrent
            | Self::CheckerPending
            | Self::DDLRecordTotal
            | Self::Timestamp => WindowType::NoWindow,
        }
    }

    pub fn get_aggregate_types(&self) -> Vec<AggregateType> {
        match self.get_window_type() {
            WindowType::NoWindow => match self {
                Self::PipelineSinkParallelUtilization => vec![
                    AggregateType::AvgByCount,
                    AggregateType::MinByCount,
                    AggregateType::MaxByCount,
                ],
                Self::PipelineSinkDurationSeconds | Self::PartitionerDurationSeconds => vec![
                    AggregateType::Sum,
                    AggregateType::AvgByCount,
                    AggregateType::MinByCount,
                    AggregateType::MaxByCount,
                ],
                _ => vec![AggregateType::Latest],
            },

            WindowType::TimeWindow => match self {
                Self::RecordsPerQuery
                | Self::RtPerQuery
                | Self::BufferSize
                | Self::SinkerWorkersPerDrain => {
                    vec![
                        AggregateType::Sum,
                        AggregateType::AvgByCount,
                        AggregateType::MaxByCount,
                        AggregateType::MinByCount,
                    ]
                }

                Self::RecordSize => {
                    vec![AggregateType::AvgByCount]
                }

                Self::BatchWriteFailures
                | Self::SerialWrites
                | Self::RecordCount
                | Self::CheckerMissCount
                | Self::CheckerDiffCount
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
