use std::{
    fmt,
    ops::{Add, AddAssign, Div},
};

use serde::Serialize;
use strum::{Display, EnumString, IntoStaticStr};

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(untagged)]
pub enum TaskMetricValue {
    Integer(u64),
    Float(f64),
}

impl TaskMetricValue {
    pub fn as_u64(self) -> Option<u64> {
        match self {
            Self::Integer(value) => Some(value),
            Self::Float(_) => None,
        }
    }

    pub fn as_f64(self) -> f64 {
        match self {
            Self::Integer(value) => value as f64,
            Self::Float(value) => value,
        }
    }

    pub fn min(self, other: Self) -> Self {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => Self::Integer(a.min(b)),
            (a, b) => Self::Float(a.as_f64().min(b.as_f64())),
        }
    }

    pub fn max(self, other: Self) -> Self {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => Self::Integer(a.max(b)),
            (a, b) => Self::Float(a.as_f64().max(b.as_f64())),
        }
    }
}

impl From<u64> for TaskMetricValue {
    fn from(value: u64) -> Self {
        Self::Integer(value)
    }
}

impl From<f64> for TaskMetricValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl Add for TaskMetricValue {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => Self::Integer(a + b),
            (a, b) => Self::Float(a.as_f64() + b.as_f64()),
        }
    }
}

impl AddAssign for TaskMetricValue {
    fn add_assign(&mut self, other: Self) {
        *self = *self + other;
    }
}

impl Div<u64> for TaskMetricValue {
    type Output = Self;
    fn div(self, count: u64) -> Self {
        match self {
            Self::Integer(value) => Self::Integer(value / count),
            Self::Float(value) => Self::Float(value / count as f64),
        }
    }
}

impl fmt::Display for TaskMetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => value.fmt(f),
            Self::Float(value) => value.fmt(f),
        }
    }
}

#[derive(
    PartialOrd,
    Ord,
    EnumString,
    IntoStaticStr,
    Display,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Copy,
    Debug,
    Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum TaskMetricsType {
    // TODO
    Delay,
    Timestamp,
    Progress,
    TotalProgressCount,
    FinishedProgressCount,
    CheckerMissCount,
    CheckerDiffCount,
    CheckerPending,
    CheckerRpsMax,
    CheckerRpsMin,
    CheckerRpsAvg,
    CheckerMissRpsMax,
    CheckerMissRpsMin,
    CheckerMissRpsAvg,
    CheckerDiffRpsMax,
    CheckerDiffRpsMin,
    CheckerDiffRpsAvg,

    // describe the overall traffic before filtering
    // TODO: some traffic need to be decoded first, e.g., sqlx row data which fields not directly map to dt row data, which need to track the size of tcp stream
    ExtractorRpsMax,
    ExtractorRpsMin,
    ExtractorRpsAvg,
    ExtractorBpsMax,
    ExtractorBpsMin,
    ExtractorBpsAvg,

    ExtractorPlanRecords,

    // describe the overall traffic after filtering
    ExtractorPushedRpsMax,
    ExtractorPushedRpsMin,
    ExtractorPushedRpsAvg,
    ExtractorPushedBpsMax,
    ExtractorPushedBpsMin,
    ExtractorPushedBpsAvg,

    PipelineQueueSize,
    PipelineQueueBytes,

    PipelineRecordSizeMax,

    SinkerRtMax,
    SinkerRtMin,
    SinkerRtAvg,

    SinkerRpsMax,
    SinkerRpsMin,
    SinkerRpsAvg,
    SinkerBpsMax,
    SinkerBpsMin,
    SinkerBpsAvg,

    SinkerWorkersConfigured,
    SinkerWorkersBusy,
    SinkerWorkersPerDrainMax,
    SinkerWorkersPerDrainAvg,

    SinkerSinkedRecords,
    SinkerSinkedBytes,

    SinkerDdlCount,

    PipelineSinkParallelUtilizationAvg,

    PipelineSinkDurationSecondsSum,
    PipelineSinkDurationSecondsAvg,

    PipelineSinkOperationsTotal,

    PartitionerDurationSecondsSum,
    PartitionerDurationSecondsAvg,
}
