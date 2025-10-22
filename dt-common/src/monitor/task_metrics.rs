use serde::Serialize;
use strum::{Display, EnumString, IntoStaticStr};

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
    // task-level general metrics
    Delay,
    Timestamp,
    Progress,
    TotalProgressCount,
    FinishedProgressCount,

    // Extractor raw throughput (pre-filter)
    ExtractorRpsMax,
    ExtractorRpsMin,
    ExtractorRpsAvg,
    ExtractorBpsMax,
    ExtractorBpsMin,
    ExtractorBpsAvg,

    ExtractorPlanRecords,

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

    SinkerSinkedRecords,
    SinkerSinkedBytes,

    SinkerDdlCount,
}
