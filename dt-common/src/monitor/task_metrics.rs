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
    // TODO
    Delay,
    Timestamp,
    Progress,
    TotalProgressCount,
    FinishedProgressCount,

    // describe the overall traffic before filtering
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

    SinkerSinkedRecords,
    SinkerSinkedBytes,

    SinkerDdlCount,
}
