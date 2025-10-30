use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use tokio::sync::Mutex;

use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::recovery::Recovery},
    Extractor,
};
use dt_common::{
    log_info, log_warn,
    meta::{avro::avro_converter::AvroConverter, position::Position, syncer::Syncer},
};

pub struct KafkaExtractor {
    pub base_extractor: BaseExtractor,
    pub url: String,
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub ack_interval_secs: u64,
    pub avro_converter: AvroConverter,
    pub syncer: Arc<Mutex<Syncer>>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[async_trait]
impl Extractor for KafkaExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if let Some(recovery) = &self.recovery {
            if let Some(position) = recovery.get_cdc_resume_position().await {
                match &position {
                    Position::Kafka { offset, .. } => {
                        self.offset = offset.to_owned();
                        log_info!("cdc recovery from offset:[{}]", offset);
                    }
                    _ => {
                        log_warn!("position:{} is not a valid kafka position", position);
                    }
                }
            }
        }

        log_info!(
            "KafkaCdcExtractor starts, topic: {}, partition: {}, offset: {}",
            self.topic,
            self.partition,
            self.offset
        );
        let consumer = self.create_consumer();
        self.extract_avro(consumer).await
    }
}

impl KafkaExtractor {
    async fn extract_avro(&mut self, consumer: StreamConsumer) -> anyhow::Result<()> {
        loop {
            let msg = consumer
                .recv()
                .await
                .with_context(|| format!("KafkaCdcExtractor failed, topic: {}", self.topic))?;
            if let Some(payload) = msg.payload() {
                let dt_data = self
                    .avro_converter
                    .avro_value_to_dt_data(payload.to_vec())?;
                let position = Position::Kafka {
                    topic: self.topic.clone(),
                    partition: self.partition,
                    offset: msg.offset(),
                };
                self.base_extractor.push_dt_data(dt_data, position).await?;
            }
        }
    }

    fn create_consumer(&self) -> StreamConsumer {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.url);
        config.set("group.id", &self.group);
        config.set("auto.offset.reset", "latest");
        config.set("session.timeout.ms", "10000");

        let consumer: StreamConsumer = config.create().unwrap();
        // only support extract data from one topic, one partition
        let mut tpl = TopicPartitionList::new();
        if self.offset >= 0 {
            tpl.add_partition_offset(&self.topic, self.partition, Offset::Offset(self.offset))
                .unwrap();
        } else {
            tpl.add_partition(&self.topic, self.partition);
        }
        consumer.assign(&tpl).unwrap();
        consumer
    }
}
