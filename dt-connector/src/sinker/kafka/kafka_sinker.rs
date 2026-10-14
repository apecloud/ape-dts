use async_trait::async_trait;
use dt_common::router::DtRouter;
use dt_common::{
    config::config_enums::DbType,
    meta::{avro::avro_converter::AvroConverter, ddl_meta::ddl_data::DdlData, row_data::RowData},
    resource::RID,
    rid,
    utils::limit_queue::LimitedQueue,
};
use kafka::producer::{Producer, Record};
use tokio::time::Instant;

use crate::{call_batch_fn, sinker::base_sinker::BaseSinker, Sinker};

pub struct KafkaSinker {
    pub batch_size: usize,
    pub router: DtRouter,
    pub producer: Producer,
    pub avro_converter: AvroConverter,
    pub base_sinker: BaseSinker,
}

#[async_trait]
impl Sinker for KafkaSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        call_batch_fn!(self, data, Self::send_avro);
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        let mut payloads = Vec::new();
        for ddl_data in data {
            let topic: String = self.router.to(ddl_data.get_id::<RID>())?;
            let payload = self.avro_converter.ddl_data_to_avro_value(ddl_data).await?;
            payloads.push((topic, String::new(), payload));
        }
        let messages: Vec<_> = payloads
            .iter()
            .map(|(topic, key, payload)| Record {
                key: key.as_bytes(),
                value: payload.as_slice(),
                topic: topic.as_str(),
                partition: -1,
            })
            .collect();
        self.producer.send_all(&messages)?;
        Ok(())
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> anyhow::Result<()> {
        self.avro_converter.refresh_meta(&data);
        Ok(())
    }
}

impl KafkaSinker {
    async fn send_avro(
        &mut self,
        data: &mut [RowData],
        sinked_count: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let task_id = self
            .base_sinker
            .task_id_for_rows(&data[sinked_count..sinked_count + batch_size]);
        self.base_sinker.ensure_monitor_for(&task_id);
        let mut data_size = 0;

        let mut payloads = Vec::new();
        for row_data in data.iter_mut().skip(sinked_count).take(batch_size) {
            data_size += row_data.get_data_size();
            row_data.convert_raw_string();
            let id = if self.router.route_db_type() == &DbType::Mssql {
                rid!(row_data.db, row_data.schema, row_data.tb)
            } else {
                rid!(row_data.schema, row_data.tb)
            };
            let topic: String = self.router.to(id)?;
            let key = self.avro_converter.row_data_to_avro_key(row_data).await?;
            let payload = self.avro_converter.row_data_to_avro_value(row_data).await?;
            payloads.push((topic, key, payload));
        }

        // TODO: Currently measuring RT for the entire message batch,
        //       as kafka producer involves internal per-broker merging logic,
        //       making it impossible to see individual broker RT. This can be optimized in the future.
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let messages: Vec<_> = payloads
            .iter()
            .map(|(topic, key, payload)| Record {
                key: key.as_bytes(),
                value: payload.as_slice(),
                topic: topic.as_str(),
                partition: -1,
            })
            .collect();
        self.producer.send_all(&messages)?;
        rts.push((
            start_time.elapsed().as_millis() as u64,
            messages.len() as u64,
        ));

        self.base_sinker
            .update_batch_monitor_for(&task_id, batch_size as u64, data_size)
            .await?;
        self.base_sinker.update_monitor_rt_for(&task_id, &rts).await
    }
}
