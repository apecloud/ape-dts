use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use anyhow::bail;
use async_trait::async_trait;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{base_pipeline::BasePipeline, Pipeline};
use dt_common::{
    log_position,
    meta::{
        avro::avro_converter::AvroConverter, dt_data::DtData, dt_queue::DtQueue,
        position::Position, syncer::Syncer,
    },
    monitor::{counter_type::CounterType, monitor::Monitor},
};
use dt_parallelizer::base_parallelizer::BaseParallelizer;

type PositionInfo = (Option<Position>, Option<Position>);

#[derive(Clone)]
pub struct HttpServerPipeline {
    pub buffer: Arc<DtQueue>,
    pub syncer: Arc<Mutex<Syncer>>,
    pub monitor: Arc<Monitor>,
    pub avro_converter: AvroConverter,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub http_host: String,
    pub http_port: u64,

    acked_batch_id: Arc<AtomicU64>,
    sent_batch_id: Arc<AtomicU64>,
    pending_ack_data: Arc<async_std::sync::Mutex<HashMap<u64, FetchResp>>>,
    pending_ack_positions: Arc<async_std::sync::Mutex<HashMap<u64, PositionInfo>>>,
}

#[derive(Deserialize)]
struct FetchNewParams {
    batch_size: usize,
    ack_batch_id: Option<u64>,
}

#[derive(Deserialize)]
struct FetchOldParams {
    old_batch_id: u64,
}

#[derive(Deserialize)]
struct AckReq {
    ack_batch_id: u64,
}

#[derive(Serialize)]
struct AckResp {
    acked_batch_id: u64,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct FetchResp {
    data: Vec<Vec<u8>>,
    batch_id: u64,
}

#[derive(Serialize)]
struct InfoResp {
    acked_batch_id: u64,
    sent_batch_id: u64,
}

impl HttpServerPipeline {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        buffer: Arc<DtQueue>,
        syncer: Arc<Mutex<Syncer>>,
        monitor: Arc<Monitor>,
        avro_converter: AvroConverter,
        checkpoint_interval_secs: u64,
        batch_sink_interval_secs: u64,
        http_host: &str,
        http_port: u64,
    ) -> Self {
        Self {
            buffer,
            syncer,
            monitor,
            avro_converter,
            checkpoint_interval_secs,
            batch_sink_interval_secs,
            http_host: http_host.into(),
            http_port,
            acked_batch_id: Default::default(),
            sent_batch_id: Default::default(),
            pending_ack_data: Default::default(),
            pending_ack_positions: Default::default(),
        }
    }
}

#[async_trait]
impl Pipeline for HttpServerPipeline {
    async fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn start(&mut self) -> anyhow::Result<()> {
        let app_data = self.clone();
        block_on(
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(app_data.clone()))
                    .service(web::resource("/info").route(web::get().to(info)))
                    .service(web::resource("/fetch_new").route(web::get().to(fetch_new)))
                    .service(web::resource("/fetch_old").route(web::get().to(fetch_old)))
                    .service(web::resource("/ack").route(web::post().to(ack)))
            })
            .bind(format!("{}:{}", self.http_host, self.http_port))
            .unwrap()
            .run(),
        )
        .unwrap();
        Ok(())
    }
}

async fn info(pipeline: web::Data<HttpServerPipeline>) -> impl Responder {
    send_response(&InfoResp {
        acked_batch_id: pipeline.acked_batch_id.load(Ordering::Acquire),
        sent_batch_id: pipeline.sent_batch_id.load(Ordering::Acquire),
    })
}

async fn fetch_new(
    query: web::Query<FetchNewParams>,
    pipeline: web::Data<HttpServerPipeline>,
) -> impl Responder {
    if let Some(ack_batch_id) = query.ack_batch_id {
        if let Err(err) = do_ack(ack_batch_id, &pipeline).await {
            return HttpResponse::BadRequest().body(err.to_string());
        }
    }

    let mut pending_ack_data = pipeline.pending_ack_data.lock().await;
    let mut pending_ack_positions = pipeline.pending_ack_positions.lock().await;
    let sent_batch_id = pipeline.sent_batch_id.load(Ordering::Acquire);

    // get data from buffer
    let mut parallelizer = BaseParallelizer {
        monitor: pipeline.monitor.clone(),
        ..Default::default()
    };
    let data = parallelizer
        .drain_by_count(&pipeline.buffer, query.batch_size)
        .await
        .unwrap();
    let (_, last_received_position, last_commit_position) = BasePipeline::fetch_raw(&data);

    // data -> avro response
    let mut response = FetchResp {
        batch_id: sent_batch_id + 1,
        data: Vec::new(),
    };

    let mut avro_converter = pipeline.avro_converter.clone();
    for i in data {
        match i.dt_data {
            DtData::Dml { row_data } => {
                let payload = avro_converter
                    .row_data_to_avro_value(row_data)
                    .await
                    .unwrap();
                response.data.push(payload);
            }

            DtData::Ddl { ddl_data } => {
                let payload = avro_converter
                    .ddl_data_to_avro_value(ddl_data)
                    .await
                    .unwrap();
                response.data.push(payload);
            }

            _ => {}
        }
    }

    // update monitor
    pipeline
        .monitor
        .add_counter(CounterType::BufferSize, pipeline.buffer.len() as u64)
        .await
        .add_counter(CounterType::SinkedRecordTotal, response.data.len() as u64)
        .await;

    // update pending_ack_data & pending_ack_positions
    let batch_id = response.batch_id;
    pipeline.sent_batch_id.store(batch_id, Ordering::Release);
    if !response.data.is_empty() {
        pending_ack_data.insert(batch_id, response);
        pending_ack_positions.insert(batch_id, (last_received_position, last_commit_position));
        send_response(pending_ack_data.get(&batch_id).unwrap())
    } else {
        send_response(&response)
    }
}

async fn fetch_old(
    query: web::Query<FetchOldParams>,
    pipeline: web::Data<HttpServerPipeline>,
) -> impl Responder {
    let acked_batch_id = pipeline.acked_batch_id.load(Ordering::Acquire);
    let sent_batch_id = pipeline.sent_batch_id.load(Ordering::Acquire);
    let old_batch_id = query.old_batch_id;

    if old_batch_id > sent_batch_id {
        return HttpResponse::BadRequest().body(format!(
            "old_batch_id: [{}] must <= sent_batch_id: [{}]",
            old_batch_id, sent_batch_id
        ));
    }

    if old_batch_id <= acked_batch_id {
        return HttpResponse::BadRequest().body(format!(
            "old_batch_id: [{}] must > acked_batch_id: [{}]",
            old_batch_id, acked_batch_id
        ));
    }

    if let Some(response) = pipeline.pending_ack_data.lock().await.get(&old_batch_id) {
        send_response(response)
    } else {
        // should never happen
        send_response(&FetchResp::default())
    }
}

async fn ack(data: web::Json<AckReq>, pipeline: web::Data<HttpServerPipeline>) -> impl Responder {
    if let Err(err) = do_ack(data.ack_batch_id, &pipeline).await {
        return HttpResponse::BadRequest().body(err.to_string());
    }
    send_response(&AckResp {
        acked_batch_id: pipeline.acked_batch_id.load(Ordering::Acquire),
    })
}

fn send_response<T: Serialize>(response: &T) -> HttpResponse {
    match serde_json::to_string(response) {
        Ok(json) => HttpResponse::Ok()
            .content_type("application/json")
            .body(json),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

async fn do_ack(ack_batch_id: u64, pipeline: &web::Data<HttpServerPipeline>) -> anyhow::Result<()> {
    let acked_batch_id = pipeline.acked_batch_id.load(Ordering::Acquire);
    let sent_batch_id = pipeline.sent_batch_id.load(Ordering::Acquire);

    if ack_batch_id > sent_batch_id {
        bail!(format!(
            "ack_batch_id: [{}] must <= sent_batch_id: [{}]",
            ack_batch_id, sent_batch_id
        ));
    }

    if ack_batch_id < acked_batch_id {
        bail!(format!(
            "ack_batch_id: [{}] must >= acked_batch_id : [{}]",
            ack_batch_id, acked_batch_id
        ));
    }

    let mut pending_ack_data = pipeline.pending_ack_data.lock().await;
    refresh_appending_ack_data(&mut pending_ack_data, ack_batch_id);

    let mut pending_ack_positions = pipeline.pending_ack_positions.lock().await;
    let max_acked_position_info =
        refresh_appending_ack_positions(&mut pending_ack_positions, ack_batch_id);

    record_checkpoint(max_acked_position_info);
    pipeline
        .acked_batch_id
        .store(ack_batch_id, Ordering::Release);
    Ok(())
}

fn refresh_appending_ack_data(
    pending_ack_data: &mut async_std::sync::MutexGuard<'_, HashMap<u64, FetchResp>>,
    ack_batch_id: u64,
) {
    pending_ack_data.retain(|&batch_id, _| batch_id > ack_batch_id);
}

fn refresh_appending_ack_positions(
    pending_ack_positions: &mut async_std::sync::MutexGuard<'_, HashMap<u64, PositionInfo>>,
    ack_batch_id: u64,
) -> PositionInfo {
    let mut max_acked_batch_id = 0;
    let mut max_acked_position_info = (None, None);
    for (batch_id, position_info) in pending_ack_positions.iter() {
        if *batch_id <= ack_batch_id && *batch_id >= max_acked_batch_id {
            max_acked_batch_id = *batch_id;
            if let Some(last_received_position) = &position_info.0 {
                max_acked_position_info.0 = Some(last_received_position.to_owned());
            }
            if let Some(last_commit_position) = &position_info.1 {
                max_acked_position_info.1 = Some(last_commit_position.to_owned());
            }
        }
    }
    pending_ack_positions.retain(|&batch_id, _| batch_id > ack_batch_id);
    max_acked_position_info
}

fn record_checkpoint(position_info: PositionInfo) {
    if let Some(current_position) = position_info.0 {
        log_position!("current_position | {}", current_position.to_string());
    }
    if let Some(checkpoint_position) = position_info.1 {
        log_position!("checkpoint_position | {}", checkpoint_position.to_string());
    }
}
