use std::{sync::Arc, time::Duration, time::Instant};

use concurrent_queue::PopError;

use dt_common::{
    meta::{
        dt_data::{DtData, DtItem},
        position::Position,
    },
    queue::{
        basic_queue::BasicQueue,
        dependency_queue::{DependencyInput, DependencyKey, DependencyQueue, DependencySpec},
        DtQueuePopError,
    },
};

const ITEMS: usize = 200_000;
const BATCH_SIZE: usize = 512;

fn item() -> DtItem {
    DtItem {
        dt_data: DtData::Heartbeat {},
        position: Position::None,
        data_origin_node: String::new(),
    }
}

fn report(name: &str, elapsed: std::time::Duration) {
    let throughput = ITEMS as f64 / elapsed.as_secs_f64();
    println!("{name:32} {elapsed:?}, {throughput:>12.0} items/s");
}

async fn drain_dependency(queue: &DependencyQueue<DtItem>) {
    let mut drained = 0;
    while drained < ITEMS {
        let nodes = queue.pop_ready_batch(BATCH_SIZE).await.unwrap().unwrap();
        drained += nodes.len();
        queue
            .ack_batch(nodes.into_iter().map(|node| node.id))
            .await
            .unwrap();
    }
}

async fn dependency_batched(keys: Option<&[DependencyKey]>) -> std::time::Duration {
    let queue = DependencyQueue::new(ITEMS);
    let started = Instant::now();
    for offset in (0..ITEMS).step_by(BATCH_SIZE) {
        let end = (offset + BATCH_SIZE).min(ITEMS);
        let inputs = (offset..end)
            .map(|index| {
                let ordered_by = keys
                    .map(|keys| vec![keys[index % keys.len()].clone()])
                    .unwrap_or_default();
                DependencyInput::new(
                    item(),
                    DependencySpec {
                        ordered_by,
                        ..Default::default()
                    },
                )
            })
            .collect();
        queue.push_batch(inputs).await.unwrap();
    }
    drain_dependency(&queue).await;
    started.elapsed()
}

async fn basic_concurrent() -> std::time::Duration {
    let queue = Arc::new(BasicQueue::new(4096, 0, None, None));
    let producer_queue = queue.clone();
    let consumer_queue = queue.clone();
    let started = Instant::now();
    let producer = tokio::spawn(async move {
        for _ in 0..ITEMS {
            producer_queue.push(item()).await.unwrap();
        }
    });
    let consumer = tokio::spawn(async move {
        let mut consumed = 0;
        while consumed < ITEMS {
            match consumer_queue.pop().await {
                Ok(_) => consumed += 1,
                Err(DtQueuePopError::Queue(PopError::Empty)) => {
                    consumer_queue.wait_for_data(Duration::from_secs(1)).await;
                }
                Err(error) => panic!("basic queue pop failed: {error:#}"),
            }
        }
    });
    let (producer, consumer) = tokio::join!(producer, consumer);
    producer.unwrap();
    consumer.unwrap();
    started.elapsed()
}

async fn dependency_concurrent(
    keys: Option<Arc<Vec<DependencyKey>>>,
    producer_batch_size: usize,
) -> std::time::Duration {
    let queue = Arc::new(DependencyQueue::new(4096));
    let producer_queue = queue.clone();
    let consumer_queue = queue.clone();
    let producer_keys = keys.clone();
    let started = Instant::now();
    let producer = tokio::spawn(async move {
        for offset in (0..ITEMS).step_by(producer_batch_size) {
            let end = (offset + producer_batch_size).min(ITEMS);
            let inputs = (offset..end)
                .map(|index| {
                    let ordered_by = producer_keys
                        .as_ref()
                        .map(|keys| vec![keys[index % keys.len()].clone()])
                        .unwrap_or_default();
                    DependencyInput::new(
                        item(),
                        DependencySpec {
                            ordered_by,
                            ..Default::default()
                        },
                    )
                })
                .collect();
            producer_queue.push_batch(inputs).await.unwrap();
        }
    });
    let consumer = tokio::spawn(async move {
        let mut consumed = 0;
        while consumed < ITEMS {
            let nodes = consumer_queue
                .pop_ready_batch(BATCH_SIZE)
                .await
                .unwrap()
                .unwrap();
            consumed += nodes.len();
            consumer_queue
                .ack_batch(nodes.into_iter().map(|node| node.id))
                .await
                .unwrap();
        }
    });
    let (producer, consumer) = tokio::join!(producer, consumer);
    producer.unwrap();
    consumer.unwrap();
    started.elapsed()
}

async fn dependency_enqueue_only(batch_size: usize) -> std::time::Duration {
    let queue = DependencyQueue::new(ITEMS);
    let started = Instant::now();
    for offset in (0..ITEMS).step_by(batch_size) {
        let end = (offset + batch_size).min(ITEMS);
        let inputs = (offset..end)
            .map(|_| DependencyInput::new(item(), DependencySpec::default()))
            .collect();
        queue.push_batch(inputs).await.unwrap();
    }
    started.elapsed()
}

async fn dependency_single_push_concurrent() -> std::time::Duration {
    let queue = Arc::new(DependencyQueue::new(4096));
    let producer_queue = queue.clone();
    let consumer_queue = queue.clone();
    let started = Instant::now();
    let producer = tokio::spawn(async move {
        for _ in 0..ITEMS {
            producer_queue
                .push(DependencyInput::new(item(), DependencySpec::default()))
                .await
                .unwrap();
        }
    });
    let consumer = tokio::spawn(async move {
        let mut consumed = 0;
        while consumed < ITEMS {
            let nodes = consumer_queue
                .pop_ready_batch(BATCH_SIZE)
                .await
                .unwrap()
                .unwrap();
            consumed += nodes.len();
            consumer_queue
                .ack_batch(nodes.into_iter().map(|node| node.id))
                .await
                .unwrap();
        }
    });
    let (producer, consumer) = tokio::join!(producer, consumer);
    producer.unwrap();
    consumer.unwrap();
    started.elapsed()
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual release-mode throughput comparison"]
async fn compare_basic_and_dependency_queue_throughput() {
    let basic = BasicQueue::new(ITEMS, 0, None, None);
    let started = Instant::now();
    for _ in 0..ITEMS {
        basic.push(item()).await.unwrap();
    }
    for _ in 0..ITEMS {
        basic.pop().await.unwrap();
    }
    report("basic single push/pop", started.elapsed());

    let dependency = DependencyQueue::new(ITEMS);
    let started = Instant::now();
    for _ in 0..ITEMS {
        dependency
            .push(DependencyInput::new(item(), DependencySpec::default()))
            .await
            .unwrap();
    }
    drain_dependency(&dependency).await;
    report("dependency single push, no key", started.elapsed());

    report("dependency batch, no key", dependency_batched(None).await);

    let partitioned_keys: Vec<_> = (0..1024)
        .map(|index| DependencyKey::Custom(Arc::from(format!("key-{index}"))))
        .collect();
    report(
        "dependency batch, 1024 keys",
        dependency_batched(Some(&partitioned_keys)).await,
    );

    let hot_key = [DependencyKey::Custom(Arc::from("hot-key"))];
    report(
        "dependency batch, one hot key",
        dependency_batched(Some(&hot_key)).await,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual release-mode concurrent throughput comparison"]
async fn compare_concurrent_queue_throughput() {
    report(
        "dependency enqueue only, single",
        dependency_enqueue_only(1).await,
    );
    report(
        "dependency enqueue only, batch 32",
        dependency_enqueue_only(32).await,
    );
    report(
        "dependency enqueue only, batch 128",
        dependency_enqueue_only(128).await,
    );
    report(
        "dependency enqueue only, batch 512",
        dependency_enqueue_only(512).await,
    );

    report("basic concurrent", basic_concurrent().await);
    report(
        "dependency concurrent, single push",
        dependency_single_push_concurrent().await,
    );
    report(
        "dependency concurrent, batch 32",
        dependency_concurrent(None, 32).await,
    );
    report(
        "dependency concurrent, batch 128",
        dependency_concurrent(None, 128).await,
    );
    report(
        "dependency concurrent, batch 512",
        dependency_concurrent(None, 512).await,
    );

    let partitioned_keys = Arc::new(
        (0..1024)
            .map(|index| DependencyKey::Custom(Arc::from(format!("key-{index}"))))
            .collect(),
    );
    report(
        "dependency concurrent, 1024 keys",
        dependency_concurrent(Some(partitioned_keys), 128).await,
    );

    let hot_key = Arc::new(vec![DependencyKey::Custom(Arc::from("hot-key"))]);
    report(
        "dependency concurrent, hot key",
        dependency_concurrent(Some(hot_key), 128).await,
    );
}
