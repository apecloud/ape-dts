use dt_common::{meta::row_data::RowData, meta::row_type::RowType};
use rand::{rngs::StdRng, Rng, SeedableRng};

pub(crate) struct DataCase {
    pub(crate) name: &'static str,
    pub(crate) data: Vec<RowData>,
    pub(crate) iterations: usize,
}

#[derive(Clone, Copy)]
pub(crate) enum RowOrder {
    Random,
    MostlyContiguous,
    FullyContiguous,
}

#[derive(Clone, Copy)]
pub(crate) enum ChunkSkew {
    Uniform,
    Hotspot {
        hot_chunk_count: usize,
        hot_ratio_percent: u8,
    },
}

pub(crate) fn sized_row(
    schema: &str,
    tb: &str,
    chunk_id: u64,
    row_type: RowType,
    data_size: usize,
) -> RowData {
    let mut row = RowData::new(
        schema.to_string(),
        tb.to_string(),
        chunk_id,
        row_type,
        None,
        None,
    );
    row.data_size = data_size;
    row
}

pub(crate) fn contiguous_chunks(rows_per_chunk: usize, chunk_count: usize) -> Vec<RowData> {
    let mut data = Vec::with_capacity(rows_per_chunk * chunk_count);
    for chunk_id in 0..chunk_count {
        for _ in 0..rows_per_chunk {
            data.push(sized_row(
                "schema",
                "apecloud_dts_table_test",
                chunk_id as u64,
                RowType::Insert,
                1,
            ));
        }
    }
    data
}

pub(crate) fn interleaved_chunks(rows_per_chunk: usize, chunk_count: usize) -> Vec<RowData> {
    let mut data = Vec::with_capacity(rows_per_chunk * chunk_count);
    for _ in 0..rows_per_chunk {
        for chunk_id in 0..chunk_count {
            data.push(sized_row(
                "schema",
                "apecloud_dts_table_test",
                chunk_id as u64,
                RowType::Insert,
                1,
            ));
        }
    }
    data
}

pub(crate) fn multi_schema_table_chunks(
    rows_per_chunk: usize,
    schema_count: usize,
    table_count: usize,
    chunk_count: usize,
) -> Vec<RowData> {
    let mut data = Vec::with_capacity(rows_per_chunk * schema_count * table_count * chunk_count);
    for schema_index in 0..schema_count {
        for table_index in 0..table_count {
            for chunk_id in 0..chunk_count {
                for _ in 0..rows_per_chunk {
                    data.push(sized_row(
                        &format!("schema_{schema_index}"),
                        &format!("table_{table_index}"),
                        chunk_id as u64,
                        RowType::Insert,
                        1,
                    ));
                }
            }
        }
    }
    data
}

pub(crate) fn skewed_contiguous_chunks(
    large_chunk_rows: usize,
    small_chunk_rows: usize,
) -> Vec<RowData> {
    let mut data = Vec::with_capacity(large_chunk_rows + small_chunk_rows * 15);
    for _ in 0..large_chunk_rows {
        data.push(sized_row(
            "schema",
            "apecloud_dts_table_test",
            0,
            RowType::Insert,
            1,
        ));
    }

    for chunk_id in 1..16 {
        for _ in 0..small_chunk_rows {
            data.push(sized_row(
                "schema",
                "apecloud_dts_table_test",
                chunk_id,
                RowType::Insert,
                1,
            ));
        }
    }
    data
}

pub(crate) fn wide_row_bytes_skew(row_count: usize, seed: u64) -> Vec<RowData> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(row_count);
    for index in 0..row_count {
        let data_size = if index % 32 == 0 {
            rng.random_range(8_000..16_000)
        } else {
            rng.random_range(32..256)
        };
        data.push(sized_row(
            "schema",
            "apecloud_dts_table_test",
            (index % 16) as u64,
            RowType::Insert,
            data_size,
        ));
    }
    data.sort_by(|left, right| {
        (left.schema.as_str(), left.tb.as_str(), left.chunk_id).cmp(&(
            right.schema.as_str(),
            right.tb.as_str(),
            right.chunk_id,
        ))
    });
    data
}

pub(crate) fn random_chunk_mix(
    seed: u64,
    row_count: usize,
    schema_count: usize,
    table_count: usize,
    chunk_count: usize,
    row_order: RowOrder,
    chunk_skew: ChunkSkew,
) -> Vec<RowData> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let schema_index = rng.random_range(0..schema_count);
        let table_index = rng.random_range(0..table_count);
        let chunk_id = choose_chunk_id(&mut rng, chunk_count, chunk_skew);
        let data_size = rng.random_range(32..512);
        data.push(sized_row(
            &format!("schema_{schema_index}"),
            &format!("table_{table_index}"),
            chunk_id,
            RowType::Insert,
            data_size,
        ));
    }

    match row_order {
        RowOrder::Random => {}
        RowOrder::FullyContiguous => sort_by_group_key(&mut data),
        RowOrder::MostlyContiguous => {
            sort_by_group_key(&mut data);
            add_deterministic_jitter(&mut data, seed ^ 0x5eed_f00d);
        }
    }

    data
}

pub(crate) fn random_uniform_fully_contiguous_chunks(row_count: usize) -> Vec<RowData> {
    random_chunk_mix(
        0x2468_ace0,
        row_count,
        4,
        8,
        128,
        RowOrder::FullyContiguous,
        ChunkSkew::Uniform,
    )
}

pub(crate) fn default_data_cases() -> Vec<DataCase> {
    vec![
        DataCase {
            name: "contiguous_16_chunks",
            data: contiguous_chunks(10_000, 16),
            iterations: 50,
        },
        DataCase {
            name: "interleaved_16_chunks",
            data: interleaved_chunks(10_000, 16),
            iterations: 30,
        },
        DataCase {
            name: "many_small_contiguous_chunks",
            data: contiguous_chunks(20, 2_000),
            iterations: 50,
        },
        DataCase {
            name: "multi_schema_table_contiguous_chunks",
            data: multi_schema_table_chunks(500, 4, 8, 8),
            iterations: 30,
        },
        DataCase {
            name: "skewed_contiguous_chunks",
            data: skewed_contiguous_chunks(100_000, 2_000),
            iterations: 30,
        },
        DataCase {
            name: "single_large_chunk",
            data: contiguous_chunks(160_000, 1),
            iterations: 50,
        },
        DataCase {
            name: "wide_row_bytes_skew",
            data: wide_row_bytes_skew(160_000, 0x2026_0603),
            iterations: 20,
        },
        DataCase {
            name: "random_uniform_chunks",
            data: random_chunk_mix(
                0x1234_abcd,
                160_000,
                4,
                8,
                128,
                RowOrder::Random,
                ChunkSkew::Uniform,
            ),
            iterations: 20,
        },
        DataCase {
            name: "random_uniform_fully_contiguous_chunks",
            data: random_uniform_fully_contiguous_chunks(160_000),
            iterations: 20,
        },
        DataCase {
            name: "random_hotspot_mostly_contiguous_chunks",
            data: random_chunk_mix(
                0x5678_dcba,
                160_000,
                4,
                8,
                128,
                RowOrder::MostlyContiguous,
                ChunkSkew::Hotspot {
                    hot_chunk_count: 8,
                    hot_ratio_percent: 80,
                },
            ),
            iterations: 20,
        },
    ]
}

fn choose_chunk_id(rng: &mut StdRng, chunk_count: usize, chunk_skew: ChunkSkew) -> u64 {
    match chunk_skew {
        ChunkSkew::Uniform => rng.random_range(0..chunk_count) as u64,
        ChunkSkew::Hotspot {
            hot_chunk_count,
            hot_ratio_percent,
        } => {
            let hot_chunk_count = hot_chunk_count.min(chunk_count).max(1);
            if rng.random_range(0..100) < hot_ratio_percent {
                rng.random_range(0..hot_chunk_count) as u64
            } else {
                rng.random_range(0..chunk_count) as u64
            }
        }
    }
}

fn sort_by_group_key(data: &mut [RowData]) {
    data.sort_by(|left, right| {
        (left.schema.as_str(), left.tb.as_str(), left.chunk_id).cmp(&(
            right.schema.as_str(),
            right.tb.as_str(),
            right.chunk_id,
        ))
    });
}

fn add_deterministic_jitter(data: &mut [RowData], seed: u64) {
    if data.len() < 2 {
        return;
    }

    let mut rng = StdRng::seed_from_u64(seed);
    let swap_count = (data.len() / 100).max(1);
    for _ in 0..swap_count {
        let left = rng.random_range(0..data.len());
        let right = rng.random_range(0..data.len());
        data.swap(left, right);
    }
}
