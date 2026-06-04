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
    FullyContiguous,
}

#[derive(Clone, Copy)]
pub(crate) enum ChunkSkew {
    Uniform,
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

pub(crate) fn variable_contiguous_chunks(rows_per_chunk: &[usize]) -> Vec<RowData> {
    let total_rows = rows_per_chunk.iter().sum::<usize>();
    let mut data = Vec::with_capacity(total_rows);
    for (chunk_id, rows) in rows_per_chunk.iter().enumerate() {
        for _ in 0..*rows {
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

pub(crate) fn multi_table_variable_contiguous_chunks(
    table_rows_per_chunk: &[&[usize]],
) -> Vec<RowData> {
    let total_rows = table_rows_per_chunk
        .iter()
        .flat_map(|rows_per_chunk| rows_per_chunk.iter())
        .sum::<usize>();
    let mut data = Vec::with_capacity(total_rows);
    for (table_index, rows_per_chunk) in table_rows_per_chunk.iter().enumerate() {
        for (chunk_id, rows) in rows_per_chunk.iter().enumerate() {
            for _ in 0..*rows {
                data.push(sized_row(
                    "schema",
                    &format!("table_{table_index}"),
                    chunk_id as u64,
                    RowType::Insert,
                    1,
                ));
            }
        }
    }
    data
}

pub(crate) fn many_table_mixed_contiguous_chunks(
    table_count: usize,
    chunk_count: usize,
) -> Vec<RowData> {
    let mut data = Vec::new();
    for table_index in 0..table_count {
        for chunk_id in 0..chunk_count {
            let rows = match (table_index + chunk_id) % 5 {
                0 => 8_000,
                1 => 3_000,
                2 => 1_200,
                3 => 500,
                _ => 200,
            };
            for _ in 0..rows {
                data.push(sized_row(
                    "schema",
                    &format!("table_{table_index}"),
                    chunk_id as u64,
                    RowType::Insert,
                    1,
                ));
            }
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
    }

    data
}

pub(crate) fn grouping_many_keys_contiguous(row_count: usize) -> Vec<RowData> {
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

pub(crate) fn grouping_data_cases() -> Vec<DataCase> {
    vec![
        DataCase {
            name: "baseline_16_contiguous_chunks",
            data: contiguous_chunks(10_000, 16),
            iterations: 50,
        },
        DataCase {
            name: "baseline_16_interleaved_chunks",
            data: interleaved_chunks(10_000, 16),
            iterations: 30,
        },
        DataCase {
            name: "grouping_many_small_contiguous_chunks",
            data: contiguous_chunks(20, 2_000),
            iterations: 50,
        },
        DataCase {
            name: "grouping_many_tables_contiguous_chunks",
            data: multi_schema_table_chunks(500, 4, 8, 8),
            iterations: 30,
        },
        DataCase {
            name: "grouping_many_keys_random_chunks",
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
            name: "grouping_many_keys_contiguous_chunks",
            data: grouping_many_keys_contiguous(160_000),
            iterations: 20,
        },
    ]
}

pub(crate) fn default_data_cases() -> Vec<DataCase> {
    vec![
        DataCase {
            name: "baseline_16_contiguous_chunks",
            data: contiguous_chunks(10_000, 16),
            iterations: 5,
        },
        DataCase {
            name: "grouping_many_small_contiguous_chunks",
            data: contiguous_chunks(20, 2_000),
            iterations: 5,
        },
        DataCase {
            name: "grouping_many_keys_random_chunks",
            data: random_chunk_mix(
                0x1234_abcd,
                160_000,
                4,
                8,
                128,
                RowOrder::Random,
                ChunkSkew::Uniform,
            ),
            iterations: 2,
        },
        DataCase {
            name: "partition_few_large_chunks",
            data: contiguous_chunks(20_000, 8),
            iterations: 3,
        },
        DataCase {
            name: "partition_mergeable_medium_chunks",
            data: contiguous_chunks(1_000, 128),
            iterations: 3,
        },
        DataCase {
            name: "partition_uneven_contiguous_chunks",
            data: variable_contiguous_chunks(&[
                50_000, 2_000, 2_000, 30_000, 1_000, 20_000, 800, 10_000,
            ]),
            iterations: 3,
        },
        DataCase {
            name: "partition_multi_table_large_chunks",
            data: multi_table_variable_contiguous_chunks(&[
                &[25_000, 25_000, 10_000, 5_000],
                &[40_000, 5_000, 5_000],
                &[12_000, 12_000, 12_000, 12_000],
            ]),
            iterations: 3,
        },
        DataCase {
            name: "partition_many_tables_mixed_chunks",
            data: many_table_mixed_contiguous_chunks(16, 8),
            iterations: 2,
        },
        DataCase {
            name: "skew_single_hot_chunk",
            data: skewed_contiguous_chunks(100_000, 2_000),
            iterations: 3,
        },
        DataCase {
            name: "skew_multiple_hot_chunks",
            data: variable_contiguous_chunks(&[
                60_000, 50_000, 40_000, 2_000, 2_000, 1_000, 1_000, 1_000,
            ]),
            iterations: 3,
        },
        DataCase {
            name: "skew_single_large_chunk",
            data: contiguous_chunks(160_000, 1),
            iterations: 5,
        },
        DataCase {
            name: "bytes_wide_row_skew",
            data: wide_row_bytes_skew(160_000, 0x2026_0603),
            iterations: 2,
        },
    ]
}

fn choose_chunk_id(rng: &mut StdRng, chunk_count: usize, chunk_skew: ChunkSkew) -> u64 {
    match chunk_skew {
        ChunkSkew::Uniform => rng.random_range(0..chunk_count) as u64,
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
