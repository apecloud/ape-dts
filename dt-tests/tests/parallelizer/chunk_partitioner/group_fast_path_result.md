# Chunk Partitioner Group Fast Path Benchmark

## contiguous_16_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 50 | 1604.850 | 1.00x |
| without_fast_path | 160000 | 50 | 294.034 | 5.46x |
| with_fast_path | 160000 | 50 | 65.764 | 24.40x |

## interleaved_16_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 30 | 1029.748 | 1.00x |
| without_fast_path | 160000 | 30 | 176.932 | 5.82x |
| with_fast_path | 160000 | 30 | 213.563 | 4.82x |

## many_small_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 40000 | 50 | 472.062 | 1.00x |
| without_fast_path | 40000 | 50 | 134.880 | 3.50x |
| with_fast_path | 40000 | 50 | 49.487 | 9.54x |

## multi_schema_table_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 128000 | 30 | 596.716 | 1.00x |
| without_fast_path | 128000 | 30 | 130.407 | 4.58x |
| with_fast_path | 128000 | 30 | 43.303 | 13.78x |

## skewed_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 130000 | 30 | 783.689 | 1.00x |
| without_fast_path | 130000 | 30 | 139.170 | 5.63x |
| with_fast_path | 130000 | 30 | 31.629 | 24.78x |

## single_large_chunk

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 50 | 1636.866 | 1.00x |
| without_fast_path | 160000 | 50 | 287.138 | 5.70x |
| with_fast_path | 160000 | 50 | 69.562 | 23.53x |

## wide_row_bytes_skew

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 20 | 668.864 | 1.00x |
| without_fast_path | 160000 | 20 | 147.446 | 4.54x |
| with_fast_path | 160000 | 20 | 39.831 | 16.79x |

## random_uniform_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 20 | 616.888 | 1.00x |
| without_fast_path | 160000 | 20 | 217.194 | 2.84x |
| with_fast_path | 160000 | 20 | 210.204 | 2.93x |

## random_uniform_fully_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 20 | 541.157 | 1.00x |
| without_fast_path | 160000 | 20 | 169.716 | 3.19x |
| with_fast_path | 160000 | 20 | 81.840 | 6.61x |

## random_hotspot_mostly_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 20 | 523.886 | 1.00x |
| without_fast_path | 160000 | 20 | 157.491 | 3.33x |
| with_fast_path | 160000 | 20 | 78.913 | 6.64x |

