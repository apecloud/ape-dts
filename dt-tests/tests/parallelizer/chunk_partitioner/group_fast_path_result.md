# Chunk Partitioner Group Fast Path Benchmark

## baseline_16_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 50 | 4586.337 | 1.00x |
| without_fast_path | 160000 | 50 | 4264.335 | 1.08x |
| with_fast_path | 160000 | 50 | 360.883 | 12.71x |

## baseline_16_interleaved_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 30 | 2837.585 | 1.00x |
| without_fast_path | 160000 | 30 | 2426.958 | 1.17x |
| with_fast_path | 160000 | 30 | 2526.197 | 1.12x |

## grouping_many_small_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 40000 | 50 | 1622.454 | 1.00x |
| without_fast_path | 40000 | 50 | 1223.119 | 1.33x |
| with_fast_path | 40000 | 50 | 312.011 | 5.20x |

## grouping_many_tables_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 128000 | 30 | 2097.950 | 1.00x |
| without_fast_path | 128000 | 30 | 1851.867 | 1.13x |
| with_fast_path | 128000 | 30 | 202.139 | 10.38x |

## grouping_many_keys_random_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 20 | 1993.831 | 1.00x |
| without_fast_path | 160000 | 20 | 1903.453 | 1.05x |
| with_fast_path | 160000 | 20 | 1872.311 | 1.06x |

## grouping_many_keys_contiguous_chunks

| impl | rows | iterations | elapsed_ms | speedup_vs_format_key |
| :--- | ---: | ---: | ---: | ---: |
| with_format_key | 160000 | 20 | 2516.742 | 1.00x |
| without_fast_path | 160000 | 20 | 1645.910 | 1.53x |
| with_fast_path | 160000 | 20 | 338.433 | 7.44x |

