# Chunk Partitioner Version Benchmark

## contiguous_16_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 50 | 354.086 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 50 | 2048.472 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 50 | 2028.379 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 50 | 353.500 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 50 | 2049.478 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 50 | 1769.406 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 50 | 355.619 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 50 | 1752.011 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| split_large_insert | string_key_basic | 160000 | 160000 | 50 | 1801.034 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 50 | 302.441 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 50 | 1753.436 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_basic | 160000 | 160000 | 50 | 1761.216 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## interleaved_16_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 30 | 336.403 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 30 | 1259.730 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 30 | 1180.370 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 30 | 374.778 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 30 | 1220.034 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 30 | 1226.197 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 30 | 347.824 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 30 | 1238.470 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| split_large_insert | string_key_basic | 160000 | 160000 | 30 | 1248.921 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 30 | 343.850 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 30 | 1239.541 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_basic | 160000 | 160000 | 30 | 1332.937 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## many_small_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 40000 | 40000 | 50 | 154.546 | 2000 | 20 | 20 | 20.00 | 0.00 |
| none | string_key_row_rebalance | 40000 | 40000 | 50 | 526.620 | 2000 | 20 | 20 | 20.00 | 0.00 |
| none | string_key_basic | 40000 | 40000 | 50 | 582.595 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 40000 | 40000 | 50 | 117.465 | 2000 | 20 | 20 | 20.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 40000 | 40000 | 50 | 558.397 | 2000 | 20 | 20 | 20.00 | 0.00 |
| chunk_largest_first | string_key_basic | 40000 | 40000 | 50 | 513.271 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 40000 | 40000 | 50 | 94.195 | 2000 | 20 | 20 | 20.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 40000 | 40000 | 50 | 552.176 | 2000 | 20 | 20 | 20.00 | 0.00 |
| split_large_insert | string_key_basic | 40000 | 40000 | 50 | 516.408 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 40000 | 40000 | 50 | 103.426 | 2000 | 20 | 20 | 20.00 | 0.00 |
| adaptive | string_key_row_rebalance | 40000 | 40000 | 50 | 569.250 | 2000 | 20 | 20 | 20.00 | 0.00 |
| adaptive | string_key_basic | 40000 | 40000 | 50 | 525.594 | 2000 | 20 | 20 | 20.00 | 0.00 |


## multi_schema_table_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 128000 | 128000 | 30 | 175.598 | 256 | 500 | 500 | 500.00 | 0.00 |
| none | string_key_row_rebalance | 128000 | 128000 | 30 | 692.125 | 256 | 500 | 500 | 500.00 | 0.00 |
| none | string_key_basic | 128000 | 128000 | 30 | 690.409 | 256 | 500 | 500 | 500.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 128000 | 128000 | 30 | 161.889 | 256 | 500 | 500 | 500.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 128000 | 128000 | 30 | 781.532 | 256 | 500 | 500 | 500.00 | 0.00 |
| chunk_largest_first | string_key_basic | 128000 | 128000 | 30 | 739.376 | 256 | 500 | 500 | 500.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 128000 | 128000 | 30 | 178.820 | 256 | 500 | 500 | 500.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 128000 | 128000 | 30 | 765.755 | 256 | 500 | 500 | 500.00 | 0.00 |
| split_large_insert | string_key_basic | 128000 | 128000 | 30 | 886.402 | 256 | 500 | 500 | 500.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 128000 | 128000 | 30 | 161.915 | 256 | 500 | 500 | 500.00 | 0.00 |
| adaptive | string_key_row_rebalance | 128000 | 128000 | 30 | 730.571 | 256 | 500 | 500 | 500.00 | 0.00 |
| adaptive | string_key_basic | 128000 | 128000 | 30 | 716.576 | 256 | 500 | 500 | 500.00 | 0.00 |


## skewed_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 130000 | 130000 | 30 | 163.553 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| none | string_key_row_rebalance | 130000 | 130000 | 30 | 1031.988 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| none | string_key_basic | 130000 | 130000 | 30 | 960.143 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 130000 | 130000 | 30 | 195.758 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| chunk_largest_first | string_key_row_rebalance | 130000 | 130000 | 30 | 977.306 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| chunk_largest_first | string_key_basic | 130000 | 130000 | 30 | 923.624 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 130000 | 130000 | 30 | 145.549 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| split_large_insert | string_key_row_rebalance | 130000 | 130000 | 30 | 877.412 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| split_large_insert | string_key_basic | 130000 | 130000 | 30 | 873.714 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 130000 | 130000 | 30 | 133.873 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| adaptive | string_key_row_rebalance | 130000 | 130000 | 30 | 1084.489 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| adaptive | string_key_basic | 130000 | 130000 | 30 | 892.459 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |


## single_large_chunk

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 50 | 357.892 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 50 | 1857.955 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 50 | 2281.278 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 50 | 312.594 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 50 | 1987.379 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 50 | 2292.459 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 50 | 360.764 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 50 | 2571.939 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| split_large_insert | string_key_basic | 160000 | 160000 | 50 | 2516.968 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 50 | 272.458 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 50 | 2195.460 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_basic | 160000 | 160000 | 50 | 2237.760 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## wide_row_bytes_skew

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 20 | 124.783 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 20 | 712.047 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 20 | 701.857 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 20 | 144.184 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 20 | 960.311 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 20 | 728.023 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 20 | 127.052 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 20 | 718.496 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| split_large_insert | string_key_basic | 160000 | 160000 | 20 | 698.782 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 20 | 143.306 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 20 | 704.783 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_basic | 160000 | 160000 | 20 | 669.523 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## random_uniform_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 20 | 384.762 | 4096 | 15 | 67 | 39.06 | 39.31 |
| none | string_key_row_rebalance | 160000 | 160000 | 20 | 1021.939 | 4096 | 15 | 67 | 39.06 | 39.31 |
| none | string_key_basic | 160000 | 160000 | 20 | 1023.007 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 20 | 374.433 | 4096 | 15 | 67 | 39.06 | 39.31 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 20 | 1006.986 | 4096 | 15 | 67 | 39.06 | 39.31 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 20 | 1114.999 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 20 | 391.612 | 4096 | 15 | 67 | 39.06 | 39.31 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 20 | 1056.878 | 4096 | 15 | 67 | 39.06 | 39.31 |
| split_large_insert | string_key_basic | 160000 | 160000 | 20 | 1047.420 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 20 | 374.982 | 4096 | 15 | 67 | 39.06 | 39.31 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 20 | 1046.964 | 4096 | 15 | 67 | 39.06 | 39.31 |
| adaptive | string_key_basic | 160000 | 160000 | 20 | 970.032 | 4096 | 15 | 67 | 39.06 | 39.31 |


## random_uniform_fully_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 20 | 184.967 | 4096 | 18 | 66 | 39.06 | 39.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 20 | 634.664 | 4096 | 18 | 66 | 39.06 | 39.00 |
| none | string_key_basic | 160000 | 160000 | 20 | 650.701 | 4096 | 18 | 66 | 39.06 | 39.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 20 | 172.937 | 4096 | 18 | 66 | 39.06 | 39.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 20 | 628.846 | 4096 | 18 | 66 | 39.06 | 39.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 20 | 636.175 | 4096 | 18 | 66 | 39.06 | 39.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 20 | 176.234 | 4096 | 18 | 66 | 39.06 | 39.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 20 | 622.642 | 4096 | 18 | 66 | 39.06 | 39.00 |
| split_large_insert | string_key_basic | 160000 | 160000 | 20 | 633.387 | 4096 | 18 | 66 | 39.06 | 39.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 20 | 177.665 | 4096 | 18 | 66 | 39.06 | 39.00 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 20 | 637.367 | 4096 | 18 | 66 | 39.06 | 39.00 |
| adaptive | string_key_basic | 160000 | 160000 | 20 | 653.589 | 4096 | 18 | 66 | 39.06 | 39.00 |


## random_hotspot_mostly_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 20 | 168.487 | 4095 | 1 | 585 | 39.07 | 14718.72 |
| none | string_key_row_rebalance | 160000 | 160000 | 20 | 650.301 | 4095 | 1 | 585 | 39.07 | 14718.72 |
| none | string_key_basic | 160000 | 160000 | 20 | 621.860 | 4095 | 1 | 585 | 39.07 | 14718.72 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 20 | 181.895 | 4095 | 1 | 585 | 39.07 | 14718.72 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 20 | 724.518 | 4095 | 1 | 585 | 39.07 | 14718.72 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 20 | 701.836 | 4095 | 1 | 585 | 39.07 | 14718.72 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 20 | 252.853 | 4095 | 1 | 585 | 39.07 | 14718.72 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 20 | 651.592 | 4095 | 1 | 585 | 39.07 | 14718.72 |
| split_large_insert | string_key_basic | 160000 | 160000 | 20 | 673.336 | 4095 | 1 | 585 | 39.07 | 14718.72 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 20 | 264.399 | 4095 | 1 | 585 | 39.07 | 14718.72 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 20 | 667.053 | 4095 | 1 | 585 | 39.07 | 14718.72 |
| adaptive | string_key_basic | 160000 | 160000 | 20 | 642.340 | 4095 | 1 | 585 | 39.07 | 14718.72 |


