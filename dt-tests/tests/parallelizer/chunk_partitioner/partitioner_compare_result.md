# Chunk Partitioner Version Benchmark

## baseline_16_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 5 | 120.373 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 5 | 474.588 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 5 | 529.485 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 5 | 114.734 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 5 | 481.186 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 5 | 478.740 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 160000 | 160000 | 5 | 125.200 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| auto_split | string_key_row_rebalance | 160000 | 160000 | 5 | 508.516 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| auto_split | string_key_basic | 160000 | 160000 | 5 | 512.097 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 160000 | 160000 | 5 | 122.774 | 800 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 160000 | 160000 | 5 | 481.446 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| table_min_rows | string_key_basic | 160000 | 160000 | 5 | 486.684 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 160000 | 160000 | 5 | 122.374 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| table_even | string_key_row_rebalance | 160000 | 160000 | 5 | 543.501 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| table_even | string_key_basic | 160000 | 160000 | 5 | 470.518 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## grouping_many_small_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 40000 | 40000 | 5 | 56.845 | 2000 | 20 | 20 | 20.00 | 0.00 |
| none | string_key_row_rebalance | 40000 | 40000 | 5 | 207.758 | 2000 | 20 | 20 | 20.00 | 0.00 |
| none | string_key_basic | 40000 | 40000 | 5 | 142.501 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 40000 | 40000 | 5 | 54.354 | 2000 | 20 | 20 | 20.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 40000 | 40000 | 5 | 141.729 | 2000 | 20 | 20 | 20.00 | 0.00 |
| chunk_largest_first | string_key_basic | 40000 | 40000 | 5 | 139.889 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 40000 | 40000 | 5 | 54.408 | 2000 | 20 | 20 | 20.00 | 0.00 |
| auto_split | string_key_row_rebalance | 40000 | 40000 | 5 | 154.140 | 2000 | 20 | 20 | 20.00 | 0.00 |
| auto_split | string_key_basic | 40000 | 40000 | 5 | 153.823 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 40000 | 40000 | 5 | 61.221 | 200 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 40000 | 40000 | 5 | 143.860 | 2000 | 20 | 20 | 20.00 | 0.00 |
| table_min_rows | string_key_basic | 40000 | 40000 | 5 | 141.378 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 40000 | 40000 | 5 | 56.938 | 16 | 2400 | 2600 | 2500.00 | 10000.00 |
| table_even | string_key_row_rebalance | 40000 | 40000 | 5 | 206.819 | 2000 | 20 | 20 | 20.00 | 0.00 |
| table_even | string_key_basic | 40000 | 40000 | 5 | 162.243 | 2000 | 20 | 20 | 20.00 | 0.00 |


## grouping_many_keys_random_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 2 | 250.301 | 4096 | 15 | 67 | 39.06 | 39.31 |
| none | string_key_row_rebalance | 160000 | 160000 | 2 | 331.483 | 4096 | 15 | 67 | 39.06 | 39.31 |
| none | string_key_basic | 160000 | 160000 | 2 | 281.825 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 2 | 234.377 | 4096 | 15 | 67 | 39.06 | 39.31 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 2 | 258.849 | 4096 | 15 | 67 | 39.06 | 39.31 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 2 | 242.923 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 160000 | 160000 | 2 | 271.461 | 4096 | 15 | 67 | 39.06 | 39.31 |
| auto_split | string_key_row_rebalance | 160000 | 160000 | 2 | 376.166 | 4096 | 15 | 67 | 39.06 | 39.31 |
| auto_split | string_key_basic | 160000 | 160000 | 2 | 250.340 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 160000 | 160000 | 2 | 246.395 | 816 | 7 | 200 | 196.08 | 510.78 |
| table_min_rows | string_key_row_rebalance | 160000 | 160000 | 2 | 262.831 | 4096 | 15 | 67 | 39.06 | 39.31 |
| table_min_rows | string_key_basic | 160000 | 160000 | 2 | 247.970 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 160000 | 160000 | 2 | 222.877 | 512 | 200 | 400 | 312.50 | 9432.32 |
| table_even | string_key_row_rebalance | 160000 | 160000 | 2 | 263.727 | 4096 | 15 | 67 | 39.06 | 39.31 |
| table_even | string_key_basic | 160000 | 160000 | 2 | 241.955 | 4096 | 15 | 67 | 39.06 | 39.31 |


## partition_few_large_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 3 | 69.984 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 3 | 279.101 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 3 | 403.722 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 3 | 71.913 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 3 | 291.285 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 3 | 276.055 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 160000 | 160000 | 3 | 71.110 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| auto_split | string_key_row_rebalance | 160000 | 160000 | 3 | 286.306 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| auto_split | string_key_basic | 160000 | 160000 | 3 | 299.226 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 160000 | 160000 | 3 | 80.313 | 800 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 160000 | 160000 | 3 | 298.945 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| table_min_rows | string_key_basic | 160000 | 160000 | 3 | 304.150 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 160000 | 160000 | 3 | 65.940 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| table_even | string_key_row_rebalance | 160000 | 160000 | 3 | 281.295 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| table_even | string_key_basic | 160000 | 160000 | 3 | 330.589 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## partition_mergeable_medium_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 128000 | 128000 | 3 | 58.375 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| none | string_key_row_rebalance | 128000 | 128000 | 3 | 256.846 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| none | string_key_basic | 128000 | 128000 | 3 | 338.908 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 128000 | 128000 | 3 | 90.326 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 128000 | 128000 | 3 | 273.608 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 128000 | 128000 | 3 | 244.211 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 128000 | 128000 | 3 | 58.229 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| auto_split | string_key_row_rebalance | 128000 | 128000 | 3 | 241.360 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| auto_split | string_key_basic | 128000 | 128000 | 3 | 254.965 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 128000 | 128000 | 3 | 65.055 | 640 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 128000 | 128000 | 3 | 268.197 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| table_min_rows | string_key_basic | 128000 | 128000 | 3 | 229.702 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 128000 | 128000 | 3 | 61.781 | 16 | 8000 | 8000 | 8000.00 | 0.00 |
| table_even | string_key_row_rebalance | 128000 | 128000 | 3 | 233.621 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| table_even | string_key_basic | 128000 | 128000 | 3 | 231.080 | 128 | 1000 | 1000 | 1000.00 | 0.00 |


## partition_uneven_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 115800 | 115800 | 3 | 50.894 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| none | string_key_row_rebalance | 115800 | 115800 | 3 | 203.193 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| none | string_key_basic | 115800 | 115800 | 3 | 212.425 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 115800 | 115800 | 3 | 49.832 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| chunk_largest_first | string_key_row_rebalance | 115800 | 115800 | 3 | 238.697 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| chunk_largest_first | string_key_basic | 115800 | 115800 | 3 | 206.135 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 115800 | 115800 | 3 | 51.406 | 16 | 800 | 12600 | 7237.50 | 15141093.75 |
| auto_split | string_key_row_rebalance | 115800 | 115800 | 3 | 208.107 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
| auto_split | string_key_basic | 115800 | 115800 | 3 | 212.963 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 115800 | 115800 | 3 | 54.529 | 579 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 115800 | 115800 | 3 | 205.245 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| table_min_rows | string_key_basic | 115800 | 115800 | 3 | 205.663 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 115800 | 115800 | 3 | 49.432 | 16 | 7200 | 7400 | 7237.50 | 6093.75 |
| table_even | string_key_row_rebalance | 115800 | 115800 | 3 | 204.999 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| table_even | string_key_basic | 115800 | 115800 | 3 | 208.910 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |


## partition_multi_table_large_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 163000 | 163000 | 3 | 117.778 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| none | string_key_row_rebalance | 163000 | 163000 | 3 | 258.951 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| none | string_key_basic | 163000 | 163000 | 3 | 231.504 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 163000 | 163000 | 3 | 72.128 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| chunk_largest_first | string_key_row_rebalance | 163000 | 163000 | 3 | 219.031 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| chunk_largest_first | string_key_basic | 163000 | 163000 | 3 | 233.119 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 163000 | 163000 | 3 | 73.475 | 16 | 5000 | 12600 | 10187.50 | 7217343.75 |
| auto_split | string_key_row_rebalance | 163000 | 163000 | 3 | 220.875 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
| auto_split | string_key_basic | 163000 | 163000 | 3 | 223.321 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 163000 | 163000 | 3 | 78.048 | 815 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 163000 | 163000 | 3 | 263.368 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| table_min_rows | string_key_basic | 163000 | 163000 | 3 | 222.257 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 163000 | 163000 | 3 | 67.862 | 48 | 3000 | 4200 | 3395.83 | 230815.97 |
| table_even | string_key_row_rebalance | 163000 | 163000 | 3 | 219.609 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| table_even | string_key_basic | 163000 | 163000 | 3 | 228.705 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |


## partition_many_tables_mixed_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 334700 | 334700 | 2 | 108.034 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| none | string_key_row_rebalance | 334700 | 334700 | 2 | 328.081 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| none | string_key_basic | 334700 | 334700 | 2 | 313.241 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 334700 | 334700 | 2 | 123.527 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| chunk_largest_first | string_key_row_rebalance | 334700 | 334700 | 2 | 329.804 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| chunk_largest_first | string_key_basic | 334700 | 334700 | 2 | 324.526 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 334700 | 334700 | 2 | 100.492 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| auto_split | string_key_row_rebalance | 334700 | 334700 | 2 | 323.557 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| auto_split | string_key_basic | 334700 | 334700 | 2 | 330.868 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 334700 | 334700 | 2 | 147.467 | 1677 | 100 | 200 | 199.58 | 41.57 |
| table_min_rows | string_key_row_rebalance | 334700 | 334700 | 2 | 352.116 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| table_min_rows | string_key_basic | 334700 | 334700 | 2 | 321.894 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 334700 | 334700 | 2 | 100.227 | 256 | 800 | 1600 | 1307.42 | 68265.23 |
| table_even | string_key_row_rebalance | 334700 | 334700 | 2 | 329.677 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| table_even | string_key_basic | 334700 | 334700 | 2 | 350.993 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |


## skew_single_hot_chunk

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 130000 | 130000 | 3 | 59.981 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| none | string_key_row_rebalance | 130000 | 130000 | 3 | 239.615 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| none | string_key_basic | 130000 | 130000 | 3 | 233.231 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 130000 | 130000 | 3 | 57.071 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| chunk_largest_first | string_key_row_rebalance | 130000 | 130000 | 3 | 227.399 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| chunk_largest_first | string_key_basic | 130000 | 130000 | 3 | 230.964 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 130000 | 130000 | 3 | 58.276 | 23 | 2000 | 12600 | 5652.17 | 25012930.06 |
| auto_split | string_key_row_rebalance | 130000 | 130000 | 3 | 238.329 | 23 | 2000 | 12500 | 5652.17 | 25009451.80 |
| auto_split | string_key_basic | 130000 | 130000 | 3 | 232.198 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 130000 | 130000 | 3 | 59.679 | 650 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 130000 | 130000 | 3 | 272.103 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| table_min_rows | string_key_basic | 130000 | 130000 | 3 | 225.306 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 130000 | 130000 | 3 | 55.486 | 16 | 8000 | 8200 | 8125.00 | 9375.00 |
| table_even | string_key_row_rebalance | 130000 | 130000 | 3 | 238.182 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| table_even | string_key_basic | 130000 | 130000 | 3 | 223.346 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |


## skew_multiple_hot_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 157000 | 157000 | 3 | 63.845 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| none | string_key_row_rebalance | 157000 | 157000 | 3 | 330.295 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| none | string_key_basic | 157000 | 157000 | 3 | 309.880 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 157000 | 157000 | 3 | 72.183 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| chunk_largest_first | string_key_row_rebalance | 157000 | 157000 | 3 | 341.578 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| chunk_largest_first | string_key_basic | 157000 | 157000 | 3 | 325.614 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 157000 | 157000 | 3 | 70.423 | 17 | 1000 | 15000 | 9235.29 | 28594048.44 |
| auto_split | string_key_row_rebalance | 157000 | 157000 | 3 | 310.095 | 17 | 1000 | 15000 | 9235.29 | 28591695.50 |
| auto_split | string_key_basic | 157000 | 157000 | 3 | 285.859 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 157000 | 157000 | 3 | 79.623 | 785 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 157000 | 157000 | 3 | 317.580 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| table_min_rows | string_key_basic | 157000 | 157000 | 3 | 283.937 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 157000 | 157000 | 3 | 69.036 | 16 | 9800 | 10000 | 9812.50 | 2343.75 |
| table_even | string_key_row_rebalance | 157000 | 157000 | 3 | 531.848 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| table_even | string_key_basic | 157000 | 157000 | 3 | 285.023 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |


## skew_single_large_chunk

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 5 | 115.518 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 5 | 472.691 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 5 | 515.526 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 5 | 137.345 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 5 | 477.947 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 5 | 512.963 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 160000 | 160000 | 5 | 118.141 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| auto_split | string_key_row_rebalance | 160000 | 160000 | 5 | 577.697 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| auto_split | string_key_basic | 160000 | 160000 | 5 | 885.978 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 160000 | 160000 | 5 | 141.526 | 800 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 160000 | 160000 | 5 | 480.472 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| table_min_rows | string_key_basic | 160000 | 160000 | 5 | 910.186 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 160000 | 160000 | 5 | 138.675 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| table_even | string_key_row_rebalance | 160000 | 160000 | 5 | 482.229 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| table_even | string_key_basic | 160000 | 160000 | 5 | 524.695 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## bytes_wide_row_skew

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 2 | 54.621 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 2 | 198.685 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 2 | 315.278 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 2 | 57.117 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 2 | 210.102 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 2 | 183.593 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| auto_split | current_indexed_plan | 160000 | 160000 | 2 | 53.503 | 23 | 1200 | 10000 | 6956.52 | 17370283.55 |
| auto_split | string_key_row_rebalance | 160000 | 160000 | 2 | 191.715 | 23 | 1239 | 10000 | 6956.52 | 17367700.60 |
| auto_split | string_key_basic | 160000 | 160000 | 2 | 181.833 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_min_rows | current_indexed_plan | 160000 | 160000 | 2 | 48.636 | 800 | 200 | 200 | 200.00 | 0.00 |
| table_min_rows | string_key_row_rebalance | 160000 | 160000 | 2 | 190.211 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| table_min_rows | string_key_basic | 160000 | 160000 | 2 | 179.514 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| table_even | current_indexed_plan | 160000 | 160000 | 2 | 46.235 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| table_even | string_key_row_rebalance | 160000 | 160000 | 2 | 187.968 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| table_even | string_key_basic | 160000 | 160000 | 2 | 181.117 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


