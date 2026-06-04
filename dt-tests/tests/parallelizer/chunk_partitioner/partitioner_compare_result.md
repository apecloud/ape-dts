# Chunk Partitioner Version Benchmark

## baseline_16_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 5 | 122.715 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 5 | 481.016 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 5 | 456.472 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 5 | 114.293 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 5 | 473.705 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 5 | 462.349 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 5 | 113.472 | 32 | 5000 | 5000 | 5000.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 5 | 470.862 | 32 | 5000 | 5000 | 5000.00 | 0.00 |
| split_large_insert | string_key_basic | 160000 | 160000 | 5 | 456.603 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 5 | 113.054 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 5 | 467.308 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_basic | 160000 | 160000 | 5 | 460.440 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 160000 | 160000 | 5 | 116.339 | 800 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 160000 | 160000 | 5 | 482.064 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| min_rows | string_key_basic | 160000 | 160000 | 5 | 495.655 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 160000 | 160000 | 5 | 111.902 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| group_even | string_key_row_rebalance | 160000 | 160000 | 5 | 473.126 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| group_even | string_key_basic | 160000 | 160000 | 5 | 470.553 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## grouping_many_small_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 40000 | 40000 | 5 | 60.232 | 2000 | 20 | 20 | 20.00 | 0.00 |
| none | string_key_row_rebalance | 40000 | 40000 | 5 | 187.183 | 2000 | 20 | 20 | 20.00 | 0.00 |
| none | string_key_basic | 40000 | 40000 | 5 | 141.878 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 40000 | 40000 | 5 | 52.589 | 2000 | 20 | 20 | 20.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 40000 | 40000 | 5 | 145.137 | 2000 | 20 | 20 | 20.00 | 0.00 |
| chunk_largest_first | string_key_basic | 40000 | 40000 | 5 | 141.159 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 40000 | 40000 | 5 | 52.696 | 2000 | 20 | 20 | 20.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 40000 | 40000 | 5 | 143.918 | 2000 | 20 | 20 | 20.00 | 0.00 |
| split_large_insert | string_key_basic | 40000 | 40000 | 5 | 137.619 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 40000 | 40000 | 5 | 51.592 | 2000 | 20 | 20 | 20.00 | 0.00 |
| adaptive | string_key_row_rebalance | 40000 | 40000 | 5 | 141.952 | 2000 | 20 | 20 | 20.00 | 0.00 |
| adaptive | string_key_basic | 40000 | 40000 | 5 | 138.744 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 40000 | 40000 | 5 | 56.335 | 200 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 40000 | 40000 | 5 | 146.926 | 2000 | 20 | 20 | 20.00 | 0.00 |
| min_rows | string_key_basic | 40000 | 40000 | 5 | 141.027 | 2000 | 20 | 20 | 20.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 40000 | 40000 | 5 | 53.791 | 16 | 2400 | 2600 | 2500.00 | 10000.00 |
| group_even | string_key_row_rebalance | 40000 | 40000 | 5 | 184.529 | 2000 | 20 | 20 | 20.00 | 0.00 |
| group_even | string_key_basic | 40000 | 40000 | 5 | 141.543 | 2000 | 20 | 20 | 20.00 | 0.00 |


## grouping_many_keys_random_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 2 | 214.758 | 4096 | 15 | 67 | 39.06 | 39.31 |
| none | string_key_row_rebalance | 160000 | 160000 | 2 | 255.138 | 4096 | 15 | 67 | 39.06 | 39.31 |
| none | string_key_basic | 160000 | 160000 | 2 | 228.835 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 2 | 212.418 | 4096 | 15 | 67 | 39.06 | 39.31 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 2 | 232.845 | 4096 | 15 | 67 | 39.06 | 39.31 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 2 | 221.280 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 2 | 211.353 | 4096 | 15 | 67 | 39.06 | 39.31 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 2 | 225.728 | 4096 | 15 | 67 | 39.06 | 39.31 |
| split_large_insert | string_key_basic | 160000 | 160000 | 2 | 223.977 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 2 | 237.603 | 4096 | 15 | 67 | 39.06 | 39.31 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 2 | 227.429 | 4096 | 15 | 67 | 39.06 | 39.31 |
| adaptive | string_key_basic | 160000 | 160000 | 2 | 232.698 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 160000 | 160000 | 2 | 228.762 | 816 | 7 | 200 | 196.08 | 510.78 |
| min_rows | string_key_row_rebalance | 160000 | 160000 | 2 | 228.361 | 4096 | 15 | 67 | 39.06 | 39.31 |
| min_rows | string_key_basic | 160000 | 160000 | 2 | 223.280 | 4096 | 15 | 67 | 39.06 | 39.31 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 160000 | 160000 | 2 | 215.540 | 512 | 200 | 400 | 312.50 | 9432.32 |
| group_even | string_key_row_rebalance | 160000 | 160000 | 2 | 220.118 | 4096 | 15 | 67 | 39.06 | 39.31 |
| group_even | string_key_basic | 160000 | 160000 | 2 | 242.136 | 4096 | 15 | 67 | 39.06 | 39.31 |


## partition_few_large_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 3 | 66.775 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 3 | 342.811 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 3 | 272.383 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 3 | 67.793 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 3 | 273.559 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 3 | 271.826 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 3 | 70.110 | 32 | 5000 | 5000 | 5000.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 3 | 280.314 | 32 | 5000 | 5000 | 5000.00 | 0.00 |
| split_large_insert | string_key_basic | 160000 | 160000 | 3 | 273.595 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 3 | 69.865 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 3 | 279.984 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_basic | 160000 | 160000 | 3 | 271.894 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 160000 | 160000 | 3 | 72.687 | 800 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 160000 | 160000 | 3 | 284.657 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| min_rows | string_key_basic | 160000 | 160000 | 3 | 277.230 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 160000 | 160000 | 3 | 64.379 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| group_even | string_key_row_rebalance | 160000 | 160000 | 3 | 286.271 | 8 | 20000 | 20000 | 20000.00 | 0.00 |
| group_even | string_key_basic | 160000 | 160000 | 3 | 292.387 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## partition_mergeable_medium_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 128000 | 128000 | 3 | 56.347 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| none | string_key_row_rebalance | 128000 | 128000 | 3 | 230.681 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| none | string_key_basic | 128000 | 128000 | 3 | 225.843 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 128000 | 128000 | 3 | 58.844 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 128000 | 128000 | 3 | 233.135 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 128000 | 128000 | 3 | 228.430 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 128000 | 128000 | 3 | 56.837 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 128000 | 128000 | 3 | 229.904 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| split_large_insert | string_key_basic | 128000 | 128000 | 3 | 232.753 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 128000 | 128000 | 3 | 57.088 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| adaptive | string_key_row_rebalance | 128000 | 128000 | 3 | 229.871 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| adaptive | string_key_basic | 128000 | 128000 | 3 | 239.660 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 128000 | 128000 | 3 | 59.236 | 640 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 128000 | 128000 | 3 | 227.122 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| min_rows | string_key_basic | 128000 | 128000 | 3 | 237.725 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 128000 | 128000 | 3 | 53.395 | 16 | 8000 | 8000 | 8000.00 | 0.00 |
| group_even | string_key_row_rebalance | 128000 | 128000 | 3 | 228.479 | 128 | 1000 | 1000 | 1000.00 | 0.00 |
| group_even | string_key_basic | 128000 | 128000 | 3 | 228.580 | 128 | 1000 | 1000 | 1000.00 | 0.00 |


## partition_uneven_contiguous_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 115800 | 115800 | 3 | 48.630 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| none | string_key_row_rebalance | 115800 | 115800 | 3 | 201.650 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| none | string_key_basic | 115800 | 115800 | 3 | 205.733 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 115800 | 115800 | 3 | 47.902 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| chunk_largest_first | string_key_row_rebalance | 115800 | 115800 | 3 | 201.745 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| chunk_largest_first | string_key_basic | 115800 | 115800 | 3 | 205.183 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 115800 | 115800 | 3 | 49.991 | 32 | 800 | 6200 | 3618.75 | 1498398.44 |
| split_large_insert | string_key_row_rebalance | 115800 | 115800 | 3 | 209.953 | 32 | 800 | 6250 | 3618.75 | 1512539.06 |
| split_large_insert | string_key_basic | 115800 | 115800 | 3 | 205.788 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 115800 | 115800 | 3 | 48.232 | 16 | 800 | 12600 | 7237.50 | 15141093.75 |
| adaptive | string_key_row_rebalance | 115800 | 115800 | 3 | 209.088 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
| adaptive | string_key_basic | 115800 | 115800 | 3 | 200.469 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 115800 | 115800 | 3 | 50.143 | 579 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 115800 | 115800 | 3 | 212.698 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| min_rows | string_key_basic | 115800 | 115800 | 3 | 215.786 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 115800 | 115800 | 3 | 50.151 | 16 | 7200 | 7400 | 7237.50 | 6093.75 |
| group_even | string_key_row_rebalance | 115800 | 115800 | 3 | 207.784 | 8 | 800 | 50000 | 14475.00 | 279179375.00 |
| group_even | string_key_basic | 115800 | 115800 | 3 | 207.753 | 16 | 800 | 12500 | 7237.50 | 15213281.25 |


## partition_multi_table_large_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 163000 | 163000 | 3 | 75.098 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| none | string_key_row_rebalance | 163000 | 163000 | 3 | 227.553 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| none | string_key_basic | 163000 | 163000 | 3 | 219.470 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 163000 | 163000 | 3 | 71.190 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| chunk_largest_first | string_key_row_rebalance | 163000 | 163000 | 3 | 219.065 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| chunk_largest_first | string_key_basic | 163000 | 163000 | 3 | 214.218 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 163000 | 163000 | 3 | 86.529 | 32 | 3000 | 6200 | 5093.75 | 1097460.94 |
| split_large_insert | string_key_row_rebalance | 163000 | 163000 | 3 | 239.483 | 32 | 3125 | 6250 | 5093.75 | 1144531.25 |
| split_large_insert | string_key_basic | 163000 | 163000 | 3 | 212.828 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 163000 | 163000 | 3 | 70.003 | 16 | 5000 | 12600 | 10187.50 | 7217343.75 |
| adaptive | string_key_row_rebalance | 163000 | 163000 | 3 | 245.865 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
| adaptive | string_key_basic | 163000 | 163000 | 3 | 214.765 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 163000 | 163000 | 3 | 73.163 | 815 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 163000 | 163000 | 3 | 222.983 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| min_rows | string_key_basic | 163000 | 163000 | 3 | 216.238 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 163000 | 163000 | 3 | 67.486 | 48 | 3000 | 4200 | 3395.83 | 230815.97 |
| group_even | string_key_row_rebalance | 163000 | 163000 | 3 | 232.275 | 11 | 5000 | 40000 | 14818.18 | 107785123.97 |
| group_even | string_key_basic | 163000 | 163000 | 3 | 214.435 | 16 | 5000 | 12500 | 10187.50 | 7214843.75 |


## partition_many_tables_mixed_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 334700 | 334700 | 2 | 103.858 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| none | string_key_row_rebalance | 334700 | 334700 | 2 | 318.480 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| none | string_key_basic | 334700 | 334700 | 2 | 313.298 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 334700 | 334700 | 2 | 101.972 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| chunk_largest_first | string_key_row_rebalance | 334700 | 334700 | 2 | 320.788 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| chunk_largest_first | string_key_basic | 334700 | 334700 | 2 | 326.477 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 334700 | 334700 | 2 | 95.668 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| split_large_insert | string_key_row_rebalance | 334700 | 334700 | 2 | 317.923 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| split_large_insert | string_key_basic | 334700 | 334700 | 2 | 311.339 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 334700 | 334700 | 2 | 98.121 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| adaptive | string_key_row_rebalance | 334700 | 334700 | 2 | 319.080 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| adaptive | string_key_basic | 334700 | 334700 | 2 | 316.040 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 334700 | 334700 | 2 | 99.994 | 1677 | 100 | 200 | 199.58 | 41.57 |
| min_rows | string_key_row_rebalance | 334700 | 334700 | 2 | 320.370 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| min_rows | string_key_basic | 334700 | 334700 | 2 | 310.559 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 334700 | 334700 | 2 | 95.116 | 256 | 800 | 1600 | 1307.42 | 68265.23 |
| group_even | string_key_row_rebalance | 334700 | 334700 | 2 | 320.597 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |
| group_even | string_key_basic | 334700 | 334700 | 2 | 310.342 | 128 | 200 | 8000 | 2614.84 | 8339857.79 |


## skew_single_hot_chunk

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 130000 | 130000 | 3 | 60.294 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| none | string_key_row_rebalance | 130000 | 130000 | 3 | 231.838 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| none | string_key_basic | 130000 | 130000 | 3 | 220.469 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 130000 | 130000 | 3 | 54.467 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| chunk_largest_first | string_key_row_rebalance | 130000 | 130000 | 3 | 230.684 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| chunk_largest_first | string_key_basic | 130000 | 130000 | 3 | 223.723 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 130000 | 130000 | 3 | 56.041 | 32 | 2000 | 6400 | 4062.50 | 4266093.75 |
| split_large_insert | string_key_row_rebalance | 130000 | 130000 | 3 | 240.042 | 32 | 2000 | 6250 | 4062.50 | 4291992.19 |
| split_large_insert | string_key_basic | 130000 | 130000 | 3 | 221.903 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 130000 | 130000 | 3 | 55.698 | 23 | 2000 | 12600 | 5652.17 | 25012930.06 |
| adaptive | string_key_row_rebalance | 130000 | 130000 | 3 | 234.015 | 23 | 2000 | 12500 | 5652.17 | 25009451.80 |
| adaptive | string_key_basic | 130000 | 130000 | 3 | 222.797 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 130000 | 130000 | 3 | 64.429 | 650 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 130000 | 130000 | 3 | 224.670 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| min_rows | string_key_basic | 130000 | 130000 | 3 | 224.043 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 130000 | 130000 | 3 | 55.895 | 16 | 8000 | 8200 | 8125.00 | 9375.00 |
| group_even | string_key_row_rebalance | 130000 | 130000 | 3 | 334.651 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |
| group_even | string_key_basic | 130000 | 130000 | 3 | 227.421 | 16 | 2000 | 100000 | 8125.00 | 562734375.00 |


## skew_multiple_hot_chunks

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 157000 | 157000 | 3 | 74.607 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| none | string_key_row_rebalance | 157000 | 157000 | 3 | 283.503 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| none | string_key_basic | 157000 | 157000 | 3 | 300.213 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 157000 | 157000 | 3 | 66.100 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| chunk_largest_first | string_key_row_rebalance | 157000 | 157000 | 3 | 278.404 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| chunk_largest_first | string_key_basic | 157000 | 157000 | 3 | 293.515 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 157000 | 157000 | 3 | 68.931 | 32 | 1000 | 7600 | 4906.25 | 3647460.94 |
| split_large_insert | string_key_row_rebalance | 157000 | 157000 | 3 | 312.934 | 32 | 1000 | 7500 | 4906.25 | 3713867.19 |
| split_large_insert | string_key_basic | 157000 | 157000 | 3 | 291.439 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 157000 | 157000 | 3 | 70.026 | 17 | 1000 | 15000 | 9235.29 | 28594048.44 |
| adaptive | string_key_row_rebalance | 157000 | 157000 | 3 | 409.054 | 17 | 1000 | 15000 | 9235.29 | 28591695.50 |
| adaptive | string_key_basic | 157000 | 157000 | 3 | 338.545 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 157000 | 157000 | 3 | 69.909 | 785 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 157000 | 157000 | 3 | 284.157 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| min_rows | string_key_basic | 157000 | 157000 | 3 | 294.890 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 157000 | 157000 | 3 | 67.720 | 16 | 9800 | 10000 | 9812.50 | 2343.75 |
| group_even | string_key_row_rebalance | 157000 | 157000 | 3 | 279.563 | 8 | 1000 | 60000 | 19625.00 | 578734375.00 |
| group_even | string_key_basic | 157000 | 157000 | 3 | 326.732 | 16 | 1000 | 20000 | 9812.50 | 37214843.75 |


## skew_single_large_chunk

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 5 | 130.579 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 5 | 495.021 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 5 | 519.806 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 5 | 115.191 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 5 | 503.568 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 5 | 560.305 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 5 | 115.655 | 32 | 5000 | 5000 | 5000.00 | 0.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 5 | 528.437 | 32 | 5000 | 5000 | 5000.00 | 0.00 |
| split_large_insert | string_key_basic | 160000 | 160000 | 5 | 519.417 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 5 | 260.066 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 5 | 726.776 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| adaptive | string_key_basic | 160000 | 160000 | 5 | 529.097 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 160000 | 160000 | 5 | 118.695 | 800 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 160000 | 160000 | 5 | 493.443 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| min_rows | string_key_basic | 160000 | 160000 | 5 | 506.278 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 160000 | 160000 | 5 | 115.887 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| group_even | string_key_row_rebalance | 160000 | 160000 | 5 | 662.364 | 1 | 160000 | 160000 | 160000.00 | 0.00 |
| group_even | string_key_basic | 160000 | 160000 | 5 | 504.457 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


## bytes_wide_row_skew

| strategy | impl | input_rows | output_rows | iterations | elapsed_ms | partitions | min_rows | max_rows | avg_rows | row_variance |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| none | current_indexed_plan | 160000 | 160000 | 2 | 58.893 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_row_rebalance | 160000 | 160000 | 2 | 190.038 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| none | string_key_basic | 160000 | 160000 | 2 | 183.137 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| chunk_largest_first | current_indexed_plan | 160000 | 160000 | 2 | 50.658 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_row_rebalance | 160000 | 160000 | 2 | 186.994 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| chunk_largest_first | string_key_basic | 160000 | 160000 | 2 | 179.840 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| split_large_insert | current_indexed_plan | 160000 | 160000 | 2 | 53.959 | 32 | 400 | 10000 | 5000.00 | 22062500.00 |
| split_large_insert | string_key_row_rebalance | 160000 | 160000 | 2 | 189.656 | 32 | 312 | 10000 | 5000.00 | 22064222.25 |
| split_large_insert | string_key_basic | 160000 | 160000 | 2 | 180.915 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| adaptive | current_indexed_plan | 160000 | 160000 | 2 | 52.405 | 23 | 1200 | 10000 | 6956.52 | 17370283.55 |
| adaptive | string_key_row_rebalance | 160000 | 160000 | 2 | 187.832 | 23 | 1239 | 10000 | 6956.52 | 17367700.60 |
| adaptive | string_key_basic | 160000 | 160000 | 2 | 178.992 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| min_rows | current_indexed_plan | 160000 | 160000 | 2 | 49.060 | 800 | 200 | 200 | 200.00 | 0.00 |
| min_rows | string_key_row_rebalance | 160000 | 160000 | 2 | 190.180 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| min_rows | string_key_basic | 160000 | 160000 | 2 | 181.762 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
|  |  |  |  |  |  |  |  |  |  |  |
| group_even | current_indexed_plan | 160000 | 160000 | 2 | 47.097 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| group_even | string_key_row_rebalance | 160000 | 160000 | 2 | 193.478 | 16 | 10000 | 10000 | 10000.00 | 0.00 |
| group_even | string_key_basic | 160000 | 160000 | 2 | 180.201 | 16 | 10000 | 10000 | 10000.00 | 0.00 |


