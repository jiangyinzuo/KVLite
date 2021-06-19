# Benchmark for Different Data Layout

## Range Query
```shell
cargo bench  --bench range_query
```

test column_group      ... bench:  27,465,383 ns/iter (+/- 10,432,322)  
test column_group_mmap ... bench:      16,195 ns/iter (+/- 5,008)  
test column_major      ... bench:  15,795,473 ns/iter (+/- 1,538,330)  
test column_major_mmap ... bench:      16,683 ns/iter (+/- 4,467)  
test pax               ... bench:  15,521,720 ns/iter (+/- 502,522)  
test pax_mmap          ... bench:      18,327 ns/iter (+/- 6,208)  
test row_major         ... bench:  25,923,908 ns/iter (+/- 429,811)  
test row_major_mmap    ... bench:      21,692 ns/iter (+/- 1,247)  

### Cache miss
**row major**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench range_query row_major -- --exact
```
       11,412,032      cache-misses                                                

       8.391571900 seconds time elapsed

       4.815651000 seconds user
       3.576033000 seconds sys

**row major(mmap)**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench range_query row_major_mmap -- --exact
```
       4,035,675      cache-misses

       0.740237464 seconds time elapsed

       0.600044000 seconds user
       0.139296000 seconds sys


**column group**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench range_query column_group -- --exact
```

       7,104,426      cache-misses                                                

       8.379969384 seconds time elapsed

       4.811131000 seconds user
       3.560372000 seconds sys

       
**column group(mmap)**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench range_query column_group_mmap -- --exact
```
       2,379,901      cache-misses                                                

       0.431064025 seconds time elapsed

       0.282282000 seconds user
       0.148979000 seconds sys


**column major**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench range_query column_major -- --exact
```
       5,070,179      cache-misses

       5.144238402 seconds time elapsed

       2.582204000 seconds user
       2.560708000 seconds sys


**column major(mmap)**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench range_query column_major_mmap -- --exact
```
       
       2,351,548      cache-misses

       0.541872131 seconds time elapsed

       0.388946000 seconds user
       0.153218000 seconds sys

**pax**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench range_query pax -- --exact
```
       5,930,294      cache-misses                                                

       5.311424854 seconds time elapsed

       2.691388000 seconds user
       2.619586000 seconds sys


**pax(mmap)**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench range_query pax_mmap -- --exact
```

       2,357,614      cache-misses

       0.362656948 seconds time elapsed

       0.205804000 seconds user
       0.157036000 seconds sys

## Point Query
```shell
cargo bench  --bench point_query
```

test column_group      ... bench:  13,603,911 ns/iter (+/- 1,246,323)  
test column_group_mmap ... bench:      66,440 ns/iter (+/- 982)  
test column_major      ... bench:  25,985,027 ns/iter (+/- 446,480)  
test column_major_mmap ... bench:      79,368 ns/iter (+/- 2,166)  
test pax               ... bench:  26,130,006 ns/iter (+/- 396,396)  
test pax_mmap          ... bench:     103,731 ns/iter (+/- 14,580)  
test row_major         ... bench:   6,518,487 ns/iter (+/- 61,287)  
test row_major_mmap    ... bench:      39,470 ns/iter (+/- 2,292)  

### Cache miss
**row major**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench point_query row_major -- --exact
```
         4,897,011      cache-misses                                                

       6.317308701 seconds time elapsed

       3.762277000 seconds user
       2.555464000 seconds sys

**row major(mmap)**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench point_query row_major_mmap -- --exact
```
         2,421,301      cache-misses                                                

       0.440399151 seconds time elapsed

       0.307719000 seconds user
       0.132833000 seconds sys

**column group**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench point_query column_group -- --exact
```
         5,198,266      cache-misses                                                

       4.337057565 seconds time elapsed

       2.595810000 seconds user
       1.723701000 seconds sys

**column group(mmap)**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench point_query column_group_mmap -- --exact
```
         2,636,826      cache-misses                                                

       0.471145259 seconds time elapsed

       0.326314000 seconds user
       0.145054000 seconds sys

**column major**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench point_query column_major -- --exact
```
        6,072,409      cache-misses                                                

       8.289976760 seconds time elapsed

       4.902182000 seconds user
       3.382110000 seconds sys

**column major(mmap)**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench point_query column_major_mmap -- --exact
```
         6,017,361      cache-misses                                                

       3.925098100 seconds time elapsed

       3.767167000 seconds user
       0.152270000 seconds sys

**pax**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench point_query pax -- --exact
```

         4,802,884      cache-misses                                                

       8.156773110 seconds time elapsed

       4.817050000 seconds user
       3.339964000 seconds sys

**pax(mmap)**
```shell
perf stat -e cache-misses cargo bench --package kvlite --bench point_query pax_mmap -- --exact
```
         2,578,719      cache-misses                                                

       0.519525315 seconds time elapsed

       0.378931000 seconds user
       0.140768000 seconds sys
