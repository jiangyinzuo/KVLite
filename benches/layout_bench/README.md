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
