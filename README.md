# RocksDB with Mobius Cache

Mobius is a high-concurrency, lock-free cache eviction policy developed from [SIEVE](https://github.com/cacheMon/NSDI24-SIEVE). 

- Mobius utilizes two cyclic FIFO queues to achieve lock-free cache operations.
- Mobius uses a consecutive detection mechanism to mitigate data races in eviction.
- Mobius uses the same algorithm as SIEVE, achieving state-of-the-art efficiency on skewed workloads.

This repository contains code for Mobius Cache in RocksDB. Mobius Cache is an alternative block cache with higher concurrency than LRUCache.

## Introduction
Mobius Cache reuses the code of the original LRUCache, not yet at a production level. We made the following changes to the original code:

1. Insert a shared-lock pool into the Hashtable, making it thread-safe. The modified hashtable is in a fixed size ($2^17$) after initialization.
2. No longer free the memory of evicted cache entries. Instead, re-assign new data to the entries for reuse. This is to avoid awful dangling pointers in multi-threaded scenarios.
3. Replace the cache behaviors of LRU with that of Mobius, including Insert, Lookup, Erase, Ref, and Release. All those behaviors keep thread safety using atomic operations and CAS.

## Usage
1. Download the source code of RocksDB from https://github.com/facebook/rocksdb.
2. Replace the files `./cache/lru_cache.h` and `./cache/lru_cache.cc`.
3. Build RocksDB.
   ```shell
   $ mkdir build && cd build
   $ cmake .. -DCMAKE_BUILD_TYPE=Release -DWITH_ZSTD=on
   $ make -j
  ```

## Reproduction

#### cache_bench
Test the parallel throughput of block cache: 
```shell
for nThread in 2 4 6 8 10 12 14 16 18 20; do
  ./cache_bench -ops_per_thread 10000000 -num_shard_bits 0 -threads ${nThread}
done
```

#### db_bench

1. use the official script `./tools/benchmark.sh`
2. preload the database: `DB_DIR="./db" WAL_DIR="./db" ./benchmark.sh bulkload`
3. Update the parameters in benchmark.sh.
  - cache_size = 8 * $G
  - cache_numshardbits = 4
4. run workloads of readrandom and fwdrange.
```shell
DB_DIR="./db" WAL_DIR="./db" DURATION=4000 ./benchmark.sh readrandom
DB_DIR="./db" WAL_DIR="./db" DURATION=4000 ./benchmark.sh fwdrange
```

*Note: Mobius cache has passed the above tests and shows efficiency higher than LRUCache, but the support for other operations of RocksDB is still untested. 
