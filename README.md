# CacheLib with Mobius

This repository is a fork from [Thesys-lab/cachelib-sosp23](https://github.com/Thesys-lab/cachelib-sosp23). The original repository is modified [CacheLib](https://github.com/facebook/CacheLib) with several noval eviction policies, such as [SIEVE](https://yazhuozhang.com/assets/publication/nsdi24-sieve.pdf) and [S3FIFO](https://jasony.me/publication/sosp23-s3fifo.pdf). We further modify the repository as following:

- We add two lock-free eviction policies: CLOCK and Mobius. Both them are implemented using CAS instead of locks (in `cachelib/allocator/`).
- We make minor changes to flag operations for thread safety (`cachelib/allocator/Refcount.h`).

Mobius is a high-concurrency, lock-free cache eviction policy developed from SIEVE.

- Mobius utilizes two cyclic FIFO queues to achieve lock-free cache operations.
- Mobius uses a consecutive detection mechanism to mitigate data races in eviction.
- Mobius uses the same algorithm as SIEVE, achieving state-of-the-art efficiency on skewed workloads.

## Build

First of all, make sure that your OS can build CacheLib properly. Recommended OSs are: debian10|11, centos 8, ubuntu18.04|20.04|21.04|22.04, fedora 34|35|36, rocky 8|9, arch\*|manjaro\*.

Evaluation tool locates in the file `mybench`. To build the project, run the following commands:
```bash
$ cd mybench
$ # turnoff turobo boose and change to performance mode, this is very important for getting consistent results
$ bash turboboost.sh disable;
$ # build cachelib
$ bash build.sh; 
```

This script will compile the project and generate the the following binary file in `mybench/_build/`:  clock, lru, mobius, s3fifo, sieve, strictlru, tinylfu, and twoq. 


## Reproduction

Our evaluation divides into two parts: synthetic workloads and real-word workloads. 

#### Synthetic workloads

In directory `mybench/traces`, there is a script `synthetic_traces_gen.py` to generate the synthetic workloads. This script is sourced from the [S3FIFO repo](https://github.com/Thesys-lab/sosp23-s3fifo/blob/main/libCacheSim/scripts/data_gen.py). Using the script to generate Zipfian workloads. 

```bash
$ # Enter the directory `mybench/traces`
$ cd traces
$ # Generate 100M requests to 1M objects with zipfian distribution. The skewness $\alpha=1$.
$ python3 synthetic_traces_gen.py -m 1000000 -n 100000000 --alpha 1.0 --bin-output zipf1.0_1_100.dat
```

#### Real-world workloads

The real-world workloads are sourced from Meta (https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_FB_HW_eval/). 


1. Downloads traces.
  - Install and setup the AWS CLI
  - Download the tracedata (All traces are totally around 130 GB)
  ``` bash
  $ # kv-1
  $ aws s3 cp --no-sign-request --recursive s3://cachelib-workload-sharing/pub/kvcache/202206/ ./
  $ # kv-2
  $ aws s3 cp --no-sign-request --recursive s3://cachelib-workload-sharing/pub/kvcache/202401/ ./
  $ # cdn
  $ aws s3 cp --no-sign-request --recursive s3://cachelib-workload-sharing/pub/cdn/ ./
  $ # block
  $ aws s3 cp --no-sign-request --recursive s3://cachelib-workload-sharing/pub/storage/202312/ ./
  ```

2. Transform the csv file to binary data. (Please update the python scrpit for different workloads. The default is for kv-1)
``` bash
$ python3 trace_transform.py
```


#### Run

As the workloads are generated, we use the script `mybench/run.sh` to evaluate performance. 
```bash
$ # Return the directory of `mybench`
$ cd ..
$ # The first parameter is the eviction policy. The second is the cache size (MB). The thrid is the trace file name. This script will run the evaluation 5 times, each with the thread count of 1, 2, 4, 8, 16, respectively.
$ bash run.sh mobius 1000 zipf1.0_1_100.dat
```
