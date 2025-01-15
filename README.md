# 3L-Cache: Low Overhead and Precise Learning-based Eviction Policy for Web Caches

This is the implementation repository of *3L-CACHE: Low Overhead and Precise Learning-based Eviction Policy for Web Caches*. This artifact provides the source code of 3L-Cache and scripts to reproduce experiment results in our paper.

#### 3L Cache is implemented in the [libCacheSim](https://github.com/1a1a11a/libCacheSim) library, and its experimental environment configuration is consistent with libCacheSim.

 ## Supported Platforms
- Software Requirements: Ubuntu 18.04, cmake 3.28.6

## Trace Format
Request traces are expected to be in a space-separated format with 3 columns.
- time should be a long long int
- id should be a long long int
- size should be uint32

| time |  id | size |
| ---- | --- | ---- |
|   1  |  1  |  120 |
|   2  |  2  |   64 |
|   3  |  2  |   64 |

## Build and Install 
We provide some scripts for installation.
```bash
cd scripts && bash install_dependency.sh && bash install_libcachesim.sh
```

## Usage
After building and installing, cachesim should be in the _build/bin/ directory.
```bash
~/libCacheSim/_build/bin/cachesim trace_path trace_type eviction_algo cache_size [OPTION...]
```

## Traces


| Dataset       | Year |    Type   |                                      Original Link                                                |
|---------------|------|:---------:|:-------------------------------------------------------------------------------------------------:|
| Tencent Photo | 2018 |   object  |                      [link](http://iotta.snia.org/traces/parallel?only=27476)                     |
| WikiCDN       | 2019 |   object  |          [link](https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Caching)          |
| WikiCDN 2018  | 2018 |   object  |          [link](http://lrb.cs.princeton.edu/wiki2018.tr.tar.gz)                                   |
| WikiCDN 2019  | 2019 |   object  |          [link](http://lrb.cs.princeton.edu/wiki2019.tr.tar.gz)                                   |
| Tencent CBS   | 2020 |   block   |                      [link](http://iotta.snia.org/traces/parallel?only=27917)                     |
| Alibaba Block | 2020 |   block   |                          [link](https://github.com/alibaba/block-traces)                          |
| Twitter       | 2020 | key-value |                          [link](https://github.com/twitter/cache-traces)                          |
| MetaKV        | 2022 | key-value | [link](https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_FB_HW_eval/#list-of-traces) |
| MetaCDN       | 2023 | object    | [link](https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_FB_HW_eval/#list-of-traces) |


## Run a single cache simulation
```bash
/path/to/cachesim /path/to/tencentBlock_ns3964.csv csv 3lcache 1347453593  -t "time-col=1, obj-id-is-num=true, obj-id-col=2, obj-size-col=3"
```

## Run multiple cache simulations
```bash
/path/to/cachesim /path/to/tencentBlock_ns3964.csv csv 3lcache 1347453593,13474535 -t "time-col=1, obj-id-is-num=true, obj-id-col=2, obj-size-col=3"
```

## Examples
```bash
# unzip a trace
unzip ~/3L-Cache/data/tencentBlock_ns3964.zip

~/3L-Cache/_build/bin/cachesim ~/3L-Cache/data/tencentBlock_ns3964.csv csv 3lcache-omr 1347453593 -t "time-col=1, obj-id-is-num=true, obj-id-col=2, obj-size-col=3"
# Output object miss ratio
tencentBlock_ns3964.csv TLCache-OMR cache size     1GiB,         13625211 req, miss ratio 0.3380, throughput 0.59 MQPS

~/3L-Cache/_build/bin/cachesim ~/3L-Cache/data/tencentBlock_ns3964.csv csv 3lcache 1347453593,13474535 -t "time-col=1, obj-id-is-num=true, obj-id-col=2, obj-size-col=3"
#Output object miss ratio and byte miss ratio
result/tencentBlock_ns3964.csv                      TLCache-BMR cache size        1GiB, 13625211 req, miss ratio 0.3421, byte miss ratio 0.1034
result/tencentBlock_ns3964.csv                      TLCache-BMR cache size        0GiB, 13625211 req, miss ratio 0.5300, byte miss ratio 0.6377
```

## Evaluate algorithms through scripts
<dataset_path>  is the path of the dataset; <dataset_info> is a dictionary that records the number of unique bytes for different traces; <algo> is a list containing the caching strategies that need to be measured; <metric> only includes object miss ratio(omr) and byte miss ratio(bmr).
```bash
cd 3L-Cache/scripts
python3 miss_ratio_box_plot.py --dataset_path=<dataset_path>  --dataset_info=<dataset_info> --algo=<eviction_algo> --metric=<metric>

python3 cpu_overhead_box_plot.py --dataset_path=<dataset_path>  --dataset_info=<dataset_info> --algo=<eviction_algo>

```
<!-- The miss_ratio_simulation script can evaluate algorithms on a large number of traces, and the evaluation results will be stored in experiments/scripts/results. -->

This instruction will retrieve the trace under <dataset-path> and conduct experiments to measure the miss ratio. The generated experimental results are kept in 3LCache/scripts/result, and corresponding boxplots are generated in the figures folder.

'''bash
python3 miss_ratio_boxplot.py --algo="['3lcache', 'lecar', 'lhd', 'sieve', 'cacheus', 'gdsf', 'tinylfu', 's3fifo', 'lru','arc']" --dataset_path="../../data/" --dataset_info="./dataset_info.txt" --metric="bmr"
'''

This instruction will retrieve the trace under <dataset-path> and conduct experiments to measure the cpu overhead. The generated experimental results are kept in 3LCache/scripts/, and corresponding boxplots are generated in the figures folder.

'''bash
python3 miss_ratio_boxplot.py --algo="['3lcache', 'lecar', 'lhd', 'sieve', 'cacheus', 'gdsf', 'tinylfu', 's3fifo', 'lru','arc']" --dataset_path="../../data/" --dataset_info="./dataset_info.txt"
'''