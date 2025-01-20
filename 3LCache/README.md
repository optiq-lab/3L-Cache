
# 3L-Cache README

## 3L-Cache Algorithm
The implementation files of 3L-Cache mainly include TLCache.cpp and TLCache.h, while other files mainly include interfaces.
In the TLCache.cpp file, we mainly implemented the lookup(), admit(), and evict() interfaces.
### lookup()
When a request arrives, the lookup() interface is called to check if the requested object is in the sliding window. If so, update the metadata of the object. If the object hits, update the cache queue. Finally, the interface will return the hit status of the request.

### admit()
When the requested object is not hit, call the admit() interface. The admit() interface will insert the new object into the cache queue.

### evict()
When the cache space is insufficient to cache a new object, call the evict() interface to evict the cached object. If the model is not trained properly, the LRU policy is executed to evict the object with the longest waiting time. If the model has already been trained, sample and eviction candidates, and perform prediction and eviction. the evict() interface will be continuously called until the cache space can accommodate new objects.

## Script
The Scripts folder contains some data and executed scripts.

### 3LCache/scripts/excel
Used to save experimental results. 
Collect experimental results and record metrics such as the miss ratio of each trace under different caching policies, and save them as an Excel file.

### 3LCache/scripts/figures
Used to save the generated experimental result figure.


### 3LCache/scripts/trace_info
Save the information of the dataset for determining the cache size during the experiment. The file in the trace_info folder records the number of unique bytes(the minimum cache size required to store the entire trace). It is composed of a dictionary, where the key represents the name of the trace and the value represents the number of unique bytes.

### miss_ratio_boxplot.py
```bash

# This Python file can be executed to conduct experiments under the specified folder's traces (dataset_path), cache size (dataset_info), and eviction policy (eviction_algo).
# It measures the byte miss ratio (metric="bmr") and object miss ratio (metric="omr"), collecting the experimental results, and drawing a box plot.
# The generated experimental results are kept in 3LCache/scripts/result, and corresponding boxplots are generated in the figures folder.
# Execute the command as follows:
python3 miss_ratio_boxplot.py --dataset_path=<dataset_path>  --dataset_info=<dataset_info> --algo=<eviction_algo> --metric=<metric>

# Example
python3 miss_ratio_boxplot.py --algo="['3lcache', 'lecar', 'lhd', 'sieve', 'cacheus', 'gdsf', 'tinylfu', 's3fifo', 'lru','arc']" --dataset_path="../../data/" --dataset_info="./trace_info/dataset_info.txt" --metric="bmr"
```

### cpu_overhead_boxplot.py
```bash

# This Python file can be executed to conduct experiments under the specified folder's traces (dataset_path), cache size (dataset_info), and eviction policy (eviction_algo).
# It measures the cpu overhead, collecting the experimental results, and drawing a box plot.
# The generated experimental results are kept in 3LCache/scripts/result, and corresponding boxplots are generated in the figures folder.
# Execute the command as follows:
python3 cpu_overhead_boxplot.py --dataset_path=<dataset_path>  --dataset_info=<dataset_info> --algo=<eviction_algo>

# Example
python3 cpu_overhead_boxplot.py --algo="['3lcache', 'lecar', 'lhd', 'sieve', 'cacheus', 'gdsf', 'tinylfu', 's3fifo', 'lru','arc']" --dataset_path="../../data/" --dataset_info="./trace_info/dataset_info.txt"
```

### run_scripts.sh
```bash
# Executing the run_stcripts.sh script will sequentially execute the executors_libcachesim.py, libcachesim_result_collect.py, and draw_figure.py files in the current folder. 
# Their functions are to conduct experiments on specific traces, collect experimental results, and convert them into box plots.

# Execute the command as follows:
./run_scripts.sh
```