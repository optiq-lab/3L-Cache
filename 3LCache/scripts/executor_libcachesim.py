import subprocess
import os
import re
import time
from datetime import datetime
import json
import concurrent.futures
import csv

def lib_command_executor(command):
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()
    except:
        print('错误', command)

import psutil
import os

current_path = os.getcwd()
 
def file_exists_in_directory(directory, filename):
    return os.path.isfile(os.path.join(directory, filename))

if __name__ == "__main__":
    with open(f"{current_path}/trace_info/tencentblock_info.txt", "r") as file:
        trace_info2 = json.loads(file.read())
    with open(f"{current_path}/trace_info/alibaba_info.txt", "r") as file:
        trace_info3 = json.loads(file.read())
    with open(f"{current_path}/trace_info/twitter_info.txt", "r") as file:
        trace_info1 = json.loads(file.read())
    with open(f"{current_path}/trace_info/cloudphysics_info.txt", "r") as file:
        trace_info4 = json.loads(file.read())
    trace_info1.update(trace_info2)
    trace_info1.update(trace_info3)
    trace_info1.update(trace_info4)
    trace_info = trace_info1
    def get_file_sizes(folder_path):
        file_sizes = {}
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                file_sizes[file_name] = file_size
        return file_sizes
    csizes = []
    file_list = []
    webcachefile = []
    webcachefilepath = []
    folder_paths = ['../../data/']
    for folder_path in folder_paths:
        file_sizes = get_file_sizes(folder_path)
        for file_name, file_size in file_sizes.items():
            if file_name.split('.')[-1] == 'csv':
                if file_name not in trace_info:
                    continue
                webcachefile.append(file_name.split('/')[-1])
                csizes.append([trace_info[webcachefile[-1]] // 1000, trace_info[webcachefile[-1]] // 10])
                webcachefilepath.append(folder_path)
                file_list.append(folder_path + file_name)
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        for file, sizes in zip(file_list, csizes):
            cache_strategy = ['gdsf', 'tinylfu', 'arc', '3lcache', 'lecar', 'lhd', 'sieve', 'cacheus', 's3fifo', 'lru']
            for cs in cache_strategy:
                file_type = file.split('.')[-1]
                command = f'../../_build/bin/cachesim {file} {file_type} {cs} {",".join([str(size) for size in sizes])} --num-thread=2 -t "time-col=1, obj-id-col=2, obj-size-col=3, has-header=false, obj-id-is-num=true"'
                executor.submit(lib_command_executor, command) 
                
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        for file, sizes in zip(file_list, csizes):
            cache_strategy = ['gdsf', 'tinylfu', 'arc', '3lcache', 'lecar', 'lhd', 'sieve', 'cacheus', 's3fifo', 'lru']
            for cs in cache_strategy:
                for size in sizes:
                    file_type = file.split('.')[-1]
                    command = f'../../_build/bin/cachesim {file} {file_type} {cs} {size} --num-thread=1 -t "time-col=1, obj-id-col=2, obj-size-col=3, has-header=false, obj-id-is-num=true"'
                    executor.submit(lib_command_executor, command) 