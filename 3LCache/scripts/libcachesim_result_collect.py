import csv
import re
import pandas as pd
import os
import math

current_path = os.getcwd()

def get_mr_result(file_list, csizes, cache_algorthms, metric='bmr', files = ['./result/']):
    key_map = {}   
    key_map = {algo:i for i, algo in enumerate(cache_algorthms)}
    algo_performance = [cache_algorthms]
    for file, cs in zip(file_list, csizes):
        algo_performance.append([-1]*len(cache_algorthms))
        cnt = 0
        for fresult in files:
            if not os.path.exists(f'{fresult}{file}'):
                cnt += 1
                if cnt == 2:
                    algo_performance.pop()
                continue
            with open(f'{fresult}{file}', 'r') as f:
                fcsv = csv.reader(f)
                algo_performance[-1][0] = file
                for row in fcsv:
                    if not row:
                        continue
                    if len(row) < 3:
                        continue
                    re_algo = re.compile(r'\s+(.*?)\s+cache\s+size\s+(.*)', re.S)

                    algo, cachesize = re_algo.findall(row[0])[0]
                    if algo not in cache_algorthms:
                        continue
                    if cachesize[-2:] != 'iB':
                        cs1 = cs
                    elif cachesize[-3:] == 'KiB':
                        cs1 = cs / 1024
                    elif cachesize[-3:] == 'MiB':
                        cs1 = cs / 1024 / 1024
                    elif cachesize[-3:] == 'GiB':
                        cs1 = cs / 1024 / 1024 / 1024
                    elif cachesize[-3:] == 'TiB':
                        cs1 = cs / 1024 / 1024 / 1024 / 1024
                    cs1 = int(cs1)
                    if (cachesize[-2:] != 'iB' and eval(cachesize[:-1]) != cs1) or (cachesize[-2:] == 'iB' and eval(cachesize[:-3]) != cs1):
                        continue

                    re_req = re.compile(r'(\d+)', re.S)
                    req = re_req.findall(row[1])[-1]
                    re_omr = re.compile(r'(\d*\.\d+)', re.S)
                    if len(re_omr.findall(row[3])) > 0:
                        omr = re_omr.findall(row[2])[-1]
                    re_bmr = re.compile(r'(\d*\.\d+)', re.S)
                    if len(re_bmr.findall(row[3])) > 0:
                        bmr = re_bmr.findall(row[3])[-1]
                    if metric == 'omr':
                        algo_performance[-1][key_map[algo]] = omr
                    elif metric == 'bmr':
                        algo_performance[-1][key_map[algo]] = bmr
                    else:
                        print('error')

                    algo_performance[-1][1] = cachesize
                    algo_performance[-1][2] = req
    return algo_performance

def get_tp_result(file_list, csizes, cache_algorthms):
    key_map = {}     
    key_map = {algo:i for i, algo in enumerate(cache_algorthms)}
    algo_performance = [cache_algorthms]
    for trace, cs in zip(file_list, csizes):
        algo_performance.append([-1]*len(cache_algorthms))
        algo_performance[-1][0] = trace
        if not os.path.isfile(f'{current_path}/{trace}.cachesim'):
            continue
        with open(f'{current_path}/{trace}.cachesim', 'r') as f:
            fcsv = csv.reader(f)
            algo_performance[-1][0] = trace
            for row in fcsv:
                if not row:
                    continue
                if len(row) < 3:
                    continue
                re_algo = re.compile(r'\s+(.*?)\s+cache\s+size\s+(.*)', re.S)

                algo, cachesize = re_algo.findall(row[0])[0]
                if algo not in cache_algorthms:
                    continue
                if cachesize[-2:] != 'iB':
                    cs1 = cs
                elif cachesize[-3:] == 'KiB':
                    cs1 = cs / 1024
                elif cachesize[-3:] == 'MiB':
                    cs1 = cs / 1024 / 1024
                elif cachesize[-3:] == 'GiB':
                    cs1 = cs / 1024 / 1024 / 1024
                elif cachesize[-3:] == 'TiB':
                    cs1 = cs / 1024 / 1024 / 1024 / 1024
                cs1 = round(cs1)
                if (cachesize[-2:] != 'iB' and eval(cachesize[:-1]) != cs1) or (cachesize[-2:] == 'iB' and eval(cachesize[:-3]) != cs1):
                    continue
                tp = re.compile(r'throughput\s+(.*?)\s+MQPS', re.S)
                tp = tp.findall(str(row))
                if tp:
                    tp = tp[0]                
                    algo_performance[-1][key_map[algo]] = tp
                algo_performance[-1][1] = cachesize
                algo_performance[-1][2] = row[1]
    return algo_performance


def get_trace_info(trace_info = []):
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
    return trace_info

def get_cache_size(dataset, trace_info, proportion = 0.001, file_type = 'csv'):
    traces = dataset.keys()
    file_list = []
    csizes = []
    for trace in traces:
        if os.path.isfile(os.path.join('../../data/', trace)):
            csizes.append(math.ceil(trace_info[trace] * proportion))
            file_list.append(trace)
    return csizes, file_list
    
from openpyxl import Workbook
import json
def write_to_excel(arr, filename):
    wb = Workbook()
    ws = wb.active

    for row in arr:
        ws.append(row)

    wb.save('./excel/' + filename)

import pandas as pd

def mr_result(cache_algorthms):
    trace_info = get_trace_info()
    trace_paths = ['alibaba', 'twitter', 'cloudphysics', 'tencentblock']
    name = ''
    for metric in ['bmr', 'omr']:
        for trace_path in trace_paths:
            dataset_path = f"{current_path}/trace_info/{trace_path}_info.txt"
            with open(dataset_path, "r") as file:
                dataset = json.loads(file.read())
            csizes, file_list = get_cache_size(dataset, trace_info, 0.001)
            algo_performance = get_mr_result(file_list, csizes, cache_algorthms, metric)
            write_to_excel(algo_performance, f'{trace_path}_{name}bmr_1000.xlsx')

            
            csizes, file_list = get_cache_size(dataset, trace_info, 0.1)
            algo_performance = get_mr_result(file_list, csizes, cache_algorthms, metric)
            write_to_excel(algo_performance, f'{trace_path}_{name}bmr_10.xlsx')

def tp_result(cache_algorthms):
    trace_info = get_trace_info()
    trace_paths = ['alibaba', 'twitter', 'cloudphysics', 'tencentblock']
    name = 'tp'
    csizes = []
    file_list = []
    
    for prop in [0.1, 0.001]:
        for trace_path in trace_paths:
            dataset_path = f"{current_path}/trace_info/{trace_path}_info.txt"
            with open(dataset_path, "r") as file:
                dataset = json.loads(file.read())
            cs, fl = get_cache_size(dataset, trace_info, prop)
            csizes.extend(cs)
            file_list.extend(fl)
        if prop == 0.001:
            algo_performance = get_tp_result(file_list, csizes, cache_algorthms)
            write_to_excel(algo_performance, f'{name}_1000.xlsx')
        else:
            algo_performance = get_tp_result(file_list, csizes, cache_algorthms)
            write_to_excel(algo_performance, f'{name}_10.xlsx')

if __name__ == "__main__":
    cache_algorthms = ['Trace', 'Cache Size', 'Trace Size', 'LHD', 'GDSF', 'ARC', 'Sieve', 'S3FIFO-0.1000-2', 'WTinyLFU-w0.01-SLRU', 'LeCaR', 'Cacheus', 'TLCache-BMR', 'LRU']
    mr_result(cache_algorthms)
    tp_result(cache_algorthms)