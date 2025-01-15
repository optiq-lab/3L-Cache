import csv
import re
import pandas as pd
import os
import subprocess
import math
import concurrent.futures
import json
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, MaxNLocator, MultipleLocator
import numpy as np
# 修改字体样式
mpl.rcParams['font.family'] = 'Nimbus Roman'

# 修改字体大小
mpl.rcParams['font.size'] = 40
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
benchmark_algo = 'LRU'
markers = ['d', '^', '*', '>', 'o', 's', '*', 'p', 'h', '.']

def get_mr_result(file_list, csizes, cache_algorthms, metric='bmr'):
    key_map = {}   
    key_map = {algo:i for i, algo in enumerate(cache_algorthms)}
    algo_performance = [cache_algorthms]
    for file, cs in zip(file_list, csizes):
        algo_performance.append([-1]*len(cache_algorthms))
        # print(f'./result/{file}')
        with open(f'./result/{file}', 'r') as f:
            fcsv = csv.reader(f)
            # algo_performance[-1][0] = file
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
                # print(cs)
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
                    algo_performance[-1][key_map[algo]] = eval(omr)
                elif metric == 'bmr':
                    algo_performance[-1][key_map[algo]] = eval(bmr)
                else:
                    print('指标输入错误')

                # algo_performance[-1][1] = cachesize
                # algo_performance[-1][2] = req
    return algo_performance

def lib_command_executor(command):
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()
    except:
        print('错误', command)

def get_cache_size(dataset_path, dataset_info):
    file_list = []
    csizes = []
    for file in os.listdir(dataset_path):
        file = file.replace('_', '.')
        if not os.path.isdir(os.path.join(dataset_path, file)) and file in dataset_info.keys():
            csizes.append([math.ceil(dataset_info[file] * 0.001), math.ceil(dataset_info[file] * 0.1)])
            file_list.append(file)
    return csizes, file_list

def two_decimal_places(x, pos):
    return f'{x:.2f}'

def draw_box_plot(xlabels, ylabels, algorithms, df, fontsize=40):
    while benchmark_algo in algorithms:
        algorithms.remove(benchmark_algo)
    
    key_map={}
    for algo in algorithms:
        if algo[:3] == 'LRB':
            key_map[algo] = 'LRB'
        elif algo[:7] == 'TLCache':
            key_map[algo] = '3L-Cache'
        elif algo[:7] == 'Cacheus':
            key_map[algo] = 'CACHEUS'
        elif algo[:7] == 'GLCache':
            key_map[algo] = 'GL-Cache'
        elif algo[:7] == 'Sieve':
            key_map[algo] = 'SIEVE'
        elif algo[:8] == 'WTinyLFU':
            key_map[algo] = 'TinyLFU'
        elif algo[:6] == 'S3FIFO':
            key_map[algo] = 'S3-FIFO'
        else:
            key_map[algo] = algo
    mpl.rcParams['font.size'] = fontsize
    x = [i for i in range(1, 1 + len(algorithms))]
    print( df[benchmark_algo])
    baseline = df[benchmark_algo].to_numpy().reshape(-1, 1)
    
    y = []
    for algo in algorithms:
        positions = df.index[df[algo] != -1].tolist()
        columns_data = df[algo].to_numpy().reshape(-1, 1)
        columns_data = (baseline - columns_data) / baseline
        y.append(columns_data[positions].reshape(-1,).tolist())
    plt.boxplot(y, showfliers=False, whis=(10, 90))
    mean_filtered_values = []
    for col in y:
        mean_filtered_values.append(sum(col)/len(col))
    plt.scatter(x, mean_filtered_values, marker='v', s=100, color='#EF949E', edgecolor='#C81D31')
    plt.ylabel(ylabels)
    flattened_data = [item for sublist in y for item in sublist]
    y = np.array(flattened_data)
    p25 = np.percentile(y, 10)
    p25 = int(p25 * 100) / 100
    pmax = np.percentile(y, 90)
    if  pmax - p25 < 0.06:
        plt.gca().yaxis.set_major_locator(MultipleLocator(0.01))  # 自动调整刻度
    elif pmax - p25 < 0.3:
        plt.gca().yaxis.set_major_locator(MultipleLocator(0.05))  # 自动调整刻度
    elif pmax - p25 < 0.7:
        plt.gca().yaxis.set_major_locator(MultipleLocator(0.1))  # 自动调整刻度
    else:
        plt.gca().yaxis.set_major_locator(MultipleLocator(0.1))  # 自动调整刻度
    
    plt.gca().yaxis.set_major_formatter(FuncFormatter(two_decimal_places))
    xticks = [key_map[algo] for algo in algorithms]
    plt.xticks([i + 1 for i in range(len(algorithms))], xticks, fontsize=int(fontsize * 0.95))
    plt.xlabel(xlabels, fontsize=int(fontsize*1.05))
    plt.gca().set_xticklabels(xticks, rotation=45)
    plt.grid(True, linestyle='--')

    plt.ylim(p25)
import argparse
if __name__ == "__main__":
    file_path = os.path.abspath('../../')
    file_path = file_path.replace('\\', '/')
    cachesim_file_path = f'{file_path}/_build/bin/cachesim'
    # 创建解析器
    parser = argparse.ArgumentParser(description="Parse command-line arguments.")
    # 添加参数，设置默认值
    parser.add_argument('--algo', type=str, default="", help="The algorithm to use, default is ['FIFO','LRU','sieve'].")
    parser.add_argument('--dataset_path', type=str, default="", help="The storage path of trace.")
    parser.add_argument('--dataset_info', type=str, default="")
    parser.add_argument('--metric', type=str, default="bmr", help="The default is bmr.")
    args = parser.parse_args()

    if args.dataset_info == "":
        dataset_info_path = './dataset_info.txt'
    else:
        dataset_info_path = args.dataset_info
    if args.dataset_path == "":
        dataset_path = '../../data/'
    else:
        dataset_path = args.dataset_path
    if args.algo == "":
        cache_strategy = ['3lcache', 'lecar', 'lhd', 'sieve', 'cacheus', 'gdsf', 'tinylfu', 's3fifo', 'lru','arc']
    else:
        cache_strategy = eval(args.algo)
    # print(cache_strategy)
    with open(dataset_info_path, "r") as file:
        dataset_info = json.loads(file.read())
    
    csizes, file_list = get_cache_size(dataset_path, dataset_info)
    

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        for sizes, file in zip(csizes, file_list):
            for cs in cache_strategy:
                sizes = [str(size) for size in sizes]
                command = f'{cachesim_file_path} {dataset_path}{file} csv {cs} {",".join(sizes)} -t "time-col=1, obj-id-is-num=true, obj-id-col=2, obj-size-col=3, has-header=false, delimiter=\\t"'
                executor.submit(lib_command_executor, command)
    algorithms = []
    for cs in cache_strategy:
        if cs.lower() == '3lcache':
            algorithms.append('TLCache-BMR')
        elif cs.lower() == '3lcache-omr':
            algorithms.append('TLCache-OMR')
        elif cs.lower() == 'lhd':
            algorithms.append('LHD')
        elif cs.lower() == 'gdsf':
            algorithms.append('GDSF')
        elif cs.lower() == 'arc':
            algorithms.append('ARC')
        elif cs.lower() == 'sieve':
            algorithms.append('Sieve')
        elif cs.lower() == 's3fifo':
            algorithms.append('S3FIFO-0.1000-2')
        elif cs.lower() == 'tinylfu':
            algorithms.append('WTinyLFU-w0.01-SLRU')
        elif cs.lower() == 'lecar':
            algorithms.append('LeCaR')
        elif cs.lower() == 'cacheus':
            algorithms.append('Cacheus')
        elif cs.lower() == 'lru':
            algorithms.append('LRU')
        elif cs.lower() == 'fifo':
            algorithms.append('FIFO')
        elif cs.lower() == 'glcache':
            algorithms.append('GLCache')
    # algorithms = ['LHD', 'GDSF', 'ARC', 'Sieve', 'S3FIFO-0.1000-2', 'WTinyLFU-w0.01-SLRU', 'LeCaR', 'Cacheus', 'TLCache-BMR', 'LRU']  
    small_cache_sizes = []
    large_cache_sizes = []
    for cs in csizes:
        small_cache_sizes.append(cs[0])
        large_cache_sizes.append(cs[1])
    
    plt.figure(figsize=(16, 9))
    xlabels = ['Small cache size', 'Large cache size']
    if args.metric == "bmr":
        ylabel = 'Byte miss ratio reduction from LRU'
    else:
        ylabel = 'Object miss ratio reduction from LRU'
    for i, xlabel in enumerate(xlabels):
        if benchmark_algo not in algorithms:
            algorithms.append(benchmark_algo)
        results = get_mr_result(file_list, small_cache_sizes, algorithms, args.metric)
        print(results)
        df = pd.DataFrame(results[1:], columns=results[0])
        print(df)
        draw_box_plot(xlabels[i], ylabel, algorithms, df, fontsize=40)
        if i == 0:
            plt.savefig(f'./figures/{args.metric}_for_small_cache_size.pdf', format='pdf', dpi=900, bbox_inches='tight')
        else:
            plt.savefig(f'./figures/{args.metric}_for_large_cache_size.pdf', format='pdf', dpi=900, bbox_inches='tight')
