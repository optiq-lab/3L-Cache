import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, MaxNLocator, MultipleLocator
import os
current_path = os.getcwd()
# 修改字体样式
benchmark_algo = 'LRU'

markers = ['d', '^', '*', '>', 'o', 's', '*', 'p', 'h', '.']
def draw_scatter(xlabels, ylabels, files, algorithms, filename, traces = ['Tencent Photo', 'Wikipedia'], fontsize=28):
    key_map={}
    for algo in algorithms:
        if algo[:3] == 'LRB':
            key_map[algo] = 'LRB'
        elif algo[:4] == 'HALP':
            key_map[algo] = 'HALP'
        elif algo[:7] == 'TLCache':
            key_map[algo] = '3L-Cache'
        elif algo[:7] == 'Cacheus':
            key_map[algo] = 'CACHEUS'
        elif algo[:7] == 'Sieve':
            key_map[algo] = 'SIEVE'
        elif algo[:7] == 'GLCache':
            key_map[algo] = 'GL-Cache'
        else:
            key_map[algo] = algo
    colors = ['r', 'b', 'g']
    mpl.rcParams['font.size'] = fontsize
    x = [i for i in range(1, 1 + len(algorithms))]
    plt.figure(figsize=(16 * len(files), 6))
    for i, f in enumerate(files):
        plt.subplot(1, len(files), i + 1)
        df = pd.read_excel(f, sheet_name='Sheet')

        baseline = df[benchmark_algo].to_numpy().reshape(-1, 1)

        df[algorithms] = (baseline - df[algorithms]) / baseline
        
        y = df[algorithms]
        # yticks = []
        alphas = [1, 1]
        for j in range(0, df.shape[0]):
            if j > 1:
                plt.scatter(x, list(y.iloc[j, :]), s=300, c=colors[0], marker='>', alpha=alphas[0])
                alphas[0] /= 2
            else:
                plt.scatter(x, list(y.iloc[j, :]), s=300, c=colors[1], marker='s', alpha=alphas[1])
                alphas[1] /= 2
        
        plt.xlabel(xlabels[i])
        plt.ylabel(ylabels[i])
        xticks = [key_map[algo] for algo in algorithms]
        plt.xticks([i + 1 for i in range(len(algorithms))], xticks)
        plt.grid(True, linestyle='--')
        plt.gca().set_xticklabels(xticks, rotation=45)
    legend_elements = []
    for i, trace in enumerate(traces):
        legend_elements.append(Line2D([0], [0], marker='s', color=colors[i], markersize=30,  label=trace))

    plt.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(-0.05, 1.15), ncol=len(traces), prop={'size': 24}, frameon=False, handlelength=0)
    plt.savefig(filename, format='pdf', dpi=900, bbox_inches='tight')

def two_decimal_places(x, pos):
    return f'{x:.2f}'

def draw_box_plot(xlabels, ylabels, files, algorithms, s_idx, metric, fontsize=40):
    key_map={}
    for algo in algorithms:
        if algo[:3] == 'LRB':
            key_map[algo] = 'LRB'
        elif algo[:4] == 'HALP':
            key_map[algo] = 'HALP'
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
    for i, f in enumerate(files):
        plt.subplot(metric, len(files), i + s_idx)
        df = pd.read_excel(f, sheet_name=0)
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
        if i == 0 and i + s_idx < len(files):
            ylabel = plt.ylabel(ylabels[i])
        flattened_data = [item for sublist in y for item in sublist]
        y = np.array(flattened_data)
        p25 = np.percentile(y, 10)
        p25 = int(p25 * 100) / 100
        pmax = np.percentile(y, 90)
        if  pmax - p25 < 0.06:
            plt.gca().yaxis.set_major_locator(MultipleLocator(0.01))
        elif pmax - p25 < 0.3:
            plt.gca().yaxis.set_major_locator(MultipleLocator(0.05)) 
        elif pmax - p25 < 0.7:
            plt.gca().yaxis.set_major_locator(MultipleLocator(0.1)) 
        else:
            plt.gca().yaxis.set_major_locator(MultipleLocator(0.1)) 
        
        plt.gca().yaxis.set_major_formatter(FuncFormatter(two_decimal_places))
        if i == 0:
            plt.gca().yaxis.set_label_coords(-0.1, -0.35) 
        xticks = [key_map[algo] for algo in algorithms]
        plt.xticks([i + 1 for i in range(len(algorithms))], xticks, fontsize=int(fontsize * 0.95))
        plt.xlabel(xlabels[i], fontsize=int(fontsize*1.05))
        plt.gca().set_xticklabels(xticks, rotation=45)
        plt.grid(True, linestyle='--')
    
        plt.ylim(p25)


def draw_miss_ratio_figure():
    mpl.rcParams['font.family'] = 'Nimbus Roman'
    mpl.rcParams['font.size'] = 40
    mpl.rcParams['pdf.fonttype'] = 42
    mpl.rcParams['ps.fonttype'] = 42
    algorithms = ['LHD', 'GDSF', 'ARC', 'Sieve', 'S3FIFO-0.1000-2', 'WTinyLFU-w0.01-SLRU', 'LeCaR', 'Cacheus', 'TLCache-BMR']
    metrics =  ['bmr']
    for i, metric in enumerate(metrics):
        for smetric in metrics:
            files = [f"{current_path}/excel/cloudphysics_{metric}_1000.xlsx", 
                    f"{current_path}/excel/tencentblock_{metric}_1000.xlsx", 
                    f"{current_path}/excel/twitter_{metric}_1000.xlsx",
                    f"{current_path}/excel/alibaba_{metric}_1000.xlsx",
                    f"{current_path}/excel/cloudphysics_{metric}_10.xlsx", 
                    f"{current_path}/excel/tencentblock_{metric}_10.xlsx", 
                    f"{current_path}/excel/twitter_{metric}_10.xlsx",
                    f"{current_path}/excel/alibaba_{metric}_10.xlsx",
                    ]
            xlabels = ['(a) CloudPhysics, small cache size', '(b) Tencent CBS, small cache size', '(c) Twitter, small cache size', '(d) Alibaba, small cache size',
                    '(e) CloudPhysics, large cache size', '(f) Tencent CBS, large cache size', '(g) Twitter, large cache size', '(h) Alibaba, large cache size']
            if metric == 'bmr':
                ylabels = [f'Byte miss ratio reduction from {benchmark_algo}'] * len(files)
            else:
                ylabels = [f'Object miss ratio reduction from {benchmark_algo}'] * len(files)
            plt.figure(figsize=(16 * len(files), 32))
            cnt = 1
            plt.subplots_adjust(hspace=0.5, wspace=0.12)
            for i, j in [[0, 1000], [len(files)//2, 10]]:
                draw_box_plot(xlabels[i: i+ len(files) // 2], ylabels[i: i+len(files) // 2], files[i: i+len(files) // 2], algorithms, cnt, 2, fontsize=68)
                cnt += len(files)//2
            plt.savefig(f'./figures/{metric}.pdf', format='pdf', dpi=900, bbox_inches='tight')


def draw_tp_box_plot(f, algorithms, ax1, ax2, cnt, fontsize=40):
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
    df = pd.read_excel(f, sheet_name=0)
    mpl.rcParams['font.size'] = fontsize
    x = [i for i in range(1, 1 + len(algorithms))]
    baseline = df[benchmark_algo].to_numpy().reshape(-1, 1)
    increase_over_baseline = []
    algo_mean = []
    for algo in algorithms:
        positions = df.index[df[algo] != -1].tolist()
        y = baseline / df[algo].to_numpy()
        y = y[positions]
        increase_over_baseline.append(y.reshape(-1,).tolist())
        algo_mean.append(sum(increase_over_baseline[-1]) / len(increase_over_baseline[-1]))
    ax1[cnt].scatter(x, algo_mean, marker='v', s=50, color='#EF949E', edgecolor='#C81D31')
    ax2[cnt].scatter(x, algo_mean, marker='v', s=50, color='#EF949E', edgecolor='#C81D31')
    ax1[cnt].boxplot(increase_over_baseline, showfliers=False, whis=(10, 90))
    ax2[cnt].boxplot(increase_over_baseline, showfliers=False, whis=(10, 90))
    if cnt == 1:
        ax2[1].set_xlabel('(b) Large cache size', fontsize = 44)
    else:
        ax2[1].set_xlabel('(a) Small cache size', fontsize = 44)
    ax2[cnt].set_ylabel(f'CPU overhead relative to {benchmark_algo}')
    xticks = [key_map[algo] for algo in algorithms]
    ax2[cnt].set_xticks([i for i in range(1, len(algorithms) + 1)], xticks)
    ax2[cnt].set_ylim(0, 10)
    ax1[cnt].set_ylim(10, 300)
    ax2[cnt].set_xticklabels(xticks, rotation=45)
    ax1[cnt].grid(True, linestyle='--')
    ax2[cnt].grid(True, linestyle='--')

    ax2[1].yaxis.set_label_coords(-0.08, 0.8)
    ax2[0].yaxis.set_label_coords(-0.08, 0.8)
    plt.tight_layout()
    plt.subplots_adjust(hspace=0.1)

def draw_tp():
    mpl.rcParams['font.family'] = 'Nimbus Roman'
    mpl.rcParams['font.size'] = 40
    mpl.rcParams['pdf.fonttype'] = 42
    mpl.rcParams['ps.fonttype'] = 42
    xlabels = ['Small cache size', 'Large cache size']
    ylabel = 'CPU overhead relative to LRU'
    algorithms = ['LHD', 'GDSF', 'ARC', 'Sieve', 'S3FIFO-0.1000-2', 'WTinyLFU-w0.01-SLRU', 'LeCaR', 'Cacheus', 'TLCache-BMR']
    fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=2, sharex=True, gridspec_kw={'height_ratios': [1, 2]}, figsize=(16*2, 12))
    files = [f'{current_path}/excel/tp_1000.xlsx', f'{current_path}/excel/tp_10.xlsx']
    for i, f in enumerate(files):
        draw_tp_box_plot(f, algorithms, ax1, ax2, i, fontsize=40)
    plt.savefig(f'./figures/cpu_overhead.pdf', format='pdf', dpi=900, bbox_inches='tight')

import csv
if __name__ == "__main__":
    draw_miss_ratio_figure()
    draw_tp()