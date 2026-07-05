#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
薄膜加工产线膜厚云图绘制工具
专门用于绘制两小时间间隔的膜厚热力图
采用IEEE学术期刊风格
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.colors import LinearSegmentedColormap
from pathlib import Path
import warnings
from typing import List, Tuple, Dict
import glob
from datetime import datetime, timedelta

# 设置matplotlib支持中文显示
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.figsize'] = (15, 10)

# --- IEEE学术风格设置 ---
# 优先使用Times New Roman字体
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Times New Roman', 'DejaVu Serif', 'Liberation Serif', 'Ubuntu Serif', 'serif']
rcParams['axes.unicode_minus'] = False  # 解决负号显示为方块的问题
rcParams['axes.linewidth'] = 0.8
rcParams['grid.linewidth'] = 0.5

# 刻度线全部向内，且上下左右都显示
rcParams['xtick.direction'] = 'in'
rcParams['ytick.direction'] = 'in'
rcParams['xtick.top'] = True
rcParams['ytick.right'] = True
# 字体大小设置
rcParams['axes.labelsize'] = 10
rcParams['xtick.labelsize'] = 9
rcParams['ytick.labelsize'] = 9
rcParams['legend.fontsize'] = 8

# 其他样式设置
rcParams['figure.dpi'] = 600
rcParams['savefig.dpi'] = 600
rcParams['savefig.bbox'] = 'tight'
rcParams['savefig.facecolor'] = 'white'

warnings.filterwarnings('ignore')

# 创建蓝绿红渐变色图（蓝-绿-红，绿色在目标值）
# 蓝色：低于目标值，绿色：在目标值范围内，红色：超过目标值
colors_blue_green_red = ['#0000FF', '#0080FF', '#00FF00', '#FFFF00', '#FF8000', '#FF0000']
cmap_blue_green_red = LinearSegmentedColormap.from_list('blue_green_red', colors_blue_green_red, N=256)


class HeatmapGenerator:
    """膜厚热力图生成器"""
    
    def __init__(self, data_dir: str):
        """
        初始化热力图生成器
        
        Args:
            data_dir: transposed数据文件夹路径
        """
        self.data_dir = Path(data_dir)
        self.data_dict = {}
        
    def load_data(self) -> Dict[str, pd.DataFrame]:
        """
        加载所有transposed文件数据，只取一半列（探头单程检测）
        
        Returns:
            字典：文件名 -> DataFrame
        """
        print("正在加载数据文件...")
        
        # 查找所有transposed CSV文件
        pattern = str(self.data_dir / "trip_transposed_*_merged.csv")
        csv_files = glob.glob(pattern)
        
        if not csv_files:
            raise FileNotFoundError(f"在 {self.data_dir} 中未找到transposed CSV文件")
        
        print(f"找到 {len(csv_files)} 个数据文件")
        
        for file_path in csv_files:
            file_name = Path(file_path).name
            print(f"正在处理文件: {file_name}")
            
            # 读取数据
            df = pd.read_csv(file_path)
            
            # 只取一半列（探头单程检测，避免往返重复）
            # 假设总列数为N，其中第1列是time，后面N-1列是检测点位
            total_columns = len(df.columns)
            half_columns = total_columns // 2
            
            # 保留time列和前一半检测点列
            columns_to_keep = ['time'] + list(df.columns[1:half_columns+1])
            df_half = df[columns_to_keep].copy()
            
            # 转换时间列
            df_half['time'] = pd.to_datetime(df_half['time'])
            df_half = df_half.set_index('time')
            
            # 清理数据：移除包含NaN的行
            df_half = df_half.dropna()
            
            self.data_dict[file_name] = df_half
            print(f"  - 数据形状: {df_half.shape}")
            print(f"  - 时间范围: {df_half.index.min()} 到 {df_half.index.max()}")
            print(f"  - 检测点数: {len(df_half.columns)} 个点位")
        
        return self.data_dict
    
    def _split_data_by_2hour_intervals(self, df: pd.DataFrame) -> List[Tuple]:
        """
        将数据按两小时间间隔分割
        
        Args:
            df: 时间序列数据
            
        Returns:
            列表：(开始时间, 结束时间, DataFrame)
        """
        intervals = []
        
        if len(df) == 0:
            return intervals
        
        start_time = df.index.min()
        end_time = df.index.max()
        
        current_time = start_time
        
        interval_count = 0
        while current_time < end_time:
            interval_end = current_time + timedelta(hours=2)
            
            # 提取当前两小时的数据
            interval_data = df[(df.index >= current_time) & (df.index < interval_end)]
            
            if len(interval_data) > 0:  # 只保存有数据的间隔
                intervals.append((current_time, interval_end, interval_data))
            
            current_time = interval_end
            interval_count += 1
            
            # 防止无限循环
            if interval_count > 1000:
                break
        
        return intervals
    
    def plot_2hour_interval_heatmaps(self, output_dir: str = None) -> Dict[str, List[str]]:
        """
        每两小时生成一张热力图，按文件分类保存
        纯云图，不带任何文字、图例、坐标轴标签
        
        Args:
            output_dir: 根输出目录，默认为2hour_interval_heatmaps
            每个文件的图片保存在各自的子文件夹中
            
        Returns:
            字典：文件名 -> 生成的文件路径列表
        """
        print("开始生成两小时间间隔热力图（纯云图）...")
        print("=" * 60)
        
        # 设置输出目录
        if output_dir is None:
            output_dir = self.data_dir.parent / "data" / "data" / "2hour_interval_heatmaps"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        generated_files = {}  # 记录每个文件生成的文件
        
        # 膜厚标准值和异常阈值
        target_thickness = 0.045
        threshold = 0.00225  # 5%的容差范围，即0.45±0.0225
        
        for file_name, df in self.data_dict.items():
            print(f"\n正在处理文件: {file_name}")
            
            # 为每个文件创建单独的文件夹
            file_base_name = Path(file_name).stem  # 去掉.csv扩展名
            file_output_dir = output_dir / file_base_name
            file_output_dir.mkdir(parents=True, exist_ok=True)
            
            # 创建正常(0)和异常(1)子文件夹
            normal_dir = file_output_dir / "0"
            normal_dir.mkdir(parents=True, exist_ok=True)
            
            abnormal_dir = file_output_dir / "1"
            abnormal_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"  文件输出目录: {file_output_dir}")
            print(f"  正常云图保存到: {normal_dir}")
            print(f"  异常云图保存到: {abnormal_dir}")
            
            # 将数据按两小时间间隔分割
            two_hour_windows = self._split_data_by_2hour_intervals(df)
            
            print(f"  数据时间范围: {df.index.min()} 到 {df.index.max()}")
            print(f"  分割为 {len(two_hour_windows)} 个两小时间间隔")
            
            generated_files[file_name] = []
            
            # 为每个两小时间间隔生成热力图
            for i, (start_time, end_time, window_data) in enumerate(two_hour_windows):
                if len(window_data) == 0:
                    continue
                    
                # 计算当前时间段的均值
                window_mean = window_data.values.mean()
                
                # 判断是否异常（均值与目标值相差较大）
                is_abnormal = abs(window_mean - target_thickness) > threshold
                
                # 生成纯云图 - 不带任何文字和图例
                fig, ax = plt.subplots(figsize=(10, 6))
                
                # 创建多子图布局：上方6个加工参数图，下方1个热力图
                # 高度比例：加工参数图各占1，热力图占2.5（增加y轴长度）
                fig, axes = plt.subplots(7, 1, figsize=(14, 12), sharex=True,
                                       gridspec_kw={'height_ratios': [1, 1, 1, 1, 1, 1, 2.5]})
                
                # 设置颜色范围，以目标值为中心（±0.005，即±11.1%）
                # 这样目标值0.045正好在颜色范围的中间
                vmin = target_thickness - 0.005
                vmax = target_thickness + 0.005
                
                # 创建热力图 - 蓝绿红配色
                im = ax.imshow(window_data.values, 
                              aspect='auto', 
                              cmap=cmap_blue_green_red, 
                              vmin=vmin, 
                              vmax=vmax,
                              interpolation='nearest')
                
                # 移除所有文字、标题、坐标轴标签、刻度
                ax.set_title('')
                ax.set_xlabel('')
                ax.set_ylabel('')
                ax.set_xticks([])
                ax.set_yticks([])
                
                # 移除坐标轴边框
                for spine in ax.spines.values():
                    spine.set_visible(False)
                
                # 不添加颜色条
                # 不添加网格线
                # 不添加任何文字信息
                
                # 调整布局，移除所有边距
                plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
                
                # 生成文件名
                time_start_str = start_time.strftime('%Y%m%d_%H%M')
                time_end_str = end_time.strftime('%Y%m%d_%H%M')
                time_range_str = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%H:%M')}"
                status_str = "Abnormal" if is_abnormal else "Normal"
                
                # 根据异常状态选择保存目录
                save_dir = abnormal_dir if is_abnormal else normal_dir
                output_file = save_dir / f"{file_base_name}_interval_{i+1:02d}_{time_start_str}-{time_end_str}.png"
                
                # 保存图片
                plt.savefig(output_file, dpi=300, bbox_inches='tight', 
                           facecolor='white', edgecolor='none', pad_inches=0)
                
                print(f"    Interval {i+1}: {time_range_str} -> {output_file.name} (Mean: {window_mean:.4f}, Status: {status_str})")
                
                generated_files[file_name].append(str(output_file))
                
                plt.close(fig)  # 关闭图形以释放内存
        
        print(f"\n所有两小时间间隔热力图生成完成！")
        print(f"图片保存在根目录: {output_dir}")
        print("每个文件保存在各自的子文件夹中")
        print("正常云图保存在子文件夹'0'中，异常云图保存在子文件夹'1'中")
        print("文件名格式: [文件名]_interval_[序号]_[开始时间]-[结束时间].png")
        
        return generated_files
    
    def _simulate_process_parameters(self, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
        """
        模拟加工参数数据，每隔20秒保存一个数据点
        
        Args:
            start_time: 起始时间
            end_time: 结束时间
            
        Returns:
            DataFrame: 包含时间戳和各加工参数的数据
        """
        # 生成时间轴，每隔20秒一个点
        time_axis = pd.date_range(start=start_time, end=end_time, freq='20s')
        time_points = len(time_axis)
        
        # 计算持续时间（秒）
        duration_seconds = (end_time - start_time).total_seconds()
        t = np.linspace(0, duration_seconds, time_points)
        
        # 模拟1: 主电机速度 (rpm) - 基础值+波动+周期变化
        motor_speed = 1500 + 50 * np.sin(2 * np.pi * t / 3600) + \
                     20 * np.sin(2 * np.pi * t / 600) + \
                     np.random.normal(0, 5, len(t))
        
        # 模拟2: 模头压力 (MPa) - 在中间时间段有异常波动
        die_pressure = 2.5 + 0.3 * np.sin(2 * np.pi * t / 1800) + \
                       np.random.normal(0, 0.05, len(t))
        # 在中间时段添加异常
        anomaly_start = int(len(t) * 0.3)
        anomaly_end = int(len(t) * 0.5)
        die_pressure[anomaly_start:anomaly_end] += 0.4 * np.sin(2 * np.pi * np.arange(anomaly_end - anomaly_start) / 300)
        
        # 模拟3: 喂料泵进料率 (kg/h) - 梯度变化
        feed_rate = 500 + 20 * (t / t[-1]) + \
                   10 * np.sin(2 * np.pi * t / 900) + \
                   np.random.normal(0, 3, len(t))
        
        # 模拟4: 过滤器入口压力 (MPa) - 缓慢上升
        filter_pressure = 1.8 + 0.5 * (t / t[-1]) + \
                         0.1 * np.sin(2 * np.pi * t / 1200) + \
                         np.random.normal(0, 0.02, len(t))
        
        # 模拟5: 主电机扭矩 (N·m) - 与速度相关
        motor_torque = 800 + 100 * np.sin(2 * np.pi * t / 1800) + \
                      30 * np.random.normal(0, 1, len(t))
        
        # 模拟6: GP泵速度 (rpm) - 阶梯式变化
        gp_pump_speed = np.zeros(len(t))
        gp_pump_speed[:len(t)//3] = 800
        gp_pump_speed[len(t)//3:2*len(t)//3] = 850
        gp_pump_speed[2*len(t)//3:] = 830
        gp_pump_speed += np.random.normal(0, 10, len(t))
        
        # 创建DataFrame，以时间为索引
        df = pd.DataFrame({
            'motor_speed': motor_speed,
            'die_pressure': die_pressure,
            'feed_rate': feed_rate,
            'filter_pressure': filter_pressure,
            'motor_torque': motor_torque,
            'gp_pump_speed': gp_pump_speed
        }, index=time_axis)
        
        return df
    
    def plot_2hour_heatmap_with_coordinates(self, output_dir: str = None, show_plot: bool = False) -> Dict[str, List[str]]:
        """
        绘制两小时热力图，附带清晰的坐标数值信息显示
        x轴为时间，y轴为膜头位置从低到高
        同时在上方绘制加工参数趋势图，x轴完全对齐
        
        Args:
            output_dir: 输出目录，默认为2hour_hearmap_with
            show_plot: 是否显示图表
            
        Returns:
            字典：文件名 -> 生成的文件路径列表
        """
        print("开始生成带图例信息和加工参数趋势的两小时热力图...")
        print("=" * 60)
        
        # 设置输出目录
        if output_dir is None:
            output_dir = self.data_dir.parent / "data" / "data" / "2hour_hearmap_with"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        generated_files = {}  # 记录每个文件生成的文件
        
        # 膜厚目标值
        target_thickness = 0.045
        
        for file_name, df in self.data_dict.items():
            print(f"\n正在处理文件: {file_name}")
            
            # 为每个文件创建单独的文件夹
            file_base_name = Path(file_name).stem  # 去掉.csv扩展名
            file_output_dir = output_dir / file_base_name
            file_output_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"  文件输出目录: {file_output_dir}")
            
            # 将数据按两小时间间隔分割
            two_hour_windows = self._split_data_by_2hour_intervals(df)
            
            print(f"  数据时间范围: {df.index.min()} 到 {df.index.max()}")
            print(f"  分割为 {len(two_hour_windows)} 个两小时间间隔")
            
            generated_files[file_name] = []
            
            # 为每个两小时间间隔生成热力图
            for i, (start_time, end_time, window_data) in enumerate(two_hour_windows):
                if len(window_data) == 0:
                    continue
                    
                # 转置数据，使x轴为时间，y轴为膜头位置
                # 原始数据：行为时间，列为检测点
                # 转置后：行为检测点，列为时间
                window_data_transposed = window_data.T
                
                # 获取时间点数量和实际时间轴
                n_time = len(window_data)
                time_axis = window_data.index  # 检测数据的实际时间轴
                
                # 模拟加工参数数据（每隔20秒一个点）
                process_params_df = self._simulate_process_parameters(start_time, end_time)
                
                # 将加工参数数据重采样到检测数据的时间点
                # 使用线性插值方法
                process_params_resampled = process_params_df.reindex(time_axis, method='nearest')
                
                # 创建多子图布局：上方6个加工参数图，下方1个热力图
                # 高度比例：加工参数图各占1，热力图占2.5（增加y轴长度）
                fig, axes = plt.subplots(7, 1, figsize=(14, 12), sharex=True,
                                       gridspec_kw={'height_ratios': [1, 1, 1, 1, 1, 1, 2.5]})
                
                # 设置颜色范围，以目标值为中心（±0.005，即±11.1%）
                # 这样目标值0.045正好在颜色范围的中间
                vmin = target_thickness - 0.005
                vmax = target_thickness + 0.005
                
                # --- 绘制加工参数趋势图（上方6个子图）---
                
                # 设置x轴刻度标签
                if n_time <= 20:
                    x_ticks = range(n_time)
                    x_labels = [time_axis[i].strftime('%H:%M:%S') for i in x_ticks]
                else:
                    step = max(1, n_time // 10)
                    x_ticks = range(0, n_time, step)
                    x_labels = [time_axis[i].strftime('%H:%M:%S') for i in x_ticks]
                
                # (1) 主电机速度
                axes[0].plot(range(n_time), process_params_resampled['motor_speed'], color='#1f77b4', lw=0.8)
                axes[0].set_ylabel('Motor Speed\n(rpm)', fontweight='bold', fontsize=9)
                axes[0].text(0.01, 0.82, '(a) Motor Speed', transform=axes[0].transAxes, 
                            fontweight='bold', fontsize=10)
                axes[0].set_ylim(1400, 1600)
                axes[0].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[0].tick_params(direction='in', top=True, right=True, which='both')
                
                # (2) 模头压力
                axes[1].plot(range(n_time), process_params_resampled['die_pressure'], color='#ff7f0e', lw=0.8)
                axes[1].set_ylabel('Die Pressure\n(MPa)', fontweight='bold', fontsize=9)
                axes[1].text(0.01, 0.82, '(b) Die Pressure', transform=axes[1].transAxes, 
                            fontweight='bold', fontsize=10)
                axes[1].set_ylim(1.8, 3.5)
                axes[1].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[1].tick_params(direction='in', top=True, right=True, which='both')
                
                # (3) 喂料泵进料率
                axes[2].plot(range(n_time), process_params_resampled['feed_rate'], color='#2ca02c', lw=0.8)
                axes[2].set_ylabel('Feed Rate\n(kg/h)', fontweight='bold', fontsize=9)
                axes[2].text(0.01, 0.82, '(c) Feed Rate', transform=axes[2].transAxes, 
                            fontweight='bold', fontsize=10)
                axes[2].set_ylim(480, 540)
                axes[2].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[2].tick_params(direction='in', top=True, right=True, which='both')
                
                # (4) 过滤器入口压力
                axes[3].plot(range(n_time), process_params_resampled['filter_pressure'], color='#d62728', lw=0.8)
                axes[3].set_ylabel('Filter Pressure\n(MPa)', fontweight='bold', fontsize=9)
                axes[3].text(0.01, 0.82, '(d) Filter Pressure', transform=axes[3].transAxes, 
                            fontweight='bold', fontsize=10)
                axes[3].set_ylim(1.7, 2.5)
                axes[3].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[3].tick_params(direction='in', top=True, right=True, which='both')
                
                # (5) 主电机扭矩
                axes[4].plot(range(n_time), process_params_resampled['motor_torque'], color='#9467bd', lw=0.8)
                axes[4].set_ylabel('Motor Torque\n(N·m)', fontweight='bold', fontsize=9)
                axes[4].text(0.01, 0.82, '(e) Motor Torque', transform=axes[4].transAxes, 
                            fontweight='bold', fontsize=10)
                axes[4].set_ylim(700, 950)
                axes[4].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[4].tick_params(direction='in', top=True, right=True, which='both')
                
                # (6) GP泵速度
                axes[5].plot(range(n_time), process_params_resampled['gp_pump_speed'], color='#8c564b', lw=0.8)
                axes[5].set_ylabel('GP Pump Speed\n(rpm)', fontweight='bold', fontsize=9)
                axes[5].text(0.01, 0.82, '(f) GP Pump Speed', transform=axes[5].transAxes, 
                            fontweight='bold', fontsize=10)
                axes[5].set_ylim(770, 880)
                axes[5].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[5].tick_params(direction='in', top=True, right=True, which='both')
                
                # --- 绘制热力图（下方第7个子图）---
                
                # 创建热力图 - x轴为时间，y轴为膜头位置
                im = axes[6].imshow(window_data_transposed.values, 
                                aspect='auto', 
                                cmap=cmap_blue_green_red, 
                               vmin=vmin, 
                               vmax=vmax,
                               interpolation='nearest')
                
                # 设置标题和坐标轴 - IEEE风格
                time_range_str = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%H:%M')}"
                axes[6].set_title(f'{file_base_name} - 2-Hour Interval {i+1}\n{time_range_str}', 
                             fontsize=11, fontweight='bold', pad=10)
                axes[6].set_xlabel('Time', fontsize=10)
                axes[6].set_ylabel('Die Position (Low to High)', fontsize=10)
                
                # 添加颜色条 - 放在热力图右侧外部
                cbar_ax = fig.add_axes([0.92, 0.12, 0.015, 0.22])
                cbar = fig.colorbar(im, cax=cbar_ax)
                cbar.ax.tick_params(labelsize=10)
                # 移除颜色条的文字描述
                cbar.set_label('')
                
                # 添加坐标轴刻度
                n_points = len(window_data.columns)  # 检测点数量（y轴）
                
                # 设置x轴刻度为时间 - 与加工参数图完全对齐
                axes[6].set_xticks(x_ticks)
                axes[6].set_xticklabels(x_labels, rotation=45, fontsize=9)
                
                # 设置y轴刻度为检测点位置（从低到高）
                if n_points <= 20:
                    point_ticks = range(n_points)
                    point_labels = [f'P{i+1}' for i in point_ticks]
                else:
                    step = max(1, n_points // 15)
                    point_ticks = range(0, n_points, step)
                    point_labels = [f'P{i+1}' for i in point_ticks]
                
                axes[6].set_yticks(point_ticks)
                axes[6].set_yticklabels(point_labels, fontsize=9)
                
                # 绘制网格线，清晰显示坐标 - IEEE风格（使用灰色而非白色）
                axes[6].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                
                # 设置坐标轴刻度方向 - IEEE风格
                axes[6].tick_params(direction='in', top=True, right=True, which='both')
                
                # --- 调整布局 ---
                plt.subplots_adjust(hspace=0.15, left=0.1, right=0.9, top=0.96, bottom=0.08)
                
                # 生成文件名
                time_start_str = start_time.strftime('%Y%m%d_%H%M')
                time_end_str = end_time.strftime('%Y%m%d_%H%M')
                output_file = file_output_dir / f"{file_base_name}_2hour_{i+1:02d}_{time_start_str}-{time_end_str}_with_coords.png"
                
                # 保存图片
                plt.savefig(output_file, dpi=300, bbox_inches='tight', 
                           facecolor='white', edgecolor='none')
                
                print(f"    Interval {i+1}: {time_range_str} -> {output_file.name}")
                
                generated_files[file_name].append(str(output_file))
                
                if show_plot:
                    plt.show(block=False)  # 非阻塞显示
                    plt.pause(1)  # 暂停1秒让用户查看
                else:
                    plt.close(fig)  # 关闭图形以释放内存
        
        print(f"\n所有带图例信息的两小时热力图生成完成！")
        print(f"图片保存在目录: {output_dir}")
        print("文件名格式: [文件名]_2小时_[序号]_[开始时间]-[结束时间]_with_coords.png")
        print("\n使用说明：")
        print("1. 将鼠标悬停在热力图上查看详细的坐标数值信息")
        print("2. 热力图显示了两小时内的膜厚厚度分布")
        print("3. x轴为时间，y轴为膜头位置（从低到高）")
        print("4. 颜色条显示了厚度范围，以0.45为目标值")
        
        return generated_files


def main():
    """主函数"""
    # 数据文件夹路径
    data_dir = "e:\\codes\\wanweiData2\\data\\realtime"
    
    # 创建热力图生成器
    generator = HeatmapGenerator(data_dir)
    
    print("选择操作模式：")
    print("1. 生成两小时间间隔热力图（按正常/异常分类）")
    print("2. 生成带坐标数值信息的两小时热力图")
    print("3. 同时生成两种热力图")
    
    choice = input("请输入选择 (1/2/3): ").strip()
    
    try:
        # 先加载数据
        generator.load_data()
        
        if choice == "1":
            # 生成两小时间间隔热力图
            generator.plot_2hour_interval_heatmaps()
            print("\n两小时间间隔热力图生成完成！")
            
        elif choice == "2":
            # 生成带坐标数值信息的两小时热力图
            generator.plot_2hour_heatmap_with_coordinates()
            print("\n带坐标数值信息的两小时热力图生成完成！")
            
        elif choice == "3":
            # 同时生成两种热力图
            generator.plot_2hour_interval_heatmaps()
            generator.plot_2hour_heatmap_with_coordinates()
            print("\n所有热力图生成完成！")
            
        else:
            print("无效选择，运行默认模式1...")
            generator.plot_2hour_interval_heatmaps()
            print("\n两小时间间隔热力图生成完成！")
            
    except KeyboardInterrupt:
        print("\n用户中断操作")
    except Exception as e:
        print(f"程序执行出错: {e}")
        raise


if __name__ == "__main__":
    main()