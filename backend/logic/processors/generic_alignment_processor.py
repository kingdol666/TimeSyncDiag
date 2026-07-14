"""
通用时空对齐处理器 (GenericAlignmentProcessor)

将原有的 ThicknessMapProcessor 中的时间对齐和图表生成逻辑通用化：
- 支持任意CSV上传数据
- 支持用户自定义列映射（哪些是时间列、哪些是数据列）
- 支持自定义时间间隔对齐
- 支持自定义图表参数（目标值、范围、配色、尺寸等）
- 支持多组加工参数数据 + 检测数据的灵活组合
"""

import logging
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams
from matplotlib.colors import LinearSegmentedColormap
from io import BytesIO
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import text
import json

# 抑制matplotlib字体回退警告（CJK字符在serif字体中缺失时触发，实际由后备字体渲染）
warnings.filterwarnings('ignore', message='Glyph .* missing from font')

from backend.logic.models.db_connection import DatabaseConnection
from backend.config.config_loader import config as _app_config

logger = logging.getLogger(__name__)

# 从全局配置读取默认图表参数
_tm_cfg = _app_config.thickness_map
_ca_cfg = _app_config.custom_analysis
_DEFAULT_DPI = _ca_cfg.default_chart_dpi
_DEFAULT_FIGSIZE = list(_ca_cfg.default_chart_figsize)
_DEFAULT_COLORMAP = _ca_cfg.default_chart_colormap
_DEFAULT_TARGET = _tm_cfg.target_thickness
_DEFAULT_RANGE = _tm_cfg.thickness_range

# --- IEEE学术风格设置 ---
# 同时支持西文serif和CJK字体，避免中文标题乱码
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Times New Roman', 'SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei',
                          'Noto Sans CJK SC', 'DejaVu Serif', 'Liberation Serif', 'serif']
rcParams['axes.unicode_minus'] = False
rcParams['axes.linewidth'] = 0.8
rcParams['grid.linewidth'] = 0.5
rcParams['xtick.direction'] = 'in'
rcParams['ytick.direction'] = 'in'
rcParams['xtick.top'] = True
rcParams['ytick.right'] = True
rcParams['axes.labelsize'] = 10
rcParams['xtick.labelsize'] = 9
rcParams['ytick.labelsize'] = 9
rcParams['legend.fontsize'] = 8

# 预定义配色方案
COLORMAPS = {
    "blue_green_red": ['#0000FF', '#0080FF', '#00FF00', '#FFFF00', '#FF8000', '#FF0000'],
    "viridis": "viridis",
    "coolwarm": "coolwarm",
    "RdYlBu_r": "RdYlBu_r",
    "jet": "jet",
    "seismic": "seismic",
}

# IEEE风格配色
IEEE_COLORS = ['#0072B2', '#D55E00', '#009E73', '#CC79A7', '#F0E442', '#56B4E9', '#E69F00', '#000000',
               '#882255', '#332288', '#999933', '#AA4499', '#117733', '#44AA99']


class GenericAlignmentProcessor:
    """
    通用时空对齐处理器

    核心能力：
    1. 从数据库读取上传的CSV数据（存储在动态创建的表中）
    2. 根据用户配置进行时间对齐（重采样/插值）
    3. 生成组合图表（加工参数趋势图 + 检测数据热力图）
    """

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        logger.info("通用时空对齐处理器已初始化")

    @staticmethod
    def _validate_table_name(table_name: str) -> str:
        """验证表名安全性，防止SQL注入"""
        if not table_name or not all(c.isalnum() or c == '_' for c in table_name):
            raise ValueError(f"不安全的表名: {table_name}")
        return table_name

    def load_dataset_data(self, data_table_name: str, column_info: Dict,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None) -> pd.DataFrame:
        """
        从数据库的动态表中加载数据集数据

        Args:
            data_table_name: 数据存储表名
            column_info: 列信息（包含列名、时间列、值列等映射）
            start_time: 可选的起始时间过滤
            end_time: 可选的结束时间过滤

        Returns:
            pd.DataFrame: 加载的数据
        """
        try:
            # 验证表名安全性
            safe_table = self._validate_table_name(data_table_name)

            time_col = column_info.get("time_column", "timestamp")
            value_columns = column_info.get("value_columns", [])

            # 构建SQL查询
            cols_to_select = [time_col] + value_columns
            # 安全地构建列名列表（防止SQL注入，只允许字母数字下划线）
            safe_cols = []
            for col in cols_to_select:
                if col and all(c.isalnum() or c == '_' for c in col):
                    safe_cols.append(f'"{col}"')
                else:
                    logger.warning(f"跳过不安全的列名: {col}")

            if not safe_cols:
                logger.error("没有有效的列可查询")
                return pd.DataFrame()

            sql = f"SELECT {', '.join(safe_cols)} FROM {safe_table}"

            # 添加时间过滤
            params = {}
            if start_time and end_time:
                sql += f' WHERE "{time_col}" >= :start_time AND "{time_col}" <= :end_time'
                params['start_time'] = start_time
                params['end_time'] = end_time
            elif start_time:
                sql += f' WHERE "{time_col}" >= :start_time'
                params['start_time'] = start_time
            elif end_time:
                sql += f' WHERE "{time_col}" <= :end_time'
                params['end_time'] = end_time

            sql += f' ORDER BY "{time_col}"'

            engine = self.db_connection.engine
            df = pd.read_sql(text(sql), engine, params=params)

            if df.empty:
                logger.warning(f"表 {data_table_name} 中没有数据")
                return df

            # 解析时间列
            time_format = column_info.get("time_format")
            if time_format:
                try:
                    df[time_col] = pd.to_datetime(df[time_col], format=time_format, errors='coerce')
                except Exception:
                    df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
            else:
                df[time_col] = pd.to_datetime(df[time_col], errors='coerce')

            # 删除无效时间行
            df = df.dropna(subset=[time_col])

            # 设置时间索引
            df = df.set_index(time_col)

            # 转换值列为数值
            for col in value_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"从表 {data_table_name} 加载了 {len(df)} 行数据, {len(df.columns)} 列")
            return df

        except Exception as e:
            logger.error(f"加载数据集数据失败: {e}")
            return pd.DataFrame()

    def align_data(self, datasets: List[Dict[str, Any]],
                   alignment_config: Dict[str, Any]) -> Tuple[List[pd.DataFrame], pd.DataFrame, datetime, datetime]:
        """
        对多组数据集进行时间对齐

        Args:
            datasets: 数据集列表，每个元素包含 df, column_info, role
            alignment_config: 对齐配置
                - interval_seconds: 重采样间隔（秒）
                - interpolation_method: 插值方法 (linear/nearest/none)
                - resample_enabled: 是否启用重采样

        Returns:
            Tuple: (对齐后的加工数据DataFrame列表, 检测数据DataFrame, 全局开始时间, 全局结束时间)
        """
        interval_seconds = alignment_config.get("interval_seconds", 20)
        interpolation_method = alignment_config.get("interpolation_method", "linear")
        resample_enabled = alignment_config.get("resample_enabled", True)

        # 收集所有数据的时间范围
        all_times = []
        for ds in datasets:
            df = ds.get("df")
            if df is not None and not df.empty:
                all_times.append(df.index.min())
                all_times.append(df.index.max())

        if not all_times:
            logger.error("没有可用于对齐的数据")
            return [], pd.DataFrame(), datetime.now(), datetime.now()

        global_start = min(all_times)
        global_end = max(all_times)

        aligned_process_dfs = []
        aligned_detection_df = pd.DataFrame()

        for ds in datasets:
            df = ds.get("df")
            role = ds.get("role", "process")

            if df is None or df.empty:
                continue

            if resample_enabled and not df.empty:
                # 重采样到统一间隔
                rule = f"{interval_seconds}s"
                if interpolation_method == "linear":
                    df_resampled = df.resample(rule).mean().interpolate(method='linear')
                elif interpolation_method == "nearest":
                    df_resampled = df.resample(rule).nearest()
                else:
                    df_resampled = df.resample(rule).mean()

                # 对齐到全局时间范围
                # 重新索引到统一的时间轴
                unified_index = pd.date_range(start=global_start, end=global_end, freq=rule)
                df_aligned = df_resampled.reindex(unified_index)

                if interpolation_method == "linear":
                    df_aligned = df_aligned.interpolate(method='linear', limit_direction='both')
                elif interpolation_method == "nearest":
                    df_aligned = df_aligned.interpolate(method='nearest', limit_direction='both')
            else:
                df_aligned = df

            if role == "detection":
                aligned_detection_df = df_aligned
            else:
                aligned_process_dfs.append(df_aligned)

        logger.info(f"数据对齐完成: {len(aligned_process_dfs)} 组加工数据, "
                    f"检测数据 {0 if aligned_detection_df.empty else len(aligned_detection_df)} 行, "
                    f"时间范围 {global_start} 到 {global_end}")

        return aligned_process_dfs, aligned_detection_df, global_start, global_end

    def generate_combined_chart(self,
                                process_dfs: List[pd.DataFrame],
                                detection_df: pd.DataFrame,
                                process_configs: List[Dict],
                                detection_config: Dict,
                                chart_config: Dict,
                                start_time: datetime,
                                end_time: datetime) -> bytes:
        """
        生成组合图表：上方为加工参数趋势图，下方为检测数据热力图
        如果没有检测数据（纯1D对齐），则仅生成趋势图

        Args:
            process_dfs: 加工数据DataFrame列表
            detection_df: 检测数据DataFrame（可为空）
            process_configs: 每组加工数据的配置（列标签、单位、颜色等）
            detection_config: 检测数据配置
            chart_config: 图表全局配置
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            bytes: PNG图像二进制数据
        """
        try:
            # 图表参数
            figsize = tuple(chart_config.get("figsize", _DEFAULT_FIGSIZE))
            dpi = chart_config.get("dpi", _DEFAULT_DPI)
            title = chart_config.get("title", "时空对齐分析图")
            xlabel = chart_config.get("xlabel", "Time")
            show_grid = chart_config.get("show_grid", True)
            show_colorbar = chart_config.get("show_colorbar", True)
            colormap_name = chart_config.get("colormap", _DEFAULT_COLORMAP)

            # 检测数据热力图参数
            target_value = chart_config.get("target_value", _DEFAULT_TARGET)
            value_range = chart_config.get("value_range", _DEFAULT_RANGE)

            # 获取配色方案
            if colormap_name in COLORMAPS:
                cmap_def = COLORMAPS[colormap_name]
                if isinstance(cmap_def, list):
                    cmap = LinearSegmentedColormap.from_list(colormap_name, cmap_def, N=256)
                else:
                    cmap = plt.get_cmap(cmap_def)
            else:
                cmap = LinearSegmentedColormap.from_list('blue_green_red',
                    COLORMAPS["blue_green_red"], N=256)

            # 统计子图数量
            total_process_plots = 0
            for pdf_cfg in process_configs:
                total_process_plots += len(pdf_cfg.get("value_columns", []))

            has_detection = not detection_df.empty
            n_process = max(total_process_plots, 1)
            n_total = n_process + (1 if has_detection else 0)

            if n_total == 0:
                logger.error("没有可绘制的数据")
                return b''

            # 高度比例：加工参数各占1，热力图占4
            # 纯1D模式时调整图表高度
            if has_detection:
                height_ratios = [1] * n_process + [4]
            else:
                height_ratios = [1] * n_process
                figsize = (figsize[0], max(8.0, n_process * 2.5))

            fig, axes = plt.subplots(n_total, 1, figsize=figsize,
                                     gridspec_kw={'height_ratios': height_ratios})
            if n_total == 1:
                axes = [axes]

            # 统一时间范围
            for ax in axes:
                ax.set_xlim(start_time, end_time)

            # X轴刻度
            time_span = end_time - start_time
            n_ticks = min(10, max(4, int(time_span.total_seconds() / 60)))
            x_ticks = pd.date_range(start=start_time, end=end_time, periods=n_ticks)

            # --- 绘制加工参数趋势图 ---
            color_idx = 0
            plot_idx = 0

            for df_idx, (pdf, pcfg) in enumerate(zip(process_dfs, process_configs)):
                if pdf is None or pdf.empty:
                    continue

                value_columns = pcfg.get("value_columns", [])
                column_labels = pcfg.get("column_labels", {})
                column_units = pcfg.get("column_units", {})
                column_colors = pcfg.get("column_colors", {})

                for col in value_columns:
                    if col not in pdf.columns:
                        # 显示无数据
                        axes[plot_idx].text(0.5, 0.5, 'No Data', transform=axes[plot_idx].transAxes,
                                           ha='center', va='center', fontsize=12)
                        plot_idx += 1
                        continue

                    data_values = pdf[col].values
                    time_values = pdf.index.values

                    # 获取颜色
                    color = column_colors.get(col, IEEE_COLORS[color_idx % len(IEEE_COLORS)])
                    color_idx += 1

                    # 获取标签
                    label = column_labels.get(col, col)
                    unit = column_units.get(col, "")

                    axes[plot_idx].plot(time_values, data_values, color=color, lw=1.2,
                                       marker='o', markersize=2, markeredgecolor='none')

                    # Y轴标签
                    ylabel = label
                    if unit:
                        ylabel += f"\n({unit})"
                    axes[plot_idx].set_ylabel(ylabel, fontweight='bold', fontsize=9)

                    # 动态Y轴范围
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 1
                        axes[plot_idx].set_ylim(data_min - margin, data_max + margin)

                    if show_grid:
                        axes[plot_idx].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                    axes[plot_idx].tick_params(direction='in', top=True, right=True, which='both', labelsize=8)
                    axes[plot_idx].set_xticks(x_ticks)
                    axes[plot_idx].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))

                    plot_idx += 1

            # --- 绘制检测数据热力图 ---
            if has_detection:
                ax_heatmap = axes[plot_idx]

                # 转置数据：使x轴为时间，y轴为位置
                heatmap_data = detection_df.values.T  # [位置 × 时间]

                vmin = target_value - value_range
                vmax = target_value + value_range

                # 使用并集时间范围
                time_axis_numeric = mdates.date2num(detection_df.index)
                extent = [time_axis_numeric[0], time_axis_numeric[-1], 0, heatmap_data.shape[0]]

                im = ax_heatmap.imshow(heatmap_data,
                                       aspect='auto',
                                       cmap=cmap,
                                       vmin=vmin,
                                       vmax=vmax,
                                       interpolation='nearest',
                                       extent=extent,
                                       origin='lower')

                ax_heatmap.xaxis_date()
                date_formatter = mdates.DateFormatter('%H:%M:%S')
                ax_heatmap.xaxis.set_major_formatter(date_formatter)
                ax_heatmap.set_xlim(start_time, end_time)

                # 热力图标签
                heatmap_ylabel = detection_config.get("ylabel", "Position")
                ax_heatmap.set_ylabel(heatmap_ylabel, fontweight='bold', fontsize=10)
                ax_heatmap.set_xlabel(xlabel, fontweight='bold', fontsize=10)
                ax_heatmap.tick_params(direction='in', top=True, right=True, which='both', labelsize=9)

                if show_grid:
                    ax_heatmap.grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)

                if show_colorbar:
                    cbar = fig.colorbar(im, ax=ax_heatmap, shrink=0.6)
                    cbar_label = detection_config.get("colorbar_label", "Value")
                    cbar.set_label(cbar_label, fontweight='bold', fontsize=10)

            # 纯1D模式：给最后一个子图添加X轴标签
            if not has_detection and n_total > 0:
                axes[n_total - 1].set_xlabel(xlabel, fontweight='bold', fontsize=10)

            # 设置总标题
            mode_label = " (纯时序对齐)" if not has_detection else ""
            fig.suptitle(f"{title}{mode_label}", fontweight='bold', fontsize=14, y=0.98)

            plt.tight_layout(rect=[0, 0, 1, 0.96])

            # 保存到字节流
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=dpi, bbox_inches='tight',
                       facecolor='white', edgecolor='none', pad_inches=0.1)
            buffer.seek(0)
            image_data = buffer.getvalue()
            plt.close(fig)

            logger.info(f"成功生成组合图表, 时间范围: {start_time} 到 {end_time}")
            return image_data

        except Exception as e:
            logger.error(f"生成组合图表失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return b''

    def generate_detection_chart(self, detection_df: pd.DataFrame,
                                  detection_config: Dict,
                                  chart_config: Dict) -> bytes:
        """
        生成纯检测数据热力图（用于Agent分析）

        Args:
            detection_df: 检测数据DataFrame
            detection_config: 检测数据配置
            chart_config: 图表全局配置

        Returns:
            bytes: PNG图像二进制数据
        """
        try:
            if detection_df.empty:
                return b''

            figsize = tuple(chart_config.get("figsize", [10.0, 6.0]))
            dpi = chart_config.get("dpi", _DEFAULT_DPI)
            target_value = chart_config.get("target_value", _DEFAULT_TARGET)
            value_range = chart_config.get("value_range", _DEFAULT_RANGE)
            colormap_name = chart_config.get("colormap", _DEFAULT_COLORMAP)

            if colormap_name in COLORMAPS:
                cmap_def = COLORMAPS[colormap_name]
                if isinstance(cmap_def, list):
                    cmap = LinearSegmentedColormap.from_list(colormap_name, cmap_def, N=256)
                else:
                    cmap = plt.get_cmap(cmap_def)
            else:
                cmap = LinearSegmentedColormap.from_list('blue_green_red',
                    COLORMAPS["blue_green_red"], N=256)

            heatmap_data = detection_df.values.T  # [位置 × 时间]

            vmin = target_value - value_range
            vmax = target_value + value_range

            fig, ax = plt.subplots(figsize=figsize)
            ax.imshow(heatmap_data, aspect='auto', cmap=cmap, vmin=vmin, vmax=vmax,
                     interpolation='nearest', origin='lower')

            # 无标签版本（用于CNN/Agent分析）
            ax.set_title('')
            ax.set_xlabel('')
            ax.set_ylabel('')
            ax.set_xticks([])
            ax.set_yticks([])
            for spine in ax.spines.values():
                spine.set_visible(False)

            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=dpi, bbox_inches='tight',
                       facecolor='white', edgecolor='none', pad_inches=0.1)
            buffer.seek(0)
            image_data = buffer.getvalue()
            plt.close(fig)

            return image_data

        except Exception as e:
            logger.error(f"生成检测图表失败: {e}")
            return b''

    def create_data_table(self, table_name: str, columns: List[str], time_column: str):
        """
        动态创建数据存储表

        Args:
            table_name: 表名
            columns: 所有列名
            time_column: 时间列名
        """
        try:
            # 验证表名安全性
            safe_table = self._validate_table_name(table_name)
            engine = self.db_connection.engine

            # 构建CREATE TABLE SQL
            col_defs = []
            for col in columns:
                safe_col = col.replace('"', '')
                if not all(c.isalnum() or c == '_' for c in safe_col):
                    raise ValueError(f"不安全的列名: {col}")
                if col == time_column:
                    col_defs.append(f'"{safe_col}" TIMESTAMP')
                else:
                    col_defs.append(f'"{safe_col}" DOUBLE PRECISION')

            # 添加id主键
            col_defs.insert(0, 'id SERIAL PRIMARY KEY')

            sql = f'CREATE TABLE IF NOT EXISTS {safe_table} ({", ".join(col_defs)})'

            with engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()

            logger.info(f"创建数据表 {safe_table} 成功, 列: {columns}")

        except Exception as e:
            logger.error(f"创建数据表 {table_name} 失败: {e}")
            raise

    def insert_data_to_table(self, table_name: str, df: pd.DataFrame, time_column: str):
        """
        将DataFrame数据插入到动态表中

        Args:
            table_name: 表名
            df: 数据DataFrame
            time_column: 时间列名
        """
        try:
            safe_table = self._validate_table_name(table_name)
            engine = self.db_connection.engine

            # 使用pandas的to_sql快速插入
            df_to_insert = df.copy()
            # 确保时间列是字符串格式
            if time_column in df_to_insert.columns:
                df_to_insert[time_column] = pd.to_datetime(df_to_insert[time_column])

            df_to_insert.to_sql(safe_table, engine, if_exists='append', index=False, chunksize=500)

            logger.info(f"成功插入 {len(df_to_insert)} 行数据到表 {safe_table}")

        except Exception as e:
            logger.error(f"插入数据到表 {table_name} 失败: {e}")
            raise

    def drop_data_table(self, table_name: str):
        """删除数据表"""
        try:
            safe_table = self._validate_table_name(table_name)
            engine = self.db_connection.engine
            with engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS {safe_table}'))
                conn.commit()
            logger.info(f"删除数据表 {safe_table} 成功")
        except Exception as e:
            logger.error(f"删除数据表 {table_name} 失败: {e}")

    @staticmethod
    def parse_csv_file(file_content: bytes, encoding: str = 'utf-8') -> Tuple[pd.DataFrame, List[str], Dict[str, str]]:
        """
        解析上传的CSV文件，自动检测列名和类型

        Args:
            file_content: 文件内容字节
            encoding: 文件编码

        Returns:
            Tuple: (DataFrame, 列名列表, 列类型映射)
        """
        from io import StringIO

        # 尝试多种编码
        for enc in [encoding, 'utf-8', 'gbk', 'gb2312', 'latin1']:
            try:
                content = file_content.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            content = file_content.decode('utf-8', errors='replace')

        # 尝试不同的分隔符
        sep = ','  # 默认逗号分隔
        for try_sep in [',', '\t', ';', '|']:
            try:
                df = pd.read_csv(StringIO(content), sep=try_sep, nrows=5)
                if len(df.columns) > 1:
                    sep = try_sep
                    break
            except Exception:
                continue
        else:
            df = pd.read_csv(StringIO(content), nrows=5)

        # 完整读取
        df = pd.read_csv(StringIO(content), sep=sep)

        columns = list(df.columns)

        # 检测列类型
        column_types = {}
        for col in columns:
            if df[col].dtype == 'object':
                # 尝试解析为时间
                try:
                    pd.to_datetime(df[col].head(10), errors='raise')
                    column_types[col] = 'datetime'
                except Exception:
                    # 尝试解析为数值
                    try:
                        pd.to_numeric(df[col].head(10), errors='raise')
                        column_types[col] = 'float'
                    except Exception:
                        column_types[col] = 'string'
            elif df[col].dtype in ['int64', 'float64']:
                column_types[col] = 'float'
            elif df[col].dtype == 'bool':
                column_types[col] = 'boolean'
            else:
                column_types[col] = 'string'

        logger.info(f"CSV解析完成: {len(columns)} 列, {len(df)} 行")
        return df, columns, column_types

    @staticmethod
    def auto_detect_time_column(columns: List[str], column_types: Dict[str, str]) -> Optional[str]:
        """自动检测时间列"""
        # 优先匹配类型为datetime的列
        for col in columns:
            if column_types.get(col) == 'datetime':
                return col

        # 匹配常见的时间列名
        time_keywords = ['time', 'timestamp', 'date', 'datetime', '时间', '时间段', '时刻']
        for col in columns:
            col_lower = col.lower().strip()
            for kw in time_keywords:
                if kw in col_lower:
                    return col

        return None

    @staticmethod
    def auto_detect_value_columns(columns: List[str], column_types: Dict[str, str],
                                   time_column: Optional[str] = None) -> List[str]:
        """自动检测数值列"""
        value_cols = []
        for col in columns:
            if col == time_column:
                continue
            if column_types.get(col) in ['float', 'int']:
                value_cols.append(col)
        return value_cols

    @staticmethod
    def get_sample_data(df: pd.DataFrame, n_rows: int = 5) -> List[Dict]:
        """获取样本数据用于前端预览"""
        sample = df.head(n_rows)
        return sample.to_dict(orient='records')

    # ==================== 滑动窗口模拟分析 ====================

    def align_data_in_window(self, datasets: List[Dict[str, Any]],
                             alignment_config: Dict[str, Any],
                             window_start: datetime,
                             window_end: datetime) -> Tuple[List[pd.DataFrame], pd.DataFrame]:
        """
        对指定时间窗口内的数据进行对齐

        Args:
            datasets: 数据集列表
            alignment_config: 对齐配置
            window_start: 窗口开始时间
            window_end: 窗口结束时间

        Returns:
            Tuple: (对齐后的加工数据列表, 检测数据)
        """
        interval_seconds = alignment_config.get("interval_seconds", 20)
        interpolation_method = alignment_config.get("interpolation_method", "linear")
        resample_enabled = alignment_config.get("resample_enabled", True)

        aligned_process_dfs = []
        aligned_detection_df = pd.DataFrame()

        for ds in datasets:
            df = ds.get("df")
            role = ds.get("role", "process")

            if df is None or df.empty:
                continue

            # 裁剪到窗口范围
            df_window = df[(df.index >= window_start) & (df.index <= window_end)].copy()

            if df_window.empty:
                if role == "detection":
                    aligned_detection_df = pd.DataFrame()
                else:
                    aligned_process_dfs.append(pd.DataFrame())
                continue

            if resample_enabled:
                rule = f"{interval_seconds}s"
                if interpolation_method == "linear":
                    df_resampled = df_window.resample(rule).mean().interpolate(method='linear')
                elif interpolation_method == "nearest":
                    df_resampled = df_window.resample(rule).nearest()
                else:
                    df_resampled = df_window.resample(rule).mean()

                # 对齐到窗口时间范围
                unified_index = pd.date_range(start=window_start, end=window_end, freq=rule)
                df_aligned = df_resampled.reindex(unified_index)

                if interpolation_method == "linear":
                    df_aligned = df_aligned.interpolate(method='linear', limit_direction='both')
                elif interpolation_method == "nearest":
                    df_aligned = df_aligned.interpolate(method='nearest', limit_direction='both')
            else:
                df_aligned = df_window

            if role == "detection":
                aligned_detection_df = df_aligned
            else:
                aligned_process_dfs.append(df_aligned)

        return aligned_process_dfs, aligned_detection_df

    def generate_window_chart(self,
                              process_dfs: List[pd.DataFrame],
                              detection_df: pd.DataFrame,
                              process_configs: List[Dict],
                              detection_config: Dict,
                              chart_config: Dict,
                              window_start: datetime,
                              window_end: datetime,
                              step_number: int = 0) -> bytes:
        """
        为滑动窗口生成图表（带步进标注）

        Args:
            process_dfs: 加工数据
            detection_df: 检测数据
            process_configs: 加工数据配置
            detection_config: 检测数据配置
            chart_config: 图表配置
            window_start: 窗口开始时间
            window_end: 窗口结束时间
            step_number: 步进编号（用于标题标注）

        Returns:
            bytes: PNG图像数据
        """
        try:
            # 复用组合图表生成逻辑
            image = self.generate_combined_chart(
                process_dfs=process_dfs,
                detection_df=detection_df,
                process_configs=process_configs,
                detection_config=detection_config,
                chart_config=chart_config,
                start_time=window_start,
                end_time=window_end
            )

            if image:
                # 在标题中添加步进信息
                # 由于generate_combined_chart已经包含了标题，这里不做二次处理
                # 图表已经包含了窗口的时间范围信息
                pass

            logger.info(f"生成窗口图表: Step {step_number}, "
                       f"时间范围 {window_start} 到 {window_end}")
            return image

        except Exception as e:
            logger.error(f"生成窗口图表失败: {e}")
            return b''

    def compute_window_summary(self,
                               process_dfs: List[pd.DataFrame],
                               detection_df: pd.DataFrame,
                               process_configs: List[Dict]) -> Dict[str, Any]:
        """
        计算窗口数据的统计摘要

        Args:
            process_dfs: 加工数据列表
            detection_df: 检测数据
            process_configs: 加工数据配置

        Returns:
            Dict: 统计摘要
        """
        summary = {
            "process_stats": [],
            "detection_stats": None,
        }

        # 加工数据统计
        for i, (pdf, pcfg) in enumerate(zip(process_dfs, process_configs)):
            if pdf is None or pdf.empty:
                continue

            value_columns = pcfg.get("value_columns", [])
            column_labels = pcfg.get("column_labels", {})

            for col in value_columns:
                if col not in pdf.columns:
                    continue

                values = pdf[col].dropna()
                if values.empty:
                    continue

                label = column_labels.get(col, col)
                summary["process_stats"].append({
                    "dataset_index": i,
                    "column": col,
                    "label": label,
                    "mean": float(values.mean()),
                    "std": float(values.std()) if len(values) > 1 else 0.0,
                    "min": float(values.min()),
                    "max": float(values.max()),
                    "count": int(len(values)),
                })

        # 检测数据统计
        if not detection_df.empty:
            values = detection_df.values.flatten()
            values = values[~np.isnan(values)]
            if len(values) > 0:
                summary["detection_stats"] = {
                    "mean": float(np.mean(values)),
                    "std": float(np.std(values)) if len(values) > 1 else 0.0,
                    "min": float(np.min(values)),
                    "max": float(np.max(values)),
                    "count": int(len(values)),
                }

        return summary

    @staticmethod
    def compute_simulation_steps(global_start: datetime,
                                  global_end: datetime,
                                  simulation_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        根据模拟配置计算所有步进的时间点

        Args:
            global_start: 全局数据开始时间
            global_end: 全局数据结束时间
            simulation_config: 模拟配置
                - alignment_window_seconds: 对齐时间范围（窗口大小，秒）
                - step_seconds: 步长时间（每次向前推进多少秒）
                - step_interval_seconds: 推进间隔（多久推进一次，秒）
                - vlm_diagnosis_interval_steps: VLM诊断间隔（每N步诊断一次）
                - vlm_diagnosis_interval_seconds: VLM诊断间隔（按时间，秒）

        Returns:
            List[Dict]: 每步的信息
                - step_number: 步骤编号
                - step_time: 该步骤的时间点
                - window_start: 窗口开始时间
                - window_end: 窗口结束时间
                - do_vlm: 是否进行VLM诊断
        """
        window_seconds = simulation_config.get("alignment_window_seconds", 300)
        step_seconds = simulation_config.get("step_seconds", 60)
        step_interval = simulation_config.get("step_interval_seconds", step_seconds)
        vlm_interval_steps = simulation_config.get("vlm_diagnosis_interval_steps", 5)
        vlm_interval_seconds = simulation_config.get("vlm_diagnosis_interval_seconds", 0)

        # 使用步进间隔作为推进单位
        advance_seconds = step_interval if step_interval > 0 else step_seconds

        steps = []
        step_number = 0
        # 第一个窗口的结束时间从 global_start + window_seconds 开始
        # 或者从 global_start 开始（窗口 = [global_start, global_start + window_seconds]）
        current_time = global_start
        last_vlm_time = None

        while current_time <= global_end:
            window_start = current_time
            window_end = current_time + timedelta(seconds=window_seconds)

            # 如果窗口结束超过全局结束时间，调整
            if window_end > global_end:
                window_end = global_end

            # 判断是否进行VLM诊断
            do_vlm = False
            if step_number == 0:
                do_vlm = True
            elif vlm_interval_steps > 0 and step_number % vlm_interval_steps == 0:
                do_vlm = True
            elif vlm_interval_seconds > 0:
                if last_vlm_time is None:
                    do_vlm = True
                elif (current_time - last_vlm_time).total_seconds() >= vlm_interval_seconds:
                    do_vlm = True

            if do_vlm:
                last_vlm_time = current_time

            steps.append({
                "step_number": step_number,
                "step_time": current_time,
                "window_start": window_start,
                "window_end": window_end,
                "do_vlm": do_vlm,
            })

            step_number += 1
            current_time = current_time + timedelta(seconds=advance_seconds)

            # 如果窗口已经覆盖了全部数据，停止
            if window_end >= global_end:
                break

        return steps
