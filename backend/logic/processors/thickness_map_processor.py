import logging
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # 设置matplotlib使用非交互式后端
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.colors as colors
from io import BytesIO
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session, load_only, defer
from sqlalchemy import and_, func
import asyncio
import threading
import tempfile

# 导入自定义模块
from ..models.models import DetectionDeviceData, ThicknessMap, MotouData, OtherData
from ..models.db_connection import DatabaseConnection
from ..services.remote_template import CNNImageClassificationService
import backend.state as state
import uuid
from ..models.mini_connection import MinioConnector
from backend.config.config_loader import config

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- IEEE学术风格设置 ---
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Times New Roman', 'DejaVu Serif', 'Liberation Serif', 'Ubuntu Serif', 'serif']
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
# 默认DPI从配置读取，启动时同步
rcParams['figure.dpi'] = config.thickness_map.dpi
rcParams['savefig.dpi'] = config.thickness_map.dpi
rcParams['savefig.bbox'] = 'tight'
rcParams['savefig.facecolor'] = 'white'

# 创建蓝绿红渐变色图（蓝-绿-红，绿色在目标值）
# 蓝色：低于目标值，绿色：在目标值范围内，红色：超过目标值
colors_blue_green_red = ['#0000FF', '#0080FF', '#00FF00', '#FFFF00', '#FF8000', '#FF0000']
cmap_blue_green_red = LinearSegmentedColormap.from_list('blue_green_red', colors_blue_green_red, N=256)

class ThicknessMapProcessor:
    """
    膜厚温度云图处理器，用于从膜厚数据生成温度云图
    """
    def __init__(self, db_connection: DatabaseConnection):
        """
        初始化膜厚温度云图处理器
        
        参数:
            db_connection: 数据库连接实例
        """
        self.db_connection = db_connection
        self.cnn_service = CNNImageClassificationService()
        self.mask_data = None
        self.last_is_abnormal = False
        logger.info("膜厚温度云图处理器已初始化")
    
    # def generate_thickness_map(self, start_time: datetime, end_time: datetime) -> Optional[ThicknessMap]:
    #     """
    #     生成指定时间范围内的膜厚温度云图
        
    #     参数:
    #         start_time: 开始时间
    #         end_time: 结束时间
            
    #     返回:
    #         ThicknessMap: 生成的膜厚温度云图对象，如果没有数据则返回None
    #     """
    #     try:
    #         # 从数据库获取指定时间范围内的膜厚数据
    #         thickness_data = self._get_thickness_data(start_time, end_time)
            
    #         if not thickness_data:
    #             logger.warning(f"在时间范围 {start_time} 到 {end_time} 内没有找到膜厚数据")
    #             return None
            
    #         # 生成带坐标和图例的温度云图
    #         map_image = self._create_heatmap(thickness_data, with_labels=True)
            
    #         # 生成纯的温度云图（不带坐标和图例）
    #         pure_map_image = self._create_heatmap(thickness_data, with_labels=False)
            
    #         # 创建ThicknessMap对象
    #         thickness_map = ThicknessMap.from_thickness_data(
    #             start_time=start_time,
    #             end_time=end_time,
    #             thickness_data=thickness_data,
    #             map_image=map_image,
    #             pure_map_image=pure_map_image
    #         )
            
    #         logger.info(f"成功生成膜厚温度云图，时间范围: {start_time} 到 {end_time}，数据点数: {len(thickness_data)}")
    #         return thickness_map
            
    #     except Exception as e:
    #         logger.error(f"生成膜厚温度云图时发生错误: {e}")
    #         return None
    
    def generate_thickness_map_by_id(self) -> Optional[ThicknessMap]:
        """
        生成基于最新ID向上40行数据的膜厚温度云图
        
        返回:
            ThicknessMap: 生成的膜厚温度云图对象，如果没有数据则返回None
        """
        try:
            # 从数据库获取基于ID的膜厚数据
            thickness_data, start_time, end_time = self._get_thickness_data_by_id()
            
            if not thickness_data:
                logger.warning("没有找到足够的膜厚数据来生成云图")
                return None
            
            # 生成带坐标和图例的温度云图
            map_image = self._create_heatmap(thickness_data, with_labels=True)
            
            # 生成纯的温度云图（不带坐标和图例）
            pure_map_image = self._create_heatmap(thickness_data, with_labels=False)
            
            # 创建一个uuid
            thickness_map_uuid = str(uuid.uuid4())
            
            # 创建ThicknessMap对象
            thickness_map = ThicknessMap.from_thickness_data(
                thickness_map_uuid=thickness_map_uuid,
                start_time=start_time,
                end_time=end_time,
                thickness_data=thickness_data,
                map_image=map_image,
                pure_map_image=pure_map_image
            )
            
            logger.info(f"成功生成膜厚温度云图，时间范围: {start_time} 到 {end_time}，数据点数: {len(thickness_data)}")
            return thickness_map
            
        except Exception as e:
            logger.error(f"基于ID生成膜厚温度云图时发生错误: {e}")
            return None
    
    def save_thickness_map(self, thickness_map: ThicknessMap) -> bool:
        """
        保存膜厚温度云图到数据库
        
        参数:
            thickness_map: 要保存的膜厚温度云图对象
            
        返回:
            bool: 保存是否成功
        """
        try:
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 保存到数据库
                session.add(thickness_map)
                session.commit()
                
                logger.info(f"成功保存膜厚温度云图到数据库，ID: {thickness_map.id}")
                return True
                
            except Exception as e:
                # 发生错误时回滚事务
                session.rollback()
                logger.error(f"保存膜厚温度云图到数据库失败: {e}")
                return False
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            logger.error(f"保存膜厚温度云图时发生错误: {e}")
            return False
    
    def generate_and_save_latest_map(self) -> Optional[ThicknessMap]:
        """
        生成并保存基于最新ID向上40行数据的膜厚温度云图
        
        返回:
            ThicknessMap: 生成的膜厚温度云图对象，如果没有数据则返回None
        """
        try:
            # 生成温度云图（基于最新ID向上40行数据）
            thickness_map = self.generate_thickness_map_by_id()
            
            if thickness_map:
                # 保存到数据库
                if self.save_thickness_map(thickness_map):
                    return thickness_map
                else:
                    logger.error("保存膜厚温度云图失败")
                    return None
            else:
                logger.warning("没有生成膜厚温度云图，可能是因为没有数据")
                return None
                
        except Exception as e:
            logger.error(f"生成并保存基于最新ID的膜厚温度云图时发生错误: {e}")
            return None
    
    def generate_pure_map(self, start_time: datetime, end_time: datetime) -> Optional[bytes]:
        """
        生成指定时间范围内的纯膜厚云图（不带坐标和图例）
        
        参数:
            start_time: 开始时间
            end_time: 结束时间
            
        返回:
            bytes: 纯膜厚云图的二进制数据，如果没有数据则返回None
        """
        try:
            # 从数据库获取指定时间范围内的膜厚数据
            thickness_data = self._get_thickness_data(start_time, end_time)
            
            if not thickness_data:
                logger.warning(f"在时间范围 {start_time} 到 {end_time} 内没有找到膜厚数据")
                return None
            
            # 生成纯的温度云图（不带坐标和图例）
            pure_map_image = self._create_heatmap(thickness_data, with_labels=False)
            
            logger.info(f"成功生成纯膜厚云图，时间范围: {start_time} 到 {end_time}，数据点数: {len(thickness_data)}")
            return pure_map_image
            
        except Exception as e:
            logger.error(f"生成纯膜厚云图时发生错误: {e}")
            return None
    
    def _get_thickness_data(self, start_time: datetime, end_time: datetime) -> List[List[float]]:
        """
        从数据库获取指定时间范围内的膜厚数据
        
        参数:
            start_time: 开始时间
            end_time: 结束时间
            
        返回:
            List[List[float]]: 二维膜厚数据数组[时间×模头位置]
        """
        try:
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 查询指定时间范围内的膜厚数据
                # 使用load_only只加载需要的字段，避免加载raw_data等大字段
                query = session.query(DetectionDeviceData).options(
                    load_only(
                        DetectionDeviceData.id,
                        DetectionDeviceData.values,
                        DetectionDeviceData.start_time,
                        DetectionDeviceData.end_time
                    )
                ).filter(
                    and_(
                        DetectionDeviceData.start_time >= start_time,
                        DetectionDeviceData.end_time <= end_time
                    )
                ).order_by(DetectionDeviceData.start_time)
                
                # 提取二维膜厚数据（时间×模头位置）
                thickness_data_2d = []
                for record in query.all():
                    if record.values:
                        thickness_data_2d.append(record.values)
                    else:
                        thickness_data_2d.append([])
                
                logger.info(f"从数据库获取到 {len(thickness_data_2d)} 条记录")
                if thickness_data_2d:
                    logger.info(f"每条记录包含 {len(thickness_data_2d[0])} 个模头位置数据")
                return thickness_data_2d
                
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            logger.error(f"从数据库获取膜厚数据时发生错误: {e}")
            return []
    
    def get_latest_thickness_time_range(self, record_count: int = 40) -> Optional[tuple[datetime, datetime]]:
        """
        获取最新膜厚数据的时间范围
        
        参数:
            record_count: 要获取的记录数量，默认为40
            
        返回:
            Optional[tuple[datetime, datetime]]: (开始时间, 结束时间)，如果没有数据则返回None
        """
        try:
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 获取最新记录的ID
                latest_record = session.query(DetectionDeviceData).order_by(
                    DetectionDeviceData.id.desc()
                ).first()
                
                if not latest_record:
                    logger.warning("数据库中没有找到任何膜厚数据")
                    return None
                
                # 计算ID范围（最新ID向上指定行数）
                latest_id = latest_record.id
                min_id = max(1, latest_id - (record_count - 1))  # 确保ID不小于1
                
                # 查询指定ID范围内的膜厚数据
                query = session.query(DetectionDeviceData).filter(
                    DetectionDeviceData.id >= min_id,
                    DetectionDeviceData.id <= latest_id
                ).order_by(DetectionDeviceData.id)
                
                # 提取时间范围
                start_time = None
                end_time = None
                
                for record in query.all():
                    # 记录时间范围
                    if start_time is None or record.start_time < start_time:
                        start_time = record.start_time
                    if end_time is None or record.end_time > end_time:
                        end_time = record.end_time
                
                if start_time and end_time:
                    logger.info(f"从数据库获取到膜厚数据时间范围: {start_time} 到 {end_time}，ID范围: {min_id} 到 {latest_id}")
                    return (start_time, end_time)
                else:
                    logger.warning("未能获取到有效的时间范围")
                    return None
                
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            logger.error(f"获取膜厚数据时间范围时发生错误: {e}")
            return None
    
    def _get_thickness_data_by_id(self) -> tuple[List[List[float]], datetime, datetime]:
        """
        从数据库获取基于最新ID向上 N 行的膜厚数据
        
        返回:
            tuple: (二维膜厚数据数组[时间×模头位置], 开始时间, 结束时间)
        """
        try:
            record_count = config.thickness_map.record_count
            
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 获取最新记录的ID - 只加载id字段
                latest_record = session.query(DetectionDeviceData).options(
                    load_only(DetectionDeviceData.id)
                ).order_by(
                    DetectionDeviceData.id.desc()
                ).first()
                
                if not latest_record:
                    logger.warning("数据库中没有找到任何膜厚数据")
                    return [], None, None
                
                # 计算ID范围（最新ID向上 record_count 行）
                latest_id = latest_record.id
                min_id = max(1, latest_id - (record_count - 1))  # 确保ID不小于1
                
                # 查询指定ID范围内的膜厚数据 - 使用load_only只加载需要的字段
                query = session.query(DetectionDeviceData).options(
                    load_only(
                        DetectionDeviceData.id,
                        DetectionDeviceData.values,
                        DetectionDeviceData.start_time,
                        DetectionDeviceData.end_time
                    )
                ).filter(
                    DetectionDeviceData.id >= min_id,
                    DetectionDeviceData.id <= latest_id
                ).order_by(DetectionDeviceData.id)
                
                # 提取二维膜厚数据（时间×模头位置）和时间范围
                thickness_data_2d = []
                start_time = None
                end_time = None
                
                for record in query.all():
                    if record.values:
                        thickness_data_2d.append(record.values)
                    else:
                        thickness_data_2d.append([])
                    
                    # 记录时间范围
                    if start_time is None or record.start_time < start_time:
                        start_time = record.start_time
                    if end_time is None or record.end_time > end_time:
                        end_time = record.end_time
                
                logger.info(f"从数据库获取到 {len(thickness_data_2d)} 条记录，ID范围: {min_id} 到 {latest_id}")
                if thickness_data_2d:
                    logger.info(f"每条记录包含 {len(thickness_data_2d[0])} 个模头位置数据")
                return thickness_data_2d, start_time, end_time
                
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            logger.error(f"从数据库基于ID获取膜厚数据时发生错误: {e}")
            return [], None, None
    
    def _create_heatmap(self, thickness_data: List[List[float]], with_labels: bool = True, mask: Optional[np.ndarray] = None) -> bytes:
        """
        从膜厚数据创建温度云图
        
        参数:
            thickness_data: 二维膜厚数据数组[时间×模头位置]
            with_labels: 是否包含坐标轴和图例，默认为True
            mask: 掩码数组，True表示需要遮盖的区域，None表示不使用掩码
            
        返回:
            bytes: 温度云图的二进制数据
        """
        try:
            # 将二维数据转换为numpy数组
            heatmap_data = np.array(thickness_data)
            
            # 从配置读取目标膜厚、颜色范围和图形参数
            target_thickness = config.thickness_map.target_thickness
            thickness_range = config.thickness_map.thickness_range
            figsize = tuple(config.thickness_map.figsize)
            dpi = config.thickness_map.dpi
            mask_preserve_rows = config.thickness_map.mask_preserve_rows
            
            # 如果存在mask，将mask区域的值设置为目标值
            # mask永远不遮挡最后 mask_preserve_rows 行数据
            if mask is not None and mask.shape == heatmap_data.shape:
                if heatmap_data.shape[0] > mask_preserve_rows:
                    mask[-mask_preserve_rows:, :] = False
                    heatmap_data = np.where(mask, target_thickness, heatmap_data)
                else:
                    pass
            
            # 数据结构：[时间点数 × 模头位置数]
            # 使用imshow绘制：横坐标是模头位置（列），纵坐标是时间（行，从上到下）
            xlabel = 'Die Position'
            ylabel = 'Time'
            
            # 设置颜色范围，以目标值为中心
            vmin = target_thickness - thickness_range
            vmax = target_thickness + thickness_range
            
            # 创建图形
            fig, ax = plt.subplots(figsize=figsize)
            
            # 创建热力图 - 蓝绿红配色
            # aspect='auto' 自动调整纵横比，使热力图填满图形
            # origin='upper' 确保时间从上到下绘制
            im = ax.imshow(heatmap_data, 
                          aspect='auto', 
                          cmap=cmap_blue_green_red, 
                          vmin=vmin, 
                          vmax=vmax,
                          interpolation='nearest',
                          origin='upper')
            
            if with_labels:
                # 设置横坐标刻度（模头位置）
                num_positions = heatmap_data.shape[1]
                x_ticks = np.linspace(0, num_positions - 1, min(10, num_positions), dtype=int)
                ax.set_xticks(x_ticks)
                ax.set_xticklabels([str(pos) for pos in x_ticks])
                
                # 设置纵坐标刻度（时间，从上到下）
                num_time_points = heatmap_data.shape[0]
                y_ticks = np.linspace(0, num_time_points - 1, min(10, num_time_points), dtype=int)
                ax.set_yticks(y_ticks)
                ax.set_yticklabels([str(t) for t in y_ticks])
                
                # 添加颜色条 - 放在热力图右侧外部
                cbar_ax = fig.add_axes([0.92, 0.15, 0.02, 0.7])
                cbar = fig.colorbar(im, cax=cbar_ax)
                cbar.ax.tick_params(labelsize=10)
                cbar.set_label('Thickness (mm)', fontweight='bold', fontsize=10)
                
                # 设置标题和标签
                ax.set_title('Thickness Heatmap', fontweight='bold', fontsize=12)
                ax.set_xlabel(xlabel, fontweight='bold', fontsize=10)
                ax.set_ylabel(ylabel, fontweight='bold', fontsize=10)
                
                # 设置刻度方向向内
                ax.tick_params(direction='in', top=True, right=True, which='both')
                
                # 添加网格线
                ax.grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
            else:
                # 移除所有文字、标题、坐标轴标签、刻度
                ax.set_title('')
                ax.set_xlabel('')
                ax.set_ylabel('')
                ax.set_xticks([])
                ax.set_yticks([])
                
                # 移除坐标轴边框
                for spine in ax.spines.values():
                    spine.set_visible(False)
            
            # 将图形保存到内存中的字节流
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=dpi, bbox_inches='tight', 
                       facecolor='white', edgecolor='none', pad_inches=0.1)
            buffer.seek(0)
            
            # 获取二进制数据
            image_data = buffer.getvalue()
            
            # 关闭图形，释放内存
            plt.close(fig)
            
            return image_data
            
        except Exception as e:
            logger.error(f"创建温度云图时发生错误: {e}")
            # 返回空字节数组
            return b''
    
    def _get_process_parameters(self, start_time: datetime, end_time: datetime) -> Dict[str, pd.DataFrame]:
        """
        从数据库获取指定时间范围内的加工参数数据
        
        参数:
            start_time: 开始时间
            end_time: 结束时间
            
        返回:
            Dict[str, pd.DataFrame]: 包含模头数据和其他设备数据的字典
        """
        try:
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 查询模头数据
                motou_query = session.query(MotouData).filter(
                    and_(
                        MotouData.timestamp >= start_time,
                        MotouData.timestamp <= end_time
                    )
                ).order_by(MotouData.timestamp)
                
                # 转换为DataFrame
                motou_data = []
                for record in motou_query.all():
                    motou_data.append({
                        'timestamp': record.timestamp,
                        'motou_resin_pressure': record.motou_resin_pressure,
                        'motou_pressure_y': record.motou_pressure_y
                    })
                
                motou_df = pd.DataFrame(motou_data)
                if not motou_df.empty:
                    motou_df = motou_df.set_index('timestamp')
                
                # 查询其他设备数据
                other_query = session.query(OtherData).filter(
                    and_(
                        OtherData.timestamp >= start_time,
                        OtherData.timestamp <= end_time
                    )
                ).order_by(OtherData.timestamp)
                
                # 转换为DataFrame
                other_data = []
                for record in other_query.all():
                    other_data.append({
                        'timestamp': record.timestamp,
                        'main_motor_speed_pv': record.main_motor_speed_pv,
                        'main_motor_speed_sv': record.main_motor_speed_sv,
                        'main_motor_torque_pv': record.main_motor_torque_pv,
                        'gp_pump_speed_pv': record.gp_pump_speed_pv,
                        'gp_pump_speed_sv': record.gp_pump_speed_sv,
                        'gp_pump_torque_pv': record.gp_pump_torque_pv,
                        'gp_inlet_pv': record.gp_inlet_pv,
                        'gp_transparent_pressure': record.gp_transparent_pressure,
                        'gp_outlet_pressure': record.gp_outlet_pressure,
                        'tk_1090db_temperature': record.tk_1090db_temperature,
                        'extruder_die_pressure': record.extruder_die_pressure,
                        'filter_inlet_pressure': record.filter_inlet_pressure,
                        'feed_pump_rate': record.feed_pump_rate
                    })
                
                other_df = pd.DataFrame(other_data)
                if not other_df.empty:
                    other_df = other_df.set_index('timestamp')
                
                logger.info(f"从数据库获取到模头数据: {len(motou_df)} 条记录")
                logger.info(f"从数据库获取到其他设备数据: {len(other_df)} 条记录")
                
                # 打印查询的时间范围
                logger.info(f"查询时间范围: {start_time} 到 {end_time}")
                
                # 打印原始数据的时间范围
                if not other_df.empty:
                    logger.info(f"other_df 原始时间范围: {other_df.index.min()} 到 {other_df.index.max()}")
                    logger.info(f"other_df 前5行原始数据:\n{other_df.head()}")
                    logger.info(f"other_df 后5行原始数据:\n{other_df.tail()}")
                
                if not motou_df.empty:
                    logger.info(f"motou_df 原始时间范围: {motou_df.index.min()} 到 {motou_df.index.max()}")
                    logger.info(f"motou_df 前5行原始数据:\n{motou_df.head()}")
                    logger.info(f"motou_df 后5行原始数据:\n{motou_df.tail()}")
                
                return {
                    'motou': motou_df,
                    'other': other_df
                }
                
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            logger.error(f"从数据库获取加工参数数据时发生错误: {e}")
            return {'motou': pd.DataFrame(), 'other': pd.DataFrame()}
    
    def _create_combined_heatmap(self, thickness_data: List[List[float]], 
                                  start_time: datetime, end_time: datetime) -> bytes:
        """
        创建包含加工参数趋势图和膜厚热力图的完整图像
        
        参数:
            thickness_data: 二维膜厚数据数组[时间×模头位置]
            start_time: 开始时间
            end_time: 结束时间
            
        返回:
            bytes: 完整图像的二进制数据
        """
        try:
            # 获取加工参数数据
            process_params = self._get_process_parameters(start_time, end_time)
            
            # 将二维膜厚数据转换为numpy数组
            heatmap_data = np.array(thickness_data)
            
            # 获取膜厚数据的时间轴
            n_time = len(thickness_data)
            time_axis = pd.date_range(start=start_time, end=end_time, periods=n_time)
            
            # 转置数据，使x轴为时间，y轴为膜头位置
            heatmap_data_transposed = heatmap_data.T
            
            # 从配置读取参数
            target_thickness = config.thickness_map.target_thickness
            thickness_range = config.thickness_map.thickness_range
            combined_figsize = tuple(config.thickness_map.combined_figsize)
            dpi = config.thickness_map.dpi
            
            # 创建多子图布局：上方8个加工参数图，下方1个热力图
            # 高度比例：加工参数图各占1，热力图占3
            # 不使用sharex，因为热力图需要特殊处理x轴
            fig, axes = plt.subplots(9, 1, figsize=combined_figsize,
                                   gridspec_kw={'height_ratios': [1, 1, 1, 1, 1, 1, 1, 1, 4]})
            
            # 设置颜色范围，以目标值为中心
            vmin = target_thickness - thickness_range
            vmax = target_thickness + thickness_range
            
            # --- 绘制加工参数趋势图（上方8个子图）---
            
            # 准备加工参数数据
            other_df = process_params['other']
            motou_df = process_params['motou']
            
            logger.info(f"other_df 形状: {other_df.shape}, 时间范围: {other_df.index.min() if not other_df.empty else '空'} 到 {other_df.index.max() if not other_df.empty else '空'}")
            logger.info(f"motou_df 形状: {motou_df.shape}, 时间范围: {motou_df.index.min() if not motou_df.empty else '空'} 到 {motou_df.index.max() if not motou_df.empty else '空'}")
            
            # 计算加工参数数据的实际时间范围
            param_start_time = start_time
            param_end_time = end_time
            
            if not other_df.empty:
                param_start_time = min(param_start_time, other_df.index.min())
                param_end_time = max(param_end_time, other_df.index.max())
            
            if not motou_df.empty:
                param_start_time = min(param_start_time, motou_df.index.min())
                param_end_time = max(param_end_time, motou_df.index.max())
            
            logger.info(f"加工参数数据时间范围: {param_start_time} 到 {param_end_time}")
            logger.info(f"膜厚数据时间范围: {start_time} 到 {end_time}")
            
            # 设置统一的x轴范围和刻度，确保所有子图时间轴对齐
            # 使用加工参数数据和膜厚数据的并集时间范围
            plot_start_time = min(start_time, param_start_time)
            plot_end_time = max(end_time, param_end_time)
            
            if n_time <= 20:
                x_ticks = time_axis
            else:
                step = max(1, n_time // 10)
                x_ticks = time_axis[::step]
            
            # 为所有子图设置相同的x轴范围（使用并集时间范围）
            for ax in axes:
                ax.set_xlim(plot_start_time, plot_end_time)
            
            # 定义IEEE论文风格的配色方案
            ieee_colors = ['#0072B2', '#D55E00', '#009E73', '#CC79A7', '#F0E442', '#56B4E9', '#E69F00', '#000000']
            
            # (1) 主电机速度
            if not other_df.empty and 'main_motor_speed_pv' in other_df.columns:
                data_values = other_df['main_motor_speed_pv'].values
                time_values = other_df.index.values
                logger.info(f"主电机速度数据值范围: {data_values.min():.2f} - {data_values.max():.2f}")
                logger.info(f"主电机速度时间点数量: {len(time_values)}")
                # 使用真实时间戳绘制
                axes[0].plot(time_values, data_values, color=ieee_colors[0], lw=1.2, marker='o', markersize=3, markeredgecolor='none')
                axes[0].set_ylabel('Motor Speed\n(rpm)', fontweight='bold', fontsize=10)
                # 动态设置Y轴范围，添加10%的边距
                if len(data_values) > 0 and not np.all(np.isnan(data_values)):
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 10
                        axes[0].set_ylim(data_min - margin, data_max + margin)
                axes[0].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[0].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
                # 设置x轴刻度
                axes[0].set_xticks(x_ticks)
                axes[0].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            else:
                axes[0].text(0.5, 0.5, 'No Data', transform=axes[0].transAxes, 
                            ha='center', va='center', fontsize=12)
            
            # (2) 模头压力
            if not motou_df.empty and 'motou_pressure_y' in motou_df.columns:
                data_values = motou_df['motou_pressure_y'].values
                time_values = motou_df.index.values
                logger.info(f"模头压力数据值范围: {data_values.min():.2f} - {data_values.max():.2f}")
                logger.info(f"模头压力时间点数量: {len(time_values)}")
                # 使用真实时间戳绘制
                axes[1].plot(time_values, data_values, color=ieee_colors[1], lw=1.2, marker='o', markersize=3, markeredgecolor='none')
                axes[1].set_ylabel('Die Pressure\n(MPa)', fontweight='bold', fontsize=10)
                # 动态设置Y轴范围，添加10%的边距
                if len(data_values) > 0 and not np.all(np.isnan(data_values)):
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 0.1
                        axes[1].set_ylim(data_min - margin, data_max + margin)
                axes[1].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[1].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
                # 设置x轴刻度
                axes[1].set_xticks(x_ticks)
                axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            else:
                axes[1].text(0.5, 0.5, 'No Data', transform=axes[1].transAxes, 
                            ha='center', va='center', fontsize=12)
            
            # (3) 喂料泵进料率
            if not other_df.empty and 'feed_pump_rate' in other_df.columns:
                data_values = other_df['feed_pump_rate'].values
                time_values = other_df.index.values
                logger.info(f"喂料泵进料率数据值范围: {data_values.min():.2f} - {data_values.max():.2f}")
                logger.info(f"喂料泵进料率时间点数量: {len(time_values)}")
                # 使用真实时间戳绘制
                axes[2].plot(time_values, data_values, color=ieee_colors[2], lw=1.2, marker='o', markersize=3, markeredgecolor='none')
                axes[2].set_ylabel('Feed Rate\n(kg/h)', fontweight='bold', fontsize=10)
                # 动态设置Y轴范围，添加10%的边距
                if len(data_values) > 0 and not np.all(np.isnan(data_values)):
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 10
                        axes[2].set_ylim(data_min - margin, data_max + margin)
                axes[2].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[2].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
                # 设置x轴刻度
                axes[2].set_xticks(x_ticks)
                axes[2].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            else:
                axes[2].text(0.5, 0.5, 'No Data', transform=axes[2].transAxes, 
                            ha='center', va='center', fontsize=12)
            
            # (4) GP泵入口压力
            if not other_df.empty and 'gp_inlet_pv' in other_df.columns:
                data_values = other_df['gp_inlet_pv'].values
                time_values = other_df.index.values
                logger.info(f"GP泵入口压力数据值范围: {data_values.min():.2f} - {data_values.max():.2f}")
                logger.info(f"GP泵入口压力时间点数量: {len(time_values)}")
                # 使用真实时间戳绘制
                axes[3].plot(time_values, data_values, color=ieee_colors[3], lw=1.2, marker='o', markersize=3, markeredgecolor='none')
                axes[3].set_ylabel('GP Pump Inlet\nPressure (MPa)', fontweight='bold', fontsize=10)
                # 动态设置Y轴范围，添加10%的边距
                if len(data_values) > 0 and not np.all(np.isnan(data_values)):
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 0.1
                        axes[3].set_ylim(data_min - margin, data_max + margin)
                axes[3].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[3].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
                # 设置x轴刻度
                axes[3].set_xticks(x_ticks)
                axes[3].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            else:
                axes[3].text(0.5, 0.5, 'No Data', transform=axes[3].transAxes, 
                            ha='center', va='center', fontsize=12)
            
            # (5) GP泵出口压力
            if not other_df.empty and 'gp_outlet_pressure' in other_df.columns:
                data_values = other_df['gp_outlet_pressure'].values
                time_values = other_df.index.values
                logger.info(f"GP泵出口压力数据值范围: {data_values.min():.2f} - {data_values.max():.2f}")
                logger.info(f"GP泵出口压力时间点数量: {len(time_values)}")
                # 使用真实时间戳绘制
                axes[4].plot(time_values, data_values, color=ieee_colors[4], lw=1.2, marker='o', markersize=3, markeredgecolor='none')
                axes[4].set_ylabel('GP Pump Outlet\nPressure (MPa)', fontweight='bold', fontsize=10)
                # 动态设置Y轴范围，添加10%的边距
                if len(data_values) > 0 and not np.all(np.isnan(data_values)):
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 0.1
                        axes[4].set_ylim(data_min - margin, data_max + margin)
                axes[4].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[4].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
                # 设置x轴刻度
                axes[4].set_xticks(x_ticks)
                axes[4].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            else:
                axes[4].text(0.5, 0.5, 'No Data', transform=axes[4].transAxes, 
                            ha='center', va='center', fontsize=12)
            
            # (6) 过滤器入口压力
            if not other_df.empty and 'filter_inlet_pressure' in other_df.columns:
                data_values = other_df['filter_inlet_pressure'].values
                time_values = other_df.index.values
                logger.info(f"过滤器入口压力数据值范围: {data_values.min():.2f} - {data_values.max():.2f}")
                logger.info(f"过滤器入口压力时间点数量: {len(time_values)}")
                # 使用真实时间戳绘制
                axes[5].plot(time_values, data_values, color=ieee_colors[5], lw=1.2, marker='o', markersize=3, markeredgecolor='none')
                axes[5].set_ylabel('Filter Inlet\nPressure (MPa)', fontweight='bold', fontsize=10)
                # 动态设置Y轴范围，添加10%的边距
                if len(data_values) > 0 and not np.all(np.isnan(data_values)):
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 0.1
                        axes[5].set_ylim(data_min - margin, data_max + margin)
                axes[5].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[5].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
                # 设置x轴刻度
                axes[5].set_xticks(x_ticks)
                axes[5].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            else:
                axes[5].text(0.5, 0.5, 'No Data', transform=axes[5].transAxes, 
                            ha='center', va='center', fontsize=12)
            
            # (7) 主电机扭矩
            if not other_df.empty and 'main_motor_torque_pv' in other_df.columns:
                data_values = other_df['main_motor_torque_pv'].values
                time_values = other_df.index.values
                logger.info(f"主电机扭矩数据值范围: {data_values.min():.2f} - {data_values.max():.2f}")
                logger.info(f"主电机扭矩时间点数量: {len(time_values)}")
                # 使用真实时间戳绘制
                axes[6].plot(time_values, data_values, color=ieee_colors[6], lw=1.2, marker='o', markersize=3, markeredgecolor='none')
                axes[6].set_ylabel('Motor Torque\n(N·m)', fontweight='bold', fontsize=10)
                # 动态设置Y轴范围，添加10%的边距
                if len(data_values) > 0 and not np.all(np.isnan(data_values)):
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 10
                        axes[6].set_ylim(data_min - margin, data_max + margin)
                axes[6].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[6].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
                # 设置x轴刻度
                axes[6].set_xticks(x_ticks)
                axes[6].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            else:
                axes[6].text(0.5, 0.5, 'No Data', transform=axes[6].transAxes, 
                            ha='center', va='center', fontsize=12)
            
            # (8) GP泵速度
            if not other_df.empty and 'gp_pump_speed_pv' in other_df.columns:
                data_values = other_df['gp_pump_speed_pv'].values
                time_values = other_df.index.values
                logger.info(f"GP泵速度数据值范围: {data_values.min():.2f} - {data_values.max():.2f}")
                logger.info(f"GP泵速度时间点数量: {len(time_values)}")
                # 使用真实时间戳绘制
                axes[7].plot(time_values, data_values, color=ieee_colors[7], lw=1.2, marker='o', markersize=3, markeredgecolor='none')
                axes[7].set_ylabel('GP Pump Speed\n(rpm)', fontweight='bold', fontsize=10)
                # 动态设置Y轴范围，添加10%的边距
                if len(data_values) > 0 and not np.all(np.isnan(data_values)):
                    valid_data = data_values[~np.isnan(data_values)]
                    if len(valid_data) > 0:
                        data_min, data_max = valid_data.min(), valid_data.max()
                        margin = (data_max - data_min) * 0.1 if data_max != data_min else 10
                        axes[7].set_ylim(data_min - margin, data_max + margin)
                axes[7].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
                axes[7].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
                # 设置x轴刻度
                axes[7].set_xticks(x_ticks)
                axes[7].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            else:
                axes[7].text(0.5, 0.5, 'No Data', transform=axes[7].transAxes, 
                            ha='center', va='center', fontsize=12)
            
            # --- 绘制膜厚热力图（下方1个子图）---
            # 将时间轴转换为数值（matplotlib内部使用）
            time_axis_numeric = mdates.date2num(time_axis)
            
            # 设置热力图的范围：[xmin, xmax, ymin, ymax]
            # 使用并集时间范围，确保热力图与加工参数图对齐
            plot_start_time_numeric = mdates.date2num(plot_start_time)
            plot_end_time_numeric = mdates.date2num(plot_end_time)
            extent = [plot_start_time_numeric, plot_end_time_numeric, 0, heatmap_data_transposed.shape[0]]
            
            im = axes[8].imshow(heatmap_data_transposed, 
                                aspect='auto', 
                                cmap=cmap_blue_green_red, 
                                vmin=vmin, 
                                vmax=vmax,
                                interpolation='nearest',
                                extent=extent,
                                origin='lower')
            
            # 设置x轴为日期格式
            axes[8].xaxis_date()
            date_formatter = mdates.DateFormatter('%H:%M:%S')
            axes[8].xaxis.set_major_formatter(date_formatter)
            
            # 设置x轴刻度 - 使用并集时间范围，与其他子图保持一致
            axes[8].set_xlim(plot_start_time, plot_end_time)
            
            # 为热力图设置x轴刻度，与其他子图保持一致
            if n_time <= 20:
                heatmap_x_ticks = time_axis
            else:
                step = max(1, n_time // 10)
                heatmap_x_ticks = time_axis[::step]
            axes[8].set_xticks(heatmap_x_ticks)
            
            # 设置标签
            axes[8].set_xlabel('Time', fontweight='bold', fontsize=10)
            axes[8].set_ylabel('Position', fontweight='bold', fontsize=10)
            
            # 设置刻度方向向内
            axes[8].tick_params(direction='in', top=True, right=True, which='both', labelsize=9)
            
            # 添加网格线
            axes[8].grid(True, alpha=0.3, color='gray', linestyle='--', linewidth=0.5)
            
            # 调整布局
            plt.tight_layout()
            
            # 将图形保存到内存中的字节流
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=dpi, bbox_inches='tight', 
                       facecolor='white', edgecolor='none', pad_inches=0.1)
            buffer.seek(0)
            
            # 获取二进制数据
            image_data = buffer.getvalue()
            
            # 关闭图形，释放内存
            plt.close(fig)
            
            logger.info(f"成功创建包含加工参数的完整热力图，时间范围: {start_time} 到 {end_time}")
            return image_data
            
        except Exception as e:
            logger.error(f"创建包含加工参数的完整热力图时发生错误: {e}")
            # 返回空字节数组
            return b''
    
    def generate_thickness_map_with_process_params(self, start_time: datetime, end_time: datetime) -> Optional[ThicknessMap]:
        """
        生成包含加工参数趋势图的膜厚温度云图
        
        参数:
            start_time: 开始时间
            end_time: 结束时间
            
        返回:
            ThicknessMap: 生成的膜厚温度云图对象，如果没有数据则返回None
        """
        try:
            # 从数据库获取指定时间范围内的膜厚数据
            thickness_data = self._get_thickness_data(start_time, end_time)
            
            if not thickness_data:
                logger.warning(f"在时间范围 {start_time} 到 {end_time} 内没有找到膜厚数据")
                return None
            
            # 如果上一次判定是异常，先更新mask
            if self.last_is_abnormal:
                self._update_mask(thickness_data)
                logger.info("上一次判定为异常，已更新mask数据")
            else:
                self._reduce_mask(thickness_data)
                logger.info("上一次判定为正常，已减少mask覆盖")
            
            # 生成纯的温度云图（不带坐标和图例），用于CNN诊断
            pure_map_image = self._create_heatmap(thickness_data, with_labels=False, mask=self.mask_data)
            
            # 使用CNN服务诊断纯云图是否异常
            is_abnormal = False
            try:
                prediction_result = self.cnn_service.predict_from_bytes_sync(
                    pure_map_image, filename=f"thickness_map_{start_time.strftime('%Y%m%d_%H%M%S')}.png"
                )
                is_abnormal = prediction_result.prediction == 1
                logger.info(f"CNN诊断结果: {prediction_result.label} (置信度: {prediction_result.confidence:.2f})")
            except Exception as e:
                logger.warning(f"CNN诊断失败，默认为正常: {e}")
                is_abnormal = False
            
            # 更新上一次的异常状态
            self.last_is_abnormal = is_abnormal
            
            # 生成所有图片（应用更新后的mask）
            # 注意：所有图片将上传到MinIO对象存储，ThicknessMap只保留MinIO路径元数据
            map_image = self._create_heatmap(thickness_data, with_labels=True)
            combined_image = self._create_combined_heatmap(thickness_data, start_time, end_time)
            
            # 创建一个uuid
            thickness_map_uuid = str(uuid.uuid4())

            # 如果识别为异常，将uuid和图片传入Agent进行诊断（在后台线程中执行）
            if is_abnormal:
                # 将uuid和图片传入Agent进行诊断（在后台线程中执行）
                def run_agent_diagnosis():
                    try:
                        logger.info(f"开始Agent诊断，UUID: {thickness_map_uuid}")
                        
                        # 检查 vl_agent 是否已初始化
                        if state.vl_agent is None:
                            logger.error("state.vl_agent 未初始化，无法进行诊断")
                            return
                        
                        # 将bytes图片保存为临时文件
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_detect:
                            temp_detect.write(map_image)
                            temp_detect_path = temp_detect.name
                        
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_process:
                            temp_process.write(combined_image)
                            temp_process_path = temp_process.name
                        
                        logger.info(f"临时文件已创建: detect={temp_detect_path}, process={temp_process_path}")
                        
                        # 调用agent的工作流方法
                        logger.info("开始调用run_workflow方法")
                        result = state.vl_agent.run_workflow(
                            imageDetect_path=temp_detect_path,
                            imageProcess_path=temp_process_path,
                            thickness_map_uuid=thickness_map_uuid,
                            stream=False
                        )
                        logger.info(f"run_workflow方法调用完成，结果: {result}")
                        
                        # 清理临时文件
                        try:
                            import os
                            os.unlink(temp_detect_path)
                            os.unlink(temp_process_path)
                            logger.info("临时文件已清理")
                        except Exception as cleanup_error:
                            logger.warning(f"清理临时文件失败: {cleanup_error}")
                        
                        logger.info(f"Agent诊断完成，UUID: {thickness_map_uuid}")
                    except Exception as e:
                        import traceback
                        logger.error(f"Agent诊断失败，UUID: {thickness_map_uuid}, 错误: {e}")
                        logger.error(f"详细错误堆栈: {traceback.format_exc()}")
                
                # 启动后台线程进行诊断
                logger.info("准备启动后台线程进行诊断")
                diagnosis_thread = threading.Thread(target=run_agent_diagnosis, daemon=True)
                diagnosis_thread.start()
                logger.info("检测到异常，已更新mask数据，正在后台进行Agent诊断")
            else:
                self._reduce_mask(thickness_data)
                logger.info("预测正常，已减少mask覆盖")
            

            
            # 创建ThicknessMap对象
            thickness_map = ThicknessMap.from_thickness_data(
                thickness_map_uuid=thickness_map_uuid,
                start_time=start_time,
                end_time=end_time,
                thickness_data=thickness_data,
                map_image=map_image,
                combined_image=combined_image,
                pure_map_image=pure_map_image,
                is_abnormal=is_abnormal
            )
            
            # 保存膜厚温度云图到数据库
            self.save_thickness_map(thickness_map)
            
            logger.info(f"成功生成包含加工参数的膜厚温度云图，时间范围: {start_time} 到 {end_time}，数据点数: {len(thickness_data)}")
            return thickness_map
            
        except Exception as e:
            logger.error(f"生成包含加工参数的膜厚温度云图时发生错误: {e}")
            return None
    
    def _update_mask(self, thickness_data: List[List[float]]):
        """
        更新mask数据，只要检测到异常，就将整个数据区域标记为mask
        
        参数:
            thickness_data: 二维膜厚数据数组[时间×模头位置]
        """
        try:
            mask_preserve_rows = config.thickness_map.mask_preserve_rows
            
            # 将膜厚数据转换为numpy数组
            data_array = np.array(thickness_data)
            
            # 创建新的mask（全部标记为True，即整个区域都被遮盖）
            new_mask = np.ones(data_array.shape, dtype=bool)
            
            # 如果已有mask，进行逻辑或运算保留之前的mask区域
            if self.mask_data is not None and self.mask_data.shape == new_mask.shape:
                self.mask_data = np.logical_or(self.mask_data, new_mask)
            else:
                self.mask_data = new_mask
            
            # 确保最后 mask_preserve_rows 行不被mask覆盖
            if data_array.shape[0] > mask_preserve_rows:
                self.mask_data[-mask_preserve_rows:, :] = False
            
            logger.info(f"Mask已更新，异常区域数: {np.sum(self.mask_data)} / {self.mask_data.size}")
            
        except Exception as e:
            logger.error(f"更新mask时发生错误: {e}")
    
    def _reduce_mask(self, thickness_data: List[List[float]]):
        """
        减少mask覆盖，当预测为正常时将mask向上移动，显示更多最新膜厚数据
        mask永远不遮挡最后 mask_preserve_rows 行数据
        
        参数:
            thickness_data: 二维膜厚数据数组[时间×模头位置]
        """
        try:
            mask_preserve_rows = config.thickness_map.mask_preserve_rows
            reduce_mask_ratio = config.thickness_map.reduce_mask_ratio
            
            if self.mask_data is None:
                return
            
            # 将膜厚数据转换为numpy数组
            data_array = np.array(thickness_data)
            
            # 如果mask数据形状与当前数据不匹配，调整mask形状
            if self.mask_data.shape != data_array.shape:
                old_mask = self.mask_data
                new_mask = np.zeros(data_array.shape, dtype=bool)
                
                # 复制旧mask内容到新mask
                old_rows, old_cols = old_mask.shape
                new_rows, new_cols = data_array.shape
                
                # 行数处理：取旧mask的后new_rows行（如果旧mask更长）或全部（如果旧mask更短）
                if old_rows >= new_rows:
                    mask_row_start = old_rows - new_rows
                    mask_row_end = old_rows
                    new_mask_row_start = 0
                    new_mask_row_end = new_rows
                else:
                    mask_row_start = 0
                    mask_row_end = old_rows
                    new_mask_row_start = new_rows - old_rows
                    new_mask_row_end = new_rows
                
                # 列数处理：取min(old_cols, new_cols)列
                cols_to_copy = min(old_cols, new_cols)
                
                # 复制mask数据
                new_mask[new_mask_row_start:new_mask_row_end, :cols_to_copy] = old_mask[mask_row_start:mask_row_end, :cols_to_copy]
                
                self.mask_data = new_mask
                logger.info(f"Mask形状已从{old_mask.shape}调整为{new_mask.shape}")
            
            # 确保最后 mask_preserve_rows 行不被mask覆盖
            if data_array.shape[0] > mask_preserve_rows:
                self.mask_data[-mask_preserve_rows:, :] = False
            
            # 计算mask覆盖的行数（排除最后 mask_preserve_rows 行）
            if data_array.shape[0] > mask_preserve_rows:
                mask_rows = np.any(self.mask_data[:-mask_preserve_rows, :], axis=1)
            else:
                mask_rows = np.any(self.mask_data, axis=1)
            covered_row_count = np.sum(mask_rows)
            
            # 如果没有mask覆盖，直接返回
            if covered_row_count == 0:
                return
            
            # 计算向上移动的行数（每次移动 reduce_mask_ratio 比例的覆盖行数，至少移动1行）
            shift_rows = max(1, int(covered_row_count * reduce_mask_ratio))
            
            # 将mask向上移动（只移动除最后 mask_preserve_rows 行外的区域）
            if data_array.shape[0] > mask_preserve_rows:
                movable_rows = data_array.shape[0] - mask_preserve_rows
                if movable_rows > shift_rows:
                    self.mask_data[:-mask_preserve_rows-shift_rows, :] = self.mask_data[shift_rows:-mask_preserve_rows, :]
                    self.mask_data[-mask_preserve_rows-shift_rows:-mask_preserve_rows, :] = False
                else:
                    self.mask_data[:-mask_preserve_rows, :] = False
                logger.info(f"Mask已向上移动{shift_rows}行，异常区域数: {np.sum(self.mask_data)} / {self.mask_data.size}")
            else:
                self.mask_data[:] = False
                logger.info(f"Mask已全部清除")
            
        except Exception as e:
            logger.error(f"减少mask时发生错误: {e}")