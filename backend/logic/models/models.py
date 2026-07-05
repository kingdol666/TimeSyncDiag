from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, ARRAY, ForeignKey, Text, Index
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .db_connection import Base
import pytz
import uuid

class SensorData(Base):
    """
    传感器数据模型类，用于映射到TimescaleDB数据库
    """
    __tablename__ = "sensor_data"
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 传感器基本信息
    sensor_id = Column(String(50), nullable=False, index=True)
    sensor_type = Column(String(20), nullable=False, index=True)
    
    # 传感器值相关字段
    value = Column(Float)
    boolean_value = Column(Boolean)
    unit = Column(String(20))
    
    # 时间戳 (TimescaleDB超表的时间维度)
    timestamp = Column(DateTime(timezone=False), server_default=func.now(), index=True)
    
    # 位置信息，使用JSON格式存储复杂结构
    location = Column(JSON)
    
    # 原始消息存储
    raw_data = Column(JSON)
    
    def __repr__(self):
        """
        返回对象的字符串表示
        """
        return f"<SensorData(sensor_id='{self.sensor_id}', sensor_type='{self.sensor_type}', " \
               f"value={self.value}, timestamp='{self.timestamp}')>"
    
    @classmethod
    def from_kafka_message(cls, message):
        """
        从Kafka消息创建传感器数据对象
        
        参数:
            message: Kafka消息中的数据字典
            
        返回:
            SensorData: 传感器数据对象
        """
        # 创建数据对象
        data = cls(
            sensor_id=message.get('sensor_id'),
            sensor_type=message.get('sensor_type'),
            unit=message.get('unit'),
            location=message.get('location'),
            raw_data=message
        )
        
        # 根据值的类型设置不同的字段
        value = message.get('value')
        if isinstance(value, bool):
            data.boolean_value = value
        else:
            data.value = value
        
        # 设置时间戳
        timestamp = message.get('timestamp')
        if timestamp:
            from datetime import datetime
            if isinstance(timestamp, (int, float)):
                # 如果是Unix时间戳，转换为datetime对象
                data.timestamp = datetime.fromtimestamp(timestamp)
            elif isinstance(timestamp, str):
                # 如果是字符串，尝试解析为datetime对象
                try:
                    data.timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    pass
        
        return data


class ThicknessMap(Base):
    """
    膜厚温度云图模型类，用于存储膜厚数据的温度云图
    """
    __tablename__ = "thickness_map"
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # UUID字段 - 用于唯一标识膜厚云图
    thickness_map_uuid = Column(String(36), nullable=False, unique=True, index=True)
    
    # 时间范围 - 添加复合索引优化时间范围查询
    start_time = Column(DateTime(timezone=False), nullable=False, index=True)  # 开始时间
    end_time = Column(DateTime(timezone=False), nullable=False, index=True)    # 结束时间
    
    # 温度云图数据 (以MinIO对象路径存储)
    map_image_path = Column(String(512), nullable=False)  # 带坐标和图例的云图MinIO路径
    pure_map_image_path = Column(String(512))  # 纯的膜厚云图MinIO路径，不带坐标和图例
    combined_image_path = Column(String(512))  # 带加工参数趋势图和膜厚热力图的完整图像MinIO路径
    
    # 统计信息
    data_points_count = Column(Integer, nullable=False)  # 用于生成云图的数据点数量
    min_thickness = Column(Float)  # 最小膜厚值
    max_thickness = Column(Float)  # 最大膜厚值
    avg_thickness = Column(Float)  # 平均膜厚值
    
    # 异常标记
    is_abnormal = Column(Boolean, default=False, nullable=False, index=True)  # 图像是否异常
    
    # 原始数据摘要 (可选，用于调试或分析)
    data_summary = Column(JSON)
    
    # 创建时间
    created_at = Column(DateTime(timezone=False), server_default=func.now(), index=True)
    
    __table_args__ = (
        Index('idx_thickness_map_time_range', 'start_time', 'end_time'),
        Index('idx_thickness_map_created_at', 'created_at'),
    )
    
    def __repr__(self):
        """
        返回对象的字符串表示
        """
        return f"<ThicknessMap(id={self.id}, start_time='{self.start_time}', " \
               f"end_time='{self.end_time}', data_points={self.data_points_count})>"
    
    @classmethod
    def from_thickness_data(cls, thickness_map_uuid, start_time, end_time, thickness_data, map_image, combined_image=None, pure_map_image=None, is_abnormal=False):
        """
        从膜厚数据和温度云图创建ThicknessMap对象
        
        参数:
            thickness_map_uuid: 膜厚云图的UUID
            start_time: 数据开始时间
            end_time: 数据结束时间
            thickness_data: 二维膜厚数据数组[时间×模头位置]
            map_image: 温度云图的二进制数据（带坐标和图例）
            combined_image: 带加工参数趋势图和膜厚热力图的完整图像的二进制数据
            pure_map_image: 纯的膜厚云图的二进制数据（不带坐标和图例）
            is_abnormal: 图像是否异常（CNN诊断结果）
            
        返回:
            ThicknessMap: 膜厚温度云图对象
        """
        # 将二维数据展平以计算统计信息
        flat_data = []
        if thickness_data:
            for row in thickness_data:
                flat_data.extend(row)
        
        # 计算统计信息
        data_points_count = len(flat_data)
        min_thickness = min(flat_data) if flat_data else 0
        max_thickness = max(flat_data) if flat_data else 0
        avg_thickness = sum(flat_data) / data_points_count if flat_data else 0
        
        # 上传图片到MinIO
        import backend.state as state
        import tempfile
        import os
        
        map_image_path = None
        pure_map_image_path = None
        combined_image_path = None
        
        try:
            if state.minio_connector is None:
                raise Exception("MinIO连接未初始化")
            
            # 上传map_image
            if map_image:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                    temp_file.write(map_image)
                    temp_file_path = temp_file.name
                
                object_name = f"thickness_maps/{thickness_map_uuid}/map_image.png"
                if state.minio_connector.upload_file("test-bucket", object_name, temp_file_path):
                    map_image_path = object_name
                os.unlink(temp_file_path)
            
            # 上传pure_map_image
            if pure_map_image:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                    temp_file.write(pure_map_image)
                    temp_file_path = temp_file.name
                
                object_name = f"thickness_maps/{thickness_map_uuid}/pure_map_image.png"
                if state.minio_connector.upload_file("test-bucket", object_name, temp_file_path):
                    pure_map_image_path = object_name
                os.unlink(temp_file_path)
            
            # 上传combined_image
            if combined_image:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                    temp_file.write(combined_image)
                    temp_file_path = temp_file.name
                
                object_name = f"thickness_maps/{thickness_map_uuid}/combined_image.png"
                if state.minio_connector.upload_file("test-bucket", object_name, temp_file_path):
                    combined_image_path = object_name
                os.unlink(temp_file_path)
            
        except Exception as e:
            print(f"上传图片到MinIO失败: {e}")
        
        # 创建数据对象
        data = cls(
            thickness_map_uuid=thickness_map_uuid,
            start_time=start_time,
            end_time=end_time,
            map_image_path=map_image_path,
            pure_map_image_path=pure_map_image_path,
            data_points_count=data_points_count,
            min_thickness=min_thickness,
            max_thickness=max_thickness,
            avg_thickness=avg_thickness,
            combined_image_path=combined_image_path,
            is_abnormal=is_abnormal
        )
        
        return data


class DetectionDeviceData(Base):
    """
    检测设备传感器数据模型类，用于存储检测设备的膜厚数据
    """
    __tablename__ = "detection_device_data"
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 检测设备基本信息
    device_id = Column(String(50), nullable=False, index=True)
    detection_type = Column(String(50), nullable=False, index=True)  # 检测内容类型标识
    
    # 检测数据相关字段
    values = Column(ARRAY(Float), nullable=False)  # 存储100个膜厚数据的数组
    
    # 检测时间范围
    start_time = Column(DateTime(timezone=False), nullable=False, index=True)  # 开始检测时间
    end_time = Column(DateTime(timezone=False), nullable=False, index=True)    # 结束检测时间
    
    # 原始消息存储
    raw_data = Column(JSON)
    
    # 创建时间
    created_at = Column(DateTime(timezone=False), server_default=func.now())
    
    def __repr__(self):
        """
        返回对象的字符串表示
        """
        return f"<DetectionDeviceData(device_id='{self.device_id}', detection_type='{self.detection_type}', " \
               f"values_count={len(self.values) if self.values else 0}, " \
               f"start_time='{self.start_time}', end_time='{self.end_time}')>"
    
    @classmethod
    def from_kafka_message(cls, message):
        """
        从Kafka消息创建检测设备数据对象
        
        参数:
            message: Kafka消息中的数据字典
            
        返回:
            DetectionDeviceData: 检测设备数据对象
        """
        # 创建数据对象
        data = cls(
            device_id=message.get('device_id'),
            detection_type=message.get('detection_type'),
            raw_data=message
        )
        
        # 处理CSV数据或默认数据
        if 'csv_data' in message and message['csv_data']:
            # 从CSV数据中提取values数组
            csv_data = message['csv_data']
            # 将CSV数据字典中的值转换为数组
            values = []
            for key, value in csv_data.items():
                try:
                    # 尝试将值转换为浮点数
                    values.append(float(value))
                except (ValueError, TypeError):
                    # 如果转换失败，跳过该值
                    continue
            
            # 如果没有有效值，使用默认值
            if not values:
                values = [0.13] * 3000
            
            # 确保有3000个值，不足则补充，多余则截断
            if len(values) < 3000:
                values.extend([0.13] * (3000 - len(values)))
            elif len(values) > 3000:
                values = values[:3000]
            
            data.values = values
        elif 'values' in message and message['values']:
            # 使用默认数据中的values数组
            data.values = message['values']
        else:
            # 如果都没有，使用默认值
            data.values = [0.13] * 100
        
        # 设置时间戳
        timestamp = message.get('timestamp')
        if timestamp:
            from datetime import datetime, timedelta
            import pandas as pd
            if isinstance(timestamp, (int, float)):
                # 如果是Unix时间戳，转换为datetime对象
                data.end_time = datetime.fromtimestamp(timestamp)
                # 将start_time设置为end_time减2秒
                data.start_time = data.end_time - timedelta(seconds=2)
            elif isinstance(timestamp, str):
                # 如果是字符串，尝试解析为datetime对象
                try:
                    # 首先尝试使用datetime.fromisoformat解析
                    dt = datetime.fromisoformat(timestamp)
                    data.end_time = dt
                    # 将start_time设置为end_time减2秒
                    data.start_time = dt - timedelta(seconds=2)
                except:
                    try:
                        # 如果失败，尝试使用pandas的强大解析能力
                        dt = pd.to_datetime(timestamp, errors='coerce')
                        if not pd.isna(dt):
                            data.end_time = dt.to_pydatetime()
                            # 将start_time设置为end_time减2秒
                            data.start_time = data.end_time - timedelta(seconds=2)
                        else:
                            # 如果解析失败，使用当前时间
                            data.end_time = datetime.now()
                            # 将start_time设置为end_time减2秒
                            data.start_time = data.end_time - timedelta(seconds=2)
                    except:
                        # 如果解析失败，使用当前时间
                        data.end_time = datetime.now()
                        # 将start_time设置为end_time减2秒
                        data.start_time = data.end_time - timedelta(seconds=2)
        else:
            # 如果没有时间戳，使用当前时间
            from datetime import datetime, timedelta
            data.end_time = datetime.now()
            # 将start_time设置为end_time减2秒
            data.start_time = data.end_time - timedelta(seconds=2)  
        
        # 处理start_time和end_time（如果存在）
        start_time = message.get('start_time')
        if start_time:
            from datetime import datetime
            if isinstance(start_time, (int, float)):
                data.start_time = datetime.fromtimestamp(start_time)
            elif isinstance(start_time, str):
                try:
                    data.start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                except:
                    pass
        
        end_time = message.get('end_time')
        if end_time:
            from datetime import datetime
            if isinstance(end_time, (int, float)):
                data.end_time = datetime.fromtimestamp(end_time)
            elif isinstance(end_time, str):
                try:
                    data.end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                except:
                    pass
        
        return data


class MotouData(Base):
    """
    模头数据模型类，用于存储模头树脂压力和模头压力数据
    """
    __tablename__ = "motou_data"
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 时间戳
    timestamp = Column(DateTime(timezone=False), nullable=False, index=True)
    
    # 模头数据字段
    motou_resin_pressure = Column(Float, nullable=True)  # 模头树脂压力
    motou_pressure_y = Column(Float, nullable=True)  # 模头压力_y
    
    # 创建时间
    created_at = Column(DateTime(timezone=False), server_default=func.now())
    
    def __repr__(self):
        """
        返回对象的字符串表示
        """
        return f"<MotouData(timestamp='{self.timestamp}', " \
               f"motou_resin_pressure={self.motou_resin_pressure}, " \
               f"motou_pressure_y={self.motou_pressure_y})>"
    
    @classmethod
    def from_csv_row(cls, row):
        """
        从CSV行创建模头数据对象
        
        参数:
            row: CSV行数据字典，包含time, 模头树脂压力, 模头压力_y
            
        返回:
            MotouData: 模头数据对象
        """
        import pandas as pd
        from datetime import datetime
        
        # 解析时间戳
        time_value = row.get('time')
        if time_value is not None:
            try:
                if isinstance(time_value, str):
                    time_str = time_value.strip()
                    timestamp = pd.to_datetime(time_str, errors='coerce')
                else:
                    timestamp = pd.to_datetime(time_value, errors='coerce')
                
                if pd.isna(timestamp):
                    timestamp = datetime.now()
                else:
                    timestamp = timestamp.to_pydatetime()
            except:
                timestamp = datetime.now()
        else:
            timestamp = datetime.now()
        
        # 解析压力值
        motou_resin_pressure = None
        motou_pressure_y = None
        
        try:
            resin_pressure_value = row.get('模头树脂压力')
            if resin_pressure_value is not None:
                if isinstance(resin_pressure_value, str):
                    resin_pressure_str = resin_pressure_value.strip()
                    if resin_pressure_str:
                        motou_resin_pressure = float(resin_pressure_str)
                else:
                    motou_resin_pressure = float(resin_pressure_value)
        except (ValueError, TypeError):
            pass
        
        try:
            pressure_y_value = row.get('模头压力_y')
            if pressure_y_value is not None:
                if isinstance(pressure_y_value, str):
                    pressure_y_str = pressure_y_value.strip()
                    if pressure_y_str:
                        motou_pressure_y = float(pressure_y_str)
                else:
                    motou_pressure_y = float(pressure_y_value)
        except (ValueError, TypeError):
            pass
        
        # 创建数据对象
        data = cls(
            timestamp=timestamp,
            motou_resin_pressure=motou_resin_pressure,
            motou_pressure_y=motou_pressure_y
        )
        
        return data


class OtherData(Base):
    """
    其他设备数据模型类，用于存储主电机、GP泵等设备数据
    """
    __tablename__ = "other_data"
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 时间戳
    timestamp = Column(DateTime(timezone=False), nullable=False, index=True)
    
    # 主电机数据
    main_motor_speed_pv = Column(Float, nullable=True)  # 主电机速度PV
    main_motor_speed_sv = Column(Float, nullable=True)  # 主电机速度SV
    main_motor_torque_pv = Column(Float, nullable=True)  # 主电机扭矩PV
    
    # GP泵数据
    gp_pump_speed_pv = Column(Float, nullable=True)  # GP泵速度PV
    gp_pump_speed_sv = Column(Float, nullable=True)  # GP泵速度SV
    gp_pump_torque_pv = Column(Float, nullable=True)  # GP泵扭矩PV
    gp_inlet_pv = Column(Float, nullable=True)  # GP 入口侧 PV
    gp_transparent_pressure = Column(Float, nullable=True)  # GP 透侧压力
    gp_outlet_pressure = Column(Float, nullable=True)  # GP 出口侧压力
    
    # 其他设备数据
    tk_1090db_temperature = Column(Float, nullable=True)  # TK-1090DB 内温度调节
    extruder_die_pressure = Column(Float, nullable=True)  # 挤出机口模压力
    filter_inlet_pressure = Column(Float, nullable=True)  # 过滤器入口压力
    feed_pump_rate = Column(Float, nullable=True)  # 喂料泵进料率
    
    # 创建时间
    created_at = Column(DateTime(timezone=False), server_default=func.now())
    
    def __repr__(self):
        """
        返回对象的字符串表示
        """
        return f"<OtherData(timestamp='{self.timestamp}', " \
               f"main_motor_speed_pv={self.main_motor_speed_pv}, " \
               f"gp_pump_speed_pv={self.gp_pump_speed_pv})>"
    
    @classmethod
    def from_csv_row(cls, row):
        """
        从CSV行创建其他设备数据对象
        
        参数:
            row: CSV行数据字典，包含时间段及各设备参数
            
        返回:
            OtherData: 其他设备数据对象
        """
        import pandas as pd
        from datetime import datetime
        
        # 解析时间戳（处理带空格的列名）
        time_value = row.get('时间段                  ') or row.get('时间段')
        if time_value is not None:
            try:
                if isinstance(time_value, str):
                    time_str = time_value.strip()
                    timestamp = pd.to_datetime(time_str, errors='coerce')
                else:
                    timestamp = pd.to_datetime(time_value, errors='coerce')
                
                if pd.isna(timestamp):
                    timestamp = datetime.now()
                else:
                    timestamp = timestamp.to_pydatetime()
            except:
                timestamp = datetime.now()
        else:
            timestamp = datetime.now()
        
        # 定义字段映射（处理带空格的列名）
        field_mapping = {
            'main_motor_speed_pv': ['主电机速度PV'],
            'main_motor_speed_sv': ['主电机速度SV'],
            'main_motor_torque_pv': ['主电机扭矩PV'],
            'gp_pump_speed_pv': ['GP泵速度PV'],
            'gp_pump_speed_sv': ['GP泵速度SV'],
            'gp_pump_torque_pv': ['GP泵扭矩PV'],
            'tk_1090db_temperature': ['TK-1090DB 内温度调节'],
            'gp_inlet_pv': ['GP 入口侧 PV'],
            'gp_transparent_pressure': ['GP 透侧压力'],
            'extruder_die_pressure': ['挤出机口模压力'],
            'gp_outlet_pressure': ['GP 出口侧压力'],
            'filter_inlet_pressure': ['过滤器入口压力'],
            'feed_pump_rate': ['喂料泵进料率']
        }
        
        # 解析各字段值
        data_dict = {'timestamp': timestamp}
        for field_name, csv_headers in field_mapping.items():
            try:
                value = None
                for csv_header in csv_headers:
                    value = row.get(csv_header)
                    if value is not None:
                        break
                
                if value is not None:
                    if isinstance(value, str):
                        value_str = value.strip()
                        if value_str:
                            data_dict[field_name] = float(value_str)
                    else:
                        data_dict[field_name] = float(value)
            except (ValueError, TypeError):
                data_dict[field_name] = None
        
        # 创建数据对象
        data = cls(**data_dict)
        
        return data


class ImageAnalysisResult(Base):
    """
    加工检测图片分析结果模型类，存储各agent的分析结果
    """
    __tablename__ = "image_analysis_results"
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # UUID字段 - 作为外键关联到膜厚云图
    thickness_map_uuid = Column(String(36), ForeignKey("thickness_map.thickness_map_uuid"), nullable=False, index=True)
    
    # 各agent的分析结果
    detection_agent_result = Column(Text, nullable=True)  # 检测agent的回复内容
    processing_agent_result = Column(Text, nullable=True)  # 加工agent的回复内容
    decision_agent_result = Column(Text, nullable=True)  # 决策agent的回复内容
    
    # 批注和RAG状态
    comment = Column(String(1000))  # 批注内容
    use_rag = Column(Boolean, default=False, nullable=True)  # 是否使用RAG知识库
    
    # 时间戳
    created_at = Column(DateTime(timezone=False), nullable=True)
    updated_at = Column(DateTime(timezone=False), nullable=True)
    
    # 关系定义
    thickness_map = relationship("ThicknessMap", backref="analysis_results")
    
    def __repr__(self):
        """返回对象的字符串表示"""
        return f"<ImageAnalysisResult(id={self.id}, thickness_map_uuid={self.thickness_map_uuid}, use_rag={self.use_rag})>"
    
    @classmethod
    def from_dict(cls, data):
        """
        从字典数据创建ImageAnalysisResult对象
        
        参数:
            data: 包含分析结果数据的字典，支持以下字段:
                - thickness_map_id: 膜厚温度云图ID（必需）
                - detection_agent_result: 检测agent的回复内容
                - processing_agent_result: 加工agent的回复内容
                - decision_agent_result: 决策agent的回复内容
                - comment: 批注内容
                - use_rag: 是否使用RAG知识库
                - created_at: 创建时间
                - updated_at: 更新时间
                
        返回:
            ImageAnalysisResult: 创建的分析结果对象
        """
        from datetime import datetime
        
        # 提取必需字段
        thickness_map_id = data.get('thickness_map_id')
        if thickness_map_id is None:
            raise ValueError("thickness_map_id is required")
        
        # 解析各字段值
        result_dict = {
            'thickness_map_id': int(thickness_map_id),
            'detection_agent_result': data.get('detection_agent_result'),
            'processing_agent_result': data.get('processing_agent_result'),
            'decision_agent_result': data.get('decision_agent_result'),
            'comment': data.get('comment'),
            'use_rag': data.get('use_rag', False)
        }
        
        # 处理时间戳字段
        created_at = data.get('created_at')
        if created_at is not None:
            if isinstance(created_at, str):
                try:
                    result_dict['created_at'] = datetime.fromisoformat(created_at)
                except:
                    result_dict['created_at'] = None
            elif isinstance(created_at, datetime):
                result_dict['created_at'] = created_at
            else:
                result_dict['created_at'] = None
        
        updated_at = data.get('updated_at')
        if updated_at is not None:
            if isinstance(updated_at, str):
                try:
                    result_dict['updated_at'] = datetime.fromisoformat(updated_at)
                except:
                    result_dict['updated_at'] = None
            elif isinstance(updated_at, datetime):
                result_dict['updated_at'] = updated_at
            else:
                result_dict['updated_at'] = None
        
        # 创建数据对象
        result = cls(**result_dict)
        
        return result
    
    def to_dict(self):
        """
        将对象转换为字典
        
        返回:
            Dict: 包含对象所有字段的字典
        """
        return {
            "id": self.id,
            "thickness_map_id": self.thickness_map_id,
            "detection_agent_result": self.detection_agent_result,
            "processing_agent_result": self.processing_agent_result,
            "decision_agent_result": self.decision_agent_result,
            "comment": self.comment,
            "use_rag": self.use_rag,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }