from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, ARRAY
from sqlalchemy.sql import func
from db_connection import Base

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
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    
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
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)  # 开始检测时间
    end_time = Column(DateTime(timezone=True), nullable=False, index=True)    # 结束检测时间
    
    # 原始消息存储
    raw_data = Column(JSON)
    
    # 创建时间
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
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
            values=message.get('values', []),
            raw_data=message
        )
        
        # 设置开始检测时间
        start_time = message.get('start_time')
        if start_time:
            from datetime import datetime
            if isinstance(start_time, (int, float)):
                # 如果是Unix时间戳，转换为datetime对象
                data.start_time = datetime.fromtimestamp(start_time)
            elif isinstance(start_time, str):
                # 如果是字符串，尝试解析为datetime对象
                try:
                    data.start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                except:
                    pass
        
        # 设置结束检测时间
        end_time = message.get('end_time')
        if end_time:
            from datetime import datetime
            if isinstance(end_time, (int, float)):
                # 如果是Unix时间戳，转换为datetime对象
                data.end_time = datetime.fromtimestamp(end_time)
            elif isinstance(end_time, str):
                # 如果是字符串，尝试解析为datetime对象
                try:
                    data.end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                except:
                    pass
        
        return data