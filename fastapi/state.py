"""
全局状态管理模块
存储应用程序的全局变量和状态
"""

import sys
import os

# 添加logic目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), "logic"))

from logic.consumers.consumer import SensorDataPipeline, DetectionDataPipeline
from logic.consumers.thickness_map_pipeline import ThicknessMapPipeline
from logic.producers.producer import SensorDataProducer, DetectionDataProducer
from logic.models.db_connection import DatabaseConnection
from logic.models.mini_connection import MinioConnector
from logic.services.image_analysis_service import ImageAnalysisService

# 添加LLMAgent目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), "LLMAgent"))

BUCKET_NAME = "test-bucket"   

# 数据管道
data_pipeline = None
detection_data_pipeline = None

# 生产者
sensor_producer = None
detection_producer = None

# 温度云图管道
thickness_map_pipeline = None

# 数据库连接
db_connection = None

# MinIO连接
minio_connector = None

# QwenVL Agent
qwen_vl_agent = None

# 图片分析服务
image_analysis_service = None

# 初始化函数
def initialize_components():
    """初始化所有组件"""
    global data_pipeline, detection_data_pipeline, sensor_producer, detection_producer, thickness_map_pipeline, db_connection, minio_connector, qwen_vl_agent, image_analysis_service
    
    try:
        # 初始化数据库连接
        db_connection = DatabaseConnection()
        try:
            if not db_connection.connect():
                print("数据库连接失败")
        except Exception as e:
            print(f"数据库连接失败: {e}")
        
        # 初始化MinIO连接
        try:
            MINIO_ENDPOINT = "127.0.0.1:9000"
            MINIO_ACCESS_KEY = "minioadmin"
            MINIO_SECRET_KEY = "Minio@123456"
            minio_connector = MinioConnector(
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            minio_connector.create_bucket(BUCKET_NAME)
            print("MinIO连接初始化成功")
        except Exception as e:
            print(f"MinIO连接初始化失败: {e}")
            # MinIO连接初始化失败不影响其他组件运行
        
        # 初始化数据管道
        data_pipeline = SensorDataPipeline()
        try:
            if not data_pipeline.initialize():
                print("数据管道初始化失败")
        except Exception as e:
            print(f"数据管道初始化失败: {e}")
        
        # 初始化检测数据管道
        detection_data_pipeline = DetectionDataPipeline()
        try:
            if not detection_data_pipeline.initialize():
                print("检测数据管道初始化失败")
        except Exception as e:
            print(f"检测数据管道初始化失败: {e}")
        
        # 初始化生产者
        try:
            sensor_producer = SensorDataProducer()
            if not sensor_producer.connect():
                print("传感器生产者连接失败")
        except Exception as e:
            print(f"传感器生产者初始化失败: {e}")

        try:
            detection_producer = DetectionDataProducer()
            if not detection_producer.connect():
                print("检测生产者连接失败")
        except Exception as e:
            print(f"检测生产者初始化失败: {e}")
        
        # 初始化温度云图管道
        try:
            thickness_map_pipeline = ThicknessMapPipeline()
            if not thickness_map_pipeline.initialize():
                print("温度云图管道初始化失败")
        except Exception as e:
            print(f"温度云图管道初始化失败: {e}")
        
        # 初始化 QwenVL Agent
        try:
            from LLMAgent.QwenVLAgent import QwenVLAgent
            qwen_vl_agent = QwenVLAgent(db_connection=db_connection)
            print("QwenVL Agent 初始化成功")
        except Exception as e:
            print(f"QwenVL Agent 初始化失败: {e}")
            # QwenVL Agent 初始化失败不影响其他组件运行
        
        # 初始化图片分析服务
        try:
            image_analysis_service = ImageAnalysisService(db_connection=db_connection)
            print("图片分析服务初始化成功")
        except Exception as e:
            print(f"图片分析服务初始化失败: {e}")
            # 图片分析服务初始化失败不影响其他组件运行
        
        print("所有组件初始化成功")
        return True
        
    except Exception as e:
        print(f"初始化组件时发生错误: {e}")
        return False

# 清理函数
def cleanup_components():
    """清理所有组件"""
    global data_pipeline, detection_data_pipeline, sensor_producer, detection_producer, thickness_map_pipeline, db_connection
    
    # 停止数据管道
    if data_pipeline and hasattr(data_pipeline, 'is_running') and data_pipeline.is_running:
        data_pipeline.stop()
    
    # 停止检测数据管道
    if detection_data_pipeline and hasattr(detection_data_pipeline, 'is_running') and detection_data_pipeline.is_running:
        detection_data_pipeline.stop()
    
    # 停止生产者
    if sensor_producer and hasattr(sensor_producer, 'is_running') and sensor_producer.is_running:
        sensor_producer.stop()
    
    if detection_producer and hasattr(detection_producer, 'is_running') and detection_producer.is_running:
        detection_producer.stop()
    
    # 停止温度云图管道
    if thickness_map_pipeline and hasattr(thickness_map_pipeline, 'is_running') and thickness_map_pipeline.is_running:
        thickness_map_pipeline.stop()
    
    # 关闭数据库连接
    if db_connection:
        db_connection.close()
    
    return True