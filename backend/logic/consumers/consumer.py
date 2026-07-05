import logging
import signal
import sys
import time
import threading
import json

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 导入自定义模块
from ..models.db_connection import DatabaseConnection, build_db_url
from .kafka_consumer import KafkaMessageConsumer
from ..processors.data_processor import DataProcessor, DetectionDataProcessor
from ..processors.scheduler import KafkaToDatabaseScheduler
from ..models.models import SensorData, DetectionDeviceData
from config.config_loader import config as app_config


def build_pipeline_config(topic: str, group_id: str) -> dict:
    """根据全局配置构建管道配置字典"""
    return {
        'kafka': {
            'bootstrap_servers': app_config.kafka.bootstrap_servers,
            'topic': topic,
            'group_id': group_id,
            'poll_timeout_ms': app_config.kafka.consumer_poll_timeout_ms,
        },
        'database': {
            'url': build_db_url(),
        },
        'processor': {
            'max_workers': app_config.database.pool_size or 4,
        },
        'scheduler': {
            'interval': 1.0,
        }
    }

class SensorDataPipeline:
    """
    传感器数据处理管道，整合Kafka消费者、数据处理器和数据库连接
    """
    def __init__(self, config=None):
        """
        初始化数据处理管道
        
        参数:
            config: 配置字典，默认从全局配置读取
        """
        # 默认配置从全局配置读取
        if config is None:
            self.config = build_pipeline_config(
                topic=app_config.kafka.sensor_topic,
                group_id='sensor_data_consumer'
            )
        else:
            self.config = config
        
        # 初始化组件
        self.db_connection = None
        self.kafka_consumer = None
        self.data_processor = None
        self.scheduler = None
        
        # 运行状态标志
        self.is_running = False
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def initialize(self):
        """
        初始化所有组件
        
        返回:
            bool: 初始化是否成功
        """
        try:
            logger.info("开始初始化数据处理管道...")
            
            # 1. 初始化数据库连接
            logger.info("初始化数据库连接...")
            self.db_connection = DatabaseConnection(self.config['database']['url'])
            if not self.db_connection.connect():
                logger.error("数据库连接初始化失败")
                return False
            
            # 2. 初始化Kafka消费者
            logger.info("初始化Kafka消费者...")
            self.kafka_consumer = KafkaMessageConsumer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                topic=self.config['kafka']['topic'],
                group_id=self.config['kafka']['group_id'],
                poll_timeout_ms=self.config['kafka'].get('poll_timeout_ms', 1000)
            )
            if not self.kafka_consumer.connect():
                logger.error("Kafka消费者初始化失败")
                return False
            
            # 3. 初始化数据处理器
            logger.info("初始化数据处理器...")
            self.data_processor = DataProcessor(
                db_connection=self.db_connection,
                max_workers=self.config['processor']['max_workers']
            )
            self.data_processor.start()
            
            # 4. 初始化调度器
            logger.info("初始化调度器...")
            self.scheduler = KafkaToDatabaseScheduler(
                kafka_consumer=self.kafka_consumer,
                data_processor=self.data_processor,
                interval=self.config['scheduler']['interval'],
                pipeline=self  # 传递管道实例，以便检查is_running状态
            )
            
            logger.info("数据处理管道初始化成功")
            return True
            
        except Exception as e:
            logger.error(f"初始化数据处理管道时发生错误: {e}")
            self.shutdown()
            return False
    
    def start(self):
        """
        启动数据处理管道
        
        返回:
            bool: 启动是否成功
        """
        try:
            # 如果还没有初始化，先进行初始化
            if not self.db_connection:
                if not self.initialize():
                    return False
            
            logger.info("开始启动数据处理管道...")
            
            # 启动Kafka消费者
            if not self.kafka_consumer.start():
                logger.error("启动Kafka消费者失败")
                return False
            
            # 启动调度器
            if not self.scheduler.start():
                logger.error("启动调度器失败")
                return False
            
            # 设置运行状态为True
            self.is_running = True
            
            logger.info("数据处理管道启动成功，正在运行...")
            return True
            
        except Exception as e:
            logger.error(f"启动数据处理管道时发生错误: {e}")
            self.shutdown()
            return False
    
    def stop(self):
        """
        停止数据处理管道
        
        返回:
            bool: 停止是否成功
        """
        try:
            logger.info("正在停止数据处理管道...")
            
            # 设置运行状态为False
            self.is_running = False
            
            # 调用shutdown方法停止所有组件
            self.shutdown()
            
            logger.info("数据处理管道已停止")
            return True
            
        except Exception as e:
            logger.error(f"停止数据处理管道时发生错误: {e}")
            return False
    
    def shutdown(self):
        """
        关闭所有组件
        """
        logger.info("正在关闭数据处理管道...")
        
        # 按相反的顺序关闭组件
        if self.scheduler:
            try:
                self.scheduler.stop()
            except Exception as e:
                logger.error(f"关闭调度器时发生错误: {e}")
        
        if self.data_processor:
            try:
                self.data_processor.stop()
            except Exception as e:
                logger.error(f"关闭数据处理器时发生错误: {e}")
        
        if self.kafka_consumer:
            try:
                self.kafka_consumer.stop()
            except Exception as e:
                logger.error(f"关闭Kafka消费者时发生错误: {e}")
        
        if self.db_connection:
            try:
                self.db_connection.close()
            except Exception as e:
                logger.error(f"关闭数据库连接时发生错误: {e}")
        
        logger.info("数据处理管道已关闭")
    
    def wait_for_exit(self):
        """
        等待退出信号
        """
        try:
            logger.info("数据处理管道正在运行，按 Ctrl+C 停止...")
            
            # 主循环，保持程序运行
            while self.is_running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("用户中断程序")
        finally:
            self.stop()
    
    def signal_handler(self, signum, frame):
        """
        处理信号
        
        参数:
            signum: 信号编号
            frame: 信号帧
        """
        logger.info(f"接收到信号 {signum}")
        self.stop()
        sys.exit(0)

def create_detection_device_example():
    """
    创建检测设备数据示例
    """
    # 创建一个示例检测设备数据
    example_data = {
        'device_id': 'THK-001',
        'detection_type': 'thickness_measurement',
        'values': [0.12, 0.13, 0.11, 0.14, 0.12, 0.15, 0.13, 0.12, 0.14, 0.13] + [0.13] * 90,  # 100个膜厚数据
        'start_time': '2023-11-15T08:30:00Z',
        'end_time': '2023-11-15T08:35:00Z'
    }
    
    # 使用from_kafka_message方法创建DetectionDeviceData对象
    detection_data = DetectionDeviceData.from_kafka_message(example_data)
    
    # 打印创建的对象
    logger.info(f"创建的检测设备数据: {detection_data}")
    logger.info(f"数据点数量: {len(detection_data.values)}")
    logger.info(f"检测类型: {detection_data.detection_type}")
    logger.info(f"检测时间范围: {detection_data.start_time} 到 {detection_data.end_time}")
    
    return detection_data


class DetectionDataPipeline:
    """
    检测数据处理管道，整合Kafka消费者、数据处理器和数据库连接
    """
    def __init__(self, config=None):
        """
        初始化检测数据处理管道
        
        参数:
            config: 配置字典，默认从全局配置读取
        """
        # 默认配置从全局配置读取
        if config is None:
            self.config = build_pipeline_config(
                topic=app_config.kafka.detection_topic,
                group_id='detection_data_consumer'
            )
        else:
            self.config = config
        
        # 初始化组件
        self.db_connection = None
        self.kafka_consumer = None
        self.data_processor = None
        self.scheduler = None
        
        # 运行状态标志
        self.is_running = False
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def initialize(self):
        """
        初始化所有组件
        
        返回:
            bool: 初始化是否成功
        """
        try:
            logger.info("开始初始化检测数据处理管道...")
            
            # 1. 初始化数据库连接
            logger.info("初始化数据库连接...")
            self.db_connection = DatabaseConnection(self.config['database']['url'])
            if not self.db_connection.connect():
                logger.error("数据库连接初始化失败")
                return False
            
            # 2. 初始化Kafka消费者
            logger.info("初始化Kafka消费者...")
            self.kafka_consumer = KafkaMessageConsumer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                topic=self.config['kafka']['topic'],
                group_id=self.config['kafka']['group_id'],
                poll_timeout_ms=self.config['kafka'].get('poll_timeout_ms', 1000)
            )
            if not self.kafka_consumer.connect():
                logger.error("Kafka消费者初始化失败")
                return False
            
            # 3. 初始化检测数据处理器
            logger.info("初始化检测数据处理器...")
            self.data_processor = DetectionDataProcessor(
                db_connection=self.db_connection,
                max_workers=self.config['processor']['max_workers']
            )
            self.data_processor.start()
            
            # 4. 初始化调度器
            logger.info("初始化调度器...")
            self.scheduler = KafkaToDatabaseScheduler(
                kafka_consumer=self.kafka_consumer,
                data_processor=self.data_processor,
                interval=self.config['scheduler']['interval'],
                pipeline=self
            )
            
            logger.info("检测数据处理管道初始化成功")
            return True
            
        except Exception as e:
            logger.error(f"初始化检测数据处理管道时发生错误: {e}")
            self.shutdown()
            return False
    
    def start(self):
        """
        启动检测数据处理管道
        
        返回:
            bool: 启动是否成功
        """
        try:
            # 如果还没有初始化，先进行初始化
            if not self.db_connection:
                if not self.initialize():
                    return False
            
            logger.info("开始启动检测数据处理管道...")
            
            # 启动Kafka消费者
            if not self.kafka_consumer.start():
                logger.error("启动Kafka消费者失败")
                return False
            
            # 启动调度器
            if not self.scheduler.start():
                logger.error("启动调度器失败")
                return False
            
            # 设置运行状态为True
            self.is_running = True
            
            logger.info("检测数据处理管道启动成功，正在运行...")
            return True
            
        except Exception as e:
            logger.error(f"启动检测数据处理管道时发生错误: {e}")
            self.shutdown()
            return False
    
    def stop(self):
        """
        停止检测数据处理管道
        
        返回:
            bool: 停止是否成功
        """
        try:
            logger.info("正在停止检测数据处理管道...")
            
            # 设置运行状态为False
            self.is_running = False
            
            # 调用shutdown方法停止所有组件
            self.shutdown()
            
            logger.info("检测数据处理管道已停止")
            return True
            
        except Exception as e:
            logger.error(f"停止检测数据处理管道时发生错误: {e}")
            return False
    
    def shutdown(self):
        """
        关闭所有组件
        """
        logger.info("正在关闭检测数据处理管道...")
        
        # 按相反的顺序关闭组件
        if self.scheduler:
            try:
                self.scheduler.stop()
            except Exception as e:
                logger.error(f"关闭调度器时发生错误: {e}")
        
        if self.data_processor:
            try:
                self.data_processor.stop()
            except Exception as e:
                logger.error(f"关闭检测数据处理器时发生错误: {e}")
        
        if self.kafka_consumer:
            try:
                self.kafka_consumer.stop()
            except Exception as e:
                logger.error(f"关闭Kafka消费者时发生错误: {e}")
        
        if self.db_connection:
            try:
                self.db_connection.close()
            except Exception as e:
                logger.error(f"关闭数据库连接时发生错误: {e}")
        
        logger.info("检测数据处理管道已关闭")
    
    def wait_for_exit(self):
        """
        等待退出信号
        """
        try:
            logger.info("检测数据处理管道正在运行，按 Ctrl+C 停止...")
            
            # 主循环，保持程序运行
            while self.is_running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("用户中断程序")
        finally:
            self.stop()
    
    def signal_handler(self, signum, frame):
        """
        信号处理函数
        
        参数:
            signum: 信号编号
            frame: 当前堆栈帧
        """
        logger.info(f"接收到信号 {signum}")
        self.stop()
        sys.exit(0)


def main():
    """
    主函数
    """
    # 创建数据处理管道实例
    sensor_pipeline = SensorDataPipeline()
    
    # 创建检测数据处理管道实例
    detection_pipeline = DetectionDataPipeline()
    
    # 创建检测设备数据示例
    # create_detection_device_example()
    
    try:
        # 初始化并启动传感器数据管道
        if sensor_pipeline.initialize() and sensor_pipeline.start():
            logger.info("传感器数据管道启动成功")
        else:
            logger.error("启动传感器数据管道失败")
            sys.exit(1)
        
        # 初始化并启动检测数据处理管道
        if detection_pipeline.initialize() and detection_pipeline.start():
            logger.info("检测数据处理管道启动成功")
        else:
            logger.error("启动检测数据处理管道失败")
            sensor_pipeline.shutdown()
            sys.exit(1)
        
        # 等待退出
        try:
            logger.info("所有管道正在运行，按 Ctrl+C 停止...")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("用户中断程序")
        finally:
            # 停止所有管道
            sensor_pipeline.shutdown()
            detection_pipeline.shutdown()
            logger.info("所有管道已停止")
    
    except Exception as e:
        logger.error(f"程序运行时发生错误: {e}")
        sensor_pipeline.shutdown()
        detection_pipeline.shutdown()
        sys.exit(1)

if __name__ == "__main__":
    main()