import json
import time
import random
import threading
import signal
import sys
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SensorDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='sensor_data'):
        """
        初始化传感器数据生产者
        
        参数:
            bootstrap_servers: Kafka 服务器地址
            topic: 要发布数据的主题名称
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.running = False
        self.thread = None
        
    def connect(self):
        """连接到 Kafka 服务器"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # 等待所有副本确认
                retries=3,    # 重试次数
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            logger.info(f"已连接到 Kafka 服务器: {self.bootstrap_servers}")
            return True
        except KafkaError as e:
            logger.error(f"连接 Kafka 失败: {e}")
            return False
    
    def generate_sensor_data(self):
        """生成模拟传感器数据"""
        sensor_types = ['temperature', 'humidity', 'pressure', 'light', 'motion']
        sensor_id = f"sensor_{random.randint(1000, 9999)}"
        sensor_type = random.choice(sensor_types)
        
        # 根据传感器类型生成不同的数据
        if sensor_type == 'temperature':
            value = round(random.uniform(18.0, 35.0), 2)  # 温度范围 18-35°C
            unit = 'celsius'
        elif sensor_type == 'humidity':
            value = round(random.uniform(30.0, 90.0), 2)  # 湿度范围 30-90%
            unit = 'percent'
        elif sensor_type == 'pressure':
            value = round(random.uniform(990.0, 1030.0), 2)  # 气压范围 990-1030 hPa
            unit = 'hPa'
        elif sensor_type == 'light':
            value = round(random.uniform(0.0, 1000.0), 2)  # 光照范围 0-1000 lux
            unit = 'lux'
        else:  # motion
            value = random.choice([True, False])  # 运动检测 True/False
            unit = 'boolean'
        
        # 生成北京时间（UTC+8）的时间戳
        now_utc = datetime.now()
        # 转换为Unix时间戳
        beijing_timestamp = now_utc.timestamp()
        
        return {
            'sensor_id': sensor_id,
            'sensor_type': sensor_type,
            'value': value,
            'unit': unit,
            'timestamp': beijing_timestamp,
            'location': {
                'room': f"Room_{random.randint(1, 10)}",
                'floor': random.randint(1, 5)
            }
        }
    
    def send_data(self, data):
        """发送数据到 Kafka"""
        try:
            # 使用传感器ID作为key，确保相同传感器的数据发送到同一分区
            future = self.producer.send(
                topic=self.topic,
                key=data['sensor_id'],
                value=data
            )
            
            # 可以选择性地等待确认
            record_metadata = future.get(timeout=5)
            logger.debug(f"数据发送成功: 主题={record_metadata.topic}, 分区={record_metadata.partition}, 偏移量={record_metadata.offset}")
            return True
        except KafkaError as e:
            logger.error(f"发送数据失败: {e}")
            return False
        except Exception as e:
            logger.error(f"发送数据时发生错误: {e}")
            return False
    
    def producer_loop(self):
        """生产者循环，在单独线程中运行"""
        logger.info("传感器数据生产者线程已启动")
        
        while self.running:
            try:
                # 生成传感器数据
                sensor_data = self.generate_sensor_data()
                
                # 发送数据
                self.send_data(sensor_data)
                
                # 打印发送的数据
                logger.info(f"发送传感器数据: {sensor_data}")
                
                # 等待1秒
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"生产者循环中发生错误: {e}")
                time.sleep(1)  # 出错后等待1秒再继续
        
        logger.info("传感器数据生产者线程已停止")
    
    def start(self):
        """启动生产者线程"""
        if not self.producer:
            if not self.connect():
                return False
        
        self.running = True
        self.thread = threading.Thread(target=self.producer_loop)
        self.thread.daemon = True  # 设置为守护线程，主程序退出时自动结束
        self.thread.start()
        logger.info("传感器数据生产者已启动，按 Ctrl+C 停止")
        return True
    
    def stop(self):
        """停止生产者"""
        logger.info("正在停止传感器数据生产者...")
        self.running = False
        
        # 等待线程结束
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        # 关闭生产者
        if self.producer:
            try:
                self.producer.flush(timeout=5)  # 确保所有消息都已发送
                self.producer.close()
                logger.info("Kafka 生产者已关闭")
            except Exception as e:
                logger.error(f"关闭 Kafka 生产者时发生错误: {e}")
        
        logger.info("传感器数据生产者已停止")

class DetectionDataProducer:
    """
    检测数据生产者类，用于生成并发送检测设备数据到Kafka
    """
    def __init__(self, bootstrap_servers='localhost:9092', topic='detection_data'):
        """
        初始化Kafka生产者
        
        参数:
            bootstrap_servers: Kafka服务器地址
            topic: 要发送消息的主题名称
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.running = False
        self.thread = None
        
        # 设备ID列表
        self.device_ids = ['THK-001', 'THK-002', 'THK-003', 'THK-004', 'THK-005']
        
        # 检测类型列表
        self.detection_types = ['thickness_measurement']
    
    def connect(self):
        """
        连接到Kafka服务器
        
        返回:
            bool: 连接是否成功
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # 等待所有副本确认
                retries=3,   # 发送失败重试次数
                batch_size=16384,  # 批量发送大小
                linger_ms=10,      # 等待时间，用于批量发送
                buffer_memory=33554432  # 缓冲区大小
            )
            logger.info(f"已连接到Kafka服务器: {self.bootstrap_servers}")
            return True
        except KafkaError as e:
            logger.error(f"连接Kafka失败: {e}")
            return False
        except Exception as e:
            logger.error(f"连接Kafka时发生未知错误: {e}")
            return False
    
    def generate_detection_data(self):
        """
        生成模拟的检测设备数据
        
        返回:
            dict: 检测设备数据
        """
        # 随机选择设备ID和检测类型
        device_id = random.choice(self.device_ids)
        detection_type = random.choice(self.detection_types)
        
        # 生成100个膜厚数据
        values = []
        base_value = random.uniform(0.1, 0.2)  # 基础值
        
        for i in range(100):
            # 添加一些随机波动
            variation = random.uniform(-0.02, 0.02)
            value = round(base_value + variation, 3)
            values.append(value)
        
        # 生成检测时间范围（当前时间前5-10分钟）
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=random.randint(5, 10))
        
        # 构造数据
        data = {
            'device_id': device_id,
            'detection_type': detection_type,
            'values': values,
            'start_time': start_time.isoformat() + 'Z',
            'end_time': end_time.isoformat() + 'Z'
        }
        
        return data
    
    def produce_messages(self):
        """
        生产消息的主循环
        """
        logger.info("检测数据生产者线程已启动")
        
        try:
            while self.running:
                # 生成检测数据
                data = self.generate_detection_data()
                
                # 发送消息
                try:
                    future = self.producer.send(
                        topic=self.topic,
                        key=data['device_id'],
                        value=data
                    )
                    
                    # 等待发送完成
                    record_metadata = future.get(timeout=10)
                    
                    logger.info(f"发送检测数据成功: 设备ID={data['device_id']}, "
                               f"检测类型={data['detection_type']}, "
                               f"数据点数量={len(data['values'])}, "
                               f"分区={record_metadata.partition}, "
                               f"偏移量={record_metadata.offset}")
                    
                except KafkaError as e:
                    logger.error(f"发送消息失败: {e}")
                except Exception as e:
                    logger.error(f"发送消息时发生未知错误: {e}")
                
                # 固定等待2秒
                sleep_time = 2.0
                time.sleep(sleep_time)
                
        except Exception as e:
            logger.error(f"生产消息时发生错误: {e}")
        
        logger.info("检测数据生产者线程已停止")
    
    def start(self):
        """
        启动生产者线程
        
        返回:
            bool: 启动是否成功
        """
        if not self.producer:
            if not self.connect():
                return False
        
        self.running = True
        self.thread = threading.Thread(target=self.produce_messages)
        self.thread.daemon = True  # 设置为守护线程
        self.thread.start()
        logger.info("检测数据生产者已启动")
        return True
    
    def stop(self):
        """
        停止生产者
        """
        logger.info("正在停止检测数据生产者...")
        self.running = False
        
        # 等待线程结束
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        # 关闭生产者
        if self.producer:
            try:
                self.producer.flush()  # 确保所有消息都已发送
                self.producer.close()
                logger.info("检测数据生产者已关闭")
            except Exception as e:
                logger.error(f"关闭检测数据生产者时发生错误: {e}")
        
        logger.info("检测数据生产者已停止")

def signal_handler(signum, frame):
    """
    处理中断信号的全局函数
    """
    logger.info(f"接收到信号 {signum}，正在停止程序...")
    sys.exit(0)

def main():
    """主函数"""
    # 在Windows上，只在主线程中注册信号处理
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except (ValueError, AttributeError) as e:
        logger.warning(f"无法注册信号处理函数: {e}")
    
    # 创建传感器数据生产者
    sensor_producer = SensorDataProducer(
        bootstrap_servers='localhost:9092',  # 默认Kafka端口
        topic='sensor_data'
    )
    
    # 创建检测数据生产者
    detection_producer = DetectionDataProducer(
        bootstrap_servers='localhost:9092',
        topic='detection_data'
    )
    
    try:
        # 启动传感器数据生产者
        if not sensor_producer.start():
            logger.error("传感器数据生产者启动失败")
            sys.exit(1)
        
        # 启动检测数据生产者
        if not detection_producer.start():
            logger.error("检测数据生产者启动失败")
            sensor_producer.stop()
            sys.exit(1)
        
        # 主线程等待
        while sensor_producer.running or detection_producer.running:
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.error(f"程序运行时发生错误: {e}")
    finally:
        sensor_producer.stop()
        detection_producer.stop()

if __name__ == "__main__":
    main()