import json
import time
import random
import threading
import signal
import sys
import os
import glob
import pandas as pd
from datetime import datetime, timedelta
import pytz
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── 路径引用（注意：此文件在 logic/producers/，使用相对路径访问 utils/）──
# PROJECT_ROOT = fastapi/ 的父目录 = TimeSyncDiag
from ..utils.paths import PROJECT_ROOT, get_realtime_dir
from backend.config.config_loader import config

class SensorDataProducer:
    def __init__(self, bootstrap_servers=None, topic=None, interval=None):
        """
        初始化传感器数据生产者
        
        参数:
            bootstrap_servers: Kafka 服务器地址，默认从配置读取
            topic: 要发布数据的主题名称，默认从配置读取
            interval: 发送间隔（秒），默认从配置读取
        """
        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.topic = topic or config.kafka.sensor_topic
        self.interval = interval or config.kafka.sensor_producer_interval
        self.producer = None
        self.running = False
        self.thread = None
        
        # 注册配置变更回调，支持运行时调整发送间隔
        config.register_change_callback("kafka.sensor_producer_interval", self._on_interval_changed)
    
    def _on_interval_changed(self, key_path: str, value):
        """配置变更回调：调整传感器数据发送间隔"""
        if key_path == "kafka.sensor_producer_interval" and value is not None:
            self.interval = value
            logger.info(f"传感器数据发送间隔已调整为 {value} 秒")
    
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
                linger_ms=10
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
        shanghai_tz = pytz.timezone('Asia/Shanghai')
        now_shanghai = datetime.now(shanghai_tz)
        # 转换为Unix时间戳
        beijing_timestamp = now_shanghai.timestamp()
        
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
                
                # 等待配置指定的间隔
                time.sleep(self.interval)
                
            except Exception as e:
                logger.error(f"生产者循环中发生错误: {e}")
                time.sleep(self.interval)  # 出错后等待相同间隔再继续
        
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
    检测数据生产者类，用于从CSV文件读取数据并发送到Kafka
    """
    def __init__(self, bootstrap_servers=None, topic=None, interval=None,
                 csv_folder=None):
        """
        初始化Kafka生产者

        参数:
            bootstrap_servers: Kafka服务器地址，默认从配置读取
            topic: 要发送消息的主题名称，默认从配置读取
            interval: 发送间隔（秒），默认从配置读取
            csv_folder: CSV文件所在文件夹路径，None则使用默认值
        """
        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.topic = topic or config.kafka.detection_topic
        self.interval = interval or config.kafka.detection_producer_interval
        self.csv_folder = csv_folder or str(get_realtime_dir())
        self.producer = None
        self.running = False
        self.thread = None
        
        # 状态文件路径
        self.state_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'config', 'producer_state.json')
        
        # 设备ID列表
        self.device_ids = ['THK-001', 'THK-002', 'THK-003', 'THK-004', 'THK-005']
        
        # 检测类型列表
        self.detection_types = ['thickness_measurement']
        
        # CSV文件列表和缓存
        self.csv_files = []
        self.data_cache = {}  # 缓存已读取的CSV数据
        self.last_update_time = None  # 上次更新CSV文件列表的时间
        
        # 用于按顺序读取的状态变量
        self.current_file_index = 0
        self.current_row_index = 100
        
        # 初始化CSV文件列表
        self.update_csv_files_list()
        
        # 从JSON文件加载状态
        self.load_state()
        
        # 注册配置变更回调，支持运行时调整发送间隔
        config.register_change_callback("kafka.detection_producer_interval", self._on_interval_changed)
    
    def _on_interval_changed(self, key_path: str, value):
        """配置变更回调：调整检测数据发送间隔"""
        if key_path == "kafka.detection_producer_interval" and value is not None:
            self.interval = value
            logger.info(f"检测数据发送间隔已调整为 {value} 秒")
    
    def update_csv_files_list(self):
        """
        更新CSV文件列表
        """
        try:
            # 获取文件夹下所有CSV文件（兼容 .csv 和 .CSV）
            self.csv_files = glob.glob(os.path.join(self.csv_folder, "*.csv")) + \
                             glob.glob(os.path.join(self.csv_folder, "*.CSV"))
            shanghai_tz = pytz.timezone('Asia/Shanghai')
            self.last_update_time = datetime.now(shanghai_tz)
            logger.info(f"找到 {len(self.csv_files)} 个CSV文件")
        except Exception as e:
            logger.error(f"更新CSV文件列表时发生错误: {e}")
            self.csv_files = []
    
    def load_state(self):
        """
        从JSON文件加载状态
        """
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content.strip():
                        state = json.loads(content)
                        self.current_file_index = state.get('current_file_index', 0)
                        self.current_row_index = state.get('current_row_index', 100)
                        logger.info(f"从状态文件加载: current_file_index={self.current_file_index}, current_row_index={self.current_row_index}")
                    else:
                        logger.info("状态文件为空，使用默认值")
            else:
                logger.info("状态文件不存在，使用默认值")
        except Exception as e:
            logger.error(f"加载状态文件时发生错误: {e}")
    
    def save_state(self):
        """
        保存状态到JSON文件
        """
        try:
            state = {
                'current_file_index': self.current_file_index,
                'current_row_index': self.current_row_index
            }
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"状态已保存: current_file_index={self.current_file_index}, current_row_index={self.current_row_index}")
        except Exception as e:
            logger.error(f"保存状态文件时发生错误: {e}")
    
    def load_csv_data(self, file_path):
        """
        加载CSV文件数据到缓存
        
        参数:
            file_path: CSV文件路径
            
        返回:
            DataFrame: CSV数据
        """
        # 检查是否已缓存
        if file_path in self.data_cache:
            return self.data_cache[file_path]
        
        try:
            # 读取CSV文件
            df = pd.read_csv(file_path)
            self.data_cache[file_path] = df
            logger.debug(f"已加载CSV文件: {file_path}, 行数: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"加载CSV文件 {file_path} 时发生错误: {e}")
            return None
    
    def get_random_row_from_csv(self):
        """
        从CSV文件中按顺序获取一行数据
        
        返回:
            dict: 包含CSV行数据的字典
        """
        # 如果没有CSV文件，尝试更新列表
        if not self.csv_files:
            self.update_csv_files_list()
            
        # 如果仍然没有文件，返回None
        if not self.csv_files:
            logger.warning("没有找到CSV文件")
            return None
        
        try:
            # 按顺序选择当前文件
            csv_file = self.csv_files[self.current_file_index]
            
            # 加载CSV数据
            df = self.load_csv_data(csv_file)
            if df is None or df.empty:
                # 如果当前文件为空，移动到下一个文件
                self.current_file_index = (self.current_file_index + 1) % len(self.csv_files)
                self.current_row_index = 0
                self.save_state()
                return self.get_random_row_from_csv()
            
            # 检查当前行索引是否超出文件行数
            if self.current_row_index >= len(df):
                # 如果当前文件已读完，移动到下一个文件
                self.current_file_index = (self.current_file_index + 1) % len(self.csv_files)
                self.current_row_index = 0
                self.save_state()
                return self.get_random_row_from_csv()
            
            # 获取当前行
            selected_row = df.iloc[self.current_row_index]
            
            # 获取第一列作为时间戳（假设第一列是时间列）
            timestamp_str = selected_row.iloc[0]
            
            # 尝试解析时间戳
            try:
                # 解析字符串格式的时间戳（例如：2025-02-13 18:02:54）
                timestamp = pd.to_datetime(timestamp_str, errors='coerce')
                if pd.isna(timestamp):
                    # 如果解析失败，使用当前时间
                    timestamp = datetime.now(pytz.timezone('Asia/Shanghai'))
            except Exception:
                # 如果解析失败，使用当前时间
                timestamp = datetime.now(pytz.timezone('Asia/Shanghai'))
            
            # 获取除第一列外的前3000列数据作为有效数据
            if len(selected_row) > 1:
                # 只取前3000列数据（索引1到3000）
                valid_columns = selected_row.index[1:3001]  # 前3000列有效数据
                row_data = {col: selected_row[col] for col in valid_columns if col in selected_row}
            else:
                row_data = {}
            
            # 保存当前行信息
            result = {
                'file_name': os.path.basename(csv_file),
                'row_index': self.current_row_index,
                'data': row_data,
                'timestamp': timestamp.isoformat(),
                'device_id': 'thickness',
                'detection_type': random.choice(self.detection_types)
            }
            
            # 移动到下一行
            self.current_row_index += 1
            self.save_state()
            
            return result
            
        except Exception as e:
            logger.error(f"获取CSV行时发生错误: {e}")
            # 出错时移动到下一行
            self.current_row_index += 1
            self.save_state()
            return self.get_random_row_from_csv()
    
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
                linger_ms=10      # 等待时间，用于批量发送
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
        从CSV文件生成检测设备数据
        
        返回:
            dict: 检测设备数据
        """
        # 获取CSV随机行数据
        csv_data = self.get_random_row_from_csv()
        
        if csv_data is None:
            # 如果无法获取CSV数据，生成默认数据
            logger.warning("无法获取CSV数据，生成默认数据")
            return {
                'device_id': random.choice(self.device_ids),
                'detection_type': random.choice(self.detection_types),
                'values': [round(random.uniform(0.1, 0.2), 3) for _ in range(100)],
                'start_time': (datetime.now(pytz.timezone('Asia/Shanghai')) - timedelta(seconds=6)).isoformat(),
                'end_time': datetime.now(pytz.timezone('Asia/Shanghai')).isoformat(),
                'error': '无法获取CSV数据'
            }
        
        # 构造数据
        data = {
            'device_id': csv_data['device_id'],
            'detection_type': csv_data['detection_type'],
            'csv_file': csv_data['file_name'],
            'row_index': csv_data['row_index'],
            'csv_data': csv_data['data'],
            'timestamp': csv_data['timestamp']
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
                    
                    # 根据数据来源调整日志信息
                    if 'csv_file' in data:
                        logger.info(f"发送CSV数据成功: 设备ID={data['device_id']}, "
                                   f"检测类型={data['detection_type']}, "
                                   f"CSV文件={data['csv_file']}, "
                                   f"行号={data['row_index']}, "
                                   f"分区={record_metadata.partition}, "
                                   f"偏移量={record_metadata.offset}")
                    else:
                        logger.info(f"发送默认数据成功: 设备ID={data['device_id']}, "
                                   f"检测类型={data['detection_type']}, "
                                   f"分区={record_metadata.partition}, "
                                   f"偏移量={record_metadata.offset}")
                    
                except KafkaError as e:
                    logger.error(f"发送消息失败: {e}")
                except Exception as e:
                    logger.error(f"发送消息时发生未知错误: {e}")
                
                # 等待配置指定的间隔
                time.sleep(self.interval)
                
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
        
        # 保存状态
        self.save_state()
        
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
    
    # 创建传感器数据生产者（参数默认从配置读取）
    sensor_producer = SensorDataProducer()
    
    # 创建检测数据生产者（参数默认从配置读取）
    detection_producer = DetectionDataProducer()
    
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