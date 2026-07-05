import json
import logging
import threading
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config.config_loader import config as app_config

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaMessageConsumer:
    """
    Kafka消息消费者类，用于从Kafka主题接收传感器数据
    """
    def __init__(self, bootstrap_servers=None, topic=None, group_id=None, poll_timeout_ms=None):
        """
        初始化Kafka消费者
        
        参数:
            bootstrap_servers: Kafka服务器地址，默认从配置读取
            topic: 要订阅的主题名称，默认从配置读取
            group_id: 消费者组ID，默认从topic自动生成
            poll_timeout_ms: 轮询超时时间（毫秒），默认从配置读取
        """
        self.bootstrap_servers = bootstrap_servers or app_config.kafka.bootstrap_servers
        self.topic = topic or app_config.kafka.sensor_topic
        self.group_id = group_id or f"{self.topic}_consumer"
        self.poll_timeout_ms = poll_timeout_ms or app_config.kafka.consumer_poll_timeout_ms
        self.consumer = None
        self.running = False
        self.thread = None
        self.message_queue = []  # 用于存储接收到的消息
        self.queue_lock = threading.Lock()  # 用于线程安全的队列访问
        
        # 注册配置变更回调，支持运行时调整 poll 超时
        app_config.register_change_callback("kafka.consumer_poll_timeout_ms", self._on_poll_timeout_changed)
        
        # 注册信号处理函数
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def _on_poll_timeout_changed(self, key_path: str, value):
        """配置变更回调：调整 Kafka 消费者 poll 超时时间"""
        if key_path == "kafka.consumer_poll_timeout_ms" and value is not None:
            self.poll_timeout_ms = value
            logger.info(f"Kafka 消费者 poll 超时时间已调整为 {value} 毫秒")
    
    def connect(self):
        """
        连接到Kafka服务器
        
        返回:
            bool: 连接是否成功
        """
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',  # 从最早的消息开始消费
                enable_auto_commit=False,      # 手动提交偏移量
                group_id=self.group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                max_poll_records=50,           # 每次最多拉取50条消息
                max_poll_interval_ms=300000,   # 最大拉取间隔5分钟
                session_timeout_ms=10000       # 会话超时10秒
            )
            logger.info(f"已连接到Kafka服务器: {self.bootstrap_servers}")
            logger.info(f"已订阅主题: {self.topic}")
            return True
        except KafkaError as e:
            logger.error(f"连接Kafka失败: {e}")
            return False
        except Exception as e:
            logger.error(f"连接Kafka时发生未知错误: {e}")
            return False
    
    def consume_messages(self):
        """
        消费消息的主循环
        """
        logger.info("Kafka消费者线程已启动")
        
        try:
            while self.running:
                # 轮询消息，超时时间从配置读取
                records = self.consumer.poll(timeout_ms=self.poll_timeout_ms)
                
                # 处理接收到的消息
                for topic_partition, messages in records.items():
                    with self.queue_lock:
                        for message in messages:
                            # 将消息添加到队列中
                            self.message_queue.append({
                                'key': message.key,
                                'value': message.value,
                                'partition': message.partition,
                                'offset': message.offset
                            })
                            
                            # 记录日志
                            logger.debug(f"接收到消息: key={message.key}, partition={message.partition}, offset={message.offset}")
                
                # 如果有消息，提交偏移量
                if records:
                    try:
                        self.consumer.commit()
                        logger.debug("已提交偏移量")
                    except Exception as e:
                        logger.error(f"提交偏移量失败: {e}")
                        
        except KafkaError as e:
            logger.error(f"消费消息时发生Kafka错误: {e}")
        except Exception as e:
            logger.error(f"消费消息时发生未知错误: {e}")
        
        logger.info("Kafka消费者线程已停止")
    
    def get_messages(self, max_messages=None):
        """
        获取队列中的消息
        
        参数:
            max_messages: 最大获取消息数量，如果为None则获取所有消息
            
        返回:
            list: 消息列表
        """
        with self.queue_lock:
            if max_messages and len(self.message_queue) > max_messages:
                messages = self.message_queue[:max_messages]
                self.message_queue = self.message_queue[max_messages:]
            else:
                messages = self.message_queue.copy()
                self.message_queue.clear()
        
        return messages
    
    def clear_messages(self):
        """
        清空消息队列
        """
        with self.queue_lock:
            self.message_queue.clear()
    
    def start(self):
        """
        启动消费者线程
        
        返回:
            bool: 启动是否成功
        """
        if not self.consumer:
            if not self.connect():
                return False
        
        self.running = True
        self.thread = threading.Thread(target=self.consume_messages)
        self.thread.daemon = True  # 设置为守护线程
        self.thread.start()
        logger.info("Kafka消费者已启动")
        return True
    
    def stop(self):
        """
        停止消费者
        """
        logger.info("正在停止Kafka消费者...")
        self.running = False
        
        # 等待线程结束
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        # 关闭消费者
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka消费者已关闭")
            except Exception as e:
                logger.error(f"关闭Kafka消费者时发生错误: {e}")
        
        logger.info("Kafka消费者已停止")
    
    def signal_handler(self, signum, frame):
        """
        处理中断信号
        """
        logger.info(f"接收到信号 {signum}，正在停止程序...")
        self.stop()
        sys.exit(0)