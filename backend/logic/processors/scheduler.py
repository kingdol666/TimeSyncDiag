import logging
import threading
import time
from typing import Callable

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskScheduler:
    """
    定时任务执行器，用于定期执行指定的任务
    """
    def __init__(self, task_func: Callable, interval: float = 1.0, name: str = "TaskScheduler"):
        """
        初始化定时任务执行器
        
        参数:
            task_func: 要定期执行的任务函数
            interval: 任务执行间隔（秒），默认1秒
            name: 调度器名称
        """
        self.task_func = task_func
        self.interval = interval
        self.name = name
        self.running = False
        self.thread = None
        self.stop_event = threading.Event()
    
    def start(self):
        """
        启动定时任务
        """
        if self.running:
            logger.warning(f"调度器 {self.name} 已经在运行中")
            return False
        
        self.running = True
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run_scheduler, name=self.name)
        self.thread.daemon = True
        self.thread.start()
        
        logger.info(f"调度器 {self.name} 已启动，执行间隔: {self.interval}秒")
        return True
    
    def stop(self):
        """
        停止定时任务
        """
        if not self.running:
            logger.warning(f"调度器 {self.name} 已经停止")
            return False
        
        logger.info(f"正在停止调度器 {self.name}...")
        self.running = False
        self.stop_event.set()
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)  # 等待线程结束，最多等待5秒
        
        logger.info(f"调度器 {self.name} 已停止")
        return True
    
    def _run_scheduler(self):
        """
        调度器运行循环
        """
        logger.info(f"调度器 {self.name} 线程已启动")
        
        next_run_time = time.time()
        
        while self.running:
            try:
                # 执行任务
                self._execute_task()
                
                # 计算下次执行时间
                next_run_time += self.interval
                sleep_time = next_run_time - time.time()
                
                # 如果需要，等待到下次执行时间
                if sleep_time > 0:
                    # 使用事件等待，允许提前唤醒
                    if self.stop_event.wait(timeout=sleep_time):
                        # 如果事件被设置，表示要停止
                        break
                
            except Exception as e:
                logger.error(f"调度器 {self.name} 执行任务时发生错误: {e}")
                # 发生错误后，仍然继续执行，但是稍微延迟一下
                if not self.stop_event.is_set():
                    self.stop_event.wait(timeout=0.1)
        
        logger.info(f"调度器 {self.name} 线程已停止")
    
    def _execute_task(self):
        """
        执行具体的任务
        """
        try:
            # 记录任务开始时间
            start_time = time.time()
            
            # 执行用户定义的任务函数
            result = self.task_func()
            
            # 计算执行时间
            execution_time = time.time() - start_time
            
            # 记录任务执行情况
            if execution_time > self.interval * 0.5:
                # 如果执行时间超过间隔的一半，发出警告
                logger.warning(f"任务执行时间较长: {execution_time:.4f}秒，接近或超过了执行间隔")
            else:
                logger.debug(f"任务执行完成，耗时: {execution_time:.4f}秒")
            
            return result
            
        except Exception as e:
            logger.error(f"执行任务函数时发生错误: {e}")
            raise
    
    def is_running(self) -> bool:
        """
        检查调度器是否正在运行
        
        返回:
            bool: 调度器是否运行中
        """
        return self.running
    
    def set_interval(self, interval: float):
        """
        动态设置任务执行间隔
        
        参数:
            interval: 新的执行间隔（秒）
        """
        if interval > 0:
            self.interval = interval
            logger.info(f"调度器 {self.name} 执行间隔已设置为: {interval}秒")
        else:
            logger.error(f"无效的执行间隔: {interval}，间隔必须大于0")

class KafkaToDatabaseScheduler(TaskScheduler):
    """
    专门用于从Kafka消费数据并保存到数据库的调度器  
    """
    def __init__(self, kafka_consumer, data_processor, interval: float = 1.0, pipeline=None):
        """
        初始化Kafka到数据库的调度器
        
        参数:
            kafka_consumer: Kafka消费者实例
            data_processor: 数据处理器实例
            interval: 执行间隔（秒），默认1秒
            pipeline: 数据管道实例，用于检查is_running状态
        """
        # 定义任务函数
        def kafka_to_db_task():
            # 检查管道是否仍在运行
            if pipeline and hasattr(pipeline, 'is_running') and not pipeline.is_running:
                logger.debug("管道已停止，跳过任务执行")
                return {'total': 0, 'processed': 0, 'failed': 0}
                
            # 从Kafka消费者获取消息
            messages = kafka_consumer.get_messages()
            
            if messages:
                # 处理消息
                result = data_processor.process_messages(messages)
                return result
            else:
                logger.debug("没有新消息需要处理")
                return {'total': 0, 'processed': 0, 'failed': 0}
        
        # 调用父类初始化
        super().__init__(task_func=kafka_to_db_task, interval=interval, name="KafkaToDatabaseScheduler")
        
        # 保存引用
        self.kafka_consumer = kafka_consumer
        self.data_processor = data_processor
        self.pipeline = pipeline