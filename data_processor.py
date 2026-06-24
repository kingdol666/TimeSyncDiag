import logging
import concurrent.futures
from typing import List, Dict, Any
from sqlalchemy.orm import Session

# 导入自定义模块
from models import SensorData, DetectionDeviceData
from db_connection import DatabaseConnection

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    """
    并行数据处理类，用于处理从Kafka接收到的传感器数据
    """
    def __init__(self, db_connection: DatabaseConnection, max_workers: int = 4):
        """
        初始化数据处理器
        
        参数:
            db_connection: 数据库连接实例
            max_workers: 线程池中的最大工作线程数
        """
        self.db_connection = db_connection
        self.max_workers = max_workers
        self.executor = None
    
    def start(self):
        """
        启动数据处理器，初始化线程池
        """
        if self.executor is None:
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
            logger.info(f"数据处理器已启动，线程池大小: {self.max_workers}")
    
    def stop(self):
        """
        停止数据处理器，关闭线程池
        """
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None
            logger.info("数据处理器已停止")
    
    def process_messages(self, messages: List[Dict[str, Any]]):
        """
        并行处理消息列表
        
        参数:
            messages: 要处理的消息列表
            
        返回:
            Dict: 处理结果统计信息
        """
        if not messages:
            return {
                'total': 0,
                'processed': 0,
                'failed': 0
            }
        
        # 确保线程池已启动
        if not self.executor:
            self.start()
        
        # 提交处理任务
        futures = []
        for message in messages:
            futures.append(self.executor.submit(self._process_single_message, message))
        
        # 收集结果
        total = len(messages)
        processed = 0
        failed = 0
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"处理消息时发生异常: {e}")
                failed += 1
        
        # 记录统计信息
        logger.info(f"消息处理统计: 总计={total}, 成功={processed}, 失败={failed}")
        
        return {
            'total': total,
            'processed': processed,
            'failed': failed
        }
    
    def _process_single_message(self, message: Dict[str, Any]) -> bool:
        """
        处理单条传感器数据消息
        
        参数:
            message: 要处理的消息
            
        返回:
            bool: 处理是否成功
        """
        try:
            # 从消息中提取数据
            data = message.get('value')
            if not data:
                logger.warning("接收到空传感器数据消息")
                return False
            
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 将Kafka消息转换为传感器数据模型
                sensor_data = SensorData.from_kafka_message(data)
                
                # 保存到数据库
                session.add(sensor_data)
                session.commit()
                
                logger.debug(f"成功保存传感器数据: 传感器ID={sensor_data.sensor_id}, "
                            f"传感器类型={sensor_data.sensor_type}")
                return True
                
            except Exception as e:
                # 发生错误时回滚事务
                session.rollback()
                logger.error(f"保存传感器数据失败: {e}, 消息: {data}")
                return False
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            logger.error(f"处理传感器数据消息时发生错误: {e}, 消息: {message}")
            return False
    
    def bulk_process_messages(self, messages: List[Dict[str, Any]], batch_size: int = 100):
        """
        批量处理传感器数据消息，减少数据库连接开销
        
        参数:
            messages: 要处理的消息列表
            batch_size: 每批处理的消息数量
            
        返回:
            Dict: 处理结果统计信息
        """
        if not messages:
            return {
                'total': 0,
                'processed': 0,
                'failed': 0
            }
        
        total = len(messages)
        processed = 0
        failed = 0
        
        # 将消息分成批次
        for i in range(0, total, batch_size):
            batch = messages[i:i+batch_size]
            
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 批量处理这一批消息
                batch_processed = 0
                batch_failed = 0
                
                for message in batch:
                    try:
                        data = message.get('value')
                        if data:
                            sensor_data = SensorData.from_kafka_message(data)
                            session.add(sensor_data)
                            batch_processed += 1
                        else:
                            batch_failed += 1
                    except Exception as e:
                        logger.error(f"处理批量传感器数据消息中的单条消息失败: {e}")
                        batch_failed += 1
                
                # 提交事务
                session.commit()
                
                processed += batch_processed
                failed += batch_failed
                
                logger.info(f"批量传感器数据处理完成，批次大小={len(batch)}, 成功={batch_processed}, 失败={batch_failed}")
                
            except Exception as e:
                # 发生错误时回滚事务
                session.rollback()
                logger.error(f"批量传感器数据处理失败: {e}")
                failed += len(batch)
            finally:
                # 关闭会话
                session.close()
        
        logger.info(f"批量传感器数据消息处理统计: 总计={total}, 成功={processed}, 失败={failed}")
        
        return {
            'total': total,
            'processed': processed,
            'failed': failed
        }

class DetectionDataProcessor:
    """
    并行检测数据处理类，用于处理从Kafka接收到的检测设备数据
    """
    def __init__(self, db_connection: DatabaseConnection, max_workers: int = 4):
        """
        初始化检测数据处理器
        
        参数:
            db_connection: 数据库连接实例
            max_workers: 线程池中的最大工作线程数
        """
        self.db_connection = db_connection
        self.max_workers = max_workers
        self.executor = None
    
    def start(self):
        """
        启动数据处理器，初始化线程池
        """
        if self.executor is None:
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
            logger.info(f"检测数据处理器已启动，线程池大小: {self.max_workers}")
    
    def stop(self):
        """
        停止数据处理器，关闭线程池
        """
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None
            logger.info("检测数据处理器已停止")
    
    def process_messages(self, messages: List[Dict[str, Any]]):
        """
        并行处理消息列表
        
        参数:
            messages: 要处理的消息列表
            
        返回:
            Dict: 处理结果统计信息
        """
        if not messages:
            return {
                'total': 0,
                'processed': 0,
                'failed': 0
            }
        
        # 确保线程池已启动
        if not self.executor:
            self.start()
        
        # 提交处理任务
        futures = []
        for message in messages:
            futures.append(self.executor.submit(self._process_single_message, message))
        
        # 收集结果
        total = len(messages)
        processed = 0
        failed = 0
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"处理检测数据消息时发生异常: {e}")
                failed += 1
        
        # 记录统计信息
        logger.info(f"检测数据消息处理统计: 总计={total}, 成功={processed}, 失败={failed}")
        
        return {
            'total': total,
            'processed': processed,
            'failed': failed
        }
    
    def _process_single_message(self, message: Dict[str, Any]) -> bool:
        """
        处理单条检测数据消息
        
        参数:
            message: 要处理的消息
            
        返回:
            bool: 处理是否成功
        """
        try:
            # 从消息中提取数据
            data = message.get('value')
            if not data:
                logger.warning("接收到空检测数据消息")
                return False
            
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 将Kafka消息转换为检测数据模型
                detection_data = DetectionDeviceData.from_kafka_message(data)
                
                # 保存到数据库
                session.add(detection_data)
                session.commit()
                
                logger.debug(f"成功保存检测数据: 设备ID={detection_data.device_id}, "
                            f"检测类型={detection_data.detection_type}")
                return True
                
            except Exception as e:
                # 发生错误时回滚事务
                session.rollback()
                logger.error(f"保存检测数据失败: {e}, 消息: {data}")
                return False
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            logger.error(f"处理检测数据消息时发生错误: {e}, 消息: {message}")
            return False
    
    def bulk_process_messages(self, messages: List[Dict[str, Any]], batch_size: int = 100):
        """
        批量处理检测数据消息，减少数据库连接开销
        
        参数:
            messages: 要处理的消息列表
            batch_size: 每批处理的消息数量
            
        返回:
            Dict: 处理结果统计信息
        """
        if not messages:
            return {
                'total': 0,
                'processed': 0,
                'failed': 0
            }
        
        total = len(messages)
        processed = 0
        failed = 0
        
        # 将消息分成批次
        for i in range(0, total, batch_size):
            batch = messages[i:i+batch_size]
            
            # 创建数据库会话
            session = self.db_connection.get_session()
            
            try:
                # 批量处理这一批消息
                batch_processed = 0
                batch_failed = 0
                
                for message in batch:
                    try:
                        data = message.get('value')
                        if data:
                            detection_data = DetectionDeviceData.from_kafka_message(data)
                            session.add(detection_data)
                            batch_processed += 1
                        else:
                            batch_failed += 1
                    except Exception as e:
                        logger.error(f"处理批量检测数据消息中的单条消息失败: {e}")
                        batch_failed += 1
                
                # 提交事务
                session.commit()
                
                processed += batch_processed
                failed += batch_failed
                
                logger.info(f"批量检测数据处理完成，批次大小={len(batch)}, 成功={batch_processed}, 失败={batch_failed}")
                
            except Exception as e:
                # 发生错误时回滚事务
                session.rollback()
                logger.error(f"批量检测数据处理失败: {e}")
                failed += len(batch)
            finally:
                # 关闭会话
                session.close()
        
        logger.info(f"批量检测数据消息处理统计: 总计={total}, 成功={processed}, 失败={failed}")
        
        return {
            'total': total,
            'processed': processed,
            'failed': failed
        }