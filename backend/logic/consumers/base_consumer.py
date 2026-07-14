import logging
from abc import ABC, abstractmethod
from typing import Dict, Any

from backend.config.config_loader import config as app_config

# 配置日志
logging.basicConfig(level=getattr(logging, app_config.system.log_level, logging.INFO))
logger = logging.getLogger(__name__)

class BaseConsumer(ABC):
    """
    消费者基类，定义了消费者的通用接口和基本功能
    """
    def __init__(self, db_connection=None):
        """
        初始化消费者基类
        
        参数:
            db_connection: 数据库连接实例
        """
        self.db_connection = db_connection
        self.is_running = False
        logger.info("消费者基类已初始化")
    
    @abstractmethod
    def start(self) -> None:
        """
        启动消费者
        """
        pass
    
    @abstractmethod
    def stop(self) -> None:
        """
        停止消费者
        """
        pass
    
    @abstractmethod
    def get_status(self) -> Dict[str, Any]:
        """
        获取消费者状态
        
        返回:
            Dict[str, Any]: 消费者状态信息
        """
        pass