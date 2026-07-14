import logging
import time
import threading
from datetime import datetime
import pytz
from typing import Dict, Any

# 导入自定义模块
from ..models.db_connection import DatabaseConnection
from ..processors.thickness_map_processor import ThicknessMapProcessor
from ..consumers.base_consumer import BaseConsumer
from backend.config.config_loader import config as app_config
import pytz

# 配置日志
logging.basicConfig(level=getattr(logging, app_config.system.log_level, logging.INFO))
logger = logging.getLogger(__name__)

_SYSTEM_TIMEZONE = app_config.system.timezone

class ThicknessMapConsumer(BaseConsumer):
    """
    膜厚温度云图消费者，定期生成膜厚温度云图并保存到数据库
    """
    def __init__(self, db_connection: DatabaseConnection, interval_seconds: int = 60):
        """
        初始化膜厚温度云图消费者
        
        参数:
            db_connection: 数据库连接实例
            interval_seconds: 生成温度云图的间隔时间（秒），默认为60秒（1分钟）
        """
        super().__init__(db_connection)
        self.interval_seconds = interval_seconds
        self.thickness_map_processor = ThicknessMapProcessor(db_connection)
        self.running = False
        self.thread = None
        logger.info(f"膜厚温度云图消费者已初始化，间隔时间: {interval_seconds} 秒")
    
    def start(self) -> None:
        """
        启动消费者，开始定期生成膜厚温度云图
        """
        if self.running:
            logger.warning("膜厚温度云图消费者已在运行中")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        logger.info("膜厚温度云图消费者已启动")
    
    def stop(self) -> None:
        """
        停止消费者，停止生成膜厚温度云图
        """
        if not self.running:
            logger.warning("膜厚温度云图消费者未在运行")
            return
        
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("膜厚温度云图消费者已停止")
    
    def _run(self) -> None:
        """
        消费者主循环，定期生成膜厚温度云图
        """
        logger.info("膜厚温度云图消费者主循环已开始")
        
        while self.running:
            try:
                # 记录开始时间
                start_time = datetime.now(pytz.timezone(_SYSTEM_TIMEZONE))
                
                # 生成并保存膜厚温度云图
                thickness_map = self.thickness_map_processor.generate_and_save_latest_map()
                
                if thickness_map:
                    logger.info(f"成功生成并保存膜厚温度云图，ID: {thickness_map.id}，时间范围: {thickness_map.start_time} 到 {thickness_map.end_time}")
                else:
                    logger.warning("未能生成膜厚温度云图，可能是因为没有数据")
                
                # 计算已用时间
                elapsed_time = (datetime.now(pytz.timezone(_SYSTEM_TIMEZONE)) - start_time).total_seconds()
                
                # 计算剩余等待时间
                remaining_time = max(0, self.interval_seconds - elapsed_time)
                
                # 等待剩余时间
                if remaining_time > 0:
                    time.sleep(remaining_time)
                    
            except Exception as e:
                logger.error(f"膜厚温度云图消费者运行时发生错误: {e}")
                # 发生错误时等待一段时间再重试
                time.sleep(5)
        
        logger.info("膜厚温度云图消费者主循环已结束")
    
    def generate_map_now(self) -> Dict[str, Any]:
        """
        立即生成一次膜厚温度云图
        
        返回:
            Dict[str, Any]: 生成结果，包含成功状态和相关信息
        """
        try:
            # 生成并保存膜厚温度云图
            thickness_map = self.thickness_map_processor.generate_and_save_latest_map()
            
            if thickness_map:
                return {
                    "success": True,
                    "message": "成功生成膜厚温度云图",
                    "map_id": thickness_map.id,
                    "start_time": thickness_map.start_time.isoformat(),
                    "end_time": thickness_map.end_time.isoformat(),
                    "data_points_count": thickness_map.data_points_count,
                    "min_thickness": thickness_map.min_thickness,
                    "max_thickness": thickness_map.max_thickness,
                    "avg_thickness": thickness_map.avg_thickness
                }
            else:
                return {
                    "success": False,
                    "message": "未能生成膜厚温度云图，可能是因为没有数据"
                }
                
        except Exception as e:
            logger.error(f"立即生成膜厚温度云图时发生错误: {e}")
            return {
                "success": False,
                "message": f"生成膜厚温度云图时发生错误: {str(e)}"
            }
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取消费者状态
        
        返回:
            Dict[str, Any]: 消费者状态信息
        """
        return {
            "running": self.running,
            "interval_seconds": self.interval_seconds,
            "thread_alive": self.thread.is_alive() if self.thread else False
        }