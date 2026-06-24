import logging
import signal
import sys
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any

# 导入自定义模块
from ..models.db_connection import DatabaseConnection
from ..processors.thickness_map_processor import ThicknessMapProcessor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ThicknessMapPipeline:
    """
    膜厚温度云图处理管道，整合膜厚数据处理器和数据库连接
    """
    def __init__(self, config=None):
        """
        初始化膜厚温度云图处理管道
        
        参数:
            config: 配置字典
        """
        # 默认配置
        self.config = config or {
            'database': {
                'url': 'postgresql://user:123456@localhost:5432/tsdb'
            },
            'processor': {
                'interval': 20.0  # 每20秒执行一次
            }
        }
        
        # 初始化组件
        self.db_connection = None
        self.thickness_map_processor = None
        
        # 运行状态标志
        self.is_running = False
        self.thread = None
    
    def initialize(self):
        """
        初始化所有组件
        
        返回:
            bool: 初始化是否成功
        """
        try:
            logger.info("开始初始化膜厚温度云图处理管道...")
            
            # 1. 初始化数据库连接
            logger.info("初始化数据库连接...")
            self.db_connection = DatabaseConnection(self.config['database']['url'])
            if not self.db_connection.connect():
                logger.error("数据库连接初始化失败")
                return False
            
            # 2. 初始化膜厚数据处理器
            logger.info("初始化膜厚数据处理器...")
            self.thickness_map_processor = ThicknessMapProcessor(self.db_connection)
            
            logger.info("膜厚温度云图处理管道初始化成功")
            return True
            
        except Exception as e:
            logger.error(f"初始化膜厚温度云图处理管道时发生错误: {e}")
            self.shutdown()
            return False
    
    def start(self):
        """
        启动膜厚温度云图处理管道
        
        返回:
            bool: 启动是否成功
        """
        try:
            # 如果还没有初始化，先进行初始化
            if not self.db_connection:
                if not self.initialize():
                    return False
            
            logger.info("开始启动膜厚温度云图处理管道...")
            
            # 设置运行状态为True
            self.is_running = True
            
            # 启动处理线程
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
            
            logger.info("膜厚温度云图处理管道启动成功，正在运行...")
            return True
            
        except Exception as e:
            logger.error(f"启动膜厚温度云图处理管道时发生错误: {e}")
            self.shutdown()
            return False
    
    def stop(self):
        """
        停止膜厚温度云图处理管道
        
        返回:
            bool: 停止是否成功
        """
        try:
            logger.info("正在停止膜厚温度云图处理管道...")
            
            # 设置运行状态为False
            self.is_running = False
            
            # 等待线程结束
            if self.thread:
                self.thread.join(timeout=5)
            
            # 调用shutdown方法停止所有组件
            self.shutdown()
            
            logger.info("膜厚温度云图处理管道已停止")
            return True
            
        except Exception as e:
            logger.error(f"停止膜厚温度云图处理管道时发生错误: {e}")
            return False
    
    def shutdown(self):
        """
        关闭所有组件
        """
        logger.info("正在关闭膜厚温度云图处理管道...")
        
        if self.db_connection:
            try:
                self.db_connection.close()
            except Exception as e:
                logger.error(f"关闭数据库连接时发生错误: {e}")
        
        logger.info("膜厚温度云图处理管道已关闭")
    
    def _run(self):
        """
        膜厚温度云图处理主循环
        """
        logger.info("膜厚温度云图处理主循环已开始")
        
        while self.is_running:
            try:
                # 记录开始时间
                start_time = datetime.now()
                
                # 获取最新的膜厚时间范围
                start_time_t, end_time = self.thickness_map_processor.get_latest_thickness_time_range()
                
                # 生成并保存膜厚温度云图
                thickness_map = self.thickness_map_processor.generate_thickness_map_with_process_params(start_time_t, end_time)
                
                if thickness_map:
                    logger.info(f"成功生成并保存膜厚温度云图，ID: {thickness_map.id}，时间范围: {start_time_t} 到 {end_time}")
                else:
                    logger.warning("未能生成膜厚温度云图，可能是因为没有数据")
                
                # 计算已用时间
                elapsed_time = (datetime.now() - start_time).total_seconds()
                
                # 计算剩余等待时间
                remaining_time = max(0, self.config['processor']['interval'] - elapsed_time)
                
                # 等待剩余时间
                if remaining_time > 0:
                    time.sleep(remaining_time)
                    
            except Exception as e:
                logger.error(f"膜厚温度云图处理时发生错误: {e}")
                # 发生错误时等待一段时间再重试
                time.sleep(5)
        
        logger.info("膜厚温度云图处理主循环已结束")
    
    def generate_map_now(self, map_type: str = "thickness") -> Dict[str, Any]:
        """
        立即生成一次云图
        
        参数:
            map_type: 云图类型，支持 "thickness"（膜厚）和 "temperature"（温度）
        
        返回:
            Dict[str, Any]: 生成结果，包含成功状态和相关信息
        """
        try:
            # 根据类型生成并保存云图
            if map_type == "thickness":
                map_data = self.thickness_map_processor.generate_and_save_latest_map()
                map_name = "膜厚云图"
            elif map_type == "temperature":
                map_data = self.thickness_map_processor.generate_and_save_temperature_map()
                map_name = "温度云图"
            else:
                return {
                    "success": False,
                    "message": f"不支持的云图类型: {map_type}，支持的类型: thickness, temperature"
                }
            
            if map_data:
                return {
                    "success": True,
                    "message": f"成功生成{map_name}",
                    "map_type": map_type,
                    "map_id": map_data.id,
                    "start_time": map_data.start_time.isoformat(),
                    "end_time": map_data.end_time.isoformat(),
                    "data_points_count": map_data.data_points_count,
                    "min_value": map_data.min_value,
                    "max_value": map_data.max_value,
                    "avg_value": map_data.avg_value
                }
            else:
                return {
                    "success": False,
                    "message": f"未能生成{map_name}，可能是因为没有数据"
                }
                
        except Exception as e:
            logger.error(f"立即生成{map_name}时发生错误: {e}")
            return {
                "success": False,
                "message": f"生成{map_name}时发生错误: {str(e)}"
            }
    
    def generate_pure_map_now(self) -> Dict[str, Any]:
        """
        立即生成一次纯膜厚云图（不带坐标和图例）
        
        返回:
            Dict[str, Any]: 生成结果
        """
        try:
            # 计算时间范围（当前时间前一分钟）
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=1)
            
            # 生成纯膜厚云图
            pure_map_image = self.thickness_map_processor.generate_pure_map(start_time, end_time)
            
            if pure_map_image:
                return {
                    "success": True,
                    "message": "成功生成纯膜厚云图",
                    "map_type": "pure_thickness",
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "image_size": len(pure_map_image)
                }
            else:
                return {
                    "success": False,
                    "message": "未能生成纯膜厚云图，可能是因为没有数据"
                }
                
        except Exception as e:
            logger.error(f"立即生成纯膜厚云图时发生错误: {e}")
            return {
                "success": False,
                "message": f"生成纯膜厚云图时发生错误: {str(e)}"
            }
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取管道状态
        
        返回:
            Dict[str, Any]: 管道状态信息
        """
        return {
            "running": self.is_running,
            "interval_seconds": self.config['processor']['interval'],
            "thread_alive": self.thread.is_alive() if self.thread else False
        }
    
    def wait_for_exit(self):
        """
        等待退出信号
        """
        try:
            logger.info("膜厚温度云图处理管道正在运行，按 Ctrl+C 停止...")
            
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


def main():
    """
    主函数
    """
    # 创建膜厚温度云图处理管道实例
    thickness_map_pipeline = ThicknessMapPipeline()
    
    # 注册信号处理
    signal.signal(signal.SIGINT, thickness_map_pipeline.signal_handler)
    signal.signal(signal.SIGTERM, thickness_map_pipeline.signal_handler)
    
    try:
        # 初始化并启动膜厚温度云图处理管道
        if thickness_map_pipeline.initialize() and thickness_map_pipeline.start():
            logger.info("膜厚温度云图处理管道启动成功")
        else:
            logger.error("启动膜厚温度云图处理管道失败")
            sys.exit(1)
        
        # 等待退出
        try:
            logger.info("膜厚温度云图处理管道正在运行，按 Ctrl+C 停止...")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("用户中断程序")
        finally:
            # 停止管道
            thickness_map_pipeline.shutdown()
            logger.info("膜厚温度云图处理管道已停止")
    
    except Exception as e:
        logger.error(f"程序运行时发生错误: {e}")
        thickness_map_pipeline.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()