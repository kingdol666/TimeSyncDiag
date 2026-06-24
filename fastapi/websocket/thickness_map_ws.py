from fastapi import WebSocket, WebSocketDisconnect
from typing import List, Dict, Any
import logging
import asyncio
import base64
from logic.models.models import ThicknessMap
from logic.models.mini_connection import MinioConnector
import state

# 配置日志
logger = logging.getLogger(__name__)

class ThicknessMapWebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.update_interval = 10  # 10秒更新一次
        self.task: asyncio.Task = None
        self.diagnosis_in_progress = False  # 诊断进行中标志
    
    async def connect(self, websocket: WebSocket):
        """处理新的WebSocket连接"""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"新的WebSocket连接，当前连接数: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """处理WebSocket断开连接"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket连接断开，当前连接数: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: Dict[str, Any], websocket: WebSocket):
        """向特定客户端发送消息"""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"向客户端发送消息失败: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: Dict[str, Any]):
        """向所有连接的客户端广播消息"""
        disconnected_clients = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"广播消息失败: {e}")
                disconnected_clients.append(connection)
        
        # 移除断开连接的客户端
        for client in disconnected_clients:
            self.disconnect(client)
    
    async def get_latest_map_data(self):
        """获取最新膜厚云图数据"""
        try:
            if not state.db_connection or not state.db_connection.connected:
                return {
                    "success": False,
                    "message": "数据库未连接"
                }
            
            if not state.minio_connector:
                return {
                    "success": False,
                    "message": "MinIO连接未初始化"
                }
            
            # 从数据库获取最新的膜厚云图
            session = state.db_connection.get_session()
            try:
                # 查询最新的膜厚云图（按ID顺序取最后一个）
                latest_map = session.query(ThicknessMap).order_by(
                    ThicknessMap.id.desc()
                ).first()
                
                if latest_map:
                    # 从MinIO下载图片数据
                    map_image_bytes = None
                    combined_image_bytes = None
                    
                    # 下载map_image
                    if latest_map.map_image_path:
                        try:
                            map_image_bytes = state.minio_connector.download_file_to_bytes(
                                "test-bucket", 
                                latest_map.map_image_path
                            )
                        except Exception as e:
                            logger.error(f"从MinIO下载map_image失败: {e}")
                    
                    # 下载combined_image
                    if latest_map.combined_image_path:
                        try:
                            combined_image_bytes = state.minio_connector.download_file_to_bytes(
                                "test-bucket", 
                                latest_map.combined_image_path
                            )
                        except Exception as e:
                            logger.error(f"从MinIO下载combined_image失败: {e}")
                    
                    # 将二进制图像数据转换为base64字符串
                    map_image_base64 = base64.b64encode(map_image_bytes).decode('utf-8') if map_image_bytes else None
                    combined_image_base64 = base64.b64encode(combined_image_bytes).decode('utf-8') if combined_image_bytes else None
                    
                    return {
                        "success": True,
                        "message": "成功获取最新膜厚云图",
                        "data": {
                            "id": latest_map.id,
                            "uuid": latest_map.thickness_map_uuid,
                            "map_image": map_image_base64,
                            "map_combined": combined_image_base64,
                            "is_abnormal": latest_map.is_abnormal,
                            "start_time": latest_map.start_time.isoformat(),
                            "end_time": latest_map.end_time.isoformat(),
                            "data_points_count": latest_map.data_points_count,
                            "min_thickness": latest_map.min_thickness,
                            "max_thickness": latest_map.max_thickness,
                            "avg_thickness": latest_map.avg_thickness
                        }
                    }
                else:
                    return {
                        "success": False,
                        "message": "数据库中未找到膜厚云图数据"
                    }
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"获取最新膜厚云图时出错: {e}")
            return {
                "success": False,
                "message": f"获取膜厚云图失败: {str(e)}"
            }
    
    async def background_update_task(self):
        """后台定时更新任务"""
        logger.info("启动膜厚云图WebSocket后台更新任务")
        try:
            while True:
                # 如果诊断正在进行，跳过本次更新
                if self.diagnosis_in_progress:
                    logger.debug("诊断进行中，跳过WebSocket消息推送")
                    await asyncio.sleep(self.update_interval)
                    continue
                
                # 获取最新数据
                map_data = await self.get_latest_map_data()
                # 广播给所有客户端
                await self.broadcast(map_data)
                # 等待指定时间
                await asyncio.sleep(self.update_interval)
        except asyncio.CancelledError:
            logger.info("膜厚云图WebSocket后台更新任务已取消")
        except Exception as e:
            logger.error(f"膜厚云图WebSocket后台更新任务出错: {e}")
    
    def start_background_task(self):
        """启动后台更新任务"""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.background_update_task())
    
    def stop_background_task(self):
        """停止后台更新任务"""
        if self.task and not self.task.done():
            self.task.cancel()
            self.task = None
    
    def start_diagnosis(self):
        """开始诊断，停止WebSocket消息推送"""
        self.diagnosis_in_progress = True
        logger.info("诊断开始，WebSocket消息推送已暂停")
    
    def end_diagnosis(self):
        """结束诊断，恢复WebSocket消息推送"""
        self.diagnosis_in_progress = False
        logger.info("诊断结束，WebSocket消息推送已恢复")

# 创建WebSocket管理器实例
thickness_map_ws_manager = ThicknessMapWebSocketManager()
