from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from backend.websocket.thickness_map_ws import thickness_map_ws_manager
import logging

# 配置日志
logger = logging.getLogger(__name__)

# 创建路由
router = APIRouter(
    prefix="/api/thickness-map",
    tags=["温度云图WebSocket"]
)

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """膜厚云图WebSocket端点"""
    await thickness_map_ws_manager.connect(websocket)
    try:
        # 发送初始数据
        initial_data = await thickness_map_ws_manager.get_latest_map_data()
        await thickness_map_ws_manager.send_personal_message(initial_data, websocket)
        
        # 保持连接，处理客户端消息（如果有）
        while True:
            # 这里可以处理客户端发送的消息，如果需要的话
            # message = await websocket.receive_text()
            # 暂时不处理客户端消息，只发送数据
            await websocket.receive_text()
    except WebSocketDisconnect:
        thickness_map_ws_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket连接处理出错: {e}")
        thickness_map_ws_manager.disconnect(websocket)
