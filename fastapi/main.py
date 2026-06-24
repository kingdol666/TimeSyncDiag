from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import threading
import logging
import sys
import os

# 导入全局状态管理
import state
from state import initialize_components, cleanup_components

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行
    logger.info("FastAPI应用启动中...")
    
    try:
        # 初始化所有组件
        if not initialize_components():
            logger.error("组件初始化失败")
            yield
            return
        
        # 启动组件 - 从state模块获取最新的组件引用
        state.sensor_producer.start()
        state.detection_producer.start()
        state.data_pipeline.start()
        state.detection_data_pipeline.start()
        state.thickness_map_pipeline.start()
        
        # 导入WebSocket管理器并启动后台更新任务
        from websocket.thickness_map_ws import thickness_map_ws_manager
        thickness_map_ws_manager.start_background_task()
        
        logger.info("所有组件已成功启动")
        
        yield
        
    except Exception as e:
        logger.error(f"启动组件时出错: {e}")
        yield
    
    # 关闭时执行
    logger.info("FastAPI应用关闭中...")
    
    try:
        # 停止WebSocket后台更新任务
        from websocket.thickness_map_ws import thickness_map_ws_manager
        thickness_map_ws_manager.stop_background_task()
        
        # 停止所有组件
        cleanup_components()
            
        logger.info("所有组件已成功停止")
    except Exception as e:
        logger.error(f"停止组件时出错: {e}")

# 创建FastAPI应用
app = FastAPI(
    title="Kafka生产者和消费者管理系统",
    description="用于管理Kafka生产者和消费者的API接口",
    version="1.0.0",
    lifespan=lifespan
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 导入路由
from routes.producer import router as producer_router
from routes.consumer import router as consumer_router
from routes.thickness_map import router as thickness_map_router
from routes.qwen_vl import router as qwen_vl_router
from routes.thickness_map_ws import router as thickness_map_ws_router
from routes.image_analysis import router as image_analysis_router

# 注册路由
app.include_router(producer_router, prefix="/api/producer", tags=["生产者管理"])
app.include_router(consumer_router, prefix="/api/consumer", tags=["消费者管理"])
app.include_router(thickness_map_router, prefix="/api/thickness-map", tags=["温度云图管理"])
app.include_router(qwen_vl_router, prefix="/api/qwen-vl", tags=["Qwen-VL 模型"])
app.include_router(image_analysis_router, prefix="/api/image-analysis", tags=["图片分析管理"])
app.include_router(thickness_map_ws_router)  # 注册WebSocket路由

@app.get("/")
async def root():
    return {"message": "Kafka生产者和消费者管理系统API"}

@app.get("/health")
async def health_check():
    lifespan(app)
    return {"status": "healthy"}

# 导出应用实例
__all__ = ["app"]

# 直接运行应用
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )