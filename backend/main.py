from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import subprocess
import threading
import logging
import signal
import sys
import os

# 加载配置
from backend.config.config_loader import config

# 导入全局状态管理
import backend.state as state
from backend.state import initialize_components, cleanup_components
from backend.utils.paths import get_cnn_api_script

# 配置日志
logging.basicConfig(
    level=getattr(logging, config.system.log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# CNN API 进程
cnn_process = None


def start_cnn_api():
    """在新终端窗口中启动 CNN API 服务"""
    global cnn_process
    try:
        cnn_dir = get_cnn_api_script().parent
        if not cnn_dir.exists():
            logger.error(f"CNN 项目目录不存在: {cnn_dir}")
            return
        
        cnn_port = config.cnn_api.port
        logger.info(f"正在启动 CNN API 服务: {cnn_dir} (端口 {cnn_port})")
        
        if sys.platform == 'win32':
            # Windows: 使用 start 命令打开新的 cmd 窗口
            cmd = f'start "CNN API Server (Port {cnn_port})" cmd /K "cd /d {cnn_dir} && set CNN_PORT={cnn_port} && uv run python cnnfast.py"'
            cnn_process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            logger.info(f"CNN API 服务已在新终端窗口启动 (端口 {cnn_port})")
        else:
            # Unix: 使用 xterm 在新窗口中启动
            env = {**os.environ, "CNN_PORT": str(cnn_port)}
            cnn_process = subprocess.Popen(
                ["xterm", "-hold", "-e", f"cd {cnn_dir} && uv run python cnnfast.py"],
                cwd=str(cnn_dir),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            logger.info(f"CNN API 服务已在新终端窗口启动 (PID: {cnn_process.pid}, 端口 {cnn_port})")
    except Exception as e:
        logger.error(f"启动 CNN API 服务失败: {e}")


def stop_cnn_api():
    """停止 CNN API 服务"""
    global cnn_process
    if cnn_process:
        try:
            if sys.platform == 'win32':
                # Windows: 通过窗口标题查找并杀死进程
                subprocess.run(
                    ['taskkill', '/F', '/FI', 'WINDOWTITLE eq CNN API Server*'],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            else:
                # Unix: 杀死进程
                cnn_process.terminate()
                cnn_process.wait(timeout=5)
            logger.info("CNN API 服务已停止")
        except Exception as e:
            logger.error(f"停止 CNN API 服务失败: {e}")
        finally:
            cnn_process = None


def signal_handler(signum, frame):
    """信号处理器：确保子进程被正确清理"""
    logger.info(f"收到信号 {signum}，正在清理...")
    stop_cnn_api()
    sys.exit(0)


# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
if sys.platform == 'win32':
    # Windows 特有的信号
    try:
        signal.signal(signal.SIGBREAK, signal_handler)
    except (ValueError, AttributeError):
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行
    logger.info("FastAPI应用启动中...")
    
    try:
        # 启动 CNN API 服务（在新终端窗口中）
        start_cnn_api()
        
        # 初始化所有组件
        if not initialize_components():
            logger.error("组件初始化失败")
            yield
            return
        
        # 启动组件 - 从state模块获取最新的组件引用
        # state.sensor_producer.start()
         # state.data_pipeline.start()
        state.detection_producer.start()
        state.detection_data_pipeline.start()
        state.thickness_map_pipeline.start()
        
        # 导入WebSocket管理器并启动后台更新任务
        from backend.websocket.thickness_map_ws import thickness_map_ws_manager
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
        from backend.websocket.thickness_map_ws import thickness_map_ws_manager
        thickness_map_ws_manager.stop_background_task()
        
        # 停止 CNN API 服务
        stop_cnn_api()
        
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
from backend.routes.producer import router as producer_router
from backend.routes.consumer import router as consumer_router
from backend.routes.thickness_map import router as thickness_map_router
from backend.routes.vl import router as vl_router
from backend.routes.thickness_map_ws import router as thickness_map_ws_router
from backend.routes.image_analysis import router as image_analysis_router
from backend.routes.config import router as config_router
from backend.routes.custom_analysis import router as custom_analysis_router

# 注册路由
app.include_router(producer_router, prefix="/api/producer", tags=["生产者管理"])
app.include_router(consumer_router, prefix="/api/consumer", tags=["消费者管理"])
app.include_router(thickness_map_router, prefix="/api/thickness-map", tags=["温度云图管理"])
app.include_router(vl_router, prefix="/api/vl", tags=["视觉诊断 Agent"])
app.include_router(image_analysis_router, prefix="/api/image-analysis", tags=["图片分析管理"])
app.include_router(config_router, prefix="/api")  # 配置管理路由
app.include_router(thickness_map_ws_router)  # 注册WebSocket路由
app.include_router(custom_analysis_router, prefix="/api/custom-analysis", tags=["自定义时空对齐分析"])

@app.get("/")
async def root():
    return {"message": "Kafka生产者和消费者管理系统API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# 导出应用实例
__all__ = ["app"]

# 直接运行应用
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=config.api.host,
        port=config.api.port,
        reload=True,
        log_level="info"
    )