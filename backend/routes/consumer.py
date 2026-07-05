from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import sys
import os

# 添加上级目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# 导入全局状态模块，避免在 lifespan 初始化前捕获 None
import state

logger = logging.getLogger(__name__)
router = APIRouter()

class ConsumerStatus(BaseModel):
    running: bool
    type: str

class DetectionConsumerStatus(BaseModel):
    running: bool
    type: str

class ConsumerStats(BaseModel):
    sensor_data: Dict[str, int]
    detection_data: Dict[str, int]

class ConsumerResponse(BaseModel):
    message: str
    status: str

@router.get("/status", response_model=ConsumerStatus)
async def get_consumer_status():
    """获取传感器数据消费者状态"""
    try:
        consumer_status = {
            "running": state.data_pipeline.is_running if state.data_pipeline else False,
            "type": "data_pipeline"
        }
        
        return ConsumerStatus(**consumer_status)
    except Exception as e:
        logger.error(f"获取传感器消费者状态时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取传感器消费者状态失败: {str(e)}"
        )

@router.get("/detection/status", response_model=DetectionConsumerStatus)
async def get_detection_consumer_status():
    """获取检测数据消费者状态"""
    try:
        consumer_status = {
            "running": state.detection_data_pipeline.is_running if state.detection_data_pipeline else False,
            "type": "detection_data_pipeline"
        }
        
        return DetectionConsumerStatus(**consumer_status)
    except Exception as e:
        logger.error(f"获取检测消费者状态时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取检测消费者状态失败: {str(e)}"
        )

@router.get("/stats", response_model=ConsumerStats)
async def get_consumer_stats():
    """获取消费者统计信息"""
    try:
        # 获取传感器数据统计
        sensor_stats = {}
        if state.data_pipeline and hasattr(state.data_pipeline, 'stats'):
            sensor_stats = state.data_pipeline.stats
        
        # 获取检测数据统计
        detection_stats = {}
        if state.detection_data_pipeline and hasattr(state.detection_data_pipeline, 'stats'):
            detection_stats = state.detection_data_pipeline.stats
        
        return ConsumerStats(
            sensor_data=sensor_stats,
            detection_data=detection_stats
        )
    except Exception as e:
        logger.error(f"获取消费者统计信息时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取消费者统计信息失败: {str(e)}"
        )

@router.post("/start", response_model=ConsumerResponse)
async def start_consumer():
    """启动传感器数据消费者"""
    try:
        if not state.data_pipeline:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="数据管道未初始化"
            )
            
        if state.data_pipeline.is_running:
            return ConsumerResponse(
                message="消费者已在运行中",
                status="already_running"
            )
            
        import threading
        thread = threading.Thread(target=state.data_pipeline.start)
        thread.daemon = True
        thread.start()
        
        logger.info("消费者启动命令已发送")
        
        return ConsumerResponse(
            message="消费者启动成功",
            status="started"
        )
    except Exception as e:
        logger.error(f"启动消费者时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"启动消费者失败: {str(e)}"
        )

@router.post("/detection/start", response_model=ConsumerResponse)
async def start_detection_consumer():
    """启动检测数据消费者"""
    try:
        if not state.detection_data_pipeline:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="检测数据管道未初始化"
            )
            
        if state.detection_data_pipeline.is_running:
            return ConsumerResponse(
                message="检测消费者已在运行中",
                status="already_running"
            )
            
        import threading
        thread = threading.Thread(target=state.detection_data_pipeline.start)
        thread.daemon = True
        thread.start()
        
        logger.info("检测消费者启动命令已发送")
        
        return ConsumerResponse(
            message="检测消费者启动成功",
            status="started"
        )
    except Exception as e:
        logger.error(f"启动检测消费者时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"启动检测消费者失败: {str(e)}"
        )

@router.post("/stop", response_model=ConsumerResponse)
async def stop_consumer():
    """停止传感器数据消费者"""
    try:
        if not state.data_pipeline:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="数据管道未初始化"
            )
            
        if not state.data_pipeline.is_running:
            return ConsumerResponse(
                message="消费者未在运行",
                status="already_stopped"
            )
            
        state.data_pipeline.stop()
        
        logger.info("消费者停止命令已发送")
        
        return ConsumerResponse(
            message="消费者停止成功",
            status="stopped"
        )
    except Exception as e:
        logger.error(f"停止消费者时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"停止消费者失败: {str(e)}"
        )

@router.post("/detection/stop", response_model=ConsumerResponse)
async def stop_detection_consumer():
    """停止检测数据消费者"""
    try:
        if not state.detection_data_pipeline:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="检测数据管道未初始化"
            )
            
        if not state.detection_data_pipeline.is_running:
            return ConsumerResponse(
                message="检测消费者未在运行",
                status="already_stopped"
            )
            
        state.detection_data_pipeline.stop()
        
        logger.info("检测消费者停止命令已发送")
        
        return ConsumerResponse(
            message="检测消费者停止成功",
            status="stopped"
        )
    except Exception as e:
        logger.error(f"停止检测消费者时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"停止检测消费者失败: {str(e)}"
        )
