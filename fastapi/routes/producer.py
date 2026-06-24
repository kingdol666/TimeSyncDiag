from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Dict, Any
import logging
import sys
import os

# 添加上级目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# 导入全局变量
from state import sensor_producer, detection_producer

logger = logging.getLogger(__name__)
router = APIRouter()

class ProducerStatus(BaseModel):
    sensor_producer: Dict[str, Any]
    detection_producer: Dict[str, Any]

class ProducerResponse(BaseModel):
    message: str
    status: str

@router.get("/status", response_model=ProducerStatus)
async def get_producer_status():
    """获取生产者状态"""
    try:
        sensor_status = {
            "running": sensor_producer.running if sensor_producer else False,
            "type": "sensor_data"
        }
        
        detection_status = {
            "running": detection_producer.running if detection_producer else False,
            "type": "detection_data"
        }
        
        return ProducerStatus(
            sensor_producer=sensor_status,
            detection_producer=detection_status
        )
    except Exception as e:
        logger.error(f"获取生产者状态时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取生产者状态失败: {str(e)}"
        )

@router.post("/sensor/start", response_model=ProducerResponse)
async def start_sensor_producer():
    """启动传感器数据生产者"""
    try:
        if not sensor_producer:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="传感器生产者未初始化"
            )
            
        if sensor_producer.is_running:
            return ProducerResponse(
                message="传感器生产者已在运行中",
                status="already_running"
            )
            
        import threading
        thread = threading.Thread(target=sensor_producer.start)
        thread.daemon = True
        thread.start()
        
        logger.info("传感器生产者启动命令已发送")
        
        return ProducerResponse(
            message="传感器生产者启动成功",
            status="started"
        )
    except Exception as e:
        logger.error(f"启动传感器生产者时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"启动传感器生产者失败: {str(e)}"
        )

@router.post("/sensor/stop", response_model=ProducerResponse)
async def stop_sensor_producer():
    """停止传感器数据生产者"""
    try:
        if not sensor_producer:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="传感器生产者未初始化"
            )
            
        if not sensor_producer.running:
            return ProducerResponse(
                message="传感器生产者已停止",
                status="already_stopped"
            )
            
        sensor_producer.stop()
        
        logger.info("传感器生产者停止命令已发送")
        
        return ProducerResponse(
            message="传感器生产者停止成功",
            status="stopped"
        )
    except Exception as e:
        logger.error(f"停止传感器生产者时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"停止传感器生产者失败: {str(e)}"
        )

@router.post("/detection/start", response_model=ProducerResponse)
async def start_detection_producer():
    """启动检测数据生产者"""
    try:
        if not detection_producer:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="检测生产者未初始化"
            )
            
        if detection_producer.is_running:
            return ProducerResponse(
                message="检测生产者已在运行中",
                status="already_running"
            )
            
        import threading
        thread = threading.Thread(target=detection_producer.start)
        thread.daemon = True
        thread.start()
        
        logger.info("检测生产者启动命令已发送")
        
        return ProducerResponse(
            message="检测生产者启动成功",
            status="started"
        )
    except Exception as e:
        logger.error(f"启动检测生产者时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"启动检测生产者失败: {str(e)}"
        )

@router.post("/detection/stop", response_model=ProducerResponse)
async def stop_detection_producer():
    """停止检测数据生产者"""
    try:
        if not detection_producer:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="检测生产者未初始化"
            )
            
        if not detection_producer.running:
            return ProducerResponse(
                message="检测生产者已停止",
                status="already_stopped"
            )
            
        detection_producer.stop()
        
        logger.info("检测生产者停止命令已发送")
        
        return ProducerResponse(
            message="检测生产者停止成功",
            status="stopped"
        )
    except Exception as e:
        logger.error(f"停止检测生产者时出错: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"停止检测生产者失败: {str(e)}"
        )