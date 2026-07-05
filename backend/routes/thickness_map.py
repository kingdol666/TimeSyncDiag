from fastapi import APIRouter, HTTPException, Response
from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel
import base64
import logging
import tempfile
import os

from sqlalchemy.orm import defer, load_only

import backend.state as state


logger = logging.getLogger(__name__)

class MapResponse(BaseModel):
    """膜厚云图响应模型"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None

class TimeRangeRequest(BaseModel):
    """时间范围请求模型"""
    start_time: datetime
    end_time: datetime
    page: int = 1
    page_size: int = 10
    include_images: bool = True

router = APIRouter(tags=["thickness-map"])


@router.get("/status", summary="获取温度云图管道状态", description="获取温度云图管道的当前运行状态")
def get_pipeline_status() -> Dict[str, Any]:
    """获取温度云图管道的当前运行状态"""
    try:
        return state.thickness_map_pipeline.get_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取温度云图管道状态失败: {str(e)}")


@router.post("/generate", summary="立即生成温度云图", description="立即生成一次温度云图")
def generate_thickness_map() -> Dict[str, Any]:
    """立即生成一次温度云图"""
    try:
        return state.thickness_map_pipeline.generate_map_now()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成温度云图失败: {str(e)}")


@router.post("/generate-pure", summary="立即生成纯温度云图", description="立即生成一次纯温度云图（不带坐标和图例）")
def generate_pure_thickness_map() -> Dict[str, Any]:
    """立即生成一次纯温度云图（不带坐标和图例）"""
    try:
        return state.thickness_map_pipeline.generate_pure_map_now()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成纯温度云图失败: {str(e)}")


@router.post("/start", summary="启动温度云图管道", description="启动温度云图管道，开始定期生成温度云图")
def start_pipeline() -> Dict[str, Any]:
    """启动温度云图管道"""
    try:
        state.thickness_map_pipeline.start()
        return {"success": True, "message": "温度云图管道启动成功"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启动温度云图管道失败: {str(e)}")


@router.post("/stop", summary="停止温度云图管道", description="停止温度云图管道，停止定期生成温度云图")
def stop_pipeline() -> Dict[str, Any]:
    """停止温度云图管道"""
    try:
        state.thickness_map_pipeline.stop()
        return {"success": True, "message": "温度云图管道停止成功"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"停止温度云图管道失败: {str(e)}")

@router.get("/map/latest", response_model=MapResponse)
async def get_latest_map():
    """获取最新膜厚云图"""
    try:
        from backend.logic.models.models import ThicknessMap
        
        if not state.db_connection or not state.db_connection.connected:
            return MapResponse(
                success=False,
                message="数据库未连接"
            )
        
        # 从数据库获取最新的膜厚云图
        session = state.db_connection.get_session()
        try:
            # 查询最新的膜厚云图（按ID顺序取最后一个）
            # 使用load_only只加载需要的字段，避免加载所有大字段
            latest_map = session.query(ThicknessMap).options(
                load_only(
                    ThicknessMap.id,
                    ThicknessMap.map_image_path,
                    ThicknessMap.start_time,
                    ThicknessMap.end_time,
                    ThicknessMap.data_points_count,
                    ThicknessMap.min_thickness,
                    ThicknessMap.max_thickness,
                    ThicknessMap.avg_thickness
                )
            ).order_by(
                ThicknessMap.id.desc()
            ).first()
            
            if latest_map:
                # 从MinIO下载图片
                map_image_bytes = None
                if latest_map.map_image_path and state.minio_connector:
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                            temp_file_path = temp_file.name
                        
                        if state.minio_connector.download_file("test-bucket", latest_map.map_image_path, temp_file_path):
                            with open(temp_file_path, 'rb') as f:
                                map_image_bytes = f.read()
                            os.unlink(temp_file_path)
                    except Exception as e:
                        logger.error(f"从MinIO下载图片失败: {e}")
                
                if map_image_bytes:
                    map_image_base64 = base64.b64encode(map_image_bytes).decode('utf-8')
                    
                    return MapResponse(
                        success=True,
                        message="成功获取最新膜厚云图",
                        data={
                            "map_image": map_image_base64,
                            "start_time": latest_map.start_time.isoformat(),
                            "end_time": latest_map.end_time.isoformat(),
                            "data_points_count": latest_map.data_points_count,
                            "min_thickness": latest_map.min_thickness,
                            "max_thickness": latest_map.max_thickness,
                            "avg_thickness": latest_map.avg_thickness
                        }
                    )
                else:
                    return MapResponse(
                        success=False,
                        message="无法从MinIO获取膜厚云图图片"
                    )
            else:
                return MapResponse(
                    success=False,
                    message="数据库中未找到膜厚云图数据"
                )
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"获取最新膜厚云图时出错: {e}")
        return MapResponse(
            success=False,
            message=f"获取膜厚云图失败: {str(e)}"
        )

@router.post("/map/range", response_model=MapResponse)
async def get_map_by_time_range(request: TimeRangeRequest):
    """根据时间范围获取膜厚云图"""
    try:
        from backend.logic.models.models import ThicknessMap
        
        if not state.db_connection or not state.db_connection.connected:
            return MapResponse(
                success=False,
                message="数据库未连接"
            )
        
        # 验证分页参数
        if request.page < 1:
            request.page = 1
        if request.page_size < 1 or request.page_size > 100:
            request.page_size = 10
        
        # 计算偏移量
        offset = (request.page - 1) * request.page_size
        
        # 从数据库查询指定时间范围内的所有膜厚云图
        session = state.db_connection.get_session()
        try:
            # 先查询总数
            total_count = session.query(ThicknessMap).filter(
                ThicknessMap.start_time <= request.end_time,
                ThicknessMap.end_time >= request.start_time
            ).count()
            
            # 根据是否需要图像数据，选择不同的加载策略
            if request.include_images:
                # 需要图像数据，使用load_only只加载需要的字段
                query = session.query(ThicknessMap).options(
                    load_only(
                        ThicknessMap.id,
                        ThicknessMap.map_image_path,
                        ThicknessMap.start_time,
                        ThicknessMap.end_time,
                        ThicknessMap.data_points_count,
                        ThicknessMap.min_thickness,
                        ThicknessMap.max_thickness,
                        ThicknessMap.avg_thickness,
                        ThicknessMap.created_at
                    )
                )
            else:
                # 不需要图像数据，使用defer延迟加载大字段
                query = session.query(ThicknessMap).options(
                    defer(ThicknessMap.map_image_path),
                    defer(ThicknessMap.pure_map_image_path),
                    defer(ThicknessMap.combined_image_path),
                    defer(ThicknessMap.data_summary)
                )
            
            # 应用时间范围过滤、排序和分页
            maps_in_range = query.filter(
                ThicknessMap.start_time <= request.end_time,
                ThicknessMap.end_time >= request.start_time
            ).order_by(ThicknessMap.created_at.desc()).offset(offset).limit(request.page_size).all()
            
            if maps_in_range:
                # 将所有云图数据转换为列表
                maps_data = []
                for map_item in maps_in_range:
                    map_data = {
                        "start_time": map_item.start_time.isoformat(),
                        "end_time": map_item.end_time.isoformat(),
                        "data_points_count": map_item.data_points_count,
                        "min_thickness": map_item.min_thickness,
                        "max_thickness": map_item.max_thickness,
                        "avg_thickness": map_item.avg_thickness,
                        "created_at": map_item.created_at.isoformat()
                    }
                    
                    # 只有在需要时才加载图像数据
                    if request.include_images and hasattr(map_item, 'map_image_path') and map_item.map_image_path:
                        try:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                                temp_file_path = temp_file.name
                            
                            if state.minio_connector.download_file("test-bucket", map_item.map_image_path, temp_file_path):
                                with open(temp_file_path, 'rb') as f:
                                    map_image_bytes = f.read()
                                os.unlink(temp_file_path)
                                map_data["map_image"] = base64.b64encode(map_image_bytes).decode('utf-8')
                        except Exception as e:
                            logger.error(f"从MinIO下载图片失败: {e}")
                    
                    maps_data.append(map_data)
                
                return MapResponse(
                    success=True,
                    message=f"成功获取指定时间范围的 {len(maps_data)} 张膜厚云图",
                    data={
                        "maps": maps_data,
                        "total_count": total_count,
                        "page": request.page,
                        "page_size": request.page_size,
                        "total_pages": (total_count + request.page_size - 1) // request.page_size,
                        "time_range": {
                            "start": request.start_time.isoformat(),
                            "end": request.end_time.isoformat()
                        }
                    }
                )
            else:
                return MapResponse(
                    success=False,
                    message="未找到指定时间范围内的膜厚云图数据"
                )
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"根据时间范围获取膜厚云图时出错: {e}")
        return MapResponse(
            success=False,
            message=f"获取膜厚云图失败: {str(e)}"
        )