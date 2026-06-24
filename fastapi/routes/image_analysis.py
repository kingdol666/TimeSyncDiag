from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
from pydantic import BaseModel, Field
import logging

import state
from logic.models.schemas import ImageAnalysisWithImagesListResponse, ThicknessMapImageResponse, ImageAnalysisResultResponse, UpdateCommentRequest

logger = logging.getLogger(__name__)

class PaginationRequest(BaseModel):
    """分页请求模型"""
    page: int = Field(1, ge=1, description="页码（从1开始）")
    page_size: int = Field(10, ge=1, le=100, description="每页数量")
    use_rag: Optional[bool] = Field(None, description="是否使用RAG知识库（可选）")
    thickness_map_uuid: Optional[str] = Field(None, description="膜厚温度云图UUID（可选）")
    is_abnormal: Optional[bool] = Field(None, description="是否异常（可选）")

class ImageRequest(BaseModel):
    """图片请求模型"""
    thickness_map_uuid: str = Field(..., description="膜厚温度云图UUID")

router = APIRouter(tags=["image-analysis"])


@router.post("/paginated", response_model=ImageAnalysisWithImagesListResponse, summary="分页获取图片分析结果与图片信息")
async def get_analysis_results_with_images_paginated(request: PaginationRequest):
    """
    分页获取图片分析结果，并包含对应的膜厚温度云图图片信息
    
    参数:
        page: 页码（从1开始）
        page_size: 每页数量
        use_rag: 是否使用RAG知识库（可选，None表示不限制）
        thickness_map_uuid: 膜厚温度云图UUID（可选，None表示不限制）
        is_abnormal: 是否异常（可选，None表示不限制）
    
    返回:
        ImageAnalysisWithImagesListResponse: 包含图片信息和分析结果合并列表的响应
    """
    try:
        result = state.image_analysis_service.get_thickness_map_images_paginated(
            page=request.page,
            page_size=request.page_size,
            is_abnormal=request.is_abnormal,
        )
        
        logger.info(f"成功获取第 {request.page} 页数据，共 {len(result.items)} 条记录")
        return result
        
    except Exception as e:
        logger.error(f"分页获取图片分析结果失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取图片分析结果失败: {str(e)}")


@router.get("/paginated", response_model=ImageAnalysisWithImagesListResponse, summary="分页获取图片分析结果与图片信息（GET方式）")
async def get_analysis_results_with_images_paginated_get(
    page: int = 1,
    page_size: int = 10,
    use_rag: Optional[bool] = None,
    thickness_map_uuid: Optional[str] = None,
    is_abnormal: Optional[bool] = None
):
    """
    分页获取图片分析结果，并包含对应的膜厚温度云图图片信息（GET方式）
    
    参数:
        page: 页码（从1开始）
        page_size: 每页数量
        use_rag: 是否使用RAG知识库（可选，None表示不限制）
        thickness_map_uuid: 膜厚温度云图UUID（可选，None表示不限制）
        is_abnormal: 是否异常（可选，None表示不限制）
    
    返回:
        ImageAnalysisWithImagesListResponse: 包含图片信息和分析结果合并列表的响应
    """
    try:
        result = state.image_analysis_service.get_thickness_map_images_paginated(
            page=page,
            page_size=page_size,
            is_abnormal=is_abnormal,
        )
        
        logger.info(f"成功获取第 {page} 页数据，共 {len(result.items)} 条记录")
        return result
        
    except Exception as e:
        logger.error(f"分页获取图片分析结果失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取图片分析结果失败: {str(e)}")


@router.post("/image", summary="获取完整图片")
async def get_full_image(request: ImageRequest):
    """
    获取指定膜厚温度云图的完整图片
    
    参数:
        thickness_map_uuid: 膜厚温度云图UUID
    
    返回:
        包含完整图片base64编码的响应
    """
    try:
        result = state.image_analysis_service.get_full_image(request.thickness_map_uuid)
        
        if result is None:
            raise HTTPException(status_code=404, detail=f"未找到UUID为 {request.thickness_map_uuid} 的膜厚温度云图")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取完整图片失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取完整图片失败: {str(e)}")


@router.post("/analysis-result", response_model=ImageAnalysisResultResponse, summary="根据UUID获取分析结果")
async def get_analysis_result_by_uuid(request: ImageRequest):
    """
    根据膜厚温度云图UUID获取最新的分析结果
    
    参数:
        thickness_map_uuid: 膜厚温度云图UUID
    
    返回:
        ImageAnalysisResultResponse: 分析结果响应，如果没有找到则返回空结果
    """
    try:
        result = state.image_analysis_service.get_analysis_result_by_uuid(request.thickness_map_uuid)
        
        if result is None:
            return ImageAnalysisResultResponse(
                id=0,
                thickness_map_uuid=request.thickness_map_uuid,
                detection_agent_result=None,
                processing_agent_result=None,
                decision_agent_result=None,
                comment=None,
                use_rag=False,
                created_at=None,
                updated_at=None
            )
        
        return result
        
    except Exception as e:
        logger.error(f"获取分析结果失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取分析结果失败: {str(e)}")


@router.put("/comment", response_model=ImageAnalysisResultResponse, summary="更新批注和RAG状态")
async def update_comment(request: UpdateCommentRequest):
    """
    更新指定膜厚温度云图的批注和RAG状态
    
    参数:
        thickness_map_uuid: 膜厚温度云图UUID
        comment: 批注内容
        use_rag: 是否使用RAG知识库
    
    返回:
        ImageAnalysisResultResponse: 更新后的分析结果响应
    """
    try:
        result = state.image_analysis_service.update_comment_by_uuid(
            thickness_map_uuid=request.thickness_map_uuid,
            comment=request.comment,
            use_rag=request.use_rag
        )
        
        if result is None:
            raise HTTPException(status_code=500, detail=f"未找到UUID为 {request.thickness_map_uuid} 的分析结果")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新批注失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新批注失败: {str(e)}")
