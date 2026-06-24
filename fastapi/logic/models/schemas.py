from typing import Optional, List
from pydantic import BaseModel, Field


class ThicknessMapImageResponse(BaseModel):
    """膜厚温度云图图片响应模型"""
    thickness_map_id: int = Field(..., description="膜厚温度云图ID")
    thickness_map_uuid: str = Field(..., description="膜厚温度云图UUID")
    start_time: Optional[str] = Field(None, description="开始时间")
    end_time: Optional[str] = Field(None, description="结束时间")
    map_image_path: Optional[str] = Field(None, description="带坐标和图例的云图MinIO路径")
    pure_map_image_path: Optional[str] = Field(None, description="纯的膜厚云图MinIO路径")
    combined_image_path: Optional[str] = Field(None, description="带加工参数趋势图和膜厚热力图的完整图像MinIO路径")
    data_points_count: int = Field(0, description="用于生成云图的数据点数量")
    min_thickness: Optional[float] = Field(None, description="最小膜厚值")
    max_thickness: Optional[float] = Field(None, description="最大膜厚值")
    avg_thickness: Optional[float] = Field(None, description="平均膜厚值")
    is_abnormal: bool = Field(False, description="图像是否异常")
    created_at: Optional[str] = Field(None, description="创建时间")


# class ImageAnalysisResultResponse(BaseModel):
#     """图片分析结果响应模型"""
#     id: int = Field(..., description="分析结果ID")
#     thickness_map_id: int = Field(..., description="膜厚温度云图ID")
#     detection_agent_result: str = Field(..., description="检测agent的回复内容")
#     processing_agent_result: str = Field(..., description="加工agent的回复内容")
#     decision_agent_result: str = Field(..., description="决策agent的回复内容")
#     comment: Optional[str] = Field(None, description="批注内容")
#     use_rag: bool = Field(..., description="是否使用RAG知识库")
#     created_at: Optional[str] = Field(None, description="创建时间")
#     updated_at: Optional[str] = Field(None, description="更新时间")


# class ImageAnalysisWithImagesResponse(BaseModel):
#     """图片分析结果与图片信息合并响应模型"""
#     analysis_result: ImageAnalysisResultResponse = Field(..., description="分析结果")
#     thickness_map_image: Optional[ThicknessMapImageResponse] = Field(None, description="膜厚温度云图图片信息")


class ImageAnalysisWithImagesListResponse(BaseModel):
    """图片分析结果与图片信息合并列表响应模型"""
    items: List[ThicknessMapImageResponse] = Field(..., description="合并结果列表")
    total: int = Field(..., description="总记录数")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页数量")
    total_pages: int = Field(..., description="总页数")


class ImageAnalysisResultResponse(BaseModel):
    """图片分析结果响应模型"""
    id: int = Field(..., description="分析结果ID")
    thickness_map_uuid: str = Field(..., description="膜厚温度云图UUID")
    detection_agent_result: Optional[str] = Field(None, description="检测agent的回复内容")
    processing_agent_result: Optional[str] = Field(None, description="加工agent的回复内容")
    decision_agent_result: Optional[str] = Field(None, description="决策agent的回复内容")
    comment: Optional[str] = Field(None, description="批注内容")
    use_rag: bool = Field(False, description="是否使用RAG知识库")
    created_at: Optional[str] = Field(None, description="创建时间")
    updated_at: Optional[str] = Field(None, description="更新时间")


class UpdateCommentRequest(BaseModel):
    """更新批注请求模型"""
    thickness_map_uuid: str = Field(..., description="膜厚温度云图UUID")
    comment: str = Field(..., description="批注内容")
    use_rag: bool = Field(False, description="是否使用RAG知识库")
