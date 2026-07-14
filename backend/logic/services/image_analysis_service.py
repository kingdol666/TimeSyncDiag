import os
import sys
import logging
import base64
import io
import tempfile
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc
from PIL import Image as PILImage

from backend.logic.models.models import ImageAnalysisResult, ThicknessMap
from backend.logic.models.db_connection import DatabaseConnection
from backend.logic.models.schemas import (
    ThicknessMapImageResponse,
    ImageAnalysisWithImagesListResponse,
    ImageAnalysisResultResponse
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 添加RAG知识库支持
try:
    from backend.LLMAgent.KnowledgeDb.kb import KnowledgeBase
    RAG_AVAILABLE = True
except ImportError:
    RAG_AVAILABLE = False
    logger.warning("RAG知识库模块不可用，批注将不会存储到RAG数据库")

# 图片缓存字典
_image_cache = {}

def compress_image(image_bytes: bytes, max_width: int = 300, quality: int = 85) -> Optional[bytes]:
    """
    压缩图片并调整大小
    
    参数:
        image_bytes: 原始图片字节数据
        max_width: 最大宽度
        quality: JPEG质量 (1-100)
        
    返回:
        压缩后的图片字节数据，失败返回None
    """
    try:
        img = PILImage.open(io.BytesIO(image_bytes))
        
        # 计算新的尺寸，保持宽高比
        if img.width > max_width:
            ratio = max_width / img.width
            new_height = int(img.height * ratio)
            img = img.resize((max_width, new_height), PILImage.Resampling.LANCZOS)
        
        # 转换为RGB模式（如果需要）
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        
        # 压缩并保存到字节流
        output = io.BytesIO()
        img.save(output, format='JPEG', quality=quality, optimize=True)
        return output.getvalue()
    except Exception as e:
        logger.error(f"压缩图片失败: {e}")
        return None


class ImageAnalysisService:
    """
    图片分析结果服务类，提供增删改查、时间查询和分页查询功能
    """
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        初始化图片分析结果服务
        
        参数:
            db_connection: 数据库连接实例
        """
        self.db_connection = db_connection
        
        # 初始化RAG知识库
        self.kb = None
        if RAG_AVAILABLE:
            try:
                self.kb = KnowledgeBase(collection_name="my_knowledge_base")
                logger.info("RAG知识库初始化成功")
            except Exception as e:
                logger.error(f"RAG知识库初始化失败: {e}")
                self.kb = None
    
    def create_analysis_result(
        self,
        thickness_map_uuid: str,
        detection_agent_result: str,
        processing_agent_result: str,
        decision_agent_result: str,
        comment: Optional[str] = None,
        use_rag: bool = False,
        created_at: Optional[datetime] = None,  
        updated_at: Optional[datetime] = None
    ) -> Optional[ImageAnalysisResult]:
        """
        创建新的图片分析结果
        
        参数:
            thickness_map_uuid: 膜厚温度云图UUID
            detection_agent_result: 检测agent的回复内容
            processing_agent_result: 加工agent的回复内容
            decision_agent_result: 决策agent的回复内容
            comment: 批注内容
            use_rag: 是否使用RAG知识库
            created_at: 创建时间（可选）
            updated_at: 更新时间（可选）
            
        返回:
            ImageAnalysisResult: 创建的分析结果对象，失败返回None
        """
        session = self.db_connection.get_session()
        try:
            # 验证膜厚温度云图是否存在
            thickness_map = session.query(ThicknessMap).filter(
                ThicknessMap.thickness_map_uuid == thickness_map_uuid
            ).first()
            
            if not thickness_map:
                logger.error(f"膜厚温度云图UUID {thickness_map_uuid} 不存在")
                return None
            
            # 创建分析结果对象
            analysis_result = ImageAnalysisResult(
                thickness_map_uuid=thickness_map_uuid,
                detection_agent_result=detection_agent_result,
                processing_agent_result=processing_agent_result,
                decision_agent_result=decision_agent_result,
                comment=comment,
                use_rag=use_rag,
                created_at=created_at,
                updated_at=updated_at
            )
            
            session.add(analysis_result)
            session.commit()
            session.refresh(analysis_result)
            
            logger.info(f"成功创建图片分析结果，ID: {analysis_result.id}")
            return analysis_result
            
        except Exception as e:
            session.rollback()
            logger.error(f"创建图片分析结果失败: {e}")
            return None
        finally:
            session.close()
    
    def get_analysis_result_by_id(self, result_id: int) -> Optional[ImageAnalysisResult]:
        """
        根据ID获取图片分析结果
        
        参数:
            result_id: 分析结果ID
            
        返回:
            ImageAnalysisResult: 分析结果对象，不存在返回None
        """
        session = self.db_connection.get_session()
        try:
            result = session.query(ImageAnalysisResult).filter(
                ImageAnalysisResult.id == result_id
            ).first()
            return result
        except Exception as e:
            logger.error(f"获取图片分析结果失败: {e}")
            return None
        finally:
            session.close()
    
    def get_analysis_results_by_thickness_map_id(
        self,
        thickness_map_uuid: str
    ) -> List[ImageAnalysisResult]:
        """
        根据膜厚温度云图UUID获取所有分析结果
        
        参数:
            thickness_map_uuid: 膜厚温度云图UUID
            
        返回:
            List[ImageAnalysisResult]: 分析结果列表
        """
        session = self.db_connection.get_session()
        try:
            results = session.query(ImageAnalysisResult).filter(
                ImageAnalysisResult.thickness_map_uuid == thickness_map_uuid
            ).order_by(ImageAnalysisResult.id.desc()).all()
            return results
        except Exception as e:
            logger.error(f"获取分析结果列表失败: {e}")
            return []
        finally:
            session.close()
    
    def get_analysis_result_by_uuid(
        self,
        thickness_map_uuid: str
    ) -> Optional[ImageAnalysisResultResponse]:
        """
        根据膜厚温度云图UUID获取最新的分析结果
        
        参数:
            thickness_map_uuid: 膜厚温度云图UUID
            
        返回:
            ImageAnalysisResultResponse: 分析结果响应对象，不存在返回None
        """
        session = self.db_connection.get_session()
        try:
            result = session.query(ImageAnalysisResult).filter(
                ImageAnalysisResult.thickness_map_uuid == thickness_map_uuid
            ).order_by(ImageAnalysisResult.id.desc()).first()
            
            if not result:
                return None
            
            # 转换为响应模型
            return ImageAnalysisResultResponse(
                id=result.id,
                thickness_map_uuid=result.thickness_map_uuid,
                detection_agent_result=result.detection_agent_result,
                processing_agent_result=result.processing_agent_result,
                decision_agent_result=result.decision_agent_result,
                comment=result.comment,
                use_rag=result.use_rag,
                created_at=result.created_at.isoformat() if result.created_at else None,
                updated_at=result.updated_at.isoformat() if result.updated_at else None
            )
        except Exception as e:
            logger.error(f"获取分析结果失败: {e}")
            return None
        finally:
            session.close()
    
    def update_analysis_result(
        self,
        result_id: int,
        detection_agent_result: Optional[str] = None,
        processing_agent_result: Optional[str] = None,
        decision_agent_result: Optional[str] = None,
        comment: Optional[str] = None,
        use_rag: Optional[bool] = None,
        updated_at: Optional[datetime] = None
    ) -> Optional[ImageAnalysisResult]:
        """
        更新图片分析结果
        
        参数:
            result_id: 分析结果ID
            detection_agent_result: 检测agent的回复内容
            processing_agent_result: 加工agent的回复内容
            decision_agent_result: 决策agent的回复内容
            comment: 批注内容
            use_rag: 是否使用RAG知识库
            updated_at: 更新时间（可选）
            
        返回:
            ImageAnalysisResult: 更新后的分析结果对象，失败返回None
        """
        session = self.db_connection.get_session()
        try:
            result = session.query(ImageAnalysisResult).filter(
                ImageAnalysisResult.id == result_id
            ).first()
            
            if not result:
                logger.error(f"分析结果ID {result_id} 不存在")
                return None
            
            # 更新字段
            if detection_agent_result is not None:
                result.detection_agent_result = detection_agent_result
            if processing_agent_result is not None:
                result.processing_agent_result = processing_agent_result
            if decision_agent_result is not None:
                result.decision_agent_result = decision_agent_result
            if comment is not None:
                result.comment = comment
            if use_rag is not None:
                result.use_rag = use_rag
            if updated_at is not None:
                result.updated_at = updated_at
            
            session.commit()
            session.refresh(result)
            
            logger.info(f"成功更新图片分析结果，ID: {result.id}")
            return result
            
        except Exception as e:
            session.rollback()
            logger.error(f"更新图片分析结果失败: {e}")
            return None
        finally:
            session.close()
    
    def update_comment_by_uuid(
        self,
        thickness_map_uuid: str,
        comment: str,
        use_rag: bool = False
    ) -> Optional[ImageAnalysisResultResponse]:
        """
        根据UUID更新批注和RAG状态
        
        参数:
            thickness_map_uuid: 膜厚温度云图UUID
            comment: 批注内容
            use_rag: 是否使用RAG知识库
            
        返回:
            ImageAnalysisResultResponse: 更新后的分析结果响应对象，失败返回None
        """
        session = self.db_connection.get_session()
        try:
            result = session.query(ImageAnalysisResult).filter(
                ImageAnalysisResult.thickness_map_uuid == thickness_map_uuid
            ).first()
            
            if not result:
                logger.error(f"膜厚温度云图UUID {thickness_map_uuid} 的分析结果不存在")
                return None
            
            # 更新字段
            result.comment = comment
            result.use_rag = use_rag
            result.updated_at = datetime.now()
            
            session.commit()
            session.refresh(result)
            
            # 如果启用RAG且有批注内容，则添加到知识库
            if use_rag and comment and self.kb:
                try:
                    knowledge_text = f"膜厚云图UUID: {thickness_map_uuid}\n批注内容: {comment}\n更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    self.kb.add_knowledge([knowledge_text])
                    logger.info(f"批注已添加到RAG知识库，UUID: {thickness_map_uuid}")
                except Exception as e:
                    logger.error(f"添加批注到RAG知识库失败: {e}")
            
            logger.info(f"成功更新批注，UUID: {thickness_map_uuid}")
            
            # 转换为响应模型
            return ImageAnalysisResultResponse(
                id=result.id,
                thickness_map_uuid=result.thickness_map_uuid,
                detection_agent_result=result.detection_agent_result,
                processing_agent_result=result.processing_agent_result,
                decision_agent_result=result.decision_agent_result,
                comment=result.comment,
                use_rag=result.use_rag,
                created_at=result.created_at.isoformat() if result.created_at else None,
                updated_at=result.updated_at.isoformat() if result.updated_at else None
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"更新批注失败: {e}")
            return None
        finally:
            session.close()
    
    def delete_analysis_result(self, result_id: int) -> bool:
        """
        删除图片分析结果
        
        参数:
            result_id: 分析结果ID
            
        返回:
            bool: 删除成功返回True，失败返回False
        """
        session = self.db_connection.get_session()
        try:
            result = session.query(ImageAnalysisResult).filter(
                ImageAnalysisResult.id == result_id
            ).first()
            
            if not result:
                logger.error(f"分析结果ID {result_id} 不存在")
                return False
            
            session.delete(result)
            session.commit()
            
            logger.info(f"成功删除图片分析结果，ID: {result_id}")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"删除图片分析结果失败: {e}")
            return False
        finally:
            session.close()
    
    def get_analysis_results_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        use_rag: Optional[bool] = None
    ) -> List[ImageAnalysisResult]:
        """
        根据时间范围获取分析结果
        
        参数:
            start_time: 开始时间
            end_time: 结束时间
            use_rag: 是否使用RAG知识库（可选，None表示不限制）
            
        返回:
            List[ImageAnalysisResult]: 分析结果列表
        """
        session = self.db_connection.get_session()
        try:
            query = session.query(ImageAnalysisResult).filter(
                and_(
                    ImageAnalysisResult.created_at >= start_time,
                    ImageAnalysisResult.created_at <= end_time
                )
            )
            
            if use_rag is not None:
                query = query.filter(ImageAnalysisResult.use_rag == use_rag)
            
            results = query.order_by(ImageAnalysisResult.id.desc()).all()
            return results
        except Exception as e:
            logger.error(f"根据时间范围获取分析结果失败: {e}")
            return []
        finally:
            session.close()
    
    def get_analysis_results_paginated(
        self,
        page: int = 1,
        page_size: int = 10,
        use_rag: Optional[bool] = None,
        thickness_map_uuid: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        分页获取分析结果
        
        参数:
            page: 页码（从1开始）
            page_size: 每页数量
            use_rag: 是否使用RAG知识库（可选，None表示不限制）
            thickness_map_uuid: 膜厚温度云图UUID（可选，None表示不限制）
            
        返回:
            Dict: 包含分页结果的字典
                {
                    "items": List[ImageAnalysisResult],  # 当前页的数据
                    "total": int,                        # 总记录数
                    "page": int,                         # 当前页码
                    "page_size": int,                    # 每页数量
                    "total_pages": int                   # 总页数
                }
        """
        session = self.db_connection.get_session()
        try:
            # 构建查询
            query = session.query(ImageAnalysisResult)
            
            if use_rag is not None:
                query = query.filter(ImageAnalysisResult.use_rag == use_rag)
            
            if thickness_map_uuid is not None:
                query = query.filter(
                    ImageAnalysisResult.thickness_map_uuid == thickness_map_uuid
                )
            
            # 获取总记录数
            total = query.count()
            
            # 计算总页数
            total_pages = (total + page_size - 1) // page_size if total > 0 else 0
            
            # 获取当前页数据
            offset = (page - 1) * page_size
            items = query.order_by(ImageAnalysisResult.id.desc()).offset(offset).limit(page_size).all()
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        except Exception as e:
            logger.error(f"分页获取分析结果失败: {e}")
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }
        finally:
            session.close()
    
    def get_analysis_results_paginated_by_time(
        self,
        start_time: datetime,
        end_time: datetime,
        page: int = 1,
        page_size: int = 10,
        use_rag: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        根据时间范围分页获取分析结果
        
        参数:
            start_time: 开始时间
            end_time: 结束时间
            page: 页码（从1开始）
            page_size: 每页数量
            use_rag: 是否使用RAG知识库（可选，None表示不限制）
            
        返回:
            Dict: 包含分页结果的字典
                {
                    "items": List[ImageAnalysisResult],  # 当前页的数据
                    "total": int,                        # 总记录数
                    "page": int,                         # 当前页码
                    "page_size": int,                    # 每页数量
                    "total_pages": int                   # 总页数
                }
        """
        session = self.db_connection.get_session()
        try:
            # 构建查询
            query = session.query(ImageAnalysisResult).filter(
                and_(
                    ImageAnalysisResult.created_at >= start_time,
                    ImageAnalysisResult.created_at <= end_time
                )
            )
            
            if use_rag is not None:
                query = query.filter(ImageAnalysisResult.use_rag == use_rag)
            
            # 获取总记录数
            total = query.count()
            
            # 计算总页数
            total_pages = (total + page_size - 1) // page_size if total > 0 else 0
            
            # 获取当前页数据
            offset = (page - 1) * page_size
            items = query.order_by(ImageAnalysisResult.id.desc()).offset(offset).limit(page_size).all()
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        except Exception as e:
            logger.error(f"根据时间范围分页获取分析结果失败: {e}")
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }
        finally:
            session.close()
    
    def search_analysis_results(
        self,
        keyword: str,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """
        根据关键词搜索分析结果（在检测agent、加工agent、决策agent的回复内容和批注中搜索）
        
        参数:
            keyword: 搜索关键词
            page: 页码（从1开始）
            page_size: 每页数量
            
        返回:
            Dict: 包含分页结果的字典
                {
                    "items": List[ImageAnalysisResult],  # 当前页的数据
                    "total": int,                        # 总记录数
                    "page": int,                         # 当前页码
                    "page_size": int,                    # 每页数量
                    "total_pages": int                   # 总页数
                }
        """
        session = self.db_connection.get_session()
        try:
            # 构建查询：在多个字段中搜索关键词
            query = session.query(ImageAnalysisResult).filter(
                or_(
                    ImageAnalysisResult.detection_agent_result.like(f"%{keyword}%"),
                    ImageAnalysisResult.processing_agent_result.like(f"%{keyword}%"),
                    ImageAnalysisResult.decision_agent_result.like(f"%{keyword}%"),
                    ImageAnalysisResult.comment.like(f"%{keyword}%")
                )
            )
            
            # 获取总记录数
            total = query.count()
            
            # 计算总页数
            total_pages = (total + page_size - 1) // page_size if total > 0 else 0
            
            # 获取当前页数据
            offset = (page - 1) * page_size
            items = query.order_by(ImageAnalysisResult.id.desc()).offset(offset).limit(page_size).all()
            
            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        except Exception as e:
            logger.error(f"搜索分析结果失败: {e}")
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }
        finally:
            session.close()
    
    def get_all_analysis_results(self) -> List[ImageAnalysisResult]:
        """
        获取所有分析结果
        
        返回:
            List[ImageAnalysisResult]: 所有分析结果列表
        """
        session = self.db_connection.get_session()
        try:
            results = session.query(ImageAnalysisResult).order_by(
                ImageAnalysisResult.id.desc()
            ).all()
            return results
        except Exception as e:
            logger.error(f"获取所有分析结果失败: {e}")
            return []
        finally:
            session.close()
    
    def count_analysis_results(
        self,
        use_rag: Optional[bool] = None,
        thickness_map_uuid: Optional[str] = None
    ) -> int:
        """
        统计分析结果数量
        
        参数:
            use_rag: 是否使用RAG知识库（可选，None表示不限制）
            thickness_map_uuid: 膜厚温度云图UUID（可选，None表示不限制）
            
        返回:
            int: 分析结果数量
        """
        session = self.db_connection.get_session()
        try:
            query = session.query(ImageAnalysisResult)
            
            if use_rag is not None:
                query = query.filter(ImageAnalysisResult.use_rag == use_rag)
            
            if thickness_map_uuid is not None:
                query = query.filter(
                    ImageAnalysisResult.thickness_map_uuid == thickness_map_uuid
                )
            
            count = query.count()
            return count
        except Exception as e:
            logger.error(f"统计分析结果数量失败: {e}")
            return 0
        finally:
            session.close()
    
    def get_thickness_map_images(
        self,
        thickness_map_uuid: str
    ) -> Optional[Dict[str, Any]]:
        """
        根据thickness_map_uuid查询对应的ThicknessMap表，获取其对应的加工和检测的图片并拼接后返回
        
        参数:
            thickness_map_uuid: 膜厚温度云图UUID
            
        返回:
            Dict: 包含图片信息的字典，格式如下：
                {
                    "thickness_map_uuid": str,          # 膜厚温度云图UUID
                    "start_time": str,                 # 开始时间
                    "end_time": str,                   # 结束时间
                    "combined_image_base64": str,      # 拼接后的完整图片的Base64编码
                    "is_abnormal": bool                # 图像是否异常
                }
            如果查询失败返回None
        """
        import backend.state as state

        session = self.db_connection.get_session()
        try:
            # 查询ThicknessMap
            thickness_map = session.query(ThicknessMap).filter(
                ThicknessMap.thickness_map_uuid == thickness_map_uuid
            ).first()
            
            if not thickness_map:
                logger.error(f"膜厚温度云图UUID {thickness_map_uuid} 不存在")
                return None
            
            # 从MinIO下载图片并转换为Base64编码
            combined_image_base64 = None
            
            # 拼接后的完整图片：使用combined_image_path（带加工参数趋势图和膜厚热力图的完整图像）
            if thickness_map.combined_image_path and state.minio_connector:
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                        temp_file_path = temp_file.name
                    
                    if state.minio_connector.download_file("test-bucket", thickness_map.combined_image_path, temp_file_path):
                        with open(temp_file_path, 'rb') as f:
                            combined_image_bytes = f.read()
                        combined_image_base64 = base64.b64encode(combined_image_bytes).decode('utf-8')
                        os.unlink(temp_file_path)
                except Exception as e:
                    logger.error(f"从MinIO下载combined_image失败: {e}")
            elif thickness_map.map_image_path and state.minio_connector:
                # 如果没有combined_image，使用map_image
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                        temp_file_path = temp_file.name
                    
                    if state.minio_connector.download_file("test-bucket", thickness_map.map_image_path, temp_file_path):
                        with open(temp_file_path, 'rb') as f:
                            combined_image_bytes = f.read()
                        combined_image_base64 = base64.b64encode(combined_image_bytes).decode('utf-8')
                        os.unlink(temp_file_path)
                except Exception as e:
                    logger.error(f"从MinIO下载map_image失败: {e}")
            
            return {
                "thickness_map_uuid": thickness_map.thickness_map_uuid,
                "start_time": thickness_map.start_time.isoformat() if thickness_map.start_time else None,
                "end_time": thickness_map.end_time.isoformat() if thickness_map.end_time else None,
                "combined_image_base64": combined_image_base64,
                "is_abnormal": thickness_map.is_abnormal
            }
            
        except Exception as e:
            logger.error(f"获取膜厚温度云图图片失败: {e}")
            return None
        finally:
            session.close()
    
    def get_thickness_map_images_paginated(
        self,
        page: int = 1,
        page_size: int = 10,
        is_abnormal: Optional[bool] = None,
    ) -> ImageAnalysisWithImagesListResponse:
        """
        分页获取膜厚温度云图元数据信息
        
        参数:
            page: 页码（从1开始）
            page_size: 每页数量
            is_abnormal: 是否异常（可选，None表示不限制）
            use_thumbnail: 是否使用缩略图（已废弃，保留参数兼容性）
            
        返回:
            ImageAnalysisWithImagesListResponse: 包含分页结果的响应对象
        """
        session = self.db_connection.get_session()
        try:
            # 构建查询
            query = session.query(ThicknessMap)
            
            if is_abnormal is not None:
                query = query.filter(ThicknessMap.is_abnormal == is_abnormal)
            
            # 获取总记录数
            total = query.count()
            
            # 计算总页数
            total_pages = (total + page_size - 1) // page_size if total > 0 else 0
            
            # 获取当前页数据
            offset = (page - 1) * page_size
            thickness_maps = query.order_by(ThicknessMap.id.desc()).offset(offset).limit(page_size).all()
            
            # 转换为ThicknessMapImageResponse对象列表（只返回元数据，不返回图片数据）
            items = []
            for thickness_map in thickness_maps:
                items.append(ThicknessMapImageResponse(
                    thickness_map_id=thickness_map.id,
                    thickness_map_uuid=thickness_map.thickness_map_uuid,
                    start_time=thickness_map.start_time.isoformat() if thickness_map.start_time else None,
                    end_time=thickness_map.end_time.isoformat() if thickness_map.end_time else None,
                    map_image_path=thickness_map.map_image_path,
                    pure_map_image_path=thickness_map.pure_map_image_path,
                    combined_image_path=thickness_map.combined_image_path,
                    data_points_count=thickness_map.data_points_count,
                    min_thickness=thickness_map.min_thickness,
                    max_thickness=thickness_map.max_thickness,
                    avg_thickness=thickness_map.avg_thickness,
                    is_abnormal=thickness_map.is_abnormal,
                    created_at=thickness_map.created_at.isoformat() if thickness_map.created_at else None
                ))
            
            return ImageAnalysisWithImagesListResponse(
                items=items,
                total=total,
                page=page,
                page_size=page_size,
                total_pages=total_pages
            )
        except Exception as e:
            logger.error(f"分页获取膜厚温度云图元数据失败: {e}")
            return ImageAnalysisWithImagesListResponse(
                items=[],
                total=0,
                page=page,
                page_size=page_size,
                total_pages=0
            )
        finally:
            session.close()
    
    def get_full_image(self, thickness_map_uuid: str) -> Optional[Dict[str, Any]]:
        """
        获取指定膜厚温度云图的完整图片
        
        参数:
            thickness_map_uuid: 膜厚温度云图UUID
            
        返回:
            包含完整图片base64编码的字典，失败返回None
        """
        session = self.db_connection.get_session()
        try:
            thickness_map = session.query(ThicknessMap).filter(
                ThicknessMap.thickness_map_uuid == thickness_map_uuid
            ).first()
            
            if not thickness_map:
                return None
            
            # 检查缓存
            cache_key = f"full_{thickness_map_uuid}"
            if cache_key in _image_cache:
                combined_image_base64 = _image_cache[cache_key]
            else:
                # 从MinIO获取图片数据
                combined_image_bytes = None
                
                # 优先获取combined_image
                if thickness_map.combined_image_path:
                    combined_image_bytes = self._download_from_minio(thickness_map.combined_image_path)
                # 如果没有combined_image，尝试获取map_image
                elif thickness_map.map_image_path:
                    combined_image_bytes = self._download_from_minio(thickness_map.map_image_path)
                
                if combined_image_bytes:
                    combined_image_base64 = base64.b64encode(combined_image_bytes).decode('utf-8')
                    
                    # 缓存图片（限制缓存大小）
                    if len(_image_cache) < 100:
                        _image_cache[cache_key] = combined_image_base64
                else:
                    combined_image_base64 = None
            
            return {
                "thickness_map_uuid": thickness_map_uuid,
                "combined_image_base64": combined_image_base64
            }
        except Exception as e:
            logger.error(f"获取完整图片失败: {e}")
            return None
        finally:
            session.close()
    
    def _download_from_minio(self, object_name: str) -> Optional[bytes]:
        """
        从MinIO下载对象数据
        
        参数:
            object_name: MinIO对象名称
            
        返回:
            bytes: 对象数据，失败返回None
        """
        import backend.state as state

        try:
            if state.minio_connector is None:
                logger.error("MinIO连接未初始化")
                return None
            
            # 从MinIO下载文件到内存
            data = state.minio_connector.download_file_to_bytes("test-bucket", object_name)
            if data:
                logger.info(f"成功从MinIO下载对象: {object_name}")
            return data
        except Exception as e:
            logger.error(f"从MinIO下载对象失败: {e}")
            return None

