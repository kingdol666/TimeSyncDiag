"""
异常处理模块
定义自定义异常类和异常处理函数
"""

from typing import Optional, Any
from fastapi import HTTPException, status
import logging


logger = logging.getLogger(__name__)


class BaseAPIException(Exception):
    """基础API异常类"""
    
    def __init__(
        self,
        message: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail: Optional[Any] = None
    ):
        self.message = message
        self.status_code = status_code
        self.detail = detail
        super().__init__(self.message)


class DatabaseException(BaseAPIException):
    """数据库异常"""
    
    def __init__(self, message: str, detail: Optional[Any] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail
        )


class MinioException(BaseAPIException):
    """MinIO异常"""
    
    def __init__(self, message: str, detail: Optional[Any] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail
        )


class NotFoundException(BaseAPIException):
    """资源未找到异常"""
    
    def __init__(self, message: str, detail: Optional[Any] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail
        )


class ValidationException(BaseAPIException):
    """验证异常"""
    
    def __init__(self, message: str, detail: Optional[Any] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=detail
        )


class BusinessException(BaseAPIException):
    """业务逻辑异常"""
    
    def __init__(self, message: str, detail: Optional[Any] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail
        )


def handle_exception(exc: Exception, context: str = "") -> HTTPException:
    """
    统一异常处理函数
    
    参数:
        exc: 异常对象
        context: 上下文信息
        
    返回:
        HTTPException对象
    """
    if isinstance(exc, BaseAPIException):
        logger.error(f"{context}: {exc.message} - {exc.detail}")
        return HTTPException(
            status_code=exc.status_code,
            detail=f"{exc.message}"
        )
    elif isinstance(exc, HTTPException):
        logger.error(f"{context}: {exc.detail}")
        return exc
    else:
        logger.error(f"{context}: 未处理的异常 - {str(exc)}", exc_info=True)
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"服务器内部错误: {str(exc)}"
        )
