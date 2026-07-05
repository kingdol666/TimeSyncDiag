"""
公共工具函数模块
"""

import os
import tempfile
import base64
import logging
from typing import Optional
from datetime import datetime
from PIL import Image as PILImage
import io


logger = logging.getLogger(__name__)


def download_file_from_minio(minio_connector, bucket_name: str, object_path: str) -> Optional[bytes]:
    """
    从MinIO下载文件并返回字节数据

    参数:
        minio_connector: MinIO连接器实例
        bucket_name: 存储桶名称
        object_path: 对象路径

    返回:
        文件字节数据，失败返回None
    """
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
            temp_file_path = temp_file.name

        if minio_connector.download_file(bucket_name, object_path, temp_file_path):
            with open(temp_file_path, 'rb') as f:
                file_bytes = f.read()
            os.unlink(temp_file_path)
            return file_bytes
        else:
            logger.error(f"从MinIO下载文件失败: {object_path}")
            return None
    except Exception as e:
        logger.error(f"从MinIO下载文件时出错: {e}")
        return None

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

        if img.width > max_width:
            ratio = max_width / img.width
            new_height = int(img.height * ratio)
            img = img.resize((max_width, new_height), PILImage.Resampling.LANCZOS)

        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')

        output = io.BytesIO()
        img.save(output, format='JPEG', quality=quality, optimize=True)
        return output.getvalue()
    except Exception as e:
        logger.error(f"压缩图片失败: {e}")
        return None


def datetime_to_iso(dt: Optional[datetime]) -> Optional[str]:
    """
    将datetime对象转换为ISO格式字符串

    参数:
        dt: datetime对象

    返回:
        ISO格式字符串，如果dt为None则返回None
    """
    return dt.isoformat() if dt else None


def calculate_pagination(total: int, page: int, page_size: int) -> dict:
    """
    计算分页信息

    参数:
        total: 总记录数
        page: 当前页码
        page_size: 每页数量

    返回:
        包含分页信息的字典
    """
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    offset = (page - 1) * page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "offset": offset
    }


def start_producer_in_thread(producer, producer_name: str) -> dict:
    """
    在独立线程中启动生产者

    参数:
        producer: 生产者实例
        producer_name: 生产者名称（用于日志）

    返回:
        包含状态和消息的字典
    """
    import threading

    if not producer:
        return {
            "success": False,
            "message": f"{producer_name}未初始化",
            "status": "not_initialized"
        }

    if producer.is_running:
        return {
            "success": True,
            "message": f"{producer_name}已在运行中",
            "status": "already_running"
        }

    thread = threading.Thread(target=producer.start)
    thread.daemon = True
    thread.start()

    logger.info(f"{producer_name}启动命令已发送")

    return {
        "success": True,
        "message": f"{producer_name}启动成功",
        "status": "started"
    }


def stop_producer(producer, producer_name: str) -> dict:
    """
    停止生产者

    参数:
        producer: 生产者实例
        producer_name: 生产者名称（用于日志）

    返回:
        包含状态和消息的字典
    """
    if not producer:
        return {
            "success": False,
            "message": f"{producer_name}未初始化",
            "status": "not_initialized"
        }

    if not producer.running:
        return {
            "success": True,
            "message": f"{producer_name}已停止",
            "status": "already_stopped"
        }

    producer.stop()

    logger.info(f"{producer_name}停止命令已发送")

    return {
        "success": True,
        "message": f"{producer_name}停止成功",
        "status": "stopped"
    }


def start_consumer_in_thread(consumer, consumer_name: str) -> dict:
    """
    在独立线程中启动消费者

    参数:
        consumer: 消费者实例
        consumer_name: 消费者名称（用于日志）

    返回:
        包含状态和消息的字典
    """
    import threading

    if not consumer:
        return {
            "success": False,
            "message": f"{consumer_name}未初始化",
            "status": "not_initialized"
        }

    if consumer.is_running:
        return {
            "success": True,
            "message": f"{consumer_name}已在运行中",
            "status": "already_running"
        }

    thread = threading.Thread(target=consumer.start)
    thread.daemon = True
    thread.start()

    logger.info(f"{consumer_name}启动命令已发送")

    return {
        "success": True,
        "message": f"{consumer_name}启动成功",
        "status": "started"
    }


def stop_consumer(consumer, consumer_name: str) -> dict:
    """
    停止消费者

    参数:
        consumer: 消费者实例
        consumer_name: 消费者名称（用于日志）

    返回:
        包含状态和消息的字典
    """
    if not consumer:
        return {
            "success": False,
            "message": f"{consumer_name}未初始化",
            "status": "not_initialized"
        }

    if not consumer.is_running:
        return {
            "success": True,
            "message": f"{consumer_name}未在运行",
            "status": "already_stopped"
        }

    consumer.stop()

    logger.info(f"{consumer_name}停止命令已发送")

    return {
        "success": True,
        "message": f"{consumer_name}停止成功",
        "status": "stopped"
    }

def bytes_to_base64(byte_data: bytes) -> str:
    """
    将字节数据转换为Base64编码的字符串

    参数:
        byte_data: 字节数据

    返回:
        Base64编码的字符串
    """
    import base64
    return base64.b64encode(byte_data).decode('utf-8')
