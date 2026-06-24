"""
配置管理模块
集中管理应用程序的配置信息（数据库、Kafka、MinIO、API 等非路径配置）
路径相关的配置统一由 fastapi/utils/paths.py 管理
"""

import os
from typing import Optional
from dotenv import load_dotenv

# 加载 .env 文件（从项目根目录）
from .utils.paths import PROJECT_ROOT
load_dotenv(PROJECT_ROOT / ".env")


class Config:
    """应用程序配置类"""

    # ── 数据库配置 ──
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "tsdb")
    DB_USER: str = os.getenv("DB_USER", "user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "123456")

    # ── MinIO配置 ──
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "Minio@123456")
    MINIO_BUCKET_NAME: str = os.getenv("MINIO_BUCKET_NAME", "test-bucket")
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"

    # ── Kafka配置 ──
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_SENSOR_TOPIC: str = os.getenv("KAFKA_SENSOR_TOPIC", "sensor_data")
    KAFKA_DETECTION_TOPIC: str = os.getenv("KAFKA_DETECTION_TOPIC", "detection_data")

    # ── API配置 ──
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_TITLE: str = os.getenv("API_TITLE", "膜厚检测系统API")
    API_VERSION: str = os.getenv("API_VERSION", "1.0.0")

    # ── 文件路径配置（统一由 fastapi/utils/paths.py 管理）──
    DATA_DIR = None  # 请使用 paths.get_data_dir()
    DETECTION_DATA_FOLDER = None  # 请使用 paths.get_realtime_dir()
    IMAGES_DIR = None  # 请使用 paths.get_images_dir()
    TEMP_DIR = None  # 请使用 paths.get_temp_dir()
    
    # 分页配置
    DEFAULT_PAGE_SIZE: int = int(os.getenv("DEFAULT_PAGE_SIZE", "10"))
    MAX_PAGE_SIZE: int = int(os.getenv("MAX_PAGE_SIZE", "100"))
    
    # 图片配置
    IMAGE_MAX_WIDTH: int = int(os.getenv("IMAGE_MAX_WIDTH", "1920"))
    IMAGE_QUALITY: int = int(os.getenv("IMAGE_QUALITY", "95"))
    
    # 日志配置
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # WebSocket配置
    WS_HEARTBEAT_INTERVAL: int = int(os.getenv("WS_HEARTBEAT_INTERVAL", "30"))
    
    # LLM配置
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "qwen-vl-max")
    LLM_BASE_URL: str = os.getenv("LLM_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
    
    # QwenVLAgent配置
    QWEN_MODEL_NAME: str = os.getenv("QWEN_MODEL_NAME", "qwen3-vl-plus")
    QWEN_BASE_URL: str = os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
    QWEN_TEMPERATURE: float = float(os.getenv("QWEN_TEMPERATURE", "1.0"))
    QWEN_MAX_TOKENS: int = int(os.getenv("QWEN_MAX_TOKENS", "5000"))
    DASHSCOPE_API_KEY: str = os.getenv("DASHSCOPE_API_KEY", "")
    
    @classmethod
    def get_database_url(cls) -> str:
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

    @classmethod
    def validate(cls) -> bool:
        """
        验证配置是否有效
        
        返回:
            配置有效返回True，否则返回False
        """
        try:
            # 验证分页配置
            if cls.DEFAULT_PAGE_SIZE < 1 or cls.DEFAULT_PAGE_SIZE > cls.MAX_PAGE_SIZE:
                return False
            
            # 验证图片配置
            if cls.IMAGE_MAX_WIDTH < 1 or cls.IMAGE_QUALITY < 1 or cls.IMAGE_QUALITY > 100:
                return False
            
            # 验证端口配置
            if cls.API_PORT < 1 or cls.API_PORT > 65535:
                return False
            
            # 验证数据库端口
            if cls.DB_PORT < 1 or cls.DB_PORT > 65535:
                return False
            
            return True
        except Exception:
            return False


# 创建全局配置实例
config = Config()
