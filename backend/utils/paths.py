"""
路径管理工具模块

集中管理项目中的所有文件路径，基于项目根目录动态拼接。
所有模块应从此模块获取路径，而非硬编码路径字符串或自行拼接。
"""

import os
from pathlib import Path
from typing import Union

# ── 项目根目录 ──────────────────────────────────────────
# 此文件位于 fastapi/utils/paths.py
# 项目根目录 fastapi/ 向上 1 层 = TimeSyncDiag
FASTAPI_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = FASTAPI_DIR.parent  # TimeSyncDiag 项目根目录


def ensure_dir(path: Union[str, Path]) -> Path:
    """确保目录存在，如不存在则创建"""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── 数据目录 ────────────────────────────────────────────
def get_data_dir() -> Path:
    """数据根目录"""
    return Path(os.getenv("DATA_DIR", str(PROJECT_ROOT / "fastapi" / "data")))


def get_realtime_dir() -> Path:
    """实时 CSV 数据目录（膜厚检测数据）"""
    return Path(os.getenv("DETECTION_DATA_FOLDER", str(PROJECT_ROOT / "fastapi" / "data" / "realtime")))


def get_images_dir() -> Path:
    """测试图片目录"""
    return Path(os.getenv("IMAGES_DIR", str(PROJECT_ROOT / "fastapi" / "data" / "images")))


def get_temp_dir() -> Path:
    """临时文件目录"""
    return Path(os.getenv("TEMP_DIR", str(PROJECT_ROOT / "fastapi" / "data" / "temp")))


def get_motou_data_dir() -> Path:
    """模头数据目录"""
    return get_data_dir() / "motouData"


def get_other_data_dir() -> Path:
    """其他设备数据目录"""
    return get_data_dir() / "otherData"


# ── 配置目录 ────────────────────────────────────────────
def get_config_dir() -> Path:
    """配置文件目录"""
    return FASTAPI_DIR / "config"


# ── CNN 图像分类模块 ──────────────────────────────────────
CNN_DIR = PROJECT_ROOT / "cnn_image_classification"


def get_cnn_dir() -> Path:
    """CNN 图像分类模块目录"""
    return CNN_DIR


def get_cnn_model_path() -> Path:
    """CNN 模型文件路径"""
    return CNN_DIR / "best_resnet_model.keras"


def get_cnn_api_script() -> Path:
    """CNN API 启动脚本路径"""
    return CNN_DIR / "cnnfast.py"


def get_images_test_path(filename: str = "1.png") -> Path:
    """测试图片路径"""
    return get_images_dir() / filename


# ── CNN API 地址 ────────────────────────────────────────
CNN_API_BASE_URL: str = os.getenv("CNN_API_BASE_URL", "http://localhost:8001")
