import os
import httpx
import logging
from typing import Optional, List
from pydantic import BaseModel
from pathlib import Path
import io
from PIL.Image import Image as PILImage
from PIL import Image

logger = logging.getLogger(__name__)

# 禁用代理
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

# 从配置加载 CNN API 地址
from backend.config.config_loader import config

_cnn_host = config.cnn_api.host
_cnn_port = config.cnn_api.port
_cnn_timeout = config.cnn_diagnosis.timeout
# 将 0.0.0.0 替换为 localhost 用于客户端连接
_cnn_connect_host = 'localhost' if _cnn_host == '0.0.0.0' else _cnn_host
CNN_API_BASE_URL = f"http://{_cnn_connect_host}:{_cnn_port}"

class PredictionResponse(BaseModel):
    prediction: int
    confidence: float
    label: str
    message: str

class BatchPredictionResult(BaseModel):
    filename: Optional[str] = None
    prediction: Optional[int] = None
    confidence: Optional[float] = None
    label: Optional[str] = None
    error: Optional[str] = None

class BatchPredictionResponse(BaseModel):
    total: int
    success: int
    results: List[BatchPredictionResult]

class ModelInfo(BaseModel):
    model_path: str
    input_shape: tuple
    output_shape: tuple
    total_params: int
    image_size: str

class HealthCheckResponse(BaseModel):
    status: str
    model_loaded: bool

class RootResponse(BaseModel):
    message: str
    tensorflow_version: str
    model_loaded: bool
    model_path: str
    endpoints: dict

class CNNImageClassificationService:
    """
    CNN图像分类服务客户端
    用于调用远程CNN图像分类API
    """
    
    def __init__(self, base_url: str = CNN_API_BASE_URL, timeout: float = None):
        self.base_url = base_url
        self.timeout = timeout if timeout is not None else _cnn_timeout
        self.client = httpx.AsyncClient(
            timeout=self.timeout,
            verify=False,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
    
    async def close(self):
        """
        关闭HTTP客户端
        """
        await self.client.aclose()
    
    async def health_check(self) -> HealthCheckResponse:
        """
        健康检查
        
        Returns:
            HealthCheckResponse: 服务健康状态
        """
        try:
            response = await self.client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return HealthCheckResponse(**response.json())
        except httpx.HTTPError as e:
            raise Exception(f"健康检查失败: {str(e)}")
    
    async def get_root_info(self) -> RootResponse:
        """
        获取根路径信息
        
        Returns:
            RootResponse: API服务信息
        """
        try:
            response = await self.client.get(f"{self.base_url}/")
            response.raise_for_status()
            return RootResponse(**response.json())
        except httpx.HTTPError as e:
            raise Exception(f"获取服务信息失败: {str(e)}")
    
    async def get_model_info(self) -> ModelInfo:
        """
        获取模型信息
        
        Returns:
            ModelInfo: 模型详细信息
        """
        try:
            response = await self.client.get(f"{self.base_url}/model/info")
            response.raise_for_status()
            return ModelInfo(**response.json())
        except httpx.HTTPError as e:
            raise Exception(f"获取模型信息失败: {str(e)}")
    
    async def predict_from_file(self, image_path: str) -> PredictionResponse:
        """
        从本地文件路径预测图片
        
        Args:
            image_path: 图片文件路径
            
        Returns:
            PredictionResponse: 预测结果
        """
        try:
            # 读取图片文件
            with open(image_path, 'rb') as f:
                files = {'file': (Path(image_path).name, f, 'image/png')}
                response = await self.client.post(f"{self.base_url}/predict", files=files)
            
            response.raise_for_status()
            return PredictionResponse(**response.json())
        except FileNotFoundError:
            raise Exception(f"图片文件不存在: {image_path}")
        except httpx.HTTPError as e:
            raise Exception(f"预测失败: {str(e)}")
    
    async def predict_from_bytes(self, image_bytes: bytes, filename: str = "image.png") -> PredictionResponse:
        """
        从字节流预测图片
        
        Args:
            image_bytes: 图片字节数据
            filename: 文件名（用于上传）
            
        Returns:
            PredictionResponse: 预测结果
        """
        try:
            files = {'file': (filename, io.BytesIO(image_bytes), 'image/png')}
            response = await self.client.post(f"{self.base_url}/predict", files=files)
            response.raise_for_status()
            return PredictionResponse(**response.json())
        except httpx.HTTPError as e:
            raise Exception(f"预测失败: {str(e)}")
    
    async def predict_from_pil_image(self, pil_image: Image.Image, filename: str = "image.png") -> PredictionResponse:
        """
        从PIL Image对象预测图片
        
        Args:
            pil_image: PIL Image对象
            filename: 文件名（用于上传）
            
        Returns:
            PredictionResponse: 预测结果
        """
        try:
            # 将PIL Image转换为字节流
            img_byte_arr = io.BytesIO()
            pil_image.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            
            files = {'file': (filename, img_byte_arr, 'image/png')}
            response = await self.client.post(f"{self.base_url}/predict", files=files)
            response.raise_for_status()
            return PredictionResponse(**response.json())
        except httpx.HTTPError as e:
            raise Exception(f"预测失败: {str(e)}")
    
    async def batch_predict_from_files(self, image_paths: List[str]) -> BatchPredictionResponse:
        """
        批量预测本地文件
        
        Args:
            image_paths: 图片文件路径列表
            
        Returns:
            BatchPredictionResponse: 批量预测结果
        """
        try:
            files = []
            file_handles = []
            
            for path in image_paths:
                try:
                    f = open(path, 'rb')
                    file_handles.append(f)
                    files.append(('files', (Path(path).name, f, 'image/png')))
                except FileNotFoundError:
                    files.append(('files', (Path(path).name, io.BytesIO(b''), 'image/png')))
            
            try:
                response = await self.client.post(f"{self.base_url}/predict/batch", files=files)
                response.raise_for_status()
                return BatchPredictionResponse(**response.json())
            finally:
                for f in file_handles:
                    f.close()
                    
        except httpx.HTTPError as e:
            raise Exception(f"批量预测失败: {str(e)}")
    
    async def batch_predict_from_bytes(self, images_data: List[tuple[bytes, str]]) -> BatchPredictionResponse:
        """
        批量预测字节流
        
        Args:
            images_data: (字节数据, 文件名) 的元组列表
            
        Returns:
            BatchPredictionResponse: 批量预测结果
        """
        try:
            files = [
                ('files', (filename, io.BytesIO(img_bytes), 'image/png'))
                for img_bytes, filename in images_data
            ]
            response = await self.client.post(f"{self.base_url}/predict/batch", files=files)
            response.raise_for_status()
            return BatchPredictionResponse(**response.json())
        except httpx.HTTPError as e:
            raise Exception(f"批量预测失败: {str(e)}")
    
    def predict_from_bytes_sync(self, image_bytes: bytes, filename: str = "image.png") -> PredictionResponse:
        """
        从字节流预测图片（同步版本）
        
        Args:
            image_bytes: 图片字节数据
            filename: 文件名（用于上传）
            
        Returns:
            PredictionResponse: 预测结果
        """
        import httpx as sync_httpx
        
        try:
            with sync_httpx.Client(timeout=self.timeout, verify=False) as client:
                files = {'file': (filename, io.BytesIO(image_bytes), 'image/png')}
                response = client.post(f"{self.base_url}/predict", files=files)
                response.raise_for_status()
                return PredictionResponse(**response.json())
        except sync_httpx.HTTPError as e:
            raise Exception(f"预测失败: {str(e)}")


# 单例模式的服务实例
_cnn_service: Optional[CNNImageClassificationService] = None

async def get_cnn_service() -> CNNImageClassificationService:
    """
    获取CNN服务单例
    
    Returns:
        CNNImageClassificationService: CNN服务实例
    """
    global _cnn_service
    if _cnn_service is None:
        _cnn_service = CNNImageClassificationService()
    return _cnn_service

async def close_cnn_service():
    """
    关闭CNN服务
    """
    global _cnn_service
    if _cnn_service is not None:
        await _cnn_service.close()
        _cnn_service = None


async def test_cnn_service():
    """
    测试CNN图像分类服务
    """
    from backend.logic.utils.paths import get_images_dir

    logger.info("=== 开始测试CNN图像分类服务 ===")
    
    service = CNNImageClassificationService()
    
    try:
        # 1. 健康检查
        logger.info("1. 健康检查...")
        health = await service.health_check()
        logger.info(f"   状态: {health.status}")
        logger.info(f"   模型已加载: {health.model_loaded}")
        
        # 2. 获取根路径信息
        logger.info("2. 获取服务信息...")
        root_info = await service.get_root_info()
        logger.info(f"   消息: {root_info.message}")
        logger.info(f"   TensorFlow版本: {root_info.tensorflow_version}")
        logger.info(f"   模型路径: {root_info.model_path}")
        
        # 3. 获取模型信息
        logger.info("3. 获取模型信息...")
        try:
            model_info = await service.get_model_info()
            logger.info(f"   模型路径: {model_info.model_path}")
            logger.info(f"   输入形状: {model_info.input_shape}")
            logger.info(f"   输出形状: {model_info.output_shape}")
            logger.info(f"   总参数: {model_info.total_params}")
            logger.info(f"   图像尺寸: {model_info.image_size}")
        except Exception as e:
            logger.error(f"   获取模型信息失败: {str(e)}")
        
        # 4. 测试从文件预测
        logger.info("4. 测试从文件预测...")
        images_dir = get_images_dir()
        test_image_path = str(images_dir / "1.png")
        try:
            result = await service.predict_from_file(test_image_path)
            logger.info(f"   预测类别: {result.prediction}")
            logger.info(f"   置信度: {result.confidence}")
            logger.info(f"   标签: {result.label}")
            logger.info(f"   消息: {result.message}")
        except FileNotFoundError:
            logger.warning(f"   测试图片文件不存在: {test_image_path}")
        except Exception as e:
            logger.error(f"   预测失败: {str(e)}")
        
        # 5. 测试批量预测
        logger.info("5. 测试批量预测...")
        test_images = [
            str(images_dir / "1.png"),
            str(images_dir / "2.png")
        ]
        try:
            batch_result = await service.batch_predict_from_files(test_images)
            logger.info(f"   总数: {batch_result.total}")
            logger.info(f"   成功: {batch_result.success}")
            for idx, res in enumerate(batch_result.results):
                logger.info(f"   结果 {idx + 1}:")
                if res.error:
                    logger.info(f"      错误: {res.error}")
                else:
                    logger.info(f"      文件名: {res.filename}")
                    logger.info(f"      预测: {res.label} (置信度: {res.confidence})")
        except Exception as e:
            logger.error(f"   批量预测失败: {str(e)}")
        
        logger.info("=== 测试完成 ===")
        
    finally:
        await service.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_cnn_service())
