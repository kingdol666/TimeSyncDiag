import os
import httpx
from typing import Optional, List
from pydantic import BaseModel
from pathlib import Path
import io
from PIL.Image import Image as PILImage
from PIL import Image

# 禁用代理
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

# 从配置加载 CNN API 地址
from config.config_loader import config

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
    print("=== 开始测试CNN图像分类服务 ===\n")
    
    service = CNNImageClassificationService()
    
    try:
        # 1. 健康检查
        print("1. 健康检查...")
        health = await service.health_check()
        print(f"   状态: {health.status}")
        print(f"   模型已加载: {health.model_loaded}\n")
        
        # 2. 获取根路径信息
        print("2. 获取服务信息...")
        root_info = await service.get_root_info()
        print(f"   消息: {root_info.message}")
        print(f"   TensorFlow版本: {root_info.tensorflow_version}")
        print(f"   模型路径: {root_info.model_path}\n")
        
        # 3. 获取模型信息
        print("3. 获取模型信息...")
        try:
            model_info = await service.get_model_info()
            print(f"   模型路径: {model_info.model_path}")
            print(f"   输入形状: {model_info.input_shape}")
            print(f"   输出形状: {model_info.output_shape}")
            print(f"   总参数: {model_info.total_params}")
            print(f"   图像尺寸: {model_info.image_size}\n")
        except Exception as e:
            print(f"   获取模型信息失败: {str(e)}\n")
        
        # 4. 测试从文件预测
        print("4. 测试从文件预测...")
        test_image_path = r"d:\codes\PythonCodes\Test\TimeSyncDiag\fastapi\data\images\1.png"
        try:
            result = await service.predict_from_file(test_image_path)
            print(f"   预测类别: {result.prediction}")
            print(f"   置信度: {result.confidence}")
            print(f"   标签: {result.label}")
            print(f"   消息: {result.message}\n")
        except FileNotFoundError:
            print(f"   测试图片文件不存在: {test_image_path}\n")
        except Exception as e:
            print(f"   预测失败: {str(e)}\n")
        
        # 5. 测试批量预测
        print("5. 测试批量预测...")
        test_images = [
            r"d:\codes\PythonCodes\Test\TimeSyncDiag\fastapi\data\images\1.png",
            r"d:\codes\PythonCodes\Test\TimeSyncDiag\fastapi\data\images\2.png"
        ]
        try:
            batch_result = await service.batch_predict_from_files(test_images)
            print(f"   总数: {batch_result.total}")
            print(f"   成功: {batch_result.success}")
            for idx, res in enumerate(batch_result.results):
                print(f"   结果 {idx + 1}:")
                if res.error:
                    print(f"      错误: {res.error}")
                else:
                    print(f"      文件名: {res.filename}")
                    print(f"      预测: {res.label} (置信度: {res.confidence})")
            print()
        except Exception as e:
            print(f"   批量预测失败: {str(e)}\n")
        
        print("=== 测试完成 ===")
        
    finally:
        await service.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_cnn_service())
