import os
import numpy as np
import tensorflow as tf
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from tensorflow.keras.preprocessing.image import load_img, img_to_array
from tensorflow.keras.models import load_model
from pydantic import BaseModel
from typing import Optional
import io
from pathlib import Path
from contextlib import asynccontextmanager

# 获取当前文件所在目录
BASE_DIR = Path(__file__).parent

# 模型路径（使用相对路径）
MODEL_PATH = BASE_DIR / 'best_resnet_model.keras'

# 图像参数
IMG_HEIGHT, IMG_WIDTH = 224, 224

# 全局模型变量
model = None

class PredictionResponse(BaseModel):
    prediction: int
    confidence: float
    label: str
    message: str

def load_cnn_model():
    """
    加载训练好的CNN模型
    """
    global model
    try:
        if MODEL_PATH.exists():
            model = load_model(str(MODEL_PATH))
            print(f"模型加载成功: {MODEL_PATH}")
        else:
            print(f"警告: 模型文件不存在: {MODEL_PATH}")
    except Exception as e:
        print(f"模型加载失败: {str(e)}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    应用生命周期管理
    """
    # 启动时加载模型
    load_cnn_model()
    yield
    # 关闭时的清理工作（如果需要）
    pass

# 初始化 FastAPI 应用
app = FastAPI(
    title="CNN图像分类API",
    description="基于CNN的厚度图异常检测服务",
    version="1.0.0",
    lifespan=lifespan
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def preprocess_image(image_bytes):
    """
    预处理上传的图片
    """
    try:
        # 从字节流加载图片
        img = load_img(io.BytesIO(image_bytes), target_size=(IMG_HEIGHT, IMG_WIDTH))
        # 转换为数组
        img_array = img_to_array(img)
        # 归一化
        img_array = img_array / 255.0
        # 添加batch维度
        img_array = np.expand_dims(img_array, axis=0)
        return img_array
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"图片预处理失败: {str(e)}")

@app.get("/")
async def root():
    """
    根路径接口
    """
    return {
        "message": "CNN图像分类API服务运行中",
        "tensorflow_version": tf.__version__,
        "model_loaded": model is not None,
        "model_path": MODEL_PATH,
        "endpoints": {
            "predict": "/predict - 上传图片进行异常检测",
            "health": "/health - 健康检查",
            "model_info": "/model/info - 模型信息"
        }
    }

@app.get("/health")
async def health_check():
    """
    健康检查接口
    """
    return {
        "status": "healthy",
        "model_loaded": model is not None
    }

@app.get("/model/info")
async def model_info():
    """
    获取模型信息
    """
    if model is None:
        raise HTTPException(status_code=503, detail="模型未加载")
    
    return {
        "model_path": MODEL_PATH,
        "input_shape": model.input_shape,
        "output_shape": model.output_shape,
        "total_params": model.count_params(),
        "image_size": f"{IMG_HEIGHT}x{IMG_WIDTH}"
    }

@app.post("/predict", response_model=PredictionResponse)
async def predict_image(file: UploadFile = File(...)):
    """
    上传图片进行异常检测
    
    Args:
        file: 上传的图片文件
        
    Returns:
        PredictionResponse: 预测结果，包含预测类别、置信度和标签
    """
    if model is None:
        raise HTTPException(status_code=503, detail="模型未加载，请检查模型文件")
    
    # 检查文件类型
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="请上传图片文件")
    
    try:
        # 读取图片内容
        image_bytes = await file.read()
        
        # 预处理图片
        processed_image = preprocess_image(image_bytes)
        
        # 模型预测
        prediction = model.predict(processed_image, verbose=0)
        
        # 获取预测结果
        confidence = float(prediction[0][0])
        predicted_class = int(confidence > 0.5)
        
        # 确定标签
        if predicted_class == 0:
            label = "正常"
            confidence_score = 1 - confidence
        else:
            label = "异常"
            confidence_score = confidence
        
        return PredictionResponse(
            prediction=predicted_class,
            confidence=round(confidence_score, 4),
            label=label,
            message=f"检测完成，图片被识别为: {label}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预测失败: {str(e)}")

@app.post("/predict/batch")
async def predict_batch(files: list[UploadFile] = File(...)):
    """
    批量预测接口
    
    Args:
        files: 多个上传的图片文件
        
    Returns:
        批量预测结果列表
    """
    if model is None:
        raise HTTPException(status_code=503, detail="模型未加载，请检查模型文件")
    
    results = []
    
    for file in files:
        if not file.content_type.startswith("image/"):
            results.append({
                "filename": file.filename,
                "error": "请上传图片文件"
            })
            continue
        
        try:
            image_bytes = await file.read()
            processed_image = preprocess_image(image_bytes)
            prediction = model.predict(processed_image, verbose=0)
            
            confidence = float(prediction[0][0])
            predicted_class = int(confidence > 0.5)
            
            if predicted_class == 0:
                label = "正常"
                confidence_score = 1 - confidence
            else:
                label = "异常"
                confidence_score = confidence
            
            results.append({
                "filename": file.filename,
                "prediction": predicted_class,
                "confidence": round(confidence_score, 4),
                "label": label
            })
            
        except Exception as e:
            results.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    return {
        "total": len(files),
        "success": len([r for r in results if "error" not in r]),
        "results": results
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
