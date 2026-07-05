from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from typing import Dict, Any
from httpx import Request
from pydantic import BaseModel
import os
import tempfile
import state

router = APIRouter(tags=["视觉诊断 Agent"])

class QwenVLRequest(BaseModel):
    """Qwen-VL 请求模型"""
    question: str

@router.post("/analyze-image", summary="分析两张图片并生成整合回答", description="上传两张图片，获取视觉诊断Agent的流式回答，包括检测分析、处理分析和整合结果", response_model=None)
async def analyze_image(
    imageDetect: UploadFile = File(...),
    imageProcess: UploadFile = File(...),
    thickness_map_uuid: str = Form(None)
):
    """
    上传两张图片，获取视觉诊断Agent的流式回答
    
    Args:
        imageDetect: 检测图片文件
        imageProcess: 处理图片文件
        thickness_map_uuid: 膜厚温度云图UUID
        
    Returns:
        流式返回的模型回答
    """
    # 检查 MyVL Agent 是否已初始化
    if not state.vl_agent:
        raise HTTPException(status_code=500, detail="视觉诊断 Agent 未初始化")
    
    # 创建临时文件保存上传的检测图片
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_file:
        temp_file.write(await imageDetect.read())
        temp_detect_path = temp_file.name
    
    # 创建临时文件保存上传的处理图片
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_file:
        temp_file.write(await imageProcess.read())
        temp_process_path = temp_file.name
    
    def generate_stream():
        """生成流式响应"""
        import json
        try:
            # 尝试捕获 GeneratorExit
            for event in state.vl_agent.run_workflow(temp_detect_path, temp_process_path, thickness_map_uuid=thickness_map_uuid, stream=True):
                # 将事件转换为JSON格式，适合HTTP流式传输
                event_json = json.dumps(event, ensure_ascii=False) + "\n"
                
                # 实时返回事件
                yield event_json.encode("utf-8")
                
        except GeneratorExit:
            # 客户端断开连接时，ASGI 服务器会触发这个异常
            print("客户端断开连接，尝试终止 Agent 流程...")
            state.vl_agent.terminate_workflow()
            
        except Exception as e:
            # 捕获其他运行时异常
            print(f"流式传输发生错误: {e}")
            
        finally:
            # 确保临时文件被删除，无论流程是成功、失败还是被中断
            print("执行 finally 清理...")
            if os.path.exists(temp_detect_path):
                os.unlink(temp_detect_path)
            if os.path.exists(temp_process_path):
                os.unlink(temp_process_path)
        
    # 返回StreamingResponse
    return StreamingResponse(generate_stream(), media_type="application/x-ndjson")