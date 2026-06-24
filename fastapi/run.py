"""
FastAPI 后端启动入口
通过 uv run -m fastapi_run 启动服务
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
