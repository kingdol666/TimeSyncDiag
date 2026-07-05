"""
FastAPI 后端启动入口
通过 uv run python backend/run.py 启动服务
"""
import sys
import os
from pathlib import Path

# 计算项目根目录并切换工作目录，避免 backend/ 目录作为脚本目录导致包名解析异常
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn
from backend.config.config_loader import config

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host=config.api.host,
        port=config.api.port,
        reload=False,
        log_level=config.system.log_level.lower(),
    )
