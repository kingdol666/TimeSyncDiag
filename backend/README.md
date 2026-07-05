# TimeSyncDiag 时序对齐图像诊断系统 — API

FastAPI 后端，提供数据采集（Kafka）→ 存储（TimescaleDB + MinIO）→ AI 诊断（QwenVL Agent）的完整 REST API。

## 快速启动

```bash
cd d:\codes\PythonCodes\Test\TimeSyncDiag

# 0. 环境变量（首次）
cp .env.example .env    # 然后编辑 .env 填入你的 DASHSCOPE_API_KEY

# 1. 安装依赖
uv sync

# 2. 导入离线工艺参数（模头/设备数据，仅首次需要）
uv run python scripts/import_offline_data.py

# 3. 启动 API 服务（热重载）
uv run python fastapi/run.py

# 4. 访问
#    API:     http://localhost:8000
#    文档:    http://localhost:8000/docs
```

> **注意**：第 2 步（导入离线工艺参数）非常重要。它负责将 `data/motouData/` 和 `data/otherData/` 下的 CSV 历史数据导入 TimescaleDB。这些数据是云图生成时**加工参数趋势图**的数据来源。如果没有执行此步骤，生成的云图上方 8 个工艺参数趋势图将显示为 "No Data"。

## API 一览

- **生产者控制** — `GET/POST /api/producer/*`
- **消费者控制** — `GET/POST /api/consumer/*`
- **膜厚云图** — `GET/POST /api/thickness-map/*`
- **图片分析** — `GET/POST /api/image-analysis/*`
- **QwenVL 诊断** — `POST /api/qwen-vl/*`
- **WebSocket 实时** — `ws://.../api/thickness-map/ws`

## 路由模块

| 文件 | 路由前缀 | 说明 |
|------|---------|------|
| `routes/producer.py` | `/api/producer` | 生产者启停/状态 |
| `routes/consumer.py` | `/api/consumer` | 消费者启停/统计 |
| `routes/thickness_map.py` | `/api/thickness-map` | 膜厚云图 CRUD |
| `routes/image_analysis.py` | `/api/image-analysis` | AI 诊断分析结果 |
| `routes/qwen_vl.py` | `/api/qwen-vl` | Qwen 多模态 API |
| `routes/thickness_map_ws.py` | WebSocket | 实时云图推送 |

## 启动方式

| 命令 | 说明 |
|------|------|
| `cp .env.example .env` | 配置环境变量（首次） |
| `uv sync` | 安装依赖 |
| `uv run python scripts/import_offline_data.py` | 导入离线工艺参数 |
| `uv run python run.py` | 开发模式（热重载） |
| `uv run uvicorn main:app --host 0.0.0.0 --port 8000` | 生产模式 |
| `uv run python producer.py` | 独立启动生产者 |
| `uv run python consumer.py` | 独立启动消费者 |
| `uv sync --group cnn` | 安装 CNN (tensorflow) 依赖 |
| `uv run python ../cnn_image_classification/cnnfast.py` | 启动 CNN 诊断 API (port 8001) |

> 详细依赖和完整启动流程见根目录 `README.md`。
