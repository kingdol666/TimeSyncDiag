# TimeSyncDiag 时序对齐图像诊断系统

将 **Kafka 膜厚时序数据采集**、**Matplotlib 热力图云图生成**、**CNN 异常检测**、**QwenVL 多模态 AI 视觉诊断** 相结合，实现产线膜厚数据的**时序对齐图像诊断分析**。

## 技术栈

| 层级 | 技术 | 用途 |
|------|------|------|
| **后端框架** | FastAPI + Uvicorn (Python ≥3.11) | REST API + WebSocket |
| **包管理** | uv (后端) / pnpm (前端) | 依赖管理 |
| **时序数据库** | TimescaleDB (PostgreSQL) | 传感器时序数据、检测数据 |
| **消息队列** | Apache Kafka | 数据生产/消费管道 |
| **对象存储** | MinIO | 热力图云图图片存储 |
| **多模态 AI** | USTC LLM API (qwen3.6-chat) + LangGraph | 图像诊断 Agent |
| **知识库 RAG** | Qdrant + Sentence-Transformers | 故障知识检索 |
| **CNN 诊断** | TensorFlow + ResNet | 膜厚异常检测 |
| **热力图** | Matplotlib + NumPy + Pandas | 膜厚热力图生成 |
| **前端框架** | Next.js 16 + Ant Design + Zustand | Web 管理界面 |

---

## 项目结构

```
Test/
│
├── TimeSyncDiag/                          # ── 后端项目（Python）──
│   ├── pyproject.toml                     #   uv 依赖配置
│   ├── uv.lock                            #   依赖锁定
│   ├── config.yml                         #   运行时配置文件（可被前端设置页修改）
│   ├── .env                               #   环境变量（LLM API Key）
│   ├── docker-compose.yml                 #   Qdrant 容器（RAG 知识库）
│   │
│   ├── backend/                           #   ◈ FastAPI 后端核心
│   │   ├── main.py                        #     应用入口（生命周期 + 路由注册）
│   │   ├── run.py                         #     启动脚本
│   │   ├── state.py                       #     全局状态管理（组件初始化/清理）
│   │   │
│   │   ├── config/                        #   ● 配置管理
│   │   │   ├── config_loader.py           #     配置加载/保存/热重载
│   │   │   └── schemas.py                 #     Pydantic 配置模型 + 热生效/重启分类
│   │   │
│   │   ├── routes/                        #   ● API 路由层
│   │   │   ├── producer.py                #     /api/producer/*  生产者启停控制
│   │   │   ├── consumer.py                #     /api/consumer/*  消费者启停控制
│   │   │   ├── thickness_map.py           #     /api/thickness-map/*  膜厚云图 CRUD
│   │   │   ├── thickness_map_ws.py        #     WebSocket 实时推送
│   │   │   ├── image_analysis.py          #     /api/image-analysis/*  AI 诊断结果
│   │   │   ├── vl.py                      #     /api/vl/*  视觉诊断 Agent
│   │   │   └── config.py                  #     /api/config/*  运行时配置管理
│   │   │
│   │   ├── logic/                         #   ● 业务逻辑层
│   │   │   ├── models/                    #     ORM 模型、DB/MinIO 连接
│   │   │   ├── producers/                 #     Kafka 生产者
│   │   │   ├── consumers/                 #     Kafka 消费者 + 云图管道
│   │   │   ├── processors/               #     数据处理 + 调度器
│   │   │   ├── services/                  #     AI 诊断服务 + CNN 远程客户端
│   │   │   └── utils/                     #     工具函数
│   │   │
│   │   ├── LLMAgent/                      #   ● AI Agent
│   │   │   ├── QwenVLAgent.py             #     多模态视觉 Agent（LangGraph）
│   │   │   ├── MyVLAgent.py               #     通用视觉 Agent 封装
│   │   │   └── KnowledgeDb/               #     RAG 知识库（Qdrant）
│   │   │
│   │   ├── websocket/                     #   ● WebSocket 管理
│   │   ├── utils/                         #   ● 路径工具
│   │   └── data/                          #   ● 数据文件（CSV + 图片）
│   │
│   ├── cnn_image_classification/          #   ◈ CNN 图像分类服务（TensorFlow）
│   ├── scripts/                           #   ◈ 脚本（离线数据导入）
│   └── README.md                          #   本文件
│
├── TimeSyncDiagWeb/                       # ── 前端项目（Next.js）──
│   ├── package.json                       #   pnpm 配置
│   ├── next.config.js                     #   Next.js 配置（API 代理 → :8002）
│   ├── .env                               #   前端环境变量
│   │
│   ├── app/                               #   ◈ 页面路由
│   │   ├── page.tsx                       #     首页
│   │   ├── layout.tsx                     #     根布局
│   │   ├── client-layout.tsx              #     客户端布局（菜单+导航）
│   │   ├── dashboard/
│   │   │   ├── overview/page.tsx          #     实时膜厚云图（WebSocket）
│   │   │   └── history/page.tsx           #     历史诊断数据
│   │   └── settings/page.tsx              #     系统设置（配置管理）
│   │
│   ├── components/                        #   ◈ 组件
│   │   ├── ThickMapChart.tsx              #     膜厚云图展示（WebSocket 实时）
│   │   └── AgentChat.tsx                  #     AI Agent 交互
│   │
│   ├── request/                           #   ◈ API 请求封装
│   │   ├── config.ts                      #     配置管理 API
│   │   ├── image_analysis.ts              #     图片分析 API
│   │   └── qwen_vl.ts                     #     QwenVL 诊断 API
│   │
│   ├── store/                             #   ◈ 状态管理（Zustand）
│   ├── contexts/                          #   ◈ 语言上下文
│   ├── locales/                           #   ◈ 国际化（中/英）
│   └── constants/                         #   ◈ 菜单配置
```

---

## 系统架构与数据流

```
┌──────────────────────────────────────────────────────────────────┐
│                        基础设施（Docker / 外部）                    │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐   │
│  │TimescaleDB│    │  Kafka   │    │  MinIO   │    │ Qdrant   │   │
│  │  :5432   │    │ :9092    │    │ :9000/1  │    │ :6333    │   │
│  └────▲─────┘    └────▲─────┘    └────▲─────┘    └────▲─────┘   │
└───────┼───────────────┼───────────────┼───────────────┼──────────┘
        │               │               │               │
     ┌──┴───────────────┴───────────────┴───────────────┴──┐
     │                  FastAPI 后端 (:8002)                 │
     │                                                        │
     │  ┌─────── 初始化组件 ────────────────────────────┐     │
     │  │ ① DB Connection    ② MinIO Connector          │     │
     │  │ ③ Kafka Consumers  ④ Kafka Producers           │     │
     │  │ ⑤ ThicknessMap Pipeline  ⑥ QwenVL Agent       │     │
     │  │ ⑦ CNN API (:8003, 自动启动)  ⑧ WebSocket       │     │
     │  └────────────────────────────────────────────────┘     │
     │                                                        │
     │  ┌─────── 数据管道 ──────────────────────────────┐     │
     │  │ CSV → Kafka(detection_data) → Consumer → DB   │     │
     │  │ DB → 膜厚数据 + 工艺参数 → 热力图 → MinIO     │     │
     │  │ MinIO → CNN 诊断 → 异常时 QwenVL Agent 分析   │     │
     │  └────────────────────────────────────────────────┘     │
     │                                                        │
     │  ┌─────── API + WebSocket ───────────────────────┐     │
     │  │ REST API (:8002) + WebSocket 实时推送         │     │
     │  │ /api/config/* 运行时配置管理                  │     │
     │  └────────────────────────────────────────────────┘     │
     └────────────────────────┬───────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │  Next.js 前端      │
                    │  localhost:3000    │
                    │  API 代理 → :8002  │
                    └───────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │  CNN API 服务     │
                    │  localhost:8003   │
                    │  (后端自动启动)    │
                    └───────────────────┘
```

---

## 前置条件

| 工具 | 版本要求 | 说明 |
|------|---------|------|
| Python | ≥ 3.11 | 后端运行时 |
| uv | 最新版 | Python 包管理器 |
| Node.js | ≥ 18 | 前端运行时 |
| pnpm | ≥ 8 | 前端包管理器 |
| Docker | 最新版 | 基础设施容器 |

### 基础设施服务

需要以下服务运行中（通过 Docker 或本地安装）：

| 服务 | 端口 | 账号/密码 |
|------|------|----------|
| TimescaleDB | 5432 | `user` / `123456` / 库: `tsdb` |
| Kafka | 9092 | — |
| MinIO API | 9000 | `minioadmin` / `Minio@123456` |
| MinIO 控制台 | 9001 | 同上 |
| Qdrant（可选） | 6333 | — |

---

## 完整启动步骤

### Step 1：启动基础设施服务

确保 TimescaleDB、Kafka、MinIO 已运行。

```bash
# 如果使用 Docker 管理基础设施（根据实际 docker-compose 位置）
docker compose up -d

# 启动 Qdrant（RAG 知识库，可选）
cd TimeSyncDiag
docker compose up -d qdrant
```

### Step 2：安装后端依赖 + 导入离线工艺参数

> **重要**：导入离线工艺参数必须在启动后端之前执行。这些数据是云图上方 8 个工艺参数趋势图的数据来源。

```bash
cd TimeSyncDiag

# 1. 安装后端依赖
uv sync

# 2. 导入离线工艺参数（模头/设备数据，只需一次）
uv run python scripts/import_offline_data.py
```

### Step 3：配置环境变量

编辑 `TimeSyncDiag/.env`，设置 LLM API Key：

```env
# USTC LLM API Key for VL model
USTC_LLM_API_KEY=your_api_key_here
```

编辑 `TimeSyncDiag/config.yml` 可调整所有运行时参数（也可通过前端设置页面修改）。

### Step 4：启动后端

```bash
cd TimeSyncDiag

# 启动后端服务（端口 8002）
uv run python backend/run.py
```

后端启动时自动完成：
1. 启动 CNN API 服务（端口 8003，在新终端窗口中）
2. 初始化数据库连接（TimescaleDB）
3. 初始化 MinIO 连接
4. 初始化 Kafka 消费者（自动订阅 `sensor_data` + `detection_data`）
5. 启动检测数据生产者（逐行读取 `data/realtime/` CSV → Kafka）
6. 启动膜厚云图管道（每 20 秒自动生成云图）
7. 初始化 QwenVL Agent + RAG 知识库
8. 启动 WebSocket 实时推送

> 后端启动后，CNN API 服务会在独立终端窗口自动运行，无需手动启动。

### Step 5：启动前端

```bash
cd TimeSyncDiagWeb

# 安装依赖（首次）
pnpm install

# 启动开发服务器（端口 3000）
pnpm dev
```

访问 `http://localhost:3000`

### Step 6：（可选）CNN 模型训练

```bash
cd TimeSyncDiag
uv sync --optional cnn              # 安装 CNN 依赖（tensorflow）
uv run python cnn_image_classification/cnnfast.py    # CNN 推理 API
```

> CNN 服务默认由后端自动启动（端口 8003）。如需独立运行，可手动执行上述命令。
> 模型文件 `best_resnet_model.keras` 已包含在项目中。

---

## 配置管理

### 配置文件 `config.yml`

所有运行时参数集中在 `TimeSyncDiag/config.yml` 中，包含以下 section：

| Section | 说明 | 修改后是否需重启 |
|---------|------|-----------------|
| `thickness_map` | 膜厚云图生成参数（目标膜厚、颜色范围、DPI 等） | ❌ 热生效 |
| `cnn_diagnosis` | CNN 异常检测参数（置信度阈值、超时等） | ❌ 热生效 |
| `llm` | LLM Agent 参数（温度、RAG、Agent 开关等） | 部分热生效 |
| `websocket` | WebSocket 推送间隔 | ❌ 热生效 |
| `kafka` | Kafka 生产/消费间隔 | 部分热生效 |
| `system` | 系统参数（日志级别） | ❌ 热生效 |
| `database` | 数据库连接参数 | ✅ 需重启 |
| `minio` | MinIO 连接参数 | ✅ 需重启 |
| `api` | API 服务监听参数 | ✅ 需重启 |
| `cnn_api` | CNN API 服务参数 | ✅ 需重启 |

### 前端设置页面

访问 `http://localhost:3000/settings` 可通过 Web 界面修改配置：

- **膜厚云图参数**：目标膜厚、颜色范围、记录数、管道间隔、Mask 参数、DPI 等
- **CNN 异常检测**：启用开关、置信度阈值、超时时间
- **LLM Agent 参数**：模型名称、温度、最大 Tokens、RAG 开关及参数、各 Agent 开关
- **WebSocket 参数**：云图广播间隔、心跳间隔
- **Kafka 参数**：传感器/检测数据生产间隔、消费者 Poll 超时
- **系统参数**：日志级别

修改保存后：
- **热生效参数**立即生效，无需重启后端
- **需重启参数**保存到 `config.yml` 后，需重启后端服务才能生效
- 所有配置变更自动持久化到 `config.yml` 文件

### 配置管理 API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/config` | 获取完整配置（敏感字段已脱敏） |
| `GET` | `/api/config/{section}` | 获取指定 section 配置 |
| `PATCH` | `/api/config` | 批量更新配置（点分路径） |
| `PUT` | `/api/config/{section}` | 完整替换 section 配置 |
| `POST` | `/api/config/reload` | 从 config.yml 重新加载配置 |
| `POST` | `/api/config/reset` | 重置为默认配置 |

---

## 算法流程

### 膜厚云图生成（每 20 秒自动执行）

```
每 20 秒循环 ────────────────────────────────────────────────────

Step 1 ── 获取数据范围
    查询 detection_device_data 表最新 40 行 → (start_time, end_time)

Step 2 ── 读取膜厚 + 工艺参数
    膜厚数据: detection_device_data 表 (40行 × 3000列)
    工艺参数: motou_data + other_data 表 (8个参数)

Step 3 ── CNN 异常检测
    生成纯云图 → CNN API (:8003) 推理
    ├── 正常 → 逐步减少 Mask 遮盖
    └── 异常 → 遮盖异常区域 → 触发 QwenVL Agent 诊断

Step 4 ── 生成三张云图 → 上传 MinIO
    ├── map_image:        带坐标轴+图例的热力图
    ├── pure_map_image:   纯色块云图（CNN 诊断用）
    └── combined_image:   8张工艺参数趋势图 + 热力图 组合图

Step 5 ── 持久化到数据库
    INSERT INTO thickness_map (统计指标 + MinIO 路径 + 异常状态)
```

### QwenVL Agent 多模态诊断

当 CNN 检测到异常时，自动触发 LangGraph Agent 诊断流程：

```
异常云图 + 工艺趋势图
        │
        ▼
┌─ LangGraph 节点链 ──────────────────────────────────┐
│                                                      │
│  [检测分析] QwenVL 分析膜厚异常位置、大小、形态      │
│      ↓                                               │
│  [处理分析] QwenVL 分析加工参数趋势、工艺异常        │
│      ↓                                               │
│  [知识库检索] Qdrant RAG 检索相似故障案例（可选）    │
│      ↓                                               │
│  [整合分析] 综合输出完整诊断报告 → 写入数据库        │
│                                                      │
└──────────────────────────────────────────────────────┘
```

---

## 前端页面

| 页面 | URL | 说明 |
|------|-----|------|
| **首页** | `http://localhost:3000/` | 系统介绍 + 快速导航 |
| **实时膜厚云图** | `http://localhost:3000/dashboard/overview` | WebSocket 实时推送膜厚云图、统计指标、异常时 AI 诊断 |
| **历史诊断数据** | `http://localhost:3000/dashboard/history` | 历史云图列表、异常筛选、图片预览、AI 分析结果查看 |
| **系统设置** | `http://localhost:3000/settings` | 运行时参数配置（热生效/需重启标识） |

---

## API 文档

后端启动后，访问 `http://localhost:8002/docs` 查看交互式 Swagger 文档。

### 核心 API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/health` | 健康检查 |
| `GET` | `/api/producer/status` | 生产者状态 |
| `POST` | `/api/producer/detection/start` | 启动检测数据生产者 |
| `POST` | `/api/producer/detection/stop` | 停止检测数据生产者 |
| `GET` | `/api/consumer/status` | 消费者状态 |
| `GET` | `/api/thickness-map/status` | 云图管道状态 |
| `POST` | `/api/thickness-map/generate` | 立即生成云图 |
| `GET` | `/api/thickness-map/map/latest` | 获取最新云图 |
| `POST` | `/api/image-analysis/paginated` | 分页查询诊断结果 |
| `POST` | `/api/vl/analyze-image` | QwenVL 图像诊断 |
| `GET` | `/api/config` | 获取配置 |
| `PATCH` | `/api/config` | 更新配置 |

### WebSocket

| 路径 | 说明 |
|------|------|
| `ws://localhost:8002/ws/thickness-map` | 膜厚云图实时推送 |
| `ws://localhost:3000/api/thickness-map/ws` | 前端代理 → 后端 |

---

## 数据库表

| 表名 | 说明 |
|------|------|
| `sensor_data` | 传感器时序数据（温度/湿度/气压等） |
| `detection_device_data` | 膜厚检测数据（每行 3000 个探头值） |
| `thickness_map` | 膜厚云图记录（MinIO 路径 + 统计指标） |
| `image_analysis_results` | AI 诊断结果（关联云图 UUID） |
| `motou_data` | 模头数据（树脂压力 + 模头压力） |
| `other_data` | 其他设备数据（主电机/GP泵/过滤器等 13 个字段） |

---

## 端口说明

| 服务 | 端口 | 说明 |
|------|------|------|
| 后端 API | 8002 | FastAPI REST API + WebSocket |
| CNN API | 8003 | TensorFlow CNN 推理服务（后端自动启动） |
| 前端 | 3000 | Next.js 开发服务器 |
| TimescaleDB | 5432 | PostgreSQL 数据库 |
| Kafka | 9092 | 消息队列 |
| MinIO API | 9000 | 对象存储 |
| MinIO 控制台 | 9001 | Web 管理界面 |
| Qdrant | 6333 | 向量数据库（RAG 知识库，可选） |

---

## 完整启动命令速查

```bash
# ── 1. 启动基础设施（TimescaleDB / Kafka / MinIO）──
# 确保 Docker 容器或本地服务已运行

# ── 2. 启动 Qdrant（可选，RAG 知识库）──
cd TimeSyncDiag && docker compose up -d qdrant

# ── 3. 安装后端依赖 + 导入离线数据（首次）──
cd TimeSyncDiag
uv sync
uv run python scripts/import_offline_data.py

# ── 4. 启动后端（端口 8002，自动启动 CNN API :8003）──
uv run python backend/run.py

# ── 5. 启动前端（端口 3000）──
cd ../TimeSyncDiagWeb
pnpm install   # 首次
pnpm dev
```

---

## 注意事项

1. **基础设施必须优先启动**：TimescaleDB、Kafka、MinIO 必须在后端启动前运行
2. **离线数据导入**：首次启动前必须执行 `scripts/import_offline_data.py`，否则工艺参数趋势图无数据
3. **CNN API 自动启动**：后端启动时会自动在新终端窗口启动 CNN 服务（端口 8003），无需手动启动
4. **Qdrant 为可选组件**：未启动时 Agent 自动降级为无知识库模式
5. **配置热重载**：通过前端设置页面修改的参数，热生效参数立即生效，需重启参数保存后重启后端生效
6. **LLM API Key**：必须在 `.env` 文件中设置 `USTC_LLM_API_KEY`，否则 AI 诊断功能不可用
7. **前后端代理**：前端通过 `next.config.js` 的 `rewrites` 将 `/api/backend/*` 代理到后端 `:8002/api/*`
