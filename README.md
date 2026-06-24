# TimeSyncDiag 时序对齐图像诊断系统

将 **Kafka 膜厚时序数据采集**、**Matplotlib 热力图云图生成**、**QwenVL 多模态 AI 视觉诊断** 相结合，实现产线膜厚数据的**时序对齐图像诊断分析**。

## 技术栈

| 层级 | 技术 | 用途 |
|------|------|------|
| **后端框架** | FastAPI + Uvicorn (Python 3.12) | REST API + WebSocket |
| **包管理** | uv | Python 依赖管理 |
| **时序数据库** | TimescaleDB (PostgreSQL 15) | 传感器时序数据、检测数据 |
| **消息队列** | Apache Kafka (Confluent 7.7) | 数据生产/消费管道 |
| **对象存储** | MinIO | 热力图云图图片、测试图片 |
| **多模态 AI** | QwenVL (阿里百炼) + LangGraph | 图像诊断 Agent |
| **知识库 RAG** | Qdrant + Sentence-Transformers | 故障知识检索 |
| **热力图** | Matplotlib + NumPy + Pandas | 膜厚热力图生成 |
| **前端框架** | Next.js 16 + Ant Design + Recharts | Web 管理界面 |

---

## 项目结构

```
TimeSyncDiag/                          # ── 后端项目（Python）──
│
├── pyproject.toml                     #   uv 配置 + 项目元数据
├── uv.lock                            #   依赖锁定文件
│
├── fastapi/                           #   ◈ FastAPI 后端核心
│   ├── main.py                        #     应用入口（生命周期 + 路由注册）
│   ├── state.py                       #     全局状态管理（组件初始化/清理）
│   ├── run.py                         #     启动脚本
│   │
│   ├── routes/                        #   ● API 路由层
│   │   ├── producer.py                #     /api/producer/*  生产者启停控制
│   │   ├── consumer.py                #     /api/consumer/*  消费者启停控制
│   │   ├── thickness_map.py           #     /api/thickness-map/*  膜厚云图 CRUD
│   │   ├── image_analysis.py          #     /api/image-analysis/*  AI 诊断结果
│   │   ├── qwen_vl.py                 #     /api/qwen-vl/*  Qwen 多模态 API
│   │   └── thickness_map_ws.py        #     WebSocket 实时推送
│   │
│   ├── logic/                         #   ● 业务逻辑层
│   │   ├── config.py                  #     集中配置管理（数据库/Kafka/MinIO/LLM）
│   │   ├── exceptions.py              #     统一异常处理
│   │   │
│   │   ├── models/                    #     ▸ 数据模型
│   │   │   ├── models.py              #       ORM 模型（6张表）
│   │   │   ├── db_connection.py       #       TimescaleDB 连接 + 超表创建
│   │   │   ├── mini_connection.py     #       MinIO 客户端封装
│   │   │   └── schemas.py             #       Pydantic 请求/响应模型
│   │   │
│   │   ├── producers/                 #     ▸ Kafka 生产者
│   │   │   └── producer.py            #       SensorDataProducer + DetectionDataProducer
│   │   │
│   │   ├── consumers/                 #     ▸ Kafka 消费者
│   │   │   ├── consumer.py            #       SensorDataPipeline + DetectionDataPipeline
│   │   │   ├── kafka_consumer.py      #       底层 Kafka 消费者封装
│   │   │   ├── thickness_map_pipeline.py  #   膜厚云图生成管道
│   │   │   └── thickness_map_consumer.py  #   定时云图生成调度
│   │   │
│   │   ├── processors/                #     ▸ 数据处理
│   │   │   ├── data_processor.py      #       传感器/检测数据→DB写入
│   │   │   ├── scheduler.py           #       定时调度器
│   │   │   └── thickness_map_processor.py  #  膜厚热力图生成
│   │   │
│   │   ├── services/                  #     ▸ 业务服务
│   │   │   ├── image_analysis_service.py  #  AI 诊断分析服务
│   │   │   └── remote_template.py     #       CNN 图像分类远程客户端
│   │   │
│   │   └── utils/                     #     ▸ 工具函数
│   │       └── common.py              #       MinIO 下载/图片压缩/线程控制
│   │
│   ├── heatmap/                       #   ● 热力图绘制工具（独立脚本）
│   │   └── creatmap.py                #     IEEE 风格膜厚热力图
│   │
│   ├── LLMAgent/                      #   ● AI Agent
│   │   ├── QwenVLAgent.py             #     多模态视觉 Agent（LangGraph）
│   │   ├── QwenAgent.py               #     文本对话 Agent
│   │   └── KnowledgeDb/               #     RAG 知识库
│   │       ├── kb.py                  #       Qdrant 知识库 CRUD
│   │       └── LMclient.py            #       Agent + RAG 检索
│   │
│   └── data/                          #   ● 数据文件
│       ├── images/                    #     测试图片
│       └── realtime/                  #     CSV 实时数据文件
│
├── producer.py                        # ◈ 独立 Kafka 生产者（传感器 + 检测）
├── consumer.py                        # ◈ 独立 Kafka 消费者（→ TimescaleDB）
├── db_connection.py                   # ◈ 独立数据库连接测试
├── models.py                          # ◈ 独立数据模型
├── cnn_image_classification/          # ◈ CNN 图像分类服务（TensorFlow）
│
├── README.md                          #   本文件
├── THICKNESS_MAP_README.md            #   膜厚云图功能说明
│
└── requirements.txt                   #   pip 依赖（备用）

TimeSyncDiagWeb/                       # ── 前端项目（Next.js）──
│
├── package.json                       #   pnpm 配置
├── next.config.js                     #   Next.js 配置（API 代理）
├── pnpm-lock.yaml                     #   依赖锁定
│
├── app/                               #   ◈ 页面路由
│   ├── page.tsx                       #     首页
│   ├── layout.tsx                     #     根布局
│   ├── client-layout.tsx              #     客户端布局（菜单+导航）
│   │
│   ├── dashboard/                     #   ● 数据看板
│   │   ├── overview/page.tsx          #     概览（膜厚云图）
│   │   ├── performance/page.tsx       #     性能监控
│   │   └── history/page.tsx           #     历史诊断数据
│   │
│   ├── topics/                        #   ● 主题管理
│   │   ├── list/page.tsx              #     主题列表
│   │   └── create/page.tsx            #     创建主题
│   │
│   ├── consumers/page.tsx             #   ● 消费者组
│   ├── settings/page.tsx              #   ● 系统设置
│   └── test/                          #   ● 测试页面
│
├── components/                        #   ◈ 组件
│   └── ThickMapChart.tsx              #     膜厚云图展示组件
│
├── lib/                               #   ◈ 工具库
│   └── map-client.ts                  #     云图 API 客户端封装
│
├── request/                           #   ◈ API 请求封装
│   ├── qwen_vl.ts                     #     QwenVL 诊断 API
│   └── image_analysis.ts              #     图片分析 API
│
├── store/                             #   ◈ 状态管理（Zustand）
├── types/                             #   ◈ TypeScript 类型定义
├── locales/                           #   ◈ 国际化（中/英）
└── constants/                         #   ◈ 常量（菜单配置）
```

---

## 系统架构与数据流

```
┌──────────────────────────────────────────────────────────────────┐
│                        Docker 容器                                │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐   │
│  │TimescaleDB│    │  Kafka   │    │  MinIO   │    │Zookeeper  │   │
│  │  :5432   │    │ :9092    │    │ :9000/1  │    │  :2181    │   │
│  └────▲─────┘    └────▲─────┘    └────▲─────┘    └──────────┘   │
│       │               │               │                          │
└───────┼───────────────┼───────────────┼──────────────────────────┘
        │               │               │
        │        ┌──────┴──────┐        │
        │        │  producer.py│        │
        │        │   (独立脚本) │        │
        │        └──────┬──────┘        │
        │               │ 发送数据       │
        │               ▼               │
        │        ┌──────────────┐       │
        │        │  Kafka Topic │       │
        │        │ sensor_data  │       │
        │        │ detection_da │       │
        │        └──────┬──────┘       │
        │               │ 消费         │
        │        ┌──────┴──────┐       │
        │        │  consumer.py│       │
        │        │   (独立脚本) │       │
        └────────┼──────┬──────┼───────┘

     ┌───────────────────┼───────────────────┐
     │              FastAPI 后端              │
     │                                        │
     │  ┌─────── initialize_components() ──┐  │
     │  │ ① DB Connection (TimescaleDB)    │  │
     │  │ ② MinIO Connector               │  │
     │  │ ③ Kafka Consumers (自动订阅)     │  │
     │  │ ④ Kafka Producers (自动发送)     │  │
     │  │ ⑤ ThicknessMap Pipeline         │  │
     │  │ ⑥ QwenVL Agent + RAG           │  │
     │  └──────────────────────────────────┘  │
     │                                        │
     │  ┌─────── 数据管道 (每秒执行) ─────────┐│
     │  │ sensor_data ──► Processor ──► DB   ││
     │  │ detection_data ──► Processor ──► DB││
     │  │ detection_data ──► 热力图 ──► MinIO││
     │  └────────────────────────────────────┘│
     │                                        │
     │  ┌─────── AI 诊断 ─────────────────────┐│
     │  │ QwenVL Agent ─── 云图分析 ──► DB   ││
     │  │ RAG 知识库 ─── 故障检索             ││
     │  └────────────────────────────────────┘│
     │                                        │
     │  ┌─────── API 层 ──────────────────────┐│
     │  │ REST API (8000) + WebSocket        ││
     │  └────────────────────────────────────┘│
     └────────────────────────────────────────┘
                        │
              ┌─────────┴─────────┐
              │   Next.js 前端    │
              │   localhost:3000  │
              │   API 代理 → 8000 │
              └───────────────────┘
```

---

## 从零启动 —— 完整 7 步流程

```
Step 1  ── 启动 Docker 容器（数据库/Kafka/MinIO）
Step 2  ── 安装后端依赖 + 导入离线工艺参数（模头/设备数据）
Step 3  ── 启动后端（自动消费 data/realtime/ CSV → 数据库）
Step 4  ── 启动前端
Step 5  ── 每 20 秒 → 自动生成膜厚热力图云图
Step 6  ── CNN 诊断 + AI Agent 异常分析（含 QwenVL 多模态诊断）
Step 7  ── 前端查看结果
```

---

### Step 1：启动 Docker 容器

```bash
cd d:\docker && docker compose up -d
```

| 容器 | 端口 | 账号 |
|------|------|------|
| TimescaleDB | 5432 | `user` / `123456` / 库: `tsdb` |
| Kafka | 9092 | — |
| Zookeeper | 2181 | — |
| MinIO API | 9000 | `minioadmin` / `Minio@123456` |
| MinIO 控制台 | 9001 | `minioadmin` / `Minio@123456` |

---

### Step 2：安装依赖 + 导入离线工艺参数

> **注意**：这一步骤**必须在启动后端之前执行**。它负责将 `data/motouData/` 和 `data/otherData/` 下的 CSV 历史数据导入 TimescaleDB。这些数据是云图生成时**加工参数趋势图**的数据来源，如果没有执行此步骤，生成的云图上方 8 个工艺参数趋势图将显示为 "No Data"。

```bash
cd d:\codes\PythonCodes\Test\TimeSyncDiag

# 1. 安装依赖
uv sync

# 2. 导入离线工艺参数（模头/设备数据，只需一次）
uv run python scripts/import_offline_data.py
```

| 数据源 | 目标表 | 数据内容 |
|--------|--------|---------|
| `fastapi/data/motouData/*.csv` | `motou_data` | 模头树脂压力、模头压力_y |
| `fastapi/data/otherData/*.csv` | `other_data` | 主电机速度SV/PV、主电机扭矩、GP泵速度/压力/扭矩、过滤器入口压力、喂料泵进料率等 13 个字段 |

这些工艺参数在后续热力图生成时，会被绘制在云图**上方的 8 张趋势图中**，实现工艺参数与膜厚分布的时序对齐显示。

---

### Step 3：启动后端（自动消费 realtime/ CSV）

**后端启动时自动发生以下初始化：**

| # | 组件 | 行为 |
|---|------|------|
| ① | 数据库连接 | 连接到 TimescaleDB `tsdb` |
| ② | MinIO 连接 | 创建 `test-bucket` 存储桶 |
| ③ | Kafka 消费者 | 自动订阅 `sensor_data` + `detection_data` 主题 |
| ④ | 传感器生产者 | 每秒生成模拟传感器数据（温度/湿度/气压/光照/运动检测） |
| ⑤ | 检测数据生产者 | 逐行读取 `data/realtime/trip_transposed_*.csv` 发送到 Kafka |
| ⑥ | 膜厚云图管道 | 启动后每 20 秒执行一次云图生成周期 |
| ⑦ | QwenVL Agent | 初始化多模态诊断 Agent（含 RAG 知识库） |
| ⑧ | WebSocket | 启动实时推送 |

**`data/realtime/` CSV 自动消费链路：**

```
data/realtime/trip_transposed_*.csv     (8个文件, 共约400MB)
         │
         ▼
DetectionDataProducer  ──► Kafka(detection_data)
                                  │
                                  ▼
DetectionDataPipeline  ──► detection_device_data 表 (TimescaleDB)
         │  每秒拉取 + 写入
         │
         ▼
    数据库开始累积膜厚检测数据
```

> ⏱ 启动后约 **10~30 分钟** 完成所有 CSV 数据的消费（取决于文件大小）

---

### Step 4：启动前端

```bash
cd d:\codes\PythonCodes\Test\TimeSyncDiagWeb

# 安装依赖（首次）
pnpm install

# 启动开发服务器
pnpm dev
```

> 访问 `http://localhost:3000`，前端通过 `/api/backend/*` → `localhost:8000` 代理通信。

---

### Step 5：膜厚热力图自动生成（每 20 秒）

后端启动后，`ThicknessMapPipeline` 自动运行，每 20 秒执行一次完整的云图生成周期：

```
每 20 秒循环 ────────────────────────────────────────────────────────

Step 5a ── 获取数据范围
────────────────────────────────────────────────────────────────────
get_latest_thickness_time_range()
    → 查 detection_device_data 表的 最新 40 行 (按ID降序)
    → 返回 (start_time, end_time)

Step 5b ── 读取膜厚 + 工艺参数
────────────────────────────────────────────────────────────────────
generate_thickness_map_with_process_params(start, end)
    │
    ├── _get_thickness_data(start, end)
    │     → SELECT values FROM detection_device_data
    │     → 返回 List[List[float]] [40行 x 3000列] (时间 x 探头位置)
    │
    └── _get_process_parameters(start, end)
          → SELECT FROM motou_data     (模头树脂压力, 模头压力)
          → SELECT FROM other_data    (主电机/GP泵/过滤器等 8 个参数)
          → 返回 Dict {motou, other}

Step 5c ── 生成纯云图 → CNN 异常诊断
────────────────────────────────────────────────────────────────────
    _create_heatmap(data, with_labels=False)   → 纯云图 (IEEE风格配色)
            │
            ▼
    cnn_service.predict_from_bytes_sync(pure_map)   (localhost:8001)
            │
            ├── prediction=0 (正常) → _reduce_mask() 逐步减少遮盖
            │
            └── prediction=1 (异常) → _update_mask() 遮盖异常区域
                      │
                      ▼  (后台线程)
                QwenVLAgent.run_workflow()
                      │
                      ├── LangGraph 节点
                      │     [检测分析] → [处理分析] → [RAG检索] → [整合分析]
                      │
                      └── 写入 image_analysis_results 表

Step 5d ── 生成三张云图 → 上传 MinIO
────────────────────────────────────────────────────────────────────
    │
    ├── _create_heatmap(data, with_labels=True)    → 带坐标+图例的热力图
    │     → map_image_path → MinIO (test-bucket/thickness_maps/{uuid}/)
    │
    ├── _create_heatmap(data, with_labels=False)   → 纯云图 (CNN用)
    │     → pure_map_image_path → MinIO
    │
    └── _create_combined_heatmap(data, start, end) → 9子图完整图像
          ┌─────────────────────────────────────────────────────────┐
          │  (1) 主电机速度     (5) GP泵出口压力                     │
          │  (2) 模头压力       (6) 过滤器入口压力                   │
          │  (3) 喂料泵进料率    (7) 主电机扭矩                      │
          │  (4) GP泵入口压力   (8) GP泵速度                        │
          │  (9) 膜厚热力图 (蓝-绿-红配色, 目标值0.045mm)           │
          └─────────────────────────────────────────────────────────┘
          → combined_image_path → MinIO

Step 5e ── 持久化到数据库
────────────────────────────────────────────────────────────────────
    ThicknessMap.from_thickness_data()
        → INSERT INTO thickness_map
        → 字段: uuid, start_time, end_time
                map_image_path, pure_map_image_path, combined_image_path
                min_thickness, max_thickness, avg_thickness
                data_points_count, is_abnormal
        → 前端可通过 /api/thickness-map/map/latest 查询
```

#### 图片生成样例

| 类型 | 说明 | 保存位置 |
|------|------|---------|
| `map_image` | 带坐标轴、图例、标题的完整热力图 | MinIO `thickness_maps/{uuid}/map_image.png` |
| `pure_map_image` | 无坐标轴、纯色块的云图（用于CNN诊断） | MinIO `thickness_maps/{uuid}/pure_map_image.png` |
| `combined_image` | 8 张工艺参数趋势图 + 热力图的 9 子图组合 | MinIO `thickness_maps/{uuid}/combined_image.png` |

---

### Step 6：AI 诊断流程

#### CNN 异常检测（需额外启动 TensorFlow 服务于 port 8001）

```bash
cd d:\codes\PythonCodes\Test\TimeSyncDiag
uv sync --group cnn              # 首次安装 CNN 依赖（tensorflow）
uv run python cnn_image_classification/cnnfast.py
```

>`localhost:8001` 需单独部署。不启动时云图仍正常生成，跳过异常诊断步骤。

#### QwenVL Agent 多模态诊断（无需额外启动，使用阿里百炼 API）

Agent 基于 **LangGraph** 构建，4 个节点依次执行：

```
用户上传两张图片（检测图 + 处理图）
        │
        ▼
┌─ QwenVLState ─────────────────────────────────┐
│  imageDetect_path   = 云图 (map_image)          │
│  imageProcess_path  = 工艺趋势图 (combined_image)│
│  thickness_map_uuid = 关联的云图 UUID           │
└────────────────────────────────────────────────┘
        │
        ▼  LangGraph 节点链
                                                                                
┌──────────────────────────────────────────────────────────────────────┐
│  node_1: [检测分析]                                                    │
│    QwenVL 分析“检测图片”→ 判断膜厚异常位置、大小、形态                 │
├──────────────────────────────────────────────────────────────────────┤
│  node_2: [处理分析]                                                    │
│    QwenVL 分析“处理图片”→ 观察加工参数趋势、判断工艺异常                 │
├──────────────────────────────────────────────────────────────────────┤
│  node_3: [知识库检索] (可选，需 Qdrant 运行)                            │
│    search_knowledge_base() → RAG 检索相似故障案例                      │
├──────────────────────────────────────────────────────────────────────┤
│  node_4: [整合分析]                                                    │
│    综合检测+处理+知识库 → 输出完整诊断报告 → image_analysis_results 表  │
└──────────────────────────────────────────────────────────────────────┘
        │
        ▼
    final_result (SSE流式输出)
```

---

### Step 7：前端查看

| 页面 | URL | 查看内容 |
|------|-----|---------|
| **概览（Dashboard）** | `http://localhost:3000/dashboard/overview` | 最新膜厚热力图（由 `ThickMapChart` 组件渲染） |
| **历史诊断数据** | `http://localhost:3000/dashboard/history` | 历史诊断记录列表 |
| **图片分析（API）** | `POST /api/image-analysis/paginated` | 分页查询所有云图+诊断结果 |

---

### 完整启动命令速查

```bash
# ── 1. Docker 容器 ──
cd d:\docker && docker compose up -d

# ── 2. 安装依赖 + 导入离线工艺参数（必须在启动后端前完成）──
cd d:\codes\PythonCodes\Test\TimeSyncDiag
uv sync
uv run python scripts/import_offline_data.py

# ── 3. 后端 ──
uv run python fastapi/run.py

# ── 4. 前端 ──
cd d:\codes\PythonCodes\Test\TimeSyncDiagWeb
pnpm install && pnpm dev

# ── 5. （可选）CNN 异常诊断 ──
cd d:\codes\PythonCodes\Test\TimeSyncDiag
uv sync --group cnn
uv run python cnn_image_classification/cnnfast.py
```

---

### 从零到看到结果的时间线

```
t=0       启动 Docker 容器
t=1min    安装依赖 + 导入离线工艺参数
t=2min    启动后端 → 开始自动消费 realtime/ CSV
t=3min    启动前端
t=3min20s 第一次云图生成 (需要 ≥40 行检测数据)
t=3min40s 第二次云图生成
...
每 20s    新云图生成并入库 → 前端自动刷新
(约30min) CSV 全部消费完成 → detection_device_data 表达到 ~10K 行
```

> 如需查看已 Docker 容器中已有的历史云图数据（约 **2,326 张**），可直接通过 API 查询：
> `GET /api/thickness-map/map/latest` 或 `/api/image-analysis/paginated`

---

## API 文档

访问 `http://localhost:8000/docs` 查看交互式 Swagger 文档。

### 生产者控制 `/api/producer`

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/producer/status` | 查看传感器+检测生产者状态 |
| `POST` | `/api/producer/sensor/start` | 启动传感器数据生产者 |
| `POST` | `/api/producer/sensor/stop` | 停止传感器数据生产者 |
| `POST` | `/api/producer/detection/start` | 启动检测数据生产者 |
| `POST` | `/api/producer/detection/stop` | 停止检测数据生产者 |

### 消费者控制 `/api/consumer`

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/consumer/status` | 传感器消费者状态 |
| `GET` | `/api/consumer/detection/status` | 检测消费者状态 |
| `GET` | `/api/consumer/stats` | 消费统计信息 |
| `POST` | `/api/consumer/start` | 启动传感器消费者 |
| `POST` | `/api/consumer/stop` | 停止传感器消费者 |
| `POST` | `/api/consumer/detection/start` | 启动检测消费者 |
| `POST` | `/api/consumer/detection/stop` | 停止检测消费者 |

### 膜厚云图 `/api/thickness-map`

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/thickness-map/status` | 云图管道状态 |
| `POST` | `/api/thickness-map/generate` | 立即生成一次温度云图 |
| `POST` | `/api/thickness-map/generate-pure` | 生成纯云图（不带坐标） |
| `POST` | `/api/thickness-map/start` | 启动自动生成 |
| `POST` | `/api/thickness-map/stop` | 停止自动生成 |
| `GET` | `/api/thickness-map/map/latest` | 获取最新云图（base64） |
| `POST` | `/api/thickness-map/map/range` | 按时间范围查询云图 |

### 图片分析 `/api/image-analysis`

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/image-analysis/paginated` | 分页查询云图+分析结果 |
| `GET` | `/api/image-analysis/paginated` | 分页查询（GET 方式） |
| `POST` | `/api/image-analysis/image` | 获取指定 UUID 的完整图片 |
| `POST` | `/api/image-analysis/analysis-result` | 获取分析结果文本 |
| `PUT` | `/api/image-analysis/comment` | 更新批注和 RAG 状态 |

### QwenVL 多模态诊断 `/api/qwen-vl`

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/qwen-vl/analyze-image` | 上传两张图片，获取流式诊断回答 |

### WebSocket 实时推送

| 路径 | 说明 |
|------|------|
| `ws://localhost:8000/ws/realtime` | WebSocket 实时数据推送 |
| `ws://localhost:3000/api/thickness-map/ws` | 前端代理→后端 |

---

## 数据库模型

系统包含 **6 张表**，由 SQLAlchemy ORM 自动管理：

| 表名 | 说明 | 行数 | 模型类 |
|------|------|------|--------|
| `sensor_data` | 传感器时序数据（温度/湿度/气压/光照/运动检测） | ~81K | `SensorData` |
| `detection_device_data` | 膜厚检测数据（每行 100~3000 个探头值） | ~10K | `DetectionDeviceData` |
| `thickness_map` | 膜厚云图记录（MinIO 对象路径 + 统计指标） | ~2.5K | `ThicknessMap` |
| `image_analysis_results` | AI 诊断结果（关联云图 UUID） | ~100 | `ImageAnalysisResult` |
| `motou_data` | 模头数据（树脂压力 + 模头压力） | 160MB | `MotouData` |
| `other_data` | 其他设备数据（主电机/GP泵/过滤器等） | 286MB | `OtherData` |

`sensor_data` 表已配置 TimescaleDB **超表**（Hypertable），按 `timestamp` 自动分区，适用于时序数据的高效查询和聚合。

---

## AI 诊断模块

### QwenVL Agent（`LLMAgent/QwenVLAgent.py`）

基于 **LangGraph** 构建的多模态视觉诊断 Agent，处理流程：

```
用户上传两张图片（检测图 + 处理图）
        │
        ▼
┌─ QwenVLState ──────────────────┐
│  imageDetect_path               │
│  imageProcess_path              │
│  detect_result                  │
│  process_result                 │
│  knowledge_result（RAG 检索）    │
│  final_result（整合诊断）        │
│  thickness_map_uuid             │
└─────────────────────────────────┘
        │
        ▼  LangGraph 节点执行
┌→ [检测分析] ──► [处理分析] ──► [知识库检索] ──► [整合分析] ──┐
│                                                              │
└──────────────────────────────────────────────────────────────┘
        │
        ▼  SSE 流式输出
    final_result
```

### RAG 知识库（`LLMAgent/KnowledgeDb/`）

基于 **Qdrant** 向量数据库的故障知识检索：

| 组件 | 技术 | 说明 |
|------|------|------|
| 向量数据库 | Qdrant (localhost:6333) | 余弦相似度检索 |
| Embedding | Sentence-Transformers `all-MiniLM-L6-v2` | 384 维向量 |
| LLM | Qwen3-VL-Plus (阿里百炼) | 对话生成 + Agent 工具调用 |

> Qdrant 为可选组件，未启动时 Agent 降级为无知识库模式工作。

### CNN 图像分类（`cnn_image_classification/cnnfast.py`）

独立的 TensorFlow CNN API 服务（`http://localhost:8001`），由 `services/remote_template.py` 远程调用。

```bash
cd d:\codes\PythonCodes\Test\TimeSyncDiag
uv sync --group cnn                 # 安装 CNN 依赖（tensorflow）
uv run python cnn_image_classification/cnnfast.py
```

支持：
- `GET /` — 服务信息和模型状态
- `GET /health` — 健康检查
- `GET /model/info` — 模型参数查询
- `POST /predict` — 单图预测（返回类别、置信度、标签）
- `POST /predict/batch` — 批量预测

> 模型文件 `best_resnet_model.keras`（128MB）已包含在项目中，可直接启动。CNN 服务为可选组件，不启动时云图仍正常生成，仅跳过异常诊断步骤。

---

## Docker 数据概况

| 容器 | 数据量 | 说明 |
|------|--------|------|
| TimescaleDB | 6 张表，约 738MB | 传感器 81K 行 + 检测 10K 行 + 云图 2.5K 行 |
| MinIO | ~11GB | `test-bucket` 存储桶含 2495 张厚度云图 |
| Kafka | 自动创建 | `sensor_data` + `detection_data` 两个主题 |

---

## 注意事项

1. **Docker 容器必须优先启动**，否则后端初始化会跳过数据库/Kafka 连接
2. **第一次启动较慢**（约 1~2 分钟），因为要下载 AI 模型（sentence-transformers），之后会缓存
3. **`kafka-python` 兼容性**：当前使用 kafka-python 3.0.4，`buffer_memory` 参数已移除，不影响功能
4. **Qdrant 知识库**为可选依赖，未启动时 Agent 自动降级为无知识库模式
5. **CNN 图像分类服务** (`localhost:8001`) 需要单独部署 TensorFlow 服务，不启用不影响核心功能
6. **CLAUDE.md** 和 **.claude/** 目录为 Claude Code 项目配置文件，不影响运行
7. 前后端通过 Next.js `next.config.js` 中的 `rewrites` 代理通信，**前端访问 `localhost:3000/api/backend/*` 即等于调用后端 `localhost:8000/api/*`**

---

## uv 配置（`pyproject.toml`）

```toml
# ── 首次安装依赖 ──
#   uv sync

# ── 启动后端 API 服务 ──
#   uv run python fastapi/run.py              # 开发模式（热重载）
#   cd fastapi && uv run uvicorn main:app ... # 生产模式

# ── 启动 Kafka 生产者（发送模拟数据）──
#   uv run python producer.py

# ── 启动 Kafka 消费者（消费→写入数据库）──
#   uv run python consumer.py

# ── 独立组件 ──
#   uv run python fastapi/logic/models/db_connection.py  # 数据库连接测试
#   uv run python fastapi/heatmap/creatmap.py            # 膜厚热力图（独立工具）
#   uv run python cnn_image_classification/cnnfast.py    # CNN 诊断 API (port 8001)
```
