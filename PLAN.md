# TimeSyncDiag 工程化改造计划

> 版本：v1.1  
> 目标：将当前原型系统改造为可长期稳定运行、可运维、可配置、可审计的工业级系统。  
> 原则：**先让系统能“可控地跑”，再让系统“跑得好”**。不引入异想天开的技术栈，所有改动围绕现有 FastAPI + TimescaleDB + Kafka + MinIO + OpenAI 兼容多模态模型架构展开。

---

## 1. 项目现状与关键问题

当前系统已经实现了核心数据流：

```
Kafka 生产/消费 → TimescaleDB → 膜厚云图生成 → CNN 异常检测 → LLM 多 Agent 诊断 → 结果入库
```

但在工程化层面存在以下短板：

| 问题域 | 现状 | 风险 |
| --- | --- | --- |
| 配置管理 | 大量参数硬编码在代码中 | 无法在线调参，修改需重启、易出错 |
| LLM 引擎耦合 | `QwenVLAgent` 强绑定 DashScope/Qwen SDK | 无法切换到其他 OpenAI 兼容模型 |
| 稳定性 | CNN 子进程用 Windows 专用命令拉起，无健康检查 | 跨平台失败、故障无法自愈 |
| 可观测性 | 日志格式不统一，无指标暴露 | 出问题难以定位 |
| LLM 可信度 | 输出为自由文本，无校验和人工确认 | 工业场景下幻觉可能导致误操作 |
| 测试与交付 | 无单元测试、无容器化 | 无法持续集成、部署风险高 |

---

## 2. 总体目标

1. **可配置**：所有运行时关键参数可通过 `config.yml` 和 REST API 管理，修改后实时生效或明确提示需重启。
2. **模型解耦**：LLM 视觉 Agent 不绑定具体模型厂商，支持所有 OpenAI 兼容 API。
3. **可运维**：具备统一日志、指标监控、健康检查、链路追踪。
4. **稳定可靠**：关键依赖（DB/Kafka/MinIO/CNN/LLM）故障时可降级，不阻塞主流程。
5. **可信诊断**：LLM 输出结构化、可校验，关键建议需人工确认。
6. **可交付**：具备测试体系、容器化、CI/CD。

---

## 3. 阶段规划

### 第一阶段：配置中心 + LLM 引擎解耦 + API 实时调参（P0，最优先）

**目标**：

1. 把所有运行时关键参数从代码中抽离，实现启动加载 + API 实时修改 + `config.yml` 持久化。
2. 将 `QwenVLAgent` 重命名为 `MyVLAgent`，并使用 langchain `ChatOpenAI` 替换 Qwen 专用 SDK，使其支持任意 OpenAI 兼容接口。
3. 让 WebSocket 的关键参数也支持从配置读取和 API 实时调整。

#### 3.1.1 新增/扩展的配置项

```yaml
# ── 膜厚云图生成参数 ──
thickness_map:
  target_thickness: 0.045           # 目标膜厚（mm）
  thickness_range: 0.005            # 颜色范围 ±value
  record_count: 40                  # 每次读取最新 N 条检测记录
  pipeline_interval: 20             # 云图生成管道执行间隔（秒）
  mask_preserve_rows: 4             # mask 永远不遮挡最后 N 行
  reduce_mask_ratio: 0.1            # 正常时每次减少 mask 的比例
  dpi: 600                          # 输出图片 DPI
  figsize: [10, 6]                  # 基础云图尺寸
  combined_figsize: [16, 16]        # 组合图尺寸

# ── CNN 异常检测参数 ──
cnn_diagnosis:
  enabled: true                     # 是否启用 CNN 异常检测
  confidence_threshold: 0.5         # 置信度阈值
  fallback_on_error: true           # CNN 失败时默认正常
  timeout: 30.0                     # CNN 推理超时（秒）

# ── LLM Agent 参数（OpenAI 兼容格式，解耦具体模型厂商） ──
llm:
  model_name: qwen3-vl-plus         # 模型名称，如 gpt-4o、qwen3-vl-plus、qwen2-5-vl-72b-instruct 等
  base_url: https://dashscope.aliyuncs.com/compatible-mode/v1  # OpenAI 兼容 API 地址
  api_key_env: DASHSCOPE_API_KEY    # API Key 所在环境变量名
  temperature: 1.0
  max_tokens: 5000
  timeout: 120.0                    # LLM 调用超时（秒）
  use_rag: true                     # 是否启用 RAG
  rag_top_k: 3                      # RAG 检索条数
  rag_query_max_length: 500         # RAG 查询最大长度
  detect_agent_enabled: true        # 检测 Agent 开关
  process_agent_enabled: true       # 工艺 Agent 开关
  decision_agent_enabled: true      # 决策 Agent 开关

# ── WebSocket 参数 ──
websocket:
  update_interval: 10               # 云图广播间隔（秒）
  heartbeat_interval: 30            # 心跳间隔（秒）

# ── Kafka 参数 ──
kafka:
  bootstrap_servers: "localhost:9092"
  sensor_topic: sensor_data
  detection_topic: detection_data
  sensor_producer_interval: 1.0     # 传感器数据生产间隔（秒）
  detection_producer_interval: 1.0  # 检测数据生产间隔（秒）
  consumer_poll_timeout_ms: 1000    # 消费者 poll 超时

# ── 系统参数 ──
system:
  timezone: Asia/Shanghai
  log_level: INFO
```

#### 3.1.2 技术方案

1. **改造 `fastapi/config/config_loader.py`**
   - 保持单例模式。
   - 使用文件锁/原子写保证 `config.yml` 并发安全。
   - 新增 `load()`、`save()`、`get()`、`set()`、`update()`、`reload()`、`to_dict()`。
   - 新增 `register_change_callback(key_prefix, callback)`，支持配置变更通知。
   - 使用 Pydantic `BaseModel` 对配置做类型校验。

2. **新增 `fastapi/config/schemas.py`**
   - 定义 `ThicknessMapConfig`、`CnnDiagnosisConfig`、`LlmConfig`、`WebSocketConfig`、`KafkaConfig`、`SystemConfig`。
   - 组合为顶层 `AppConfig`。
   - 使用 `Field(..., ge=..., le=...)` 限制参数范围。

3. **新增 API 路由 `fastapi/routes/config.py`**
   - `GET /api/config`：获取完整当前配置。
   - `GET /api/config/{section}`：获取指定 section。
   - `PATCH /api/config`：部分更新配置，返回变更摘要。
   - `PUT /api/config/{section}`：完整替换某个 section。
   - `POST /api/config/reload`：从 `config.yml` 重新加载。
   - `POST /api/config/reset`：重置为默认值（可选）。

4. **改造各组件读取运行时配置**
   - `fastapi/logic/processors/thickness_map_processor.py`
   - `fastapi/logic/consumers/thickness_map_pipeline.py`
   - `fastapi/websocket/thickness_map_ws.py`
   - `fastapi/LLMAgent/MyVLAgent.py`
   - `fastapi/LLMAgent/KnowledgeDb/kb.py`
   - `fastapi/logic/services/remote_template.py`
   - `fastapi/logic/consumers/consumer.py`
   - `fastapi/logic/producers/producer.py`
   - `fastapi/logic/models/db_connection.py`

5. **LLM 引擎解耦：从 QwenVL 到通用 OpenAI 兼容模型**
   - 将 `fastapi/LLMAgent/QwenVLAgent.py` 重命名为 `fastapi/LLMAgent/MyVLAgent.py`。
   - 移除对 DashScope/Qwen 专用 SDK 的依赖，改用 langchain `ChatOpenAI` 作为统一多模态引擎。
   - 通过 `base_url` + `model_name` + `api_key_env` 支持任意 OpenAI 兼容接口：
     - 阿里云百炼（DashScope）
     - OpenAI 官方 API
     - 本地 vLLM / Ollama / Xinference
     - 其他私有部署的 OpenAI 兼容服务
   - 图片输入继续使用 OpenAI 兼容的 `image_url` / base64 格式。
   - 保持现有的 LangGraph 多 Agent 工作流（检测 Agent → 工艺 Agent → RAG → 整合 Agent）不变。
   - `state.py` 中 `qwen_vl_agent` 改名为 `vl_agent`，所有引用同步更新。

6. **实时生效策略**
   - 热生效参数：`thickness_map.*`、`websocket.*`、`llm.temperature`、`llm.max_tokens`、`llm.timeout`、`llm.use_rag`、`llm.rag_top_k` 等，通过回调让组件下次执行使用新值。
   - 需重启参数：`database.*`、`api.port`、`kafka.bootstrap_servers`、`llm.model_name`、`llm.base_url`、`llm.api_key_env` 等，API 返回 `requires_restart: true`。
   - 所有配置变更写审计日志。

#### 3.1.3 验收标准

- [ ] 启动时能从 `config.yml` 加载所有参数。
- [ ] `PATCH /api/config` 可修改参数并持久化到 `config.yml`。
- [ ] 修改 `thickness_map.target_thickness` 后，下一张云图立即使用新目标值。
- [ ] 修改 `websocket.update_interval` 后，WebSocket 广播间隔立即改变。
- [ ] 修改 `llm.temperature` 后，下一次 Agent 调用使用新温度参数。
- [ ] 修改 `llm.model_name` 时 API 返回警告，明确需重启才生效。
- [ ] 修改 `database.host` 时 API 返回警告，明确需重启才生效。
- [ ] 配置写入有并发保护，不会损坏 `config.yml`。
- [ ] 非法参数返回 HTTP 422，并说明原因。
- [ ] `MyVLAgent` 可通过配置切换为任意 OpenAI 兼容模型（如从 DashScope 切换到本地 vLLM）。

---

### 第二阶段：稳定性与健壮性（P1）

**目标**：让系统在关键依赖故障时仍能继续运行，并具备自愈能力。

#### 3.2.1 CNN 服务管理

- 短期：主服务启动时不强制拉起 CNN 子进程，改为通过配置 `cnn_diagnosis.enabled` 控制是否启用。
- 移除 `main.py` 中 `subprocess.Popen` + `start cmd /K` 的 Windows 专用启动逻辑。
- 长期：用 `docker-compose` 或 `supervisor` 独立管理 CNN 生命周期。
- 增加 CNN 健康检查轮询，失败时自动降级（禁用 CNN，记录告警）。

#### 3.2.2 数据库连接可靠性

- 连接池参数配置化：`pool_size`、`max_overflow`、`pool_recycle`。
- 启用 `pool_pre_ping=True`。
- 添加连接失败重试（指数退避）。

#### 3.2.3 Kafka 消费者可靠性

- 手动提交 offset，避免重复消费或丢失。
- 消费异常时进入重试队列，超过次数进入死信队列（DLQ）。
- 完善 `/api/consumer/start`、`/api/consumer/stop` 状态管理。

#### 3.2.4 降级策略矩阵

| 故障 | 降级行为 |
| --- | --- |
| LLM API 不可用 | 返回“诊断服务暂不可用，建议人工复核”，不阻塞数据流 |
| CNN 服务不可用 | `is_abnormal=False`，记录告警，允许后续人工触发 LLM 诊断 |
| MinIO 不可用 | 云图元数据仍写入 DB，图片本地缓存，恢复后补传 |
| Kafka 不可用 | 生产者数据暂存本地队列，恢复后补发 |
| TimescaleDB 不可用 | 消费者进入重试，数据写入本地 WAL，恢复后回放 |

---

### 第三阶段：可观测性与运维（P1）

**目标**：让系统可监控、可定位、可告警。

#### 3.3.1 统一日志

- 使用 `structlog` 或标准 logging 输出 JSON 格式日志。
- `system.log_level` 可配置。
- 关键操作记录结构化日志：云图生成、CNN 推理、LLM 调用、配置变更、异常告警。

#### 3.3.2 指标监控

接入 `prometheus-fastapi-instrumentator`，暴露：

- `thickness_map_generation_duration_seconds`
- `thickness_map_generation_total`
- `cnn_inference_duration_seconds`
- `cnn_inference_total`
- `llm_agent_duration_seconds`
- `llm_agent_total`
- `kafka_consumer_lag`
- `websocket_connected_clients`
- `config_change_total`

#### 3.3.3 健康检查

- `GET /health`：服务自身状态。
- `GET /health/ready`：依赖全部就绪才返回 200。
- `GET /health/live`：服务存活。

#### 3.3.4 链路追踪

- 接入 OpenTelemetry。
- 追踪 LLM 调用、数据库查询、Kafka 消息处理的全链路。

---

### 第四阶段：LLM 诊断安全与可信（P2）

**目标**：让 LLM 诊断结果在工业场景下“可用、可信、可控”。

#### 3.4.1 输出结构化

- 让 LLM 输出 JSON，使用 Pydantic 解析。
- 定义输出 schema：
  - `abnormal_type`
  - `confidence`
  - `time_ranges`
  - `root_cause`
  - `suggestions`
  - `uncertainty`
  - `evidence`

#### 3.4.2 幻觉防护

- 时间范围校验：LLM 说的异常时间必须在真实数据时间范围内。
- 异常类型白名单：只允许预定义类型。
- 置信度阈值：低于阈值标记为“不确定，需人工复核”。
- 要求 LLM 引用证据。

#### 3.4.3 人工确认环

扩展 `image_analysis_results` 表：

```python
status: Literal["pending", "reviewed", "approved", "rejected"]
reviewed_by: str
reviewed_at: datetime
risk_level: Literal["low", "medium", "high"]
```

高风险建议必须人工确认后才允许下发到 MES/PLC。

#### 3.4.4 RAG 优化

- Query 重写：从 `process_result` + `detect_result` 提取关键词，而非简单截断 500 字符。
- 检索结果重排序：按相关性和时间远近排序。
- 来源引用：最终诊断标注参考的知识库案例 ID。

---

### 第五阶段：测试、容器化与 CI/CD（P2）

**目标**：让系统可测试、可部署、可持续交付。

#### 3.5.1 测试体系

- 单元测试：`pytest` 覆盖配置中心、数据处理、CNN 客户端、MyVLAgent。
- 集成测试：使用 `testcontainers` 启动 PostgreSQL、Kafka、MinIO。
- 端到端测试：模拟完整数据流。

#### 3.5.2 容器化

- `Dockerfile`：主服务。
- `Dockerfile.cnn`：CNN 服务。
- `docker-compose.yml`：主服务 + CNN + TimescaleDB + Kafka + MinIO + Qdrant。

#### 3.5.3 CI/CD

- GitHub Actions / GitLab CI：
  - lint（ruff、black）
  - type check（mypy）
  - unit test
  - build Docker image
  - deploy to staging

#### 3.5.4 文档

- API 文档：Swagger（FastAPI 已支持）。
- 运维手册：部署、配置、告警、故障排查。
- 开发手册：本地开发、测试、贡献规范。

---

## 4. 执行顺序与建议工时

| 优先级 | 阶段 | 预计工时 | 关键产出 |
| --- | --- | --- | --- |
| P0 | 第一阶段：配置中心 + LLM 引擎解耦 + API 实时调参 | 4-6 天 | 可调参、模型可切换的系统 |
| P1 | 第二阶段：稳定性 | 5-7 天 | 7×24 可运行 |
| P1 | 第三阶段：可观测性 | 3-4 天 | 可运维 |
| P2 | 第四阶段：LLM 安全与可信 | 5-7 天 | 敢用于生产 |
| P2 | 第五阶段：测试与容器化 | 5-7 天 | 可交付 |

---

## 5. 第一阶段详细任务分解

| 序号 | 任务 | 涉及文件 | 说明 |
| --- | --- | --- | --- |
| 1.1 | 设计 Pydantic 配置模型 | `fastapi/config/schemas.py` | 定义所有可配置 section 的模型，包含 LlmConfig、WebSocketConfig 等 |
| 1.2 | 改造 ConfigLoader 支持读写 | `fastapi/config/config_loader.py` | 单例 + 文件锁 + 校验 + 回调 |
| 1.3 | 新增配置管理 API | `fastapi/routes/config.py` | GET/PATCH/PUT/POST/reload/reset |
| 1.4 | 注册配置路由 | `fastapi/main.py` | 把 config router 注册到 app |
| 1.5 | 改造膜厚云图处理器 | `fastapi/logic/processors/thickness_map_processor.py` | 从配置读取目标膜厚、范围、record_count、DPI 等 |
| 1.6 | 改造云图生成管道 | `fastapi/logic/consumers/thickness_map_pipeline.py` | 从配置读取 pipeline_interval |
| 1.7 | 改造 WebSocket | `fastapi/websocket/thickness_map_ws.py` | 从配置读取 update_interval、heartbeat_interval，支持热更新 |
| 1.8 | 解耦并重命名 LLM Agent | `fastapi/LLMAgent/MyVLAgent.py`（由 `QwenVLAgent.py` 重命名） | 使用 langchain `ChatOpenAI` 替换 Qwen 专用客户端，支持任意 OpenAI 兼容 API；从配置读取模型参数、RAG 开关等 |
| 1.9 | 更新全局状态引用 | `fastapi/state.py`、`fastapi/routes/qwen_vl.py`、`fastapi/logic/processors/thickness_map_processor.py` | 将 `qwen_vl_agent` 改名为 `vl_agent`，路由同步更新 |
| 1.10 | 改造 CNN 客户端 | `fastapi/logic/services/remote_template.py` | 从配置读取 timeout、confidence_threshold |
| 1.11 | 改造 Kafka 消费者/生产者 | `fastapi/logic/consumers/consumer.py`、`fastapi/logic/producers/producer.py` | 从配置读取 interval、poll timeout |
| 1.12 | 更新根目录 config.yml | `config.yml` | 加入所有新配置项，LLM 部分使用 OpenAI 兼容格式 |
| 1.13 | 写测试验证 | `tests/test_config.py`、`tests/test_vl_agent.py` | 验证配置读写、API 调参、热生效、模型切换 |
| 1.14 | 更新项目记忆 | `project_memory.md` | 记录配置中心架构和 LLM 引擎解耦方案 |

---

## 6. 风险与依赖

| 风险 | 影响 | 缓解措施 |
| --- | --- | --- |
| 配置变更并发写导致 `config.yml` 损坏 | 高 | 使用文件锁 + 原子写（先写临时文件再替换） |
| 热生效参数被组件缓存未刷新 | 中 | 明确区分热生效/需重启参数，加回调机制 |
| LLM 模型切换需要重建客户端 | 中 | API 返回 `requires_restart: true` |
| 不同 OpenAI 兼容 API 对多模态消息格式支持有差异 | 中 | 使用标准 OpenAI image_url/base64 格式，必要时做适配层 |
| 老配置文件不兼容新 schema | 中 | 启动时校验，缺失字段使用默认值并提示 |
| 测试环境缺少 Kafka/MinIO/DB | 中 | 使用 `testcontainers` 做集成测试 |

---

## 7. 立即开始

本计划已覆盖从“可调参、可切换模型”到“可交付”的完整路径。**建议立即开始第一阶段**，因为：

1. 它是所有后续改造的基础。
2. 它直接回应了“参数可变、API 控制、yml 持久化、模型解耦”的核心需求。
3. 改动范围可控，4-6 天内可见到完整成果。
