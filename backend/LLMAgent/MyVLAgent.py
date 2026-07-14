import os
import base64
import sys
import logging
from typing import Annotated, TypedDict, Optional, Iterator
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, BaseMessage
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

logger = logging.getLogger(__name__)

# 添加LLMAgent目录到Python路径，以便使用绝对导入
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from backend.logic.models.db_connection import DatabaseConnection
from backend.logic.services.image_analysis_service import ImageAnalysisService
from backend.config.config_loader import config

# 加载环境变量 - 优先使用项目根目录 .env，找不到再使用 LLMAgent 目录下的 .env
project_env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), ".env")
agent_env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(project_env_path)
load_dotenv(agent_env_path)


# ------------------------------------------------------------------
# MyVLAgent 类封装（通用 OpenAI 兼容视觉 Agent）
# ------------------------------------------------------------------
class MyVLState(TypedDict):
    """
    包含两个图片、检测结果、处理结果和整合结果的状态类型
    """
    imageDetect_path: str
    imageProcess_path: str
    detect_result: str
    process_result: str
    knowledge_result: str
    final_result: str
    messages: Annotated[list[BaseMessage], add_messages]
    thickness_map_uuid: str


class MyVLAgent:
    """
    通用多模态视觉 Agent，基于 LangGraph 工作流。
    支持任意 OpenAI 兼容 API（DashScope、OpenAI、vLLM、Ollama、Xinference 等）。
    """

    def __init__(self, db_connection: Optional[DatabaseConnection] = None):
        """
        初始化 MyVLAgent 实例

        Args:
            db_connection: 数据库连接实例，可选
        """
        # 从配置读取模型参数
        llm_cfg = config.llm
        self.model_name = llm_cfg.model_name
        self.base_url = llm_cfg.base_url
        self.api_key_env = llm_cfg.api_key_env
        self.temperature = llm_cfg.temperature
        self.max_tokens = llm_cfg.max_tokens
        self.timeout = llm_cfg.timeout
        self.use_rag = llm_cfg.use_rag
        self.rag_top_k = llm_cfg.rag_top_k
        self.rag_query_max_length = llm_cfg.rag_query_max_length

        self.api_key = os.getenv(self.api_key_env)
        self._last_full_response = ""

        # 用于跟踪当前运行的工作流的终止标志
        import threading
        self._thread_local = threading.local()

        if not self.api_key:
            raise ValueError(f"{self.api_key_env} 环境变量未设置")

        # 初始化数据库连接和服务
        if db_connection is None:
            self.db_connection = DatabaseConnection()
            if not self.db_connection.connect():
                raise RuntimeError("数据库连接失败")
        else:
            self.db_connection = db_connection

        self.image_analysis_service = ImageAnalysisService(self.db_connection)

        # 初始化 LangGraph 工作流
        self.workflow = self._create_workflow()

    def terminate_workflow(self):
        """终止当前运行的工作流"""
        if hasattr(self._thread_local, 'should_terminate'):
            self._thread_local.should_terminate = True

    def _encode_image(self, image_path: str) -> str:
        """读取图片文件并转换为 Base64 字符串"""
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"图片文件不存在: {image_path}")

        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')

    def _create_llm_instance(self) -> ChatOpenAI:
        """创建 ChatOpenAI 实例"""
        return ChatOpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
            model=self.model_name,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            timeout=self.timeout,
            streaming=True
        )

    def _save_analysis_result(
        self,
        thickness_map_uuid: str,
        detection_agent_result: Optional[str] = None,
        processing_agent_result: Optional[str] = None,
        decision_agent_result: Optional[str] = None,
        use_rag: bool = False
    ):
        """保存或更新分析结果到数据库"""
        from datetime import datetime, timezone

        try:
            existing_result = self.image_analysis_service.get_analysis_results_by_thickness_map_id(thickness_map_uuid)
            if existing_result:
                update_kwargs = {}
                if detection_agent_result is not None:
                    update_kwargs["detection_agent_result"] = detection_agent_result
                if processing_agent_result is not None:
                    update_kwargs["processing_agent_result"] = processing_agent_result
                if decision_agent_result is not None:
                    update_kwargs["decision_agent_result"] = decision_agent_result
                update_kwargs["use_rag"] = use_rag
                update_kwargs["updated_at"] = datetime.now(timezone.utc)

                self.image_analysis_service.update_analysis_result(
                    result_id=existing_result[0].id,
                    **update_kwargs
                )
            else:
                import pytz
                now = datetime.now(pytz.timezone(config.system.timezone))
                self.image_analysis_service.create_analysis_result(
                    thickness_map_uuid=thickness_map_uuid,
                    detection_agent_result=detection_agent_result or "",
                    processing_agent_result=processing_agent_result or "",
                    decision_agent_result=decision_agent_result or "",
                    use_rag=use_rag,
                    created_at=now,
                    updated_at=now
                )
        except Exception as e:
            logger.error(f"保存分析结果到数据库失败: {e}")

    def stream_invoke(self, image_path: str, question: str) -> Iterator[str]:
        """流式调用模型，实时返回结果"""
        llm = self._create_llm_instance()
        self._last_full_response = ""

        message_content = []
        if image_path:
            base64_image = self._encode_image(image_path)
            message_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
            })

        message_content.append({"type": "text", "text": question})
        human_message = HumanMessage(content=message_content)

        for chunk in llm.stream([human_message]):
            chunk_content = chunk.content
            if chunk_content:
                yield chunk_content
                self._last_full_response += chunk_content

    def get_last_response(self) -> str:
        """获取上次流式调用的完整结果"""
        return self._last_full_response

    def invoke(self, image_path: str, question: str) -> str:
        """普通调用模型，一次性返回结果"""
        llm = self._create_llm_instance()

        message_content = []
        if image_path:
            base64_image = self._encode_image(image_path)
            message_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
            })

        message_content.append({"type": "text", "text": question})
        human_message = HumanMessage(content=message_content)
        response = llm.invoke([human_message])
        return response.content

    def _analyze_detect_image(self, state: MyVLState) -> MyVLState:
        """分析检测图片，生成检测结果"""
        if not config.llm.detect_agent_enabled:
            return {"detect_result": "检测 Agent 已禁用", "messages": []}

        llm = self._create_llm_instance()
        message_content = []

        if state["imageDetect_path"]:
            base64_image = self._encode_image(state["imageDetect_path"])
            message_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
            })

        message_content.append({
            "type": "text",
            "text": "【角色设定】\n 你是一位拥有20年经验的薄膜质检专家（QC Expert）与流变数据分析师。你擅长通过分析测厚仪生成的二维云图（2D Thickness Heatmap/Contour Plot）来诊断挤出流延或双拉产线的工艺缺陷。\n 【图像定义 - 关键信息】\n 我将上传一张薄膜厚度的二维分布图，请严格按照以下物理定义进行视觉解码：\n X轴（横向）：代表 CD方向 (Cross Direction)，即模头/螺栓的空间位置。\n Y轴（纵向）：代表 MD方向 (Machine Direction)，也就是时间轴（Time），越往上/下（请根据图注判断）代表时间越晚。\n 颜色（Z轴）：代表膜厚偏差。通常红色/暖色代表偏厚，蓝色/冷色代表偏薄（请先根据图例确认这一点）。\n 采样逻辑警告：该数据是由单点扫描式测厚仪（Scanning Gauge）生成的。探头在CD往复运动时，薄膜在MD高速移动。因此，图中的每一个像素点在时空上并非绝对连续，存在\"扫描周期与产线速度的耦合\"。\n 【分析任务 - 请按步骤推理】\n 第一步：视觉校准与全局评估\n 首先读取图例（Scale Bar），告诉我当前显示的厚度波动范围（Min/Max）是多少？\n 观察整体色块分布，是呈现\"杂乱无章的斑点\"还是有明显的\"条纹特征\"？\n 第二步：特征分离与异常定位（核心任务）\n 请区分以下三种特定的工艺缺陷模式，并告诉我图中是否存在：\n MD纵向条纹 (Die Lines)：在X轴固定位置，沿Y轴延伸的垂直线条（颜色持续偏深或偏浅）。\n 物理含义：对应模头模唇的脏污、损伤或特定螺栓调节失灵。\n CD横向波动 (MD Surging)：沿X轴横贯整个幅宽的颜色带（如：一整条红带紧接着一整条蓝带）。\n 物理含义：挤出机/计量泵的供料脉动，或冷辊/牵引速度的不稳定。\n 斜向纹理 (Diagonal Patterns)：\n 物理含义：这是扫描耦合效应的典型特征。这通常意味着产线存在一个高频的MD波动，其频率与探头的扫描周期形成了\"拍频（Beat Frequency）\"。请指出是否有这种斜纹？\n 第三步：异常时段锁定\n 请根据Y轴的时间刻度，找出颜色反差最剧烈、或纹理突然改变的时间段。\n 例如：\"在Y轴约1/3处（对应时间XX:XX），出现了一次明显的横向变薄。\"\n 【输出结果】\n 请输出一份简报：\n 📊 波动概况：极差范围与主要波动模式。\n 🕒 异常时间表：列出发生显著波动的具体时间段及其表现（变厚/变薄/条纹化）。\n 🔧 归因诊断：基于发现的模式（横向/纵向/斜向），推测是模头问题（空间域）还是挤出/牵引问题（时间域）。"
        })

        human_message = HumanMessage(content=message_content)
        response = llm.invoke([human_message])

        thickness_map_uuid = state.get("thickness_map_uuid")
        if thickness_map_uuid:
            try:
                self._save_analysis_result(
                    thickness_map_uuid=thickness_map_uuid,
                    detection_agent_result=response.content,
                    use_rag=self.use_rag
                )
            except Exception as e:
                logger.error(f"保存检测结果到数据库失败: {e}")

        return {"detect_result": response.content, "messages": [human_message, response]}

    def _analyze_process_image(self, state: MyVLState) -> MyVLState:
        """分析处理图片，生成处理结果"""
        if not config.llm.process_agent_enabled:
            return {"process_result": "工艺 Agent 已禁用", "messages": []}

        llm = self._create_llm_instance()
        message_content = []

        if state["imageProcess_path"]:
            base64_image = self._encode_image(state["imageProcess_path"])
            message_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
            })

        message_content.append({
            "type": "text",
            "text": """【角色设定】
你是一位拥有20年经验的薄膜挤出工艺专家（Extrusion Expert），同时精通工业大数据可视化分析。
你现在的任务是进行一项“时空关联故障诊断”。你面前有一张组合图表：
- **上部区域 (Top Panels)**：包含8个关键工艺参数的时间序列趋势图（如螺杆转速、模头压力、熔体泵参数等）。
- **底部区域 (Bottom Panel)**：是一张“时空热力图 (Spacetime Heatmap)”，展示了薄膜厚度/质量在“时间 (X轴)”与“横向位置 (Y轴)”上的分布。

【分析指令 - 请执行严格的逻辑推演】

**第一步：全景扫描与参数识别 (Global Scan)**
1.  **读取图例**：请准确识别上部8个小图的Y轴标签（例如：Motor Speed, Die Pressure, GP Pump Speed等）。
2.  **读取时间轴**：确认X轴的时间跨度。
3.  **锁定突变点**：扫描整个时间轴，指出是否存在一个明显的“系统性崩溃”或“突变时刻”？

**第二步：工艺侧根因定位 (Process Root Cause)**
在突变发生时，哪些工艺参数率先出现了异常？
- 观察 **GP Pump Speed (熔体泵转速)** 和 **Die Pressure (模头压力)**。
- 它们是瞬间跌落、缓慢漂移还是剧烈震荡？
- **逻辑判断**：是压力的变化导致了转速波动，还是转速的指令突变导致了压力失压？

**第三步：质量侧后果验证 (Quality Impact)**
视线下移到底部的热力图 (Heatmap)：
- 在上述工艺突变的时间点，热力图的颜色发生了什么变化？
- 这种颜色变化代表了膜厚发生了什么改变？
- 这种缺陷是“全幅面的”还是“局部的”？

**第四步：综合诊断 (Synthesis)**
将工艺异常与质量异常串联起来，形成证据链。

【输出格式要求】

请按以下结构输出 Markdown 报告：

### 📊 1. 图表解构
> **时间范围**：[读取X轴起止时间]
> **关键参数列表**：[列出识别到的上部曲线名称]

### ⚠️ 2. 核心故障事件 (Critical Event Analysis)
| 突变时间点 | 触发参数 (Trigger) | 波动形态描述 | 热力图响应 (Quality Response) |
| :--- | :--- | :--- | :--- |
| [时间] | [参数] | [描述] | [响应] |

### 🕵️‍♂️ 3. 诊断结论 (Diagnosis & Hypothesis)
- **直接原因**：[基于数据推断]
- **质量后果**：[膜厚变化]
- **排查建议**：
    1. [...]
    2. [...]

### 💡 专家洞察
用一句话评价当前生产过程的稳定性。
"""
        })

        human_message = HumanMessage(content=message_content)
        response = llm.invoke([human_message])

        thickness_map_uuid = state.get("thickness_map_uuid")
        if thickness_map_uuid:
            try:
                self._save_analysis_result(
                    thickness_map_uuid=thickness_map_uuid,
                    processing_agent_result=response.content,
                    use_rag=self.use_rag
                )
            except Exception as e:
                logger.error(f"保存处理结果到数据库失败: {e}")

        return {"process_result": response.content, "messages": [human_message, response]}

    
    def _integrate_results(self, state: MyVLState) -> MyVLState:
        """整合检测结果和处理结果，生成最终结论"""
        if not config.llm.decision_agent_enabled:
            return {"final_result": "决策 Agent 已禁用", "messages": []}

        integrate_prompt = f"""【角色设定】
你是薄膜加工领域的首席工艺架构师与故障诊断指挥官 (Chief Process Architect & Diagnosis Commander)。你拥有30年的跨学科经验，精通高分子流变学、精密机械控制理论以及大数据统计学。
你的核心能力是“多模态数据综效诊断”：你不仅能看懂单一的数据，更能将“工艺时序数据（Trend Charts）”与“质量空间分布数据（2D Thickness Maps）”进行时空对齐与因果关联，从而解决单一视角无法解释的复杂难题。
【输入来源】
你将接收两份子报告和一幅时空对齐图：
来源 A (工艺侧)：由“工艺趋势分析专家”提供，包含熔体压力、温度、转速等参数的时序波动分析。
来源 B (质检侧)：由"薄膜质检专家"提供，包含膜厚云图的二维分布特征（MD/CD波动、斜纹、条纹等）。
时空对齐图 (Time-Space Alignment Map)：{state.get('imageProcess_path', '时空对齐图不可用，请基于专业知识进行诊断。')}
【知识库参考】
以下是从知识库中检索到的相关历史案例和标准参数，请参考这些信息进行诊断：
{state.get('knowledge_result', '知识库检索结果不可用，请基于专业知识进行诊断。')}
【核心思维逻辑 - 交叉验证机制】
在给出结论前，请必须执行以下逻辑推演（CoT）：
时空对齐 (Alignment)：
检查来源 A 的异常时间段与来源 B 的异常时间段是否重合？
注意滞后性：工艺波动通常先发生，膜厚异常会滞后秒（取决于传输距离和线速度）。如果滞后十分钟，则相关性极高。
特征映射 (Feature Mapping)：
MD波动验证：如果来源 B 报告了“CD横向色带（Surging）”，来源 A 是否同时报告了“熔体压力/泵速的脉动”？
是→确认为挤出稳定性问题。
否→怀疑是冷辊震动、气刀/风环风量不稳或牵引打滑（冷端问题）。
CD条纹验证：如果来源 B 报告了“MD纵向条纹”，来源 A 的压力通常应由平稳。
例外：如果来源 A 显示压力缓慢漂移，可能是滤网堵塞导致的整体流阻变化。
高频斜纹验证：如果来源 B 报告“斜向纹理”，来源 A 必须找到对应频率的高频震荡（如齿轮泵啮合频率）。
性质区分 (Differentiation)：
热 vs 机械：来源 A 的波动是渐变（温度类，周期长）还是突变（机械电气类，周期短）？这决定了调节方向。
【输出任务】
基于上述分析，请输出一份《最终诊断与执行方案》：
🎯 核心故障定性：用一句话定义问题性质。
🔗 证据链闭环：
工艺证据：[引用来源 A 的关键数据]
质量证据：[引用来源 B 的关键特征]
关联逻辑：[解释两者如何互为因果]
🛠️ 处方级解决方案 (按优先级排序)：
立即执行 (L1)：现场操作员立刻能做的调整。
停机维护 (L2)：需要停机检查的硬件问题。
工艺优化 (L3)：长期参数迭代建议。
=============================================================================================
来源 A (工艺侧)分析结果：
{state['process_result']}

来源 B (质检侧)分析结果：
{state['detect_result']}
"""

        llm = self._create_llm_instance()
        human_message = HumanMessage(content=integrate_prompt)
        response = llm.invoke([human_message])

        thickness_map_uuid = state.get("thickness_map_uuid")
        if thickness_map_uuid:
            try:
                self._save_analysis_result(
                    thickness_map_uuid=thickness_map_uuid,
                    decision_agent_result=response.content,
                    use_rag=self.use_rag
                )
            except Exception as e:
                logger.error(f"保存决策结果到数据库失败: {e}")

        return {"final_result": response.content, "messages": [human_message, response]}

    def _create_workflow(self):
        """创建 LangGraph 工作流"""
        workflow = StateGraph(MyVLState)

        workflow.add_node("analyze_detect_image", self._analyze_detect_image)
        workflow.add_node("analyze_process_image", self._analyze_process_image)
        workflow.add_node("integrate_results", self._integrate_results)

        workflow.add_edge(START, "analyze_detect_image")
        workflow.add_edge("analyze_detect_image", "analyze_process_image")
        workflow.add_edge("analyze_process_image", "integrate_results")
        workflow.add_edge("integrate_results", END)

        return workflow.compile()

    def run_workflow(self, imageDetect_path: str, imageProcess_path: str, thickness_map_uuid: str = None, stream: bool = False):
        """运行 LangGraph 工作流，分析两张图片并生成整合结果"""
        initial_state = {
            "imageDetect_path": imageDetect_path,
            "imageProcess_path": imageProcess_path,
            "detect_result": "",
            "process_result": "",
            "knowledge_result": "",
            "final_result": "",
            "messages": [],
            "thickness_map_uuid": thickness_map_uuid
        }

        from backend.websocket.thickness_map_ws import thickness_map_ws_manager
        thickness_map_ws_manager.start_diagnosis()

        try:
            if stream:
                return self._run_workflow_with_langgraph_streaming(initial_state)
            else:
                return self.workflow.invoke(initial_state)
        finally:
            thickness_map_ws_manager.end_diagnosis()

    def _run_workflow_with_langgraph_streaming(self, initial_state: dict):
        """使用 LangGraph 原生流式功能运行工作流"""
        current_node = None
        final_state = {}

        self._thread_local.should_terminate = False

        for mode, payload in self.workflow.stream(initial_state, stream_mode=["messages", "updates"]):
            if hasattr(self._thread_local, 'should_terminate') and self._thread_local.should_terminate:
                yield {"event": "workflow_terminated", "message": "工作流已被强制终止"}
                self._thread_local.should_terminate = False
                break

            if mode == "messages":
                chunk, metadata = payload
                node_name = metadata.get("langgraph_node", "")

                if node_name and node_name != current_node:
                    current_node = node_name
                    yield {"event": "node_start", "node": node_name, "message": f"开始执行: {node_name}"}

                content = chunk.content
                # LangChain may return content as a list of blocks (e.g. [{"type":"text","text":"..."}])
                # Flatten it to a plain string for consistent downstream handling
                if isinstance(content, list):
                    parts = []
                    for block in content:
                        if isinstance(block, dict) and "text" in block:
                            parts.append(block["text"])
                        elif isinstance(block, str):
                            parts.append(block)
                    content = "".join(parts)
                if content:
                    yield {"event": "stream_content", "node": node_name, "content": content}
            elif mode == "updates":
                for node_name, state_update in payload.items():
                    final_state.update(state_update)
                    yield {"event": "node_complete", "node": node_name, "message": "完成"}
                    current_node = None

        if not (hasattr(self._thread_local, 'should_terminate') and self._thread_local.should_terminate):
            processed_result = {
                "imageDetect_path": final_state.get("imageDetect_path", ""),
                "imageProcess_path": final_state.get("imageProcess_path", ""),
                "detect_result": final_state.get("detect_result", ""),
                "process_result": final_state.get("process_result", ""),
                "final_result": final_state.get("final_result", "")
            }

            yield {"event": "workflow_complete", "message": "完成", "result": processed_result}

        if hasattr(self._thread_local, 'should_terminate'):
            self._thread_local.should_terminate = False
