import os
import base64
import sys
from typing import Annotated, TypedDict, Optional, Iterator
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, BaseMessage
from langchain_core.outputs import ChatGenerationChunk
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
import uuid

# 添加LLMAgent目录到Python路径，以便使用绝对导入
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from backend.logic.models.db_connection import DatabaseConnection
from backend.logic.services.image_analysis_service import ImageAnalysisService

# RAG knowledge base search (optional - gracefully degraded if Qdrant is unavailable)
try:
    from backend.LLMAgent.KnowledgeDb.kb import KnowledgeBase
    _kb_instance = None
    def search_knowledge_base(query: str, top_k: int = 3) -> str:
        global _kb_instance
        if _kb_instance is None:
            _kb_instance = KnowledgeBase(collection_name="my_knowledge_base")
        results = _kb_instance.search_knowledge(query, top_k=top_k)
        return "\n\n".join([r["content"] for r in results])
except Exception:
    def search_knowledge_base(query: str, top_k: int = 3) -> str:
        return "知识库检索不可用"
# 加载环境变量 - 优先使用项目根目录 .env，找不到再使用 LLMAgent 目录下的 .env
project_env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), ".env")
agent_env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(project_env_path)
load_dotenv(agent_env_path)

# ------------------------------------------------------------------
# QwenVLAgent 类封装
# ------------------------------------------------------------------
# ------------------------------------------------------------------
# 1. 定义 LangGraph 状态类型
# ------------------------------------------------------------------
class QwenVLState(TypedDict):
    """
    包含两个图片、检测结果、处理结果和整合结果的状态类型
    """
    # 检测图片路径
    imageDetect_path: str
    # 处理图片路径
    imageProcess_path: str
    # 检测图片分析结果
    detect_result: str
    # 处理图片分析结果
    process_result: str
    # 知识库检索结果
    knowledge_result: str
    # 最终整合结果
    final_result: str
    # 消息历史（用于追踪对话）
    messages: Annotated[list[BaseMessage], add_messages]
    # 当前处理的膜厚温度云图UUID
    thickness_map_uuid: str

class QwenVLAgent:
    """
    Qwen-VL 模型代理类，支持流式输出和结果返回
    """
    
    def __init__(self, 
                 model_name: str = "qwen3-vl-plus",
                 base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
                 temperature: float = 1,
                 max_tokens: int = 5000,
                 db_connection: Optional[DatabaseConnection] = None):
        """
        初始化 QwenVLAgent 实例
        
        Args:
            model_name: 模型名称，默认为 "qwen3-vl-plus"
            base_url: API 基础 URL
            temperature: 生成温度，控制输出随机性
            max_tokens: 最大生成 tokens 数
            db_connection: 数据库连接实例，可选
        """
        self.model_name = model_name
        self.base_url = base_url
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.api_key = os.getenv("DASHSCOPE_API_KEY")
        self._last_full_response = ""  # 存储上次调用的完整结果
        
        # 用于跟踪当前运行的工作流的终止标志
        # 使用线程本地存储，确保每个线程（工作流）都有独立的终止标志
        import threading
        self._thread_local = threading.local()
        
        if not self.api_key:
            raise ValueError("DASHSCOPE_API_KEY 环境变量未设置")
        
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
        """
        终止当前运行的工作流
        """
        # 设置当前线程的终止标志为 True
        if hasattr(self._thread_local, 'should_terminate'):
            self._thread_local.should_terminate = True
    
    def _encode_image(self, image_path: str) -> str:
        """
        读取图片文件并转换为 Base64 字符串
        
        Args:
            image_path: 图片文件路径
            
        Returns:
            Base64 编码的图片字符串
        """
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"图片文件不存在: {image_path}")
        
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    
    def _create_llm_instance(self) -> ChatOpenAI:
        """
        创建 ChatOpenAI 实例
        
        Returns:
            ChatOpenAI 实例
        """
        return ChatOpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
            model=self.model_name,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
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
        """
        保存或更新分析结果到数据库
        
        参数:
            thickness_map_uuid: 膜厚温度云图UUID
            detection_agent_result: 检测agent的回复内容（可选）
            processing_agent_result: 加工agent的回复内容（可选）
            decision_agent_result: 决策agent的回复内容（可选）
            use_rag: 是否使用RAG知识库
        """
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
                now = datetime.now(pytz.timezone('Asia/Shanghai'))
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
            print(f"保存分析结果到数据库失败: {e}")
    
    def stream_invoke(self, image_path: str, question: str) -> Iterator[str]:
        """
        流式调用 Qwen-VL 模型，实时返回结果
        
        Args:
            image_path: 图片文件路径
            question: 提问内容
            
        Yields:
            实时的模型输出内容
        
        注意：调用完成后，可以通过 get_last_response() 方法获取完整结果
        """
        # 初始化模型
        llm = self._create_llm_instance()
        
        # 重置上次响应
        self._last_full_response = ""
        
        # 准备消息内容
        message_content = []
        
        # 添加图片
        if image_path:
            base64_image = self._encode_image(image_path)
            message_content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
            })
        
        # 添加提问
        message_content.append({
            "type": "text",
            "text": question
        })
        
        # 创建人类消息
        human_message = HumanMessage(content=message_content)
        
        # 流式调用模型
        for chunk in llm.stream([human_message]):
            chunk_content = chunk.content
            if chunk_content:
                yield chunk_content
                self._last_full_response += chunk_content
    
    def get_last_response(self) -> str:
        """
        获取上次流式调用的完整结果
        
        Returns:
            上次调用的完整结果字符串
        """
        return self._last_full_response
    
    def _generate_base_answer(self, state: QwenVLState) -> QwenVLState:
        """
        生成基础回答的节点
        
        Args:
            state: 当前状态
            
        Returns:
            更新后的状态，包含基础回答
        """
        # 初始化模型
        llm = self._create_llm_instance()
        
        # 准备消息内容
        message_content = []
        
        # 添加图片
        if state["image_path"]:
            base64_image = self._encode_image(state["image_path"])
            message_content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
            })
        
        # 添加提问
        message_content.append({
            "type": "text",
            "text": state["question"]
        })
        
        # 创建人类消息
        human_message = HumanMessage(content=message_content)
        
        # 流式调用模型
        base_answer = ""
        for chunk in llm.stream([human_message]):
            if chunk.content:
                base_answer += chunk.content
                # 这里可以添加日志或其他处理，但不yield状态
        
        # 节点结束后return最终状态
        return {
            "answer1": base_answer,
            "messages": [HumanMessage(content=base_answer)]
        }
    
    def _enhance_answer(self, state: QwenVLState) -> QwenVLState:
        """
        在基础回答上生成增强回答的节点
        
        Args:
            state: 当前状态
            
        Returns:
            更新后的状态，包含增强回答
        """
        # 初始化模型
        llm = self._create_llm_instance()
        
        # 准备增强提问
        enhance_prompt = f"""
        基于以下图片描述和基础回答，生成一个更加详细、深入和全面的增强回答：
        
        基础回答：
        {state['answer1']}
        
        增强要求：
        1. 提供更多细节和分析
        2. 深入解释相关概念
        3. 添加实用建议或应用场景
        4. 保持回答的逻辑连贯性
        """
        
        # 创建人类消息
        human_message = HumanMessage(content=enhance_prompt)
        
        # 流式调用模型
        enhance_answer = ""
        for chunk in llm.stream([human_message]):
            if chunk.content:
                enhance_answer += chunk.content
                # 这里可以添加日志或其他处理，但不yield状态
        
        # 节点结束后return最终状态
        return {
            "answer2": enhance_answer,
            "messages": [HumanMessage(content=enhance_answer)]
        }
    
    def _analyze_detect_image(self, state: QwenVLState) -> QwenVLState:
        """
        分析检测图片，生成检测结果
        
        Args:
            state: 当前状态
            
        Returns:
            更新后的状态，包含检测结果
        """
        # 初始化模型
        llm = self._create_llm_instance()
        
        # 准备消息内容
        message_content = []
        
        # 添加检测图片
        if state["imageDetect_path"]:
            base64_image = self._encode_image(state["imageDetect_path"])
            message_content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
            })
        
        # 添加提问，使用薄膜质检专家提示词
        message_content.append({
            "type": "text",
            "text": "【角色设定】\n 你是一位拥有20年经验的薄膜质检专家（QC Expert）与流变数据分析师。你擅长通过分析测厚仪生成的二维云图（2D Thickness Heatmap/Contour Plot）来诊断挤出流延或双拉产线的工艺缺陷。\n 【图像定义 - 关键信息】\n 我将上传一张薄膜厚度的二维分布图，请严格按照以下物理定义进行视觉解码：\n X轴（横向）：代表 CD方向 (Cross Direction)，即模头/螺栓的空间位置。\n Y轴（纵向）：代表 MD方向 (Machine Direction)，也就是时间轴（Time），越往上/下（请根据图注判断）代表时间越晚。\n 颜色（Z轴）：代表膜厚偏差。通常红色/暖色代表偏厚，蓝色/冷色代表偏薄（请先根据图例确认这一点）。\n 采样逻辑警告：该数据是由单点扫描式测厚仪（Scanning Gauge）生成的。探头在CD往复运动时，薄膜在MD高速移动。因此，图中的每一个像素点在时空上并非绝对连续，存在\"扫描周期与产线速度的耦合\"。\n 【分析任务 - 请按步骤推理】\n 第一步：视觉校准与全局评估\n 首先读取图例（Scale Bar），告诉我当前显示的厚度波动范围（Min/Max）是多少？\n 观察整体色块分布，是呈现\"杂乱无章的斑点\"还是有明显的\"条纹特征\"？\n 第二步：特征分离与异常定位（核心任务）\n 请区分以下三种特定的工艺缺陷模式，并告诉我图中是否存在：\n MD纵向条纹 (Die Lines)：在X轴固定位置，沿Y轴延伸的垂直线条（颜色持续偏深或偏浅）。\n 物理含义：对应模头模唇的脏污、损伤或特定螺栓调节失灵。\n CD横向波动 (MD Surging)：沿X轴横贯整个幅宽的颜色带（如：一整条红带紧接着一整条蓝带）。\n 物理含义：挤出机/计量泵的供料脉动，或冷辊/牵引速度的不稳定。\n 斜向纹理 (Diagonal Patterns)：\n 物理含义：这是扫描耦合效应的典型特征。这通常意味着产线存在一个高频的MD波动，其频率与探头的扫描周期形成了\"拍频（Beat Frequency）\"。请指出是否有这种斜纹？\n 第三步：异常时段锁定\n 请根据Y轴的时间刻度，找出颜色反差最剧烈、或纹理突然改变的时间段。\n 例如：\"在Y轴约1/3处（对应时间XX:XX），出现了一次明显的横向变薄。\"\n 【输出结果】\n 请输出一份简报：\n 📊 波动概况：极差范围与主要波动模式。\n 🕒 异常时间表：列出发生显著波动的具体时间段及其表现（变厚/变薄/条纹化）。\n 🔧 归因诊断：基于发现的模式（横向/纵向/斜向），推测是模头问题（空间域）还是挤出/牵引问题（时间域）。"
        })
        
        # 创建人类消息
        human_message = HumanMessage(content=message_content)
        
        # 调用模型
        response = llm.invoke([human_message])
        
        # 保存检测结果到数据库
        thickness_map_uuid = state.get("thickness_map_uuid")
        if thickness_map_uuid:
            try:
                self._save_analysis_result(
                    thickness_map_uuid=thickness_map_uuid,
                    detection_agent_result=response.content,
                    use_rag=False
                )
            except Exception as e:
                print(f"保存检测结果到数据库失败: {e}")
        
        # 返回更新后的状态
        return {
            "detect_result": response.content,
            "messages": [human_message, response]
        }
    
    def _analyze_process_image(self, state: QwenVLState) -> QwenVLState:
        """
        分析处理图片，生成处理结果
        
        Args:
            state: 当前状态
            
        Returns:
            更新后的状态，包含处理结果
        """
        # 初始化模型
        llm = self._create_llm_instance()
        
        # 准备消息内容
        message_content = []
        
        # 添加处理图片
        if state["imageProcess_path"]:
            base64_image = self._encode_image(state["imageProcess_path"])
            message_content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
            })
        
# 添加提问，使用针对“工艺-质量”对齐分析的高级提示词
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
3.  **锁定突变点**：扫描整个时间轴，指出是否存在一个明显的“系统性崩溃”或“突变时刻”？（提示：请关注图表右侧 07:54 之后的区域）。

**第二步：工艺侧根因定位 (Process Root Cause)**
在突变发生时，哪些工艺参数率先出现了异常？
- 观察 **GP Pump Speed (熔体泵转速)** 和 **Die Pressure (模头压力)**。
- 它们是瞬间跌落、缓慢漂移还是剧烈震荡？
- **逻辑判断**：是压力的变化导致了转速波动，还是转速的指令突变导致了压力失压？

**第三步：质量侧后果验证 (Quality Impact)**
视线下移到底部的热力图 (Heatmap)：
- 在上述工艺突变的时间点，热力图的颜色发生了什么变化？（例如：从均匀的绿色变成了蓝/红色？）
- 这种颜色变化代表了膜厚发生了什么改变？（通常蓝色代表变薄/低温，红色代表变厚/高温）。
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
| [例如: 07:54:xx] | [例如: GP Pump Speed] | [例如: 瞬间跌落至X rpm] | [例如: 出现大面积蓝色低厚度条带] |

### 🕵️‍♂️ 3. 诊断结论 (Diagnosis & Hypothesis)
- **直接原因**：[基于数据推断，例如：熔体泵因某种原因突然降速/跳停，导致模头压力瞬间失压]
- **质量后果**：[例如：挤出量不足导致薄膜瞬间变薄（热力图变蓝）]
- **排查建议**：
    1. [针对性建议，如：检查熔体泵变频器报警记录...]
    2. [针对性建议，如：检查进料是否存在架桥导致供料中断...]

### 💡 专家洞察
用一句话评价当前生产过程的稳定性（例如：“前3小时运行平稳，但在07:54出现灾难性停机/故障，需立即干预。”）。
"""
        })
        
        # 创建人类消息
        human_message = HumanMessage(content=message_content)
        
        # 调用模型
        response = llm.invoke([human_message])
        
        # 保存处理结果到数据库
        thickness_map_uuid = state.get("thickness_map_uuid")
        if thickness_map_uuid:
            try:
                self._save_analysis_result(
                    thickness_map_uuid=thickness_map_uuid,
                    processing_agent_result=response.content,
                    use_rag=False
                )
            except Exception as e:
                print(f"保存处理结果到数据库失败: {e}")
        
        # 返回更新后的状态
        return {
            "process_result": response.content,
            "messages": [human_message, response]
        }
    
    def _search_knowledge_base(self, state: QwenVLState) -> QwenVLState:
        """
        搜索知识库，获取相关的历史案例和标准参数
        
        Args:
            state: 当前状态
            
        Returns:
            更新后的状态，包含知识库检索结果
        """
        # 基于工艺和质检结果构造检索查询
        query = f"""
        薄膜加工工艺故障诊断：
        工艺侧分析：{state['process_result'][:500]}
        质检侧分析：{state['detect_result'][:500]}
        
        请搜索相关的故障案例、标准参数范围和解决方案。
        """
        
        # 调用知识库检索工具
        knowledge_result = search_knowledge_base.invoke(query)
        
        # 返回更新后的状态
        return {
            "knowledge_result": knowledge_result,
            "messages": [HumanMessage(content=f"知识库检索结果：{knowledge_result}")]
        }
    
    def _integrate_results(self, state: QwenVLState) -> QwenVLState:
        """
        整合检测结果和处理结果，生成最终结论
        
        Args:
            state: 当前状态
            
        Returns:
            更新后的状态，包含最终结论
        """
        
        # 准备整合提示文本
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
        🎯 核心故障定性：用一句话定义问题性质（例如：“因加料段固体输送不稳导致的周期性挤出喘振”）。
        🔗 证据链闭环：
        工艺证据：[引用来源 A 的关键数据]
        质量证据：[引用来源 B 的关键特征]
        关联逻辑：[解释两者如何互为因果]
        🛠️ 处方级解决方案 (按优先级排序)：
        立即执行 (L1)：现场操作员立刻能做的调整（如：调整某区温度±5℃，清洁模唇，更换滤网，计量泵转速增大或减小）。
        停机维护 (L2)：需要停机检查的硬件问题（如：检查真空泵密封，电机转速和电流，更换模头）。
        工艺优化 (L3)：长期参数迭代建议（如：优化PID参数 D项，调整配方MFI）。
        =============================================================================================
        来源 A (工艺侧)分析结果：
        {state['process_result']}

        来源 B (质检侧)分析结果：
        {state['detect_result']}
        """
        
        # 初始化模型
        llm = self._create_llm_instance()
        
        # 创建人类消息
        human_message = HumanMessage(content=integrate_prompt)
        
        # 调用模型生成最终整合结果
        response = llm.invoke([human_message])
        
        # 保存决策结果到数据库
        thickness_map_uuid = state.get("thickness_map_uuid")
        if thickness_map_uuid:
            try:
                self._save_analysis_result(
                    thickness_map_uuid=thickness_map_uuid,
                    decision_agent_result=response.content,
                    use_rag=False
                )
            except Exception as e:
                print(f"保存决策结果到数据库失败: {e}")
        
        # 返回更新后的状态
        return {
            "final_result": response.content,
            "messages": [human_message, response]
        }
    
    def _create_workflow(self):
        """
        创建 LangGraph 工作流
        
        Returns:
            编译后的 LangGraph 工作流
        """
        # 创建状态图
        workflow = StateGraph(QwenVLState)
        
        # 添加节点
        workflow.add_node("analyze_detect_image", self._analyze_detect_image)
        workflow.add_node("analyze_process_image", self._analyze_process_image)
        workflow.add_node("search_knowledge_base", self._search_knowledge_base)
        workflow.add_node("integrate_results", self._integrate_results)
        
        # 定义边：开始 -> 分析检测图片 -> 分析处理图片 -> 检索知识库 -> 整合结果 -> 结束
        workflow.add_edge(START, "analyze_detect_image")
        workflow.add_edge("analyze_detect_image", "analyze_process_image")
        workflow.add_edge("analyze_process_image", "search_knowledge_base")
        workflow.add_edge("search_knowledge_base", "integrate_results")
        workflow.add_edge("integrate_results", END)
        
        # 编译工作流
        return workflow.compile()
    

    
    def run_workflow(self, imageDetect_path: str, imageProcess_path: str, thickness_map_uuid: str = None, stream: bool = False):
        """
        运行 LangGraph 工作流，分析两张图片并生成整合结果
        
        Args:
            imageDetect_path: 检测图片文件路径
            imageProcess_path: 处理图片文件路径
            thickness_map_uuid: 膜厚温度云图UUID
            stream: 是否启用流式输出
            
        Returns:
            如果 stream 为 False，返回包含检测结果、处理结果和整合结果的完整状态
            如果 stream 为 True，返回 LangGraph 流式迭代器，实时输出执行过程
        """
        if thickness_map_uuid is None:
            thickness_map_uuid = "default-uuid"
        
        # 初始化状态
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
        
        # 开始诊断，停止WebSocket消息推送
        from backend.websocket.thickness_map_ws import thickness_map_ws_manager
        thickness_map_ws_manager.start_diagnosis()
        
        try:
            if stream:
                # 使用 LangGraph 原生流式输出，设置 stream_mode 为 ["messages", "updates"]
                return self._run_workflow_with_langgraph_streaming(initial_state)
            else:
                # 普通运行工作流
                return self.workflow.invoke(initial_state)
        finally:
            # 结束诊断，恢复WebSocket消息推送
            thickness_map_ws_manager.end_diagnosis()
    
    def _run_workflow_with_langgraph_streaming(self, initial_state: dict):
        """
        使用 LangGraph 原生流式功能运行工作流，实时输出执行过程
        
        Args:
            initial_state: 初始状态
            
        Yields:
            执行过程中的实时信息和最终结果
        """
        current_node = None
        final_state = {}
        
        # 初始化当前线程的终止标志
        self._thread_local.should_terminate = False
        
        # 使用 stream_mode=["messages", "updates"] 是实现 invoke 流式输出的核心
        for mode, payload in self.workflow.stream(initial_state, stream_mode=["messages", "updates"]):
            # 检查是否需要终止工作流
            if hasattr(self._thread_local, 'should_terminate') and self._thread_local.should_terminate:
                yield {
                    "event": "workflow_terminated",
                    "message": "工作流已被强制终止"
                }
                # 重置终止标志，以便下次运行
                self._thread_local.should_terminate = False
                break
            
            if mode == "messages":
                # 实时流 (对应 invoke 内部的 token)
                chunk, metadata = payload
                # metadata 包含当前是哪个节点在运行
                node_name = metadata.get("langgraph_node", "")
                
                # 发送节点开始事件
                if node_name and node_name != current_node:
                    current_node = node_name
                    yield {
                        "event": "node_start",
                        "node": node_name,
                        "message": f"开始执行: {node_name}"
                    }
                
                # 发送内容 (Token) - 处理 LangChain content 可能是 list 的情况
                content = chunk.content
                if isinstance(content, list):
                    parts = []
                    for block in content:
                        if isinstance(block, dict) and "text" in block:
                            parts.append(block["text"])
                        elif isinstance(block, str):
                            parts.append(block)
                    content = "".join(parts)
                if content:
                    yield {
                        "event": "stream_content",
                        "node": node_name,
                        "content": content
                    }
            elif mode == "updates":
                # 状态更新 (对应 return 语句执行后)
                # 节点完全执行完毕
                for node_name, state_update in payload.items():
                    final_state.update(state_update)
                    yield {
                        "event": "node_complete",
                        "node": node_name,
                        "message": "完成"
                    }
                    current_node = None  # 重置，等待下一个节点
        
        # 工作流执行完成或被终止
        if not (hasattr(self._thread_local, 'should_terminate') and self._thread_local.should_terminate):
            # 处理final_state，只保留必要字段
            processed_result = {
                "imageDetect_path": final_state.get("imageDetect_path", ""),
                "imageProcess_path": final_state.get("imageProcess_path", ""),
                "detect_result": final_state.get("detect_result", ""),
                "process_result": final_state.get("process_result", ""),
                "final_result": final_state.get("final_result", "")
            }
            
            yield {
                "event": "workflow_complete",
                "message": "完成",
                "result": processed_result
            }
        
        # 无论工作流如何结束，都重置终止标志
        if hasattr(self._thread_local, 'should_terminate'):
            self._thread_local.should_terminate = False

    def invoke(self, image_path: str, question: str) -> str:
        """
        普通调用 Qwen-VL 模型，一次性返回结果
        
        Args:
            image_path: 图片文件路径
            question: 提问内容
            
        Returns:
            模型输出的完整结果
        """
        # 初始化模型
        llm = self._create_llm_instance()
        
        # 准备消息内容
        message_content = []
        
        # 添加图片
        if image_path:
            base64_image = self._encode_image(image_path)
            message_content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
            })
        
        # 添加提问
        message_content.append({
            "type": "text",
            "text": question
        })
        
        # 创建人类消息
        human_message = HumanMessage(content=message_content)
        
        # 调用模型
        response = llm.invoke([human_message])
        
        return response.content

# ------------------------------------------------------------------
# 运行示例
# ------------------------------------------------------------------
if __name__ == "__main__":
    # 创建 QwenVLAgent 实例
    agent = QwenVLAgent()
    
    # 设置图片路径和提问
    image_path = "TimeSyncDiag/backend/LLMAgent/images/tt.png"  # 替换为你的图片路径
    question = "请详细描述这张图片里的内容，并告诉我图片的色调是什么。"
    
    print("正在调用 Qwen-VL 模型...")
    print(f"图片路径: {image_path}")
    print(f"提问内容: {question}")
    print("=" * 60)
    
    # 选择运行模式
    run_mode = "workflow"  # 可选值: "streaming", "normal", "workflow"
    
    try:
        if run_mode == "streaming":
            # 使用流式输出
            print("\n--- 流式输出结果 ---")
            for chunk in agent.stream_invoke(image_path, question):
                print(chunk, end="", flush=True)
            
            # 获取并输出整体结果
            full_result = agent.get_last_response()
            print("\n\n--- 整体结果 ---")
            print(full_result)
        elif run_mode == "normal":
            # 使用普通调用
            result = agent.invoke(image_path, question)
            print("\n--- 普通调用结果 ---")
            print(result)
        elif run_mode == "workflow":
            # 使用 LangGraph 工作流，生成基础回答和增强回答
            print("\n--- 运行 LangGraph 工作流 ---")
            
            # 选择是否使用流式工作流
            workflow_stream = True
            
            if workflow_stream:
                # 使用自定义的工作流流式输出
                print("\n--- 工作流流式输出 ---")
                final_result = None
                
                for event in agent.run_workflow(image_path, question, stream=True):
                    event_type = event.get("event")
                    
                    if event_type == "node_start":
                        # 节点开始执行
                        node_name = event.get("node")
                        message = event.get("message")
                        print(f"\n[{node_name}] {message}")
                        print("=" * 50)
                    elif event_type == "stream_content":
                        # 实时输出节点的流式内容
                        node_name = event.get("node")
                        content = event.get("content")
                        if content:
                            print(content, end="", flush=True)
                    elif event_type == "node_complete":
                        # 节点执行完成
                        node_name = event.get("node")
                        message = event.get("message")
                        print(f"\n\n[{node_name}] {message}")
                    elif event_type == "workflow_complete":
                        # 工作流执行完成
                        message = event.get("message")
                        final_result = event.get("result")
                        print(f"\n{message}")
                        print("=" * 50)
                
                # 输出最终结果
                if final_result:
                    print("\n--- 最终工作流结果 ---")
                    print("基础回答:")
                    print(final_result["answer1"])
                    print("\n增强回答:")
                    print(final_result["answer2"])
            else:
                # 普通运行工作流
                result = agent.run_workflow(image_path, question)
                
                print("\n--- 基础回答 ---")
                print(result["answer1"])
                
                print("\n--- 增强回答 ---")
                print(result["answer2"])
    except Exception as e:
        print(f"\n错误: {e}")
        print("请检查图片路径是否正确，以及环境变量 DASHSCOPE_API_KEY 是否配置正确。")