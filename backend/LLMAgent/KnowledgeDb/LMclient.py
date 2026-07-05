from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from langchain.agents import create_agent
from langchain.tools import tool
from langchain_classic import hub
from .kb import KnowledgeBase
import os

# ── Qwen DashScope API 配置 ──────────────────────────
# API Key 从环境变量读取，请确保 .env 中有 DASHSCOPE_API_KEY
# cp .env.example .env  然后编辑填入你的 Key
# ─────────────────────────────────────────────────────
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")

llm = ChatOpenAI(
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    api_key=DASHSCOPE_API_KEY,
    model="qwen3-vl-plus",
    temperature=0.7,
    max_tokens=4096
)

agent = None
# B. 初始化知识库（可选，如果Qdrant服务未运行则跳过）
kb = None
try:
    kb = KnowledgeBase(collection_name="my_knowledge_base")
    print("知识库初始化成功")
except Exception as e:
    print(f"知识库初始化失败（Qdrant服务未运行）: {e}")
    print("Agent将在无知识库模式下运行")

# ==========================================
# 2. 创建检索工具（使用 KnowledgeBase 的方法）
# ==========================================

@tool
def search_knowledge_base(query: str) -> str:
    """
    搜索知识库中关于 LangChain、Docker、Python、向量数据库或技术文档的相关信息。
    输入应该是一个清晰的问题或关键词。
    """
    if kb is None:
        return "知识库当前不可用（Qdrant服务未运行），将基于专业知识给出建议。"
    
    results = kb.search_knowledge(query, top_k=3)
    
    if not results:
        return "知识库中没有找到相关信息。"
    
    # 将检索结果格式化为字符串
    formatted_results = []
    for i, res in enumerate(results, 1):
        formatted_results.append(f"[文档 {i}] 相似度: {res['score']:.4f}\n内容: {res['content']}")
    
    return "\n\n".join(formatted_results)

tools = [search_knowledge_base]

# ==========================================
# 4. 创建 Agent（修复关键部分）
# ==========================================
try:
    # 拉取 React prompt（需要 langchain-classic）
    prompt = hub.pull("hwchase17/react")
    agent = create_agent(llm, tools, prompt=prompt)
except:
    # 备用方案：使用简单 system prompt
    agent = create_agent(
        llm, 
        tools, 
        system_prompt = f"""### 角色定义
你是一位薄膜加工领域的资深首席工程师（30年经验），精通高分子流变学、精密机械控制及工业大数据分析。
你的特长是进行"多模态数据综效诊断"：将"工艺时序数据（Trend Charts）"与"质量空间分布数据（2D Thickness Maps）"进行时空对齐，解决单一视角无法解释的疑难杂症。

### 核心任务
协助产线进行故障诊断和工艺决策。你的决策必须基于【知识库检索事实】优先，【专家经验】为辅。

### 知识库检索与决策工作流（必须严格遵守）
1. **问题拆解与术语转化**：
   - 用户描述往往是非标准的（如"膜面有横纹"），在调用工具前，先将其转化为专业术语（如"流道震颤"、"熔体破裂"、"周期性厚度波动"）。
   - 构造 2-3 个核心关键词组合进行检索。

2. **强制检索 (RAG)**：
   - **必须优先调用 `search_knowledge_base`**。
   - 重点检索：SOP标准参数范围、历史同类故障案例（Case Study）、设备维护手册。
   - 如果第一次检索无果，尝试放宽关键词再次检索。

3. **证据合成与逻辑推演**：
   - **情况A（知识库有确切答案）**：直接引用文档ID和内容，给出标准解决方案。
   - **情况B（知识库只有相关原理）**：结合检索到的原理，利用你的30年专家经验进行逻辑推演（CoT），构建因果链条。
   - **情况C（完全无记录）**：明确告知"知识库未收录该故障"，然后基于通用流变学理论给出假设性建议，并标注【高风险提示】。

### 输出规范
请按以下结构输出回答（工业级报告风格）：

#### 1. 现象诊断 (Diagnosis)
- 将用户描述转化为专业故障定义。
- 简述可能的物理机制（如：模头背压波动导致挤出不稳）。

#### 2. 知识库证据 (Evidence)
- 引用检索到的文档（如：*根据[文档3]的历史案例...*）。
- 如果没有查到，必须说明：*（注：知识库中未发现完全匹配的历史案例）*。

#### 3. 决策建议 (Action Plan)
请提供分级措施：
- **🔴 立即执行 (L1)**：低成本、快见效的操作（如：调整风环风量、检查真空度）。
- **🟡 停机维护 (L2)**：需要停产进行的硬件检查（如：拆洗模头、更换滤网）。
- **🟢 工艺优化 (L3)**：长期改进建议（如：修改配方熔指、升级温控模块）。

#### 4. 风险警示 (Safety)
- 如果建议涉及调整关键参数（如温度大幅升降），必须给出安全阈值警示。
"""
    )

# ==========================================
# 5. 执行提问（修复输入格式）
# ==========================================
if __name__ == "__main__":
    questions = [
        "帮我看看知识库里关于 Docker 的描述",
        "LangChain 0.3 推荐用什么构建链条？",
        "什么是向量数据库？",
        "Python 有什么特点？"
    ]
    
    print("=" * 60)
    print("Agent RAG 查询系统")
    print("=" * 60)
    
    for i, query in enumerate(questions, 1):
        print(f"\n【问题 {i}】: {query}")
        print("-" * 60)
        
        try:
            # ✅ 正确输入格式
            result = agent.invoke({
                "messages": [HumanMessage(content=query)]
            })
            # 提取最终回答
            final_message = result["messages"][-1]
            print(f"【回答】: {final_message.content}")
        except Exception as e:
            print(f"执行出错: {e}")
        
        print("=" * 60)