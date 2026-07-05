import os
from typing import Annotated, TypedDict
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, BaseMessage
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

# 加载环境变量
load_dotenv(".env")

# ------------------------------------------------------------------
# 1. 定义 LangGraph 的状态 (State)
# ------------------------------------------------------------------
class State(TypedDict):
    # add_messages 会自动处理消息的追加历史
    messages: Annotated[list[BaseMessage], add_messages]
    # 用于存储中间结果
    intermediate_result: str

# ------------------------------------------------------------------
# 2. 定义节点逻辑 (调用 Qwen 百炼)
# ------------------------------------------------------------------
def call_qwen_bailian(state: State):
    """
    该节点负责调用 Qwen 百炼模型
    """
    
    # 初始化 ChatOpenAI，指向 Qwen 百炼 API
    llm = ChatOpenAI(
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        api_key=os.getenv("DASHSCOPE_API_KEY"),
        model="qwen-plus",  # Qwen 百炼模型名称
        temperature=0.7,
        max_tokens=1000
    )
    
    # 获取当前的消息列表
    messages = state["messages"]
    
    # 调用模型
    response = llm.invoke(messages)
    
    # 返回新的消息和中间结果
    return {
        "messages": [response],
        "intermediate_result": response.content
    }

# ------------------------------------------------------------------
# 3. 定义另一个节点逻辑 (可以根据需要添加更多节点)
# ------------------------------------------------------------------
def process_result(state: State):
    """
    处理模型返回的结果
    """
    
    # 获取中间结果
    result = state["intermediate_result"]
    
    # 这里可以添加处理逻辑，比如提取关键信息、格式化输出等
    processed_result = f"处理后的结果：\n{result}"
    
    # 返回处理后的结果
    return {
        "messages": [HumanMessage(content=processed_result)]
    }

# ------------------------------------------------------------------
# 4. 构建图 (Graph Construction)
# ------------------------------------------------------------------
workflow = StateGraph(State)

# 添加节点
workflow.add_node("qwen_bailian", call_qwen_bailian)
workflow.add_node("process_result", process_result)

# 定义边：开始 -> Qwen 百炼调用 -> 结果处理 -> 结束
workflow.add_edge(START, "qwen_bailian")
workflow.add_edge("qwen_bailian", "process_result")
workflow.add_edge("process_result", END)

# 编译图
app = workflow.compile()

# ------------------------------------------------------------------
# 5. 运行示例
# ------------------------------------------------------------------
if __name__ == "__main__":
    # 初始消息
    initial_message = HumanMessage(content="请介绍一下 LangGraph 的核心功能和使用场景")
    
    print("正在调用 Qwen 百炼模型...")
    
    # 执行 Graph
    final_state = app.invoke({
        "messages": [initial_message],
        "intermediate_result": ""
    })
    
    # 输出结果
    print("\n--- 最终结果 ---")
    print(final_state["messages"][-1].content)