from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
import uuid
import os
import logging

from backend.config.config_loader import config as app_config

logger = logging.getLogger(__name__)

class KnowledgeBase:
    def __init__(self, collection_name=None):
        # 从全局配置读取 Qdrant 参数
        qdrant_cfg = app_config.qdrant
        qdrant_url = os.getenv("QDRANT_URL", qdrant_cfg.url)
        _collection_name = collection_name or qdrant_cfg.collection_name
        # 1. 连接 Qdrant
        try:
            # 临时禁用代理，避免影响本地连接
            original_http_proxy = os.environ.pop('http_proxy', None)
            original_https_proxy = os.environ.pop('https_proxy', None)
            original_HTTP_PROXY = os.environ.pop('HTTP_PROXY', None)
            original_HTTPS_PROXY = os.environ.pop('HTTPS_PROXY', None)
            
            try:
                self.client = QdrantClient(
                    url=qdrant_url,
                    timeout=qdrant_cfg.timeout
                )
                self.collection_name = _collection_name
            finally:
                # 恢复代理设置
                if original_http_proxy:
                    os.environ['http_proxy'] = original_http_proxy
                if original_https_proxy:
                    os.environ['https_proxy'] = original_https_proxy
                if original_HTTP_PROXY:
                    os.environ['HTTP_PROXY'] = original_HTTP_PROXY
                if original_HTTPS_PROXY:
                    os.environ['HTTPS_PROXY'] = original_HTTPS_PROXY
                    
        except Exception as e:
            logger.error(f"Qdrant连接失败: {e}")
            raise
        
        # 2. 加载 Embedding 模型（本地模型，也可以换成 OpenAI）
        # 该模型会将文本转换为 384 维向量
        self.encoder = SentenceTransformer('all-MiniLM-L6-v2') 
        self.vector_size = 384 

        # 3. 初始化集合（如果不存在则创建）
        self._setup_collection()

    def _setup_collection(self):
        collections = self.client.get_collections().collections
        exists = any(c.name == self.collection_name for c in collections)
        
        if not exists:
            self.client.recreate_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=self.vector_size, 
                    distance=models.Distance.COSINE # 使用余弦相似度
                ),
            )
            logger.info(f"集合 {self.collection_name} 创建成功")

    # 【增】添加知识
    def add_knowledge(self, text_list):
        points = []
        for text in text_list:
            vector = self.encoder.encode(text).tolist()
            point_id = str(uuid.uuid4()) # 生成随机ID
            points.append(models.PointStruct(
                id=point_id,
                vector=vector,
                payload={"content": text} # 原始文本存入 payload
            ))
        
        self.client.upsert(
            collection_name=self.collection_name,
            points=points
        )
        logger.info(f"成功添加 {len(text_list)} 条知识")

    # 【查】检索知识（RAG核心步骤）
    def search_knowledge(self, query, top_k=3):
        query_vector = self.encoder.encode(query).tolist()
        
        search_result = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=top_k
        )
        
        results = []
        for res in search_result.points:
            results.append({
                "score": res.score,
                "content": res.payload["content"],
                "id": res.id
            })
        return results

    # 【删】根据ID删除
    def delete_knowledge(self, point_id):
        self.client.delete(
            collection_name=self.collection_name,
            points_selector=models.PointIdsList(
                points=[point_id]
            )
        )
        logger.info(f"ID 为 {point_id} 的知识已删除")

    # 【清空】清空知识库所有内容
    def clear_knowledge(self):
        try:
            # 获取当前集合中的点数量
            collection_info = self.client.get_collection(self.collection_name)
            point_count = collection_info.points_count
            
            if point_count == 0:
                logger.info(f"知识库 '{self.collection_name}' 已经是空的")
                return
            
            # 删除集合中的所有点
            self.client.delete(
                collection_name=self.collection_name,
                points_selector=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="content",
                            match=models.MatchValue(value="")  # 空匹配，删除所有
                        )
                    ]
                ),
                wait=True
            )
            
            logger.info(f"成功清空知识库 '{self.collection_name}'，删除了 {point_count} 条知识")
            
        except Exception as e:
            logger.error(f"清空知识库时出错: {e}")

    # 【改】更新知识内容
    def update_knowledge(self, point_id, new_text):
        # 更新实际上是相同 ID 的重新写入 (Upsert)
        new_vector = self.encoder.encode(new_text).tolist()
        self.client.upsert(
            collection_name=self.collection_name,
            points=[
                models.PointStruct(
                    id=point_id,
                    vector=new_vector,
                    payload={"content": new_text}
                )
            ]
        )
        logger.info(f"ID 为 {point_id} 的知识已更新")

# --- 使用示例 ---
if __name__ == "__main__":
    kb = KnowledgeBase()

    # 4. 清空知识库
    logger.info("\n清空知识库...")
    kb.clear_knowledge()

    # 1. 增
    kb.add_knowledge([
        "Docker 是一个开源的应用容器引擎",
        "Python 是一种广泛使用的解释型编程语言",
        "向量数据库是 RAG 系统中的重要组成部分",
        "容器可以运行在任何操作系统上"
    ])

    knowledge_text = """
   基于您提供的**多维时序图与膜厚云图（Heatmap）的联合分析**，结合您描述的现象（模头压力下降、计量泵入口压力异常、膜厚纵向变薄），可以给出非常明确的诊断。

### 🚨 诊断结论：熔体“供料不足”导致计量泵（GP）低压联锁降速

**核心故障：** **前段挤出机供料量小于计量泵排量（Starvation）**，导致计量泵入口建立不起压力，最终触发了系统的**“低压保护机制”**，迫使计量泵瞬间降速，从而导致后端断流、膜厚骤减。

---

### 🕵️‍♂️ 详细证据链分析（按时间线推演）

请跟随我看图，重点关注 **07:54:08 到 08:20:35** 这一关键时间窗：

#### 1. 故障的前兆（潜伏期）
*   **证据（粉色线 - GP Pump Inlet Pressure）：**
    *   请仔细观察粉色曲线在故障发生前（左侧长达数小时）的趋势。它并不是您描述的“持续上升”，而是呈现**缓慢的、持续的下降趋势**（从约 2.5 MPa 缓慢滑落至 2.2 MPa 左右）。
    *   **物理含义：** 这意味着挤出机的挤出量略微**小于**计量泵的输送量。计量泵前的熔体“库存”正在被一点点抽干。

#### 2. 故障的爆发（触发点）
*   **证据（黑色线 - GP Pump Speed）：**
    *   在时间轴约 08:00 附近，黑色曲线出现了一个**剧烈的向下深V型跌落**。
    *   **物理含义：** 当粉色线（入口压力）跌破设定的“最低安全阈值”时，PLC控制程序为了防止计量泵**“抽空”**（干磨损坏）或为了重建入口压力，强制指令计量泵**瞬间大幅降低转速**。

#### 3. 连锁反应（您看到的现象）
*   **现象A：模头压力一直下降（橙色线 - Die Pressure）：**
    *   **原因：** 计量泵转速（黑色）都降了，泵送出去的流量瞬间减少，模头内的压力自然维持不住，随之断崖式下跌。
*   **现象B：计量泵出口压力瞬时下降（黄色线 - GP Outlet）：**
    *   **原因：** 同理，泵转慢了，出口建立的背压也就没了。
*   **现象C：膜厚出现纵向偏薄（云图 - 蓝色区域）：**
    *   **原因：** 对应的时间段（右侧），由于流出的塑料量大幅减少，而产线牵引速度（拉伸速度）没变，膜自然就被拉得极薄（蓝色代表厚度极低）。

#### 4. 迷惑项解释（为什么入口压力后来上升了？）
*   **您的疑问：** “入口压力持续上升”
*   **真相（粉色线后半段）：** 注意看，粉色线的**瞬间飙升**是发生在黑色线（泵速）降低**之后**的。
    *   **原因：** 挤出机还在转（还在不断供料），而计量泵突然减速（不往外抽了）。这时候，熔体就会瞬间在泵入口积压，导致入口压力迅速反弹、飙升。**这是故障的结果，而不是故障的原因。**

---

### 💡 为什么会发生？（根因分析）

既然确定是“供料不足”，那么源头通常在**挤出机段**：

1.  **挤出机“打滑”：** 原料配方中润滑剂过多，或者喂料段温度过高，导致物料在螺杆处打滑，实际输送效率下降。
2.  **堆积密度变化：** 如果是回收料或混合料，原料的堆积密度突然变小，同样的螺杆转速下，实际吃进去的重量减少了。
3.  **PID控制失配：** 挤出机转速与计量泵转速的**闭环控制（Pressure Closed Loop）**参数调节不当。当入口压力开始缓慢下降时，挤出机本应该自动加速来补充压力，但它反应太慢或没有反应。
4.  **下料架桥：** 料斗处可能有轻微的架桥现象，导致喂料不连续（虽然绿色Feed Rate线看起来还算稳，但不排除瞬间断料）。

---

### 🛠️ 解决方案（处理建议）

#### ✅ 紧急处理（L1 - 现场操作）
1.  **手动提升挤出机转速：** 立即增加挤出机主螺杆转速（约 2-5%），以匹配计量泵的排量，重建入口压力。
2.  **或 降低整线速度：** 如果挤出机已满负荷，则必须同步降低计量泵转速和牵引速度，维持供需平衡。

#### ⚙️ 长期优化（L2 - 工艺/设备）
1.  **启用/优化压力闭环控制：**
    *   检查 PLC 设置，确保 **"GP入口压力-挤出机转速"** 的闭环控制已开启。
    *   **参数整定：** 现在的现象是压力“慢漂”导致抽空，说明**积分作用（I值）太弱**。建议适当**增大积分增益**，让系统对长期的压力偏差更敏感，在压力跌破阈值前就自动提升挤出机转速。
2.  **检查原料与喂料：** 确认下料口是否有架桥风险，检查螺杆喂料段温度是否过高（导致架桥或打滑）。
3.  **检查过滤网：** 虽然浅蓝色线（过滤器入口）在故障前是稳的，但故障后跌落，说明流速变了。定期换网可以排除流阻干扰。

**总结：** 您的产线出现了典型的**“挤出-泵送失衡”**。请重点检查**挤出机转速为何没有自动跟踪上计量泵的需求**。
    """

    kb.add_knowledge([knowledge_text])

    # 2. 查
    logger.info("\n搜索：什么是容器？")
    res = kb.search_knowledge("LangChain 0.3 推荐用什么构建链条？")
    for r in res:
        logger.info(f"相似度: {r['score']:.4f} | 内容: {r['content']}")

    # 3. 改（假设我们要修改第一条，先拿到它的 ID）
    target_id = res[0]['id']
    # kb.update_knowledge(target_id, "Docker 可以让开发者打包应用及其依赖包到一个可移植的容器中")



    # 5. 删
    # kb.delete_knowledge(target_id)
