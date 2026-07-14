"""
自定义时空对齐分析 - API 路由

提供以下接口：
- 模板管理 (CRUD)
- CSV文件上传与自动解析
- 列映射配置
- 时间对齐与图表生成
- 自定义提示词的三模型分析
"""

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging
import base64
import os
import json
import uuid

import pandas as pd
import backend.state as state
from backend.config.config_loader import config as app_config
from backend.logic.models.custom_models import (
    AnalysisTemplate, UploadedDataset, CustomAnalysisResult,
    SimulationRun, SimulationStep
)
from backend.logic.processors.generic_alignment_processor import GenericAlignmentProcessor

logger = logging.getLogger(__name__)

router = APIRouter(tags=["自定义时空对齐分析"])


# ==================== Pydantic 请求/响应模型 ====================

class CreateTemplateRequest(BaseModel):
    """创建模板请求"""
    name: str = Field(..., description="模板名称")
    description: Optional[str] = Field(None, description="模板描述")
    category: Optional[str] = Field(None, description="分类标签")
    config: Dict[str, Any] = Field(default_factory=dict, description="数据集和图表配置")
    prompts_config: Dict[str, Any] = Field(default_factory=dict, description="LLM提示词配置")


class UpdateTemplateRequest(BaseModel):
    """更新模板请求"""
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    prompts_config: Optional[Dict[str, Any]] = None


class UpdateColumnInfoRequest(BaseModel):
    """更新列信息请求"""
    column_info: Dict[str, Any] = Field(..., description="列信息配置")


class GenerateChartRequest(BaseModel):
    """生成图表请求"""
    template_uuid: str = Field(..., description="模板UUID")
    start_time: Optional[datetime] = Field(None, description="开始时间")
    end_time: Optional[datetime] = Field(None, description="结束时间")


class RunAnalysisRequest(BaseModel):
    """运行分析请求"""
    template_uuid: str = Field(..., description="模板UUID")
    start_time: Optional[datetime] = Field(None, description="开始时间")
    end_time: Optional[datetime] = Field(None, description="结束时间")
    stream: bool = Field(True, description="是否流式返回")


class RunSimulationRequest(BaseModel):
    """运行滑动窗口模拟分析请求"""
    template_uuid: str = Field(..., description="模板UUID")
    simulation_config: Optional[Dict[str, Any]] = Field(None, description="模拟配置（为空则使用模板中保存的配置）")
    resume_run_uuid: Optional[str] = Field(None, description="继续作业：指定已有的运行UUID，从上次中断处继续")
    stream: bool = Field(True, description="是否流式返回")


class TemplateResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class TemplateListResponse(BaseModel):
    success: bool
    message: str
    data: Optional[List[Dict[str, Any]]] = None


# ==================== 默认提示词 ====================

DEFAULT_PROMPTS = {
    "detect_agent": {
        "enabled": True,
        "prompt": """【角色设定】
你是一位拥有20年经验的薄膜质检专家（QC Expert）与流变数据分析师。你擅长通过分析测厚仪生成的二维云图（2D Thickness Heatmap/Contour Plot）来诊断挤出流延或双拉产线的工艺缺陷。

【图像定义 - 关键信息】
我将上传一张薄膜厚度的二维分布图，请严格按照以下物理定义进行视觉解码：
X轴（横向）：代表 CD方向 (Cross Direction)，即模头/螺栓的空间位置。
Y轴（纵向）：代表 MD方向 (Machine Direction)，也就是时间轴（Time）。
颜色（Z轴）：代表膜厚偏差。通常红色/暖色代表偏厚，蓝色/冷色代表偏薄。

【分析任务 - 请按步骤推理】
第一步：视觉校准与全局评估
- 读取图例，告诉我当前显示的厚度波动范围（Min/Max）是多少？
- 观察整体色块分布特征。

第二步：特征分离与异常定位
- MD纵向条纹：在X轴固定位置，沿Y轴延伸的垂直线条。
- CD横向波动：沿X轴横贯整个幅宽的颜色带。
- 斜向纹理：扫描耦合效应的典型特征。

第三步：异常时段锁定
根据Y轴的时间刻度，找出颜色反差最剧烈的时间段。

【输出结果】
请输出一份简报：
📊 波动概况：极差范围与主要波动模式。
🕒 异常时间表：列出发生显著波动的具体时间段。
🔧 归因诊断：推测是模头问题（空间域）还是挤出/牵引问题（时间域）。""",
        "model_name": app_config.llm.model_name,
        "temperature": app_config.llm.temperature,
        "max_tokens": app_config.llm.max_tokens
    },
    "process_agent": {
        "enabled": True,
        "prompt": """【角色设定】
你是一位拥有20年经验的薄膜挤出工艺专家（Extrusion Expert），同时精通工业大数据可视化分析。
你现在的任务是进行一项"时空关联故障诊断"。你面前有一张组合图表：
- 上部区域：包含多个关键工艺参数的时间序列趋势图。
- 底部区域：是一张"时空热力图"，展示了薄膜厚度/质量在"时间"与"横向位置"上的分布。

【分析指令】
第一步：全景扫描与参数识别
- 识别上部各小图的Y轴标签。
- 确认X轴的时间跨度。
- 锁定突变点。

第二步：工艺侧根因定位
在突变发生时，哪些工艺参数率先出现了异常？

第三步：质量侧后果验证
视线下移到底部的热力图，在工艺突变的时间点，热力图的颜色发生了什么变化？

第四步：综合诊断
将工艺异常与质量异常串联起来，形成证据链。

【输出格式要求】
### 📊 1. 图表解构
### ⚠️ 2. 核心故障事件
### 🕵️‍♂️ 3. 诊断结论
### 💡 专家洞察""",
        "model_name": app_config.llm.model_name,
        "temperature": app_config.llm.temperature,
        "max_tokens": app_config.llm.max_tokens
    },
    "decision_agent": {
        "enabled": True,
        "prompt": """【角色设定】
你是薄膜加工领域的首席工艺架构师与故障诊断指挥官。你拥有30年的跨学科经验，精通高分子流变学、精密机械控制理论以及大数据统计学。
你的核心能力是"多模态数据综效诊断"：将"工艺时序数据"与"质量空间分布数据"进行时空对齐与因果关联。

【输入来源】
来源 A (工艺侧)：工艺趋势分析专家提供的时序波动分析。
来源 B (质检侧)：薄膜质检专家提供的膜厚云图二维分布特征。

【核心思维逻辑 - 交叉验证机制】
1. 时空对齐：检查来源A的异常时间段与来源B的异常时间段是否重合？注意滞后性。
2. 特征映射：MD波动验证、CD条纹验证、高频斜纹验证。
3. 性质区分：热vs机械，渐变vs突变。

【输出任务】
🎯 核心故障定性
🔗 证据链闭环
🛠️ 处方级解决方案 (L1/L2/L3)""",
        "model_name": app_config.llm.model_name,
        "temperature": app_config.llm.temperature,
        "max_tokens": app_config.llm.max_tokens
    },
    "llm_base_url": app_config.llm.base_url,
    "llm_api_key_env": app_config.llm.api_key_env
}


# 默认模拟配置
# 从全局配置读取默认值
_ca = app_config.custom_analysis
_tm = app_config.thickness_map

DEFAULT_SIMULATION_CONFIG = {
    "alignment_window_seconds": _ca.default_sim_window,      # 对齐时间范围（窗口大小，秒）
    "step_seconds": _ca.default_sim_step,                   # 步长时间（每次向前推进多少秒）
    "step_interval_seconds": _ca.default_sim_step,          # 推进间隔（多久推进一次，秒）
    "vlm_diagnosis_interval_steps": _ca.default_sim_vlm_interval,   # VLM诊断间隔（每N步诊断一次）
    "vlm_diagnosis_interval_seconds": 0,  # VLM诊断间隔（按时间，0表示不按时间）
}

# 默认对齐配置
DEFAULT_ALIGNMENT_CONFIG = {
    "interval_seconds": _ca.default_alignment_interval,
    "time_tolerance_seconds": _ca.default_time_tolerance,
    "interpolation_method": _ca.default_interpolation,
    "resample_enabled": _ca.default_resample_enabled,
}

# 默认图表配置
DEFAULT_CHART_CONFIG = {
    "target_value": _tm.target_thickness,
    "value_range": _tm.thickness_range,
    "figsize": list(_ca.default_chart_figsize),
    "dpi": _ca.default_chart_dpi,
    "colormap": _ca.default_chart_colormap,
    "title": "时空对齐分析图",
    "xlabel": "Time",
    "show_grid": True,
    "show_colorbar": True,
}


# ==================== 共享辅助函数 ====================

def _require_db():
    """检查数据库连接，返回数据库连接对象"""
    if not state.db_connection or not state.db_connection.connected:
        raise HTTPException(status_code=500, detail="数据库未连接")
    return state.db_connection


def _load_and_align_data(
    session,
    template: AnalysisTemplate,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> Tuple[List[pd.DataFrame], pd.DataFrame, datetime, datetime, List[Dict], Dict]:
    """
    加载模板下所有数据集并进行时间对齐（共享逻辑）

    Returns:
        (process_dfs, detection_df, global_start, global_end, process_configs, detection_config)
    """
    db = _require_db()
    config = template.config or {}
    alignment_config = config.get("alignment", DEFAULT_ALIGNMENT_CONFIG)

    processor = GenericAlignmentProcessor(db)

    datasets_info = []
    for ds in template.datasets:
        df = processor.load_dataset_data(
            ds.data_table_name, ds.column_info,
            start_time, end_time
        )
        datasets_info.append({
            "df": df, "role": ds.role,
            "column_info": ds.column_info, "name": ds.name
        })

    process_dfs, detection_df, global_start, global_end = processor.align_data(
        datasets_info, alignment_config
    )

    process_configs = [ds["column_info"] for ds in datasets_info if ds["role"] == "process"]
    detection_config = {}
    for ds_info in datasets_info:
        if ds_info["role"] == "detection":
            detection_config = ds_info["column_info"]
            break

    return process_dfs, detection_df, global_start, global_end, process_configs, detection_config


def _create_llm(agent_config: Dict, llm_base_url: str, api_key: str):
    """创建LLM客户端实例"""
    from langchain_openai import ChatOpenAI
    from backend.config.config_loader import config as app_config
    return ChatOpenAI(
        base_url=llm_base_url,
        api_key=api_key,
        model=agent_config.get("model_name", app_config.llm.model_name),
        temperature=agent_config.get("temperature", app_config.llm.temperature),
        max_tokens=agent_config.get("max_tokens", app_config.llm.max_tokens),
        timeout=app_config.llm.timeout,
        streaming=True
    )


def _encode_image(img_bytes: bytes) -> str:
    """将图片字节编码为base64字符串"""
    return base64.b64encode(img_bytes).decode('utf-8')


def _stream_llm_response(llm, message_content, node_name: str, agent_key: str):
    """流式调用LLM并生成NDJSON事件"""
    from langchain_core.messages import HumanMessage
    result_text = ""

    yield json.dumps({
        "event": "node_start",
        "node": node_name,
        "message": f"开始{node_name}分析..."
    }, ensure_ascii=False) + "\n"

    for chunk in llm.stream([HumanMessage(content=message_content)]):
        if chunk.content:
            content = chunk.content
            if isinstance(content, list):
                content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
            result_text += content
            yield json.dumps({
                "event": "stream_content",
                "node": node_name,
                "content": content
            }, ensure_ascii=False) + "\n"

    yield json.dumps({
        "event": "node_complete",
        "node": node_name,
        "message": f"{node_name}分析完成"
    }, ensure_ascii=False) + "\n"

    return result_text


def _get_llm_config(prompts_config: Dict) -> Tuple[str, str]:
    """从prompts_config中获取LLM配置，返回(base_url, api_key)
    优先使用prompts_config中的配置，若为空则回退到全局config.yml配置
    """
    # 优先从 prompts_config 获取，为空则回退到全局配置
    llm_base_url = prompts_config.get("llm_base_url") or app_config.llm.base_url
    llm_api_key_env = prompts_config.get("llm_api_key_env") or app_config.llm.api_key_env
    api_key = os.getenv(llm_api_key_env) if llm_api_key_env else None
    return llm_base_url, api_key


# ==================== 模板管理 API ====================

@router.get("/templates", response_model=TemplateListResponse, summary="获取所有分析模板")
async def list_templates(category: Optional[str] = None):
    """获取所有分析模板列表"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            query = session.query(AnalysisTemplate)
            if category:
                query = query.filter(AnalysisTemplate.category == category)
            templates = query.order_by(AnalysisTemplate.created_at.desc()).all()

            return TemplateListResponse(
                success=True,
                message=f"获取到 {len(templates)} 个模板",
                data=[t.to_dict() for t in templates]
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取模板列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates/{template_uuid}", response_model=TemplateResponse, summary="获取模板详情")
async def get_template(template_uuid: str):
    """获取指定模板的详细信息"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {template_uuid} 不存在")

            result = template.to_dict()
            # 包含数据集信息
            result["datasets"] = [ds.to_dict() for ds in template.datasets]

            return TemplateResponse(
                success=True,
                message="获取模板详情成功",
                data=result
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取模板详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/templates", response_model=TemplateResponse, summary="创建分析模板")
async def create_template(request: CreateTemplateRequest):
    """创建新的分析模板"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            # 如果没有提供prompts_config，使用默认值
            prompts_config = request.prompts_config if request.prompts_config else DEFAULT_PROMPTS

            template = AnalysisTemplate(
                name=request.name,
                description=request.description,
                category=request.category,
                config=request.config if request.config else {},
                prompts_config=prompts_config,
            )

            session.add(template)
            session.commit()
            session.refresh(template)

            return TemplateResponse(
                success=True,
                message=f"模板 '{request.name}' 创建成功",
                data=template.to_dict()
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/templates/{template_uuid}", response_model=TemplateResponse, summary="更新模板")
async def update_template(template_uuid: str, request: UpdateTemplateRequest):
    """更新模板配置"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {template_uuid} 不存在")

            if request.name is not None:
                template.name = request.name
            if request.description is not None:
                template.description = request.description
            if request.category is not None:
                template.category = request.category
            if request.config is not None:
                template.config = request.config
            if request.prompts_config is not None:
                template.prompts_config = request.prompts_config

            session.commit()
            session.refresh(template)

            return TemplateResponse(
                success=True,
                message="模板更新成功",
                data=template.to_dict()
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/templates/{template_uuid}", response_model=TemplateResponse, summary="删除模板")
async def delete_template(template_uuid: str):
    """删除模板及其关联的数据集"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {template_uuid} 不存在")

            # 删除关联的数据表
            processor = GenericAlignmentProcessor(state.db_connection)
            for ds in template.datasets:
                if ds.data_table_name:
                    processor.drop_data_table(ds.data_table_name)

            # 删除模板（级联删除数据集和结果）
            session.delete(template)
            session.commit()

            return TemplateResponse(
                success=True,
                message=f"模板 '{template.name}' 已删除",
                data=None
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates/{template_uuid}/datasets", response_model=TemplateListResponse, summary="获取模板的数据集列表")
async def list_datasets(template_uuid: str):
    """获取模板下的所有数据集"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {template_uuid} 不存在")

            return TemplateListResponse(
                success=True,
                message=f"获取到 {len(template.datasets)} 个数据集",
                data=[ds.to_dict() for ds in template.datasets]
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取数据集列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== CSV上传与解析 API ====================

@router.post("/templates/{template_uuid}/upload-csv", response_model=TemplateResponse, summary="上传CSV文件并自动解析")
async def upload_csv(
    template_uuid: str,
    file: UploadFile = File(...),
    role: str = Form("process", description="数据角色: process 或 detection"),
    dataset_name: str = Form(None, description="数据集名称")
):
    """
    上传CSV文件，自动解析列名和类型，创建数据表并存储数据
    """
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {template_uuid} 不存在")

            # 读取文件内容
            file_content = await file.read()
            file_size = len(file_content)

            # 解析CSV
            processor = GenericAlignmentProcessor(state.db_connection)
            df, columns, column_types = processor.parse_csv_file(file_content)

            if df.empty:
                raise HTTPException(status_code=400, detail="CSV文件为空或解析失败")

            # 自动检测时间列
            time_column = processor.auto_detect_time_column(columns, column_types)

            # 自动检测数值列
            value_columns = processor.auto_detect_value_columns(columns, column_types, time_column)

            # 获取样本数据
            sample_data = processor.get_sample_data(df, n_rows=5)

            # 构建列信息
            column_info = {
                "columns": columns,
                "column_types": column_types,
                "time_column": time_column,
                "time_format": None,  # 用户可后续设置
                "value_columns": value_columns,
                "column_labels": {},
                "column_units": {},
                "column_colors": {},
                "sample_data": sample_data,
            }

            # 创建数据集记录
            dataset_uuid = str(uuid.uuid4())
            data_table_name = f"custom_data_{dataset_uuid[:8]}"

            # 创建数据表
            all_cols = [time_column] + value_columns if time_column else value_columns
            processor.create_data_table(data_table_name, all_cols, time_column)

            # 插入数据
            df_to_insert = df[[time_column] + value_columns].copy() if time_column else df[value_columns].copy()
            processor.insert_data_to_table(data_table_name, df_to_insert, time_column)

            # 计算时间范围
            min_time = None
            max_time = None
            if time_column:
                try:
                    times = pd.to_datetime(df[time_column], errors='coerce').dropna()
                    if not times.empty:
                        min_time = times.min().to_pydatetime()
                        max_time = times.max().to_pydatetime()
                except Exception:
                    pass

            # 确定数据集名称
            ds_name = dataset_name or file.filename or f"数据集_{dataset_uuid[:8]}"

            dataset = UploadedDataset(
                template_id=template.id,
                dataset_uuid=dataset_uuid,
                name=ds_name,
                role=role,
                original_filename=file.filename,
                file_size=file_size,
                row_count=len(df),
                column_count=len(columns),
                column_info=column_info,
                data_table_name=data_table_name,
                min_time=min_time,
                max_time=max_time,
            )

            session.add(dataset)
            session.commit()
            session.refresh(dataset)

            return TemplateResponse(
                success=True,
                message=f"CSV文件上传解析成功: {len(df)} 行, {len(columns)} 列",
                data=dataset.to_dict()
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"CSV上传失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/datasets/{dataset_uuid}/column-info", response_model=TemplateResponse, summary="更新数据集列映射")
async def update_column_info(dataset_uuid: str, request: UpdateColumnInfoRequest):
    """
    更新数据集的列映射配置（时间列、值列、标签、单位、颜色等）
    """
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            dataset = session.query(UploadedDataset).filter(
                UploadedDataset.dataset_uuid == dataset_uuid
            ).first()

            if not dataset:
                raise HTTPException(status_code=404, detail=f"数据集 {dataset_uuid} 不存在")

            # 更新列信息
            old_info = dataset.column_info or {}
            old_info.update(request.column_info)
            dataset.column_info = old_info

            session.commit()
            session.refresh(dataset)

            return TemplateResponse(
                success=True,
                message="列映射更新成功",
                data=dataset.to_dict()
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新列映射失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/datasets/{dataset_uuid}", response_model=TemplateResponse, summary="删除数据集")
async def delete_dataset(dataset_uuid: str):
    """删除数据集及其数据表"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            dataset = session.query(UploadedDataset).filter(
                UploadedDataset.dataset_uuid == dataset_uuid
            ).first()

            if not dataset:
                raise HTTPException(status_code=404, detail=f"数据集 {dataset_uuid} 不存在")

            # 删除数据表
            processor = GenericAlignmentProcessor(state.db_connection)
            if dataset.data_table_name:
                processor.drop_data_table(dataset.data_table_name)

            ds_name = dataset.name
            session.delete(dataset)
            session.commit()

            return TemplateResponse(
                success=True,
                message=f"数据集 '{ds_name}' 已删除",
                data=None
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除数据集失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 图表生成 API ====================

@router.post("/generate-chart", response_model=TemplateResponse, summary="生成时空对齐图表")
async def generate_chart(request: GenerateChartRequest):
    """
    根据模板配置生成时空对齐图表
    """
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == request.template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {request.template_uuid} 不存在")

            if not template.datasets:
                raise HTTPException(status_code=400, detail="模板下没有数据集，请先上传数据")

            config = template.config or {}
            chart_config = config.get("chart", DEFAULT_CHART_CONFIG)
            alignment_config = config.get("alignment", DEFAULT_ALIGNMENT_CONFIG)

            # 使用共享辅助函数加载和对齐数据
            process_dfs, detection_df, global_start, global_end, process_configs, detection_config = \
                _load_and_align_data(session, template, request.start_time, request.end_time)

            processor = GenericAlignmentProcessor(state.db_connection)

            # 生成组合图表
            combined_image = processor.generate_combined_chart(
                process_dfs=process_dfs,
                detection_df=detection_df,
                process_configs=process_configs,
                detection_config=detection_config,
                chart_config=chart_config,
                start_time=global_start,
                end_time=global_end
            )

            if not combined_image:
                raise HTTPException(status_code=500, detail="图表生成失败")

            # 上传到MinIO
            combined_image_b64 = _encode_image(combined_image)

            # 生成检测图（用于Agent分析）
            detect_image_b64 = None
            if not detection_df.empty:
                detect_image = processor.generate_detection_chart(
                    detection_df, detection_config, chart_config
                )
                if detect_image:
                    detect_image_b64 = _encode_image(detect_image)

            return TemplateResponse(
                success=True,
                message="图表生成成功",
                data={
                    "combined_image": combined_image_b64,
                    "detect_image": detect_image_b64,
                    "start_time": global_start.isoformat(),
                    "end_time": global_end.isoformat(),
                    "process_dataset_count": len(process_dfs),
                    "detection_rows": 0 if detection_df.empty else len(detection_df),
                    "alignment_config": alignment_config,
                }
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"生成图表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 自定义提示词分析 API ====================

@router.post("/run-analysis", summary="运行自定义提示词的三模型分析", response_model=None)
async def run_analysis(request: RunAnalysisRequest):
    """
    运行三模型工作流分析（支持自定义提示词）
    流式返回分析结果
    """
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == request.template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {request.template_uuid} 不存在")

            config = template.config or {}
            prompts_config = template.prompts_config or {}
            chart_config = config.get("chart", DEFAULT_CHART_CONFIG)

            # 使用共享辅助函数加载和对齐数据
            process_dfs, detection_df, global_start, global_end, process_configs, detection_config = \
                _load_and_align_data(session, template, request.start_time, request.end_time)

            processor = GenericAlignmentProcessor(state.db_connection)

            # 生成图表
            combined_image = processor.generate_combined_chart(
                process_dfs, detection_df, process_configs, detection_config,
                chart_config, global_start, global_end
            )

            detect_image = b''
            if not detection_df.empty:
                detect_image = processor.generate_detection_chart(
                    detection_df, detection_config, chart_config
                )

            # 创建分析结果记录
            result_uuid = str(uuid.uuid4())
            analysis_result = CustomAnalysisResult(
                result_uuid=result_uuid,
                template_id=template.id,
                start_time=global_start,
                end_time=global_end,
                prompts_snapshot=prompts_config,
                data_points_count=len(detection_df) if not detection_df.empty else 0,
            )
            session.add(analysis_result)
            session.commit()

        finally:
            session.close()

        # 使用共享辅助函数获取LLM配置
        llm_base_url, api_key = _get_llm_config(prompts_config)

        if not api_key:
            llm_api_key_env = prompts_config.get("llm_api_key_env") or app_config.llm.api_key_env
            raise HTTPException(status_code=500, detail=f"环境变量 {llm_api_key_env} 未设置")

        def generate_stream():
            """流式生成分析结果"""
            import json as json_mod
            from langchain_core.messages import HumanMessage

            detect_result = ""
            process_result = ""
            final_result = ""

            try:
                # === 1. 检测Agent分析 ===
                detect_agent_cfg = prompts_config.get("detect_agent", {})
                if detect_agent_cfg.get("enabled", True) and detect_image:
                    yield json_mod.dumps({
                        "event": "node_start",
                        "node": "detect_agent",
                        "message": "开始检测分析..."
                    }, ensure_ascii=False) + "\n"

                    llm = _create_llm(detect_agent_cfg, llm_base_url, api_key)
                    message_content = []
                    message_content.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{_encode_image(detect_image)}"}
                    })
                    message_content.append({
                        "type": "text",
                        "text": detect_agent_cfg.get("prompt", DEFAULT_PROMPTS["detect_agent"]["prompt"])
                    })

                    for chunk in llm.stream([HumanMessage(content=message_content)]):
                        if chunk.content:
                            content = chunk.content
                            if isinstance(content, list):
                                content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
                            detect_result += content
                            yield json_mod.dumps({
                                "event": "stream_content",
                                "node": "detect_agent",
                                "content": content
                            }, ensure_ascii=False) + "\n"

                    yield json_mod.dumps({
                        "event": "node_complete",
                        "node": "detect_agent",
                        "message": "检测分析完成"
                    }, ensure_ascii=False) + "\n"

                # === 2. 工艺Agent分析 ===
                process_agent_cfg = prompts_config.get("process_agent", {})
                if process_agent_cfg.get("enabled", True) and combined_image:
                    yield json_mod.dumps({
                        "event": "node_start",
                        "node": "process_agent",
                        "message": "开始工艺分析..."
                    }, ensure_ascii=False) + "\n"

                    llm = _create_llm(process_agent_cfg, llm_base_url, api_key)
                    message_content = []
                    message_content.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{_encode_image(combined_image)}"}
                    })
                    message_content.append({
                        "type": "text",
                        "text": process_agent_cfg.get("prompt", DEFAULT_PROMPTS["process_agent"]["prompt"])
                    })

                    for chunk in llm.stream([HumanMessage(content=message_content)]):
                        if chunk.content:
                            content = chunk.content
                            if isinstance(content, list):
                                content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
                            process_result += content
                            yield json_mod.dumps({
                                "event": "stream_content",
                                "node": "process_agent",
                                "content": content
                            }, ensure_ascii=False) + "\n"

                    yield json_mod.dumps({
                        "event": "node_complete",
                        "node": "process_agent",
                        "message": "工艺分析完成"
                    }, ensure_ascii=False) + "\n"

                # === 3. 决策Agent整合 ===
                decision_agent_cfg = prompts_config.get("decision_agent", {})
                if decision_agent_cfg.get("enabled", True):
                    yield json_mod.dumps({
                        "event": "node_start",
                        "node": "decision_agent",
                        "message": "开始综合诊断..."
                    }, ensure_ascii=False) + "\n"

                    llm = _create_llm(decision_agent_cfg, llm_base_url, api_key)

                    # 构建整合提示词
                    integrate_prompt = decision_agent_cfg.get("prompt", DEFAULT_PROMPTS["decision_agent"]["prompt"])
                    integrate_prompt += f"\n\n来源 A (工艺侧)分析结果：\n{process_result}\n\n来源 B (质检侧)分析结果：\n{detect_result}"

                    for chunk in llm.stream([HumanMessage(content=integrate_prompt)]):
                        if chunk.content:
                            content = chunk.content
                            if isinstance(content, list):
                                content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
                            final_result += content
                            yield json_mod.dumps({
                                "event": "stream_content",
                                "node": "decision_agent",
                                "content": content
                            }, ensure_ascii=False) + "\n"

                    yield json_mod.dumps({
                        "event": "node_complete",
                        "node": "decision_agent",
                        "message": "综合诊断完成"
                    }, ensure_ascii=False) + "\n"

                # === 保存结果到数据库 ===
                try:
                    session2 = state.db_connection.get_session()
                    try:
                        result_record = session2.query(CustomAnalysisResult).filter(
                            CustomAnalysisResult.result_uuid == result_uuid
                        ).first()
                        if result_record:
                            result_record.detection_agent_result = detect_result
                            result_record.processing_agent_result = process_result
                            result_record.decision_agent_result = final_result
                            session2.commit()
                    finally:
                        session2.close()
                except Exception as save_err:
                    logger.error(f"保存分析结果失败: {save_err}")

                yield json_mod.dumps({
                    "event": "workflow_complete",
                    "message": "分析完成",
                    "result": {
                        "detect_result": detect_result,
                        "process_result": process_result,
                        "final_result": final_result,
                        "result_uuid": result_uuid,
                        "combined_image": _encode_image(combined_image) if combined_image else None,
                    }
                }, ensure_ascii=False) + "\n"

            except Exception as e:
                logger.error(f"分析流式传输错误: {e}")
                yield json_mod.dumps({
                    "event": "error",
                    "message": str(e)
                }, ensure_ascii=False) + "\n"

        if request.stream:
            return StreamingResponse(generate_stream(), media_type="application/x-ndjson")
        else:
            # 非流式模式：收集所有结果
            result = {}
            for event_str in generate_stream():
                event = json.loads(event_str.strip())
                if event.get("event") == "workflow_complete":
                    result = event.get("result", {})
                    break
            return {"success": True, "data": result}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"运行分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 分析结果查询 API ====================

@router.get("/templates/{template_uuid}/results", response_model=TemplateListResponse, summary="获取模板的分析结果列表")
async def list_analysis_results(template_uuid: str, page: int = 1, page_size: int = 10):
    """获取模板下的分析结果列表"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {template_uuid} 不存在")

            offset = (page - 1) * page_size
            results = session.query(CustomAnalysisResult).filter(
                CustomAnalysisResult.template_id == template.id
            ).order_by(
                CustomAnalysisResult.created_at.desc()
            ).offset(offset).limit(page_size).all()

            return TemplateListResponse(
                success=True,
                message=f"获取到 {len(results)} 条分析结果",
                data=[r.to_dict() for r in results]
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取分析结果列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/results/{result_uuid}", response_model=TemplateResponse, summary="获取分析结果详情")
async def get_analysis_result(result_uuid: str):
    """获取指定分析结果的详细信息"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            result = session.query(CustomAnalysisResult).filter(
                CustomAnalysisResult.result_uuid == result_uuid
            ).first()

            if not result:
                raise HTTPException(status_code=404, detail=f"分析结果 {result_uuid} 不存在")

            return TemplateResponse(
                success=True,
                message="获取分析结果成功",
                data=result.to_dict()
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取分析结果失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/default-prompts", response_model=TemplateResponse, summary="获取默认提示词配置")
async def get_default_prompts():
    """获取默认的三模型提示词配置"""
    return TemplateResponse(
        success=True,
        message="获取默认提示词成功",
        data=DEFAULT_PROMPTS
    )


@router.get("/default-simulation-config", response_model=TemplateResponse, summary="获取默认模拟配置")
async def get_default_simulation_config():
    """获取默认的滑动窗口模拟配置"""
    return TemplateResponse(
        success=True,
        message="获取默认模拟配置成功",
        data=DEFAULT_SIMULATION_CONFIG
    )


# ==================== 滑动窗口模拟分析 API ====================

@router.post("/run-simulation", summary="运行滑动窗口模拟分析", response_model=None)
async def run_simulation(request: RunSimulationRequest):
    """
    运行滑动窗口模拟分析：
    - 按用户配置的步长和窗口大小，逐步推进时间窗口
    - 每个窗口生成时空对齐图表
    - 按VLM诊断间隔定期调用VLM进行诊断分析
    - 支持继续作业（从上次中断处继续）
    - 流式返回每一步的结果
    """
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == request.template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {request.template_uuid} 不存在")

            if not template.datasets:
                raise HTTPException(status_code=400, detail="模板下没有数据集，请先上传数据")

            config = template.config or {}
            prompts_config = template.prompts_config or {}
            alignment_config = config.get("alignment", DEFAULT_ALIGNMENT_CONFIG)
            chart_config = config.get("chart", DEFAULT_CHART_CONFIG)

            # 获取模拟配置
            sim_config = request.simulation_config or config.get("simulation", DEFAULT_SIMULATION_CONFIG)

            # 处理继续作业
            resume_run = None
            start_step = 0
            if request.resume_run_uuid:
                resume_run = session.query(SimulationRun).filter(
                    SimulationRun.run_uuid == request.resume_run_uuid
                ).first()
                if resume_run:
                    start_step = resume_run.last_completed_step + 1
                    sim_config = resume_run.simulation_config

            processor = GenericAlignmentProcessor(state.db_connection)

            # 加载所有数据集（全量加载，后续按窗口裁剪）
            datasets_info = []
            all_times = []
            for ds in template.datasets:
                df = processor.load_dataset_data(
                    ds.data_table_name, ds.column_info,
                    None, None
                )
                datasets_info.append({
                    "df": df, "role": ds.role,
                    "column_info": ds.column_info, "name": ds.name
                })
                if df is not None and not df.empty:
                    all_times.append(df.index.min())
                    all_times.append(df.index.max())

            if not all_times:
                raise HTTPException(status_code=400, detail="没有有效的时间数据")

            global_start = min(all_times)
            global_end = max(all_times)

            # 计算所有步进
            all_steps = processor.compute_simulation_steps(global_start, global_end, sim_config)
            total_steps = len(all_steps)

            if total_steps == 0:
                raise HTTPException(status_code=400, detail="根据模拟配置计算出的步数为0，请检查参数")

            # 过滤掉已完成的步骤（继续作业时）
            steps_to_run = [s for s in all_steps if s["step_number"] >= start_step]

            # 创建或更新模拟运行记录
            if resume_run:
                run = resume_run
                run.status = "running"
                run.current_step = start_step
            else:
                run_uuid_val = str(uuid.uuid4())
                run = SimulationRun(
                    run_uuid=run_uuid_val,
                    template_id=template.id,
                    simulation_config=sim_config,
                    status="running",
                    total_steps=total_steps,
                    sim_start_time=global_start,
                    sim_end_time=global_end,
                    current_step=0,
                    last_completed_step=-1,
                )
                session.add(run)

            session.commit()
            session.refresh(run)

            run_id = run.id
            run_uuid_str = run.run_uuid

        finally:
            session.close()

        # 使用共享辅助函数获取LLM配置
        llm_base_url, api_key = _get_llm_config(prompts_config)
        llm_api_key_env = prompts_config.get("llm_api_key_env") or app_config.llm.api_key_env

        def generate_simulation_stream():
            """流式生成模拟分析结果"""
            import json as json_mod

            try:
                # 发送开始事件
                yield json_mod.dumps({
                    "event": "simulation_start",
                    "run_uuid": run_uuid_str,
                    "total_steps": total_steps,
                    "steps_to_run": len(steps_to_run),
                    "sim_config": sim_config,
                    "global_start": global_start.isoformat(),
                    "global_end": global_end.isoformat(),
                    "resumed_from_step": start_step if start_step > 0 else None,
                }, ensure_ascii=False) + "\n"

                # 如果API Key未配置，发送警告
                if not api_key:
                    yield json_mod.dumps({
                        "event": "warning",
                        "message": f"环境变量 {llm_api_key_env} 未设置，VLM诊断将被跳过（图表和统计仍正常生成）"
                    }, ensure_ascii=False) + "\n"

                process_configs = [ds["column_info"] for ds in datasets_info if ds["role"] == "process"]
                detection_config = {}
                for ds_info in datasets_info:
                    if ds_info["role"] == "detection":
                        detection_config = ds_info["column_info"]
                        break

                for step_info in steps_to_run:
                    step_num = step_info["step_number"]
                    window_start = step_info["step_time"]
                    window_end = window_start + timedelta(seconds=sim_config.get("alignment_window_seconds", app_config.custom_analysis.default_sim_window))
                    do_vlm = step_info["do_vlm"]

                    # 检查是否已被暂停（支持从外部暂停）
                    try:
                        pause_check_session = state.db_connection.get_session()
                        try:
                            run_check = pause_check_session.query(SimulationRun).filter(
                                SimulationRun.run_uuid == run_uuid_str
                            ).first()
                            if run_check and run_check.status == "paused":
                                yield json_mod.dumps({
                                    "event": "simulation_paused",
                                    "run_uuid": run_uuid_str,
                                    "message": f"模拟已在第 {step_num} 步暂停",
                                    "last_completed_step": run_check.last_completed_step,
                                }, ensure_ascii=False) + "\n"
                                return
                        finally:
                            pause_check_session.close()
                    except Exception:
                        pass

                    # 裁剪窗口结束时间
                    if window_end > global_end:
                        window_end = global_end

                    # 窗口内数据对齐
                    proc_dfs, det_df = processor.align_data_in_window(
                        datasets_info, alignment_config, window_start, window_end
                    )

                    # 生成窗口图表
                    chart_image = processor.generate_window_chart(
                        proc_dfs, det_df, process_configs, detection_config,
                        chart_config, window_start, window_end, step_num
                    )

                    chart_b64 = _encode_image(chart_image) if chart_image else None

                    # 计算数据摘要
                    data_summary = processor.compute_window_summary(
                        proc_dfs, det_df, process_configs
                    )

                    # 发送步进数据事件
                    yield json_mod.dumps({
                        "event": "step_data",
                        "step_number": step_num,
                        "total_steps": total_steps,
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "chart_image": chart_b64,
                        "data_summary": data_summary,
                        "do_vlm": do_vlm,
                        "has_detection": not det_df.empty,
                    }, ensure_ascii=False) + "\n"

                    # VLM诊断
                    vlm_detect_result = ""
                    vlm_process_result = ""
                    vlm_decision_result = ""

                    if do_vlm and api_key:
                        # 生成检测图（用于VLM分析）
                        detect_image = b''
                        if not det_df.empty:
                            detect_image = processor.generate_detection_chart(
                                det_df, detection_config, chart_config
                            )

                        from langchain_core.messages import HumanMessage

                        # 1. 检测Agent
                        detect_agent_cfg = prompts_config.get("detect_agent", {})
                        if detect_agent_cfg.get("enabled", True) and detect_image:
                            yield json_mod.dumps({
                                "event": "vlm_start",
                                "step_number": step_num,
                                "agent": "detect_agent",
                                "message": f"Step {step_num}: 开始检测分析..."
                            }, ensure_ascii=False) + "\n"

                            llm = _create_llm(detect_agent_cfg, llm_base_url, api_key)
                            message_content = [
                                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{_encode_image(detect_image)}"}},
                                {"type": "text", "text": detect_agent_cfg.get("prompt", DEFAULT_PROMPTS["detect_agent"]["prompt"])}
                            ]

                            for chunk in llm.stream([HumanMessage(content=message_content)]):
                                if chunk.content:
                                    content = chunk.content
                                    if isinstance(content, list):
                                        content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
                                    vlm_detect_result += content
                                    yield json_mod.dumps({
                                        "event": "vlm_stream",
                                        "step_number": step_num,
                                        "agent": "detect_agent",
                                        "content": content
                                    }, ensure_ascii=False) + "\n"

                            yield json_mod.dumps({
                                "event": "vlm_complete",
                                "step_number": step_num,
                                "agent": "detect_agent",
                            }, ensure_ascii=False) + "\n"

                        # 2. 工艺Agent
                        process_agent_cfg = prompts_config.get("process_agent", {})
                        if process_agent_cfg.get("enabled", True) and chart_image:
                            yield json_mod.dumps({
                                "event": "vlm_start",
                                "step_number": step_num,
                                "agent": "process_agent",
                                "message": f"Step {step_num}: 开始工艺分析..."
                            }, ensure_ascii=False) + "\n"

                            llm = _create_llm(process_agent_cfg, llm_base_url, api_key)
                            message_content = [
                                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{_encode_image(chart_image)}"}},
                                {"type": "text", "text": process_agent_cfg.get("prompt", DEFAULT_PROMPTS["process_agent"]["prompt"])}
                            ]

                            for chunk in llm.stream([HumanMessage(content=message_content)]):
                                if chunk.content:
                                    content = chunk.content
                                    if isinstance(content, list):
                                        content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
                                    vlm_process_result += content
                                    yield json_mod.dumps({
                                        "event": "vlm_stream",
                                        "step_number": step_num,
                                        "agent": "process_agent",
                                        "content": content
                                    }, ensure_ascii=False) + "\n"

                            yield json_mod.dumps({
                                "event": "vlm_complete",
                                "step_number": step_num,
                                "agent": "process_agent",
                            }, ensure_ascii=False) + "\n"

                        # 3. 决策Agent
                        decision_agent_cfg = prompts_config.get("decision_agent", {})
                        if decision_agent_cfg.get("enabled", True):
                            yield json_mod.dumps({
                                "event": "vlm_start",
                                "step_number": step_num,
                                "agent": "decision_agent",
                                "message": f"Step {step_num}: 开始综合诊断..."
                            }, ensure_ascii=False) + "\n"

                            llm = _create_llm(decision_agent_cfg, llm_base_url, api_key)
                            integrate_prompt = decision_agent_cfg.get("prompt", DEFAULT_PROMPTS["decision_agent"]["prompt"])
                            integrate_prompt += f"\n\n--- 当前步进信息 ---\n步进编号: {step_num}/{total_steps}\n时间窗口: {window_start} 到 {window_end}\n\n来源 A (工艺侧)分析结果：\n{vlm_process_result}\n\n来源 B (质检侧)分析结果：\n{vlm_detect_result}"

                            for chunk in llm.stream([HumanMessage(content=integrate_prompt)]):
                                if chunk.content:
                                    content = chunk.content
                                    if isinstance(content, list):
                                        content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
                                    vlm_decision_result += content
                                    yield json_mod.dumps({
                                        "event": "vlm_stream",
                                        "step_number": step_num,
                                        "agent": "decision_agent",
                                        "content": content
                                    }, ensure_ascii=False) + "\n"

                            yield json_mod.dumps({
                                "event": "vlm_complete",
                                "step_number": step_num,
                                "agent": "decision_agent",
                            }, ensure_ascii=False) + "\n"

                    # 保存步进记录到数据库
                    try:
                        session2 = state.db_connection.get_session()
                        try:
                            step_record = SimulationStep(
                                run_id=run_id,
                                step_number=step_num,
                                step_time=window_start,
                                window_start=window_start,
                                window_end=window_end,
                                has_vlm_diagnosis=do_vlm,
                                chart_image_b64=chart_b64,
                                vlm_detection_result=vlm_detect_result if vlm_detect_result else None,
                                vlm_process_result=vlm_process_result if vlm_process_result else None,
                                vlm_decision_result=vlm_decision_result if vlm_decision_result else None,
                                data_summary=data_summary,
                                status="completed",
                            )
                            session2.add(step_record)

                            # 更新运行状态
                            run_record = session2.query(SimulationRun).filter(
                                SimulationRun.run_uuid == run_uuid_str
                            ).first()
                            if run_record:
                                run_record.current_step = step_num + 1
                                run_record.current_time = window_end
                                run_record.last_completed_step = step_num
                                run_record.last_completed_time = window_end
                                if step_num + 1 >= total_steps:
                                    run_record.status = "completed"
                                else:
                                    run_record.status = "running"

                            session2.commit()
                        finally:
                            session2.close()
                    except Exception as save_err:
                        logger.error(f"保存步进记录失败: {save_err}")

                    # 发送步进完成事件
                    yield json_mod.dumps({
                        "event": "step_complete",
                        "step_number": step_num,
                        "total_steps": total_steps,
                        "progress": (step_num + 1) / total_steps,
                        "has_vlm_diagnosis": do_vlm,
                        "vlm_results": {
                            "detect": vlm_detect_result,
                            "process": vlm_process_result,
                            "decision": vlm_decision_result,
                        } if do_vlm else None,
                    }, ensure_ascii=False) + "\n"

                # 发送模拟完成事件
                yield json_mod.dumps({
                    "event": "simulation_complete",
                    "run_uuid": run_uuid_str,
                    "total_steps": total_steps,
                    "message": "模拟分析完成",
                }, ensure_ascii=False) + "\n"

            except Exception as e:
                logger.error(f"模拟分析流式传输错误: {e}")
                import traceback
                logger.error(traceback.format_exc())
                yield json_mod.dumps({
                    "event": "error",
                    "message": str(e)
                }, ensure_ascii=False) + "\n"

        if request.stream:
            return StreamingResponse(generate_simulation_stream(), media_type="application/x-ndjson")
        else:
            result = {}
            for event_str in generate_simulation_stream():
                event = json.loads(event_str.strip())
                if event.get("event") == "simulation_complete":
                    result = event
                    break
            return {"success": True, "data": result}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"运行模拟分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/simulation-runs/{template_uuid}", response_model=TemplateListResponse, summary="获取模板的模拟运行列表")
async def list_simulation_runs(template_uuid: str):
    """获取模板下的所有模拟运行记录"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            template = session.query(AnalysisTemplate).filter(
                AnalysisTemplate.template_uuid == template_uuid
            ).first()

            if not template:
                raise HTTPException(status_code=404, detail=f"模板 {template_uuid} 不存在")

            runs = session.query(SimulationRun).filter(
                SimulationRun.template_id == template.id
            ).order_by(SimulationRun.created_at.desc()).all()

            return TemplateListResponse(
                success=True,
                message=f"获取到 {len(runs)} 条模拟运行记录",
                data=[r.to_dict() for r in runs]
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取模拟运行列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/simulation-runs/{run_uuid}/detail", response_model=TemplateResponse, summary="获取模拟运行详情（含所有步进）")
async def get_simulation_run_detail(run_uuid: str):
    """获取模拟运行的详细信息，包括所有步进记录"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            run = session.query(SimulationRun).filter(
                SimulationRun.run_uuid == run_uuid
            ).first()

            if not run:
                raise HTTPException(status_code=404, detail=f"模拟运行 {run_uuid} 不存在")

            result = run.to_dict()
            result["steps"] = [s.to_dict() for s in run.steps]

            return TemplateResponse(
                success=True,
                message="获取模拟运行详情成功",
                data=result
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取模拟运行详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/simulation-runs/{run_uuid}/pause", response_model=TemplateResponse, summary="暂停模拟运行")
async def pause_simulation_run(run_uuid: str):
    """暂停模拟运行（将状态设置为paused）"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            run = session.query(SimulationRun).filter(
                SimulationRun.run_uuid == run_uuid
            ).first()

            if not run:
                raise HTTPException(status_code=404, detail=f"模拟运行 {run_uuid} 不存在")

            run.status = "paused"
            session.commit()

            return TemplateResponse(
                success=True,
                message=f"模拟运行已暂停（已完成 {run.last_completed_step + 1}/{run.total_steps} 步）",
                data=run.to_dict()
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"暂停模拟运行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/simulation-runs/{run_uuid}", response_model=TemplateResponse, summary="删除模拟运行")
async def delete_simulation_run(run_uuid: str):
    """删除模拟运行及其所有步进记录"""
    try:
        _require_db()
        session = state.db_connection.get_session()
        try:
            run = session.query(SimulationRun).filter(
                SimulationRun.run_uuid == run_uuid
            ).first()

            if not run:
                raise HTTPException(status_code=404, detail=f"模拟运行 {run_uuid} 不存在")

            session.delete(run)
            session.commit()

            return TemplateResponse(
                success=True,
                message="模拟运行已删除",
                data=None
            )
        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除模拟运行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
