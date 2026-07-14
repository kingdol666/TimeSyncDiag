"""
自定义时空对齐分析 - 数据库模型

包含：
- AnalysisTemplate: 分析模板（保存配置、列映射、提示词等）
- UploadedDataset: 上传的数据集记录
- CustomAnalysisResult: 自定义分析结果
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, Text, Index, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .db_connection import Base
import uuid


class AnalysisTemplate(Base):
    """
    分析模板模型 - 每次创建一组分析配置即为一个模板
    模板可复用：下次上传新数据时可选择已有模板，自动套用配置
    """
    __tablename__ = "analysis_template"

    id = Column(Integer, primary_key=True, autoincrement=True)
    template_uuid = Column(String(36), nullable=False, unique=True, index=True, default=lambda: str(uuid.uuid4()))

    # 模板基本信息
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    category = Column(String(100), nullable=True)  # 模板分类标签

    # 数据集配置（JSON存储完整的配置结构）
    # 结构示例:
    # {
    #   "datasets": [
    #     {
    #       "id": "dataset_uuid",
    #       "name": "加工参数数据",
    #       "role": "process",  # "process" | "detection"
    #       "time_column": "timestamp",
    #       "time_format": "%Y-%m-%d %H:%M:%S",
    #       "value_columns": ["main_motor_speed_pv", "die_pressure", ...],
    #       "column_labels": {"main_motor_speed_pv": "主电机速度", ...},
    #       "column_units": {"main_motor_speed_pv": "rpm", ...},
    #       "column_colors": {"main_motor_speed_pv": "#0072B2", ...}
    #     }
    #   ],
    #   "alignment": {
    #     "interval_seconds": 20,
    #     "time_tolerance_seconds": 30,
    #     "interpolation_method": "linear",  # linear | nearest | none
    #     "resample_enabled": true
    #   },
    #   "chart": {
    #     "target_value": 0.045,
    #     "value_range": 0.005,
    #     "figsize": [16.0, 16.0],
    #     "dpi": 600,
    #     "colormap": "blue_green_red",
    #     "title": "时空对齐分析图",
    #     "xlabel": "Time",
    #     "show_grid": true,
    #     "show_colorbar": true
    #   }
    # }
    config = Column(JSON, nullable=False, default=dict)

    # LLM 提示词配置（三个Agent的提示词）
    # 结构示例:
    # {
    #   "detect_agent": {
    #     "enabled": true,
    #     "prompt": "【角色设定】\n你是一位...",
    #     "model_name": "qwen3.6-chat",
    #     "temperature": 0.7,
    #     "max_tokens": 4096
    #   },
    #   "process_agent": { ... },
    #   "decision_agent": { ... },
    #   "llm_base_url": "https://api.llm.ustc.edu.cn/v1",
    #   "llm_api_key_env": "USTC_LLM_API_KEY"
    # }
    prompts_config = Column(JSON, nullable=False, default=dict)

    # 状态
    is_active = Column(Boolean, default=True, nullable=False)
    is_public = Column(Boolean, default=True, nullable=False)  # 是否公开（其他用户可用）

    # 创建者信息
    created_by = Column(String(100), nullable=True)

    # 时间戳
    created_at = Column(DateTime(timezone=False), server_default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=False), server_default=func.now(), onupdate=func.now())

    # 关系
    datasets = relationship("UploadedDataset", backref="template", cascade="all, delete-orphan")
    analysis_results = relationship("CustomAnalysisResult", backref="template", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_template_name', 'name'),
        Index('idx_template_category', 'category'),
    )

    def __repr__(self):
        return f"<AnalysisTemplate(name='{self.name}', uuid='{self.template_uuid}')>"

    def to_dict(self):
        return {
            "id": self.id,
            "template_uuid": self.template_uuid,
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "config": self.config,
            "prompts_config": self.prompts_config,
            "is_active": self.is_active,
            "is_public": self.is_public,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "dataset_count": len(self.datasets) if self.datasets else 0,
        }


class UploadedDataset(Base):
    """
    上传的数据集记录 - 每次上传的CSV文件对应一条记录
    数据本身存储在数据库的通用数据表中，这里记录元信息
    """
    __tablename__ = "uploaded_dataset"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_uuid = Column(String(36), nullable=False, unique=True, index=True, default=lambda: str(uuid.uuid4()))

    # 关联模板
    template_id = Column(Integer, ForeignKey("analysis_template.id"), nullable=False, index=True)

    # 数据集基本信息
    name = Column(String(200), nullable=False)
    role = Column(String(50), nullable=False)  # "process" | "detection"

    # 文件信息
    original_filename = Column(String(500), nullable=False)
    file_size = Column(Integer, nullable=True)  # 字节数
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)

    # 列信息（JSON存储列名、类型、映射等）
    # 结构示例:
    # {
    #   "columns": ["timestamp", "main_motor_speed_pv", "die_pressure"],
    #   "column_types": {"timestamp": "datetime", "main_motor_speed_pv": "float"},
    #   "time_column": "timestamp",
    #   "time_format": "%Y-%m-%d %H:%M:%S",
    #   "value_columns": ["main_motor_speed_pv", "die_pressure"],
    #   "column_labels": {...},
    #   "column_units": {...},
    #   "column_colors": {...},
    #   "sample_data": [...]  # 前几行样本数据，用于前端预览
    # }
    column_info = Column(JSON, nullable=False, default=dict)

    # 数据存储表名（动态创建的表名，存储实际数据）
    # 表名格式: custom_data_{dataset_uuid的前8位}
    data_table_name = Column(String(100), nullable=False)

    # 时间范围
    min_time = Column(DateTime(timezone=False), nullable=True)
    max_time = Column(DateTime(timezone=False), nullable=True)

    # 时间戳
    created_at = Column(DateTime(timezone=False), server_default=func.now(), index=True)

    __table_args__ = (
        Index('idx_dataset_template', 'template_id'),
        Index('idx_dataset_role', 'role'),
    )

    def __repr__(self):
        return f"<UploadedDataset(name='{self.name}', role='{self.role}', uuid='{self.dataset_uuid}')>"

    def to_dict(self):
        return {
            "id": self.id,
            "dataset_uuid": self.dataset_uuid,
            "template_id": self.template_id,
            "name": self.name,
            "role": self.role,
            "original_filename": self.original_filename,
            "file_size": self.file_size,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "column_info": self.column_info,
            "data_table_name": self.data_table_name,
            "min_time": self.min_time.isoformat() if self.min_time else None,
            "max_time": self.max_time.isoformat() if self.max_time else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class CustomAnalysisResult(Base):
    """
    自定义分析结果 - 每次运行分析的结果记录
    """
    __tablename__ = "custom_analysis_result"

    id = Column(Integer, primary_key=True, autoincrement=True)
    result_uuid = Column(String(36), nullable=False, unique=True, index=True, default=lambda: str(uuid.uuid4()))

    # 关联模板
    template_id = Column(Integer, ForeignKey("analysis_template.id"), nullable=False, index=True)

    # 分析时间范围
    start_time = Column(DateTime(timezone=False), nullable=True)
    end_time = Column(DateTime(timezone=False), nullable=True)

    # 生成的图表路径（MinIO）
    combined_image_path = Column(String(512), nullable=True)
    detect_image_path = Column(String(512), nullable=True)

    # 三个Agent的分析结果
    detection_agent_result = Column(Text, nullable=True)
    processing_agent_result = Column(Text, nullable=True)
    decision_agent_result = Column(Text, nullable=True)

    # 使用的提示词快照（记录当时使用的提示词）
    prompts_snapshot = Column(JSON, nullable=True)

    # 统计信息
    data_points_count = Column(Integer, nullable=True)

    # 时间戳
    created_at = Column(DateTime(timezone=False), server_default=func.now(), index=True)

    __table_args__ = (
        Index('idx_result_template', 'template_id'),
        Index('idx_result_created', 'created_at'),
    )

    def __repr__(self):
        return f"<CustomAnalysisResult(uuid='{self.result_uuid}', template_id={self.template_id})>"

    def to_dict(self):
        return {
            "id": self.id,
            "result_uuid": self.result_uuid,
            "template_id": self.template_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "combined_image_path": self.combined_image_path,
            "detect_image_path": self.detect_image_path,
            "detection_agent_result": self.detection_agent_result,
            "processing_agent_result": self.processing_agent_result,
            "decision_agent_result": self.decision_agent_result,
            "prompts_snapshot": self.prompts_snapshot,
            "data_points_count": self.data_points_count,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class SimulationRun(Base):
    """
    模拟运行记录 - 每次启动一次滑动窗口模拟分析即为一次运行
    支持继续作业：记录上次运行到的位置，下次可以从该位置继续
    """
    __tablename__ = "simulation_run"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_uuid = Column(String(36), nullable=False, unique=True, index=True, default=lambda: str(uuid.uuid4()))

    # 关联模板
    template_id = Column(Integer, ForeignKey("analysis_template.id"), nullable=False, index=True)

    # 模拟参数快照
    simulation_config = Column(JSON, nullable=False, default=dict)
    # 结构示例:
    # {
    #   "alignment_window_seconds": 300,     # 对齐时间范围（窗口大小，秒）
    #   "step_seconds": 60,                  # 步长时间（每次向前推进多少秒）
    #   "step_interval_seconds": 60,          # 推进间隔（多久推进一次，秒）
    #   "vlm_diagnosis_interval_steps": 5,   # VLM诊断间隔（每N步进行一次VLM诊断）
    #   "vlm_diagnosis_interval_seconds": 300, # VLM诊断间隔（按时间算，秒）
    #   "total_steps": 100,                  # 总步数
    #   "start_time": "2024-01-01T00:00:00", # 模拟起始时间
    #   "end_time": "2024-01-01T02:00:00",   # 模拟结束时间
    # }

    # 运行状态
    status = Column(String(20), nullable=False, default="pending")  # pending | running | paused | completed | failed

    # 进度
    current_step = Column(Integer, nullable=False, default=0)
    total_steps = Column(Integer, nullable=False, default=0)
    current_time = Column(DateTime(timezone=False), nullable=True)  # 当前模拟到的时间点

    # 运行时间范围
    sim_start_time = Column(DateTime(timezone=False), nullable=True)
    sim_end_time = Column(DateTime(timezone=False), nullable=True)

    # 上次运行到的位置（用于继续作业）
    last_completed_step = Column(Integer, nullable=False, default=0)
    last_completed_time = Column(DateTime(timezone=False), nullable=True)

    # 时间戳
    created_at = Column(DateTime(timezone=False), server_default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=False), server_default=func.now(), onupdate=func.now())

    # 关系
    template = relationship("AnalysisTemplate", backref="simulation_runs")
    steps = relationship("SimulationStep", backref="run", cascade="all, delete-orphan",
                         order_by="SimulationStep.step_number")

    __table_args__ = (
        Index('idx_simrun_template', 'template_id'),
        Index('idx_simrun_status', 'status'),
    )

    def __repr__(self):
        return f"<SimulationRun(uuid='{self.run_uuid}', status='{self.status}', step={self.current_step}/{self.total_steps})>"

    def to_dict(self):
        return {
            "id": self.id,
            "run_uuid": self.run_uuid,
            "template_id": self.template_id,
            "simulation_config": self.simulation_config,
            "status": self.status,
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "current_time": self.current_time.isoformat() if self.current_time else None,
            "sim_start_time": self.sim_start_time.isoformat() if self.sim_start_time else None,
            "sim_end_time": self.sim_end_time.isoformat() if self.sim_end_time else None,
            "last_completed_step": self.last_completed_step,
            "last_completed_time": self.last_completed_time.isoformat() if self.last_completed_time else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "step_count": len(self.steps) if self.steps else 0,
        }


class SimulationStep(Base):
    """
    模拟步进记录 - 每一步的数据快照和诊断结果
    """
    __tablename__ = "simulation_step"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("simulation_run.id"), nullable=False, index=True)

    step_number = Column(Integer, nullable=False)
    step_time = Column(DateTime(timezone=False), nullable=False)  # 该步骤对应的时间点

    # 窗口时间范围
    window_start = Column(DateTime(timezone=False), nullable=True)
    window_end = Column(DateTime(timezone=False), nullable=True)

    # 是否进行了VLM诊断
    has_vlm_diagnosis = Column(Boolean, default=False, nullable=False)

    # 图表base64（该窗口的时空对齐图）
    chart_image_b64 = Column(Text, nullable=True)

    # VLM诊断结果
    vlm_detection_result = Column(Text, nullable=True)
    vlm_process_result = Column(Text, nullable=True)
    vlm_decision_result = Column(Text, nullable=True)

    # 数据摘要（该窗口内的关键统计信息）
    data_summary = Column(JSON, nullable=True)

    # 状态
    status = Column(String(20), nullable=False, default="completed")  # completed | failed

    # 时间戳
    created_at = Column(DateTime(timezone=False), server_default=func.now())

    __table_args__ = (
        Index('idx_simstep_run', 'run_id'),
        Index('idx_simstep_number', 'run_id', 'step_number'),
    )

    def __repr__(self):
        return f"<SimulationStep(run_id={self.run_id}, step={self.step_number}, time={self.step_time})>"

    def to_dict(self, include_chart: bool = True):
        result = {
            "id": self.id,
            "run_id": self.run_id,
            "step_number": self.step_number,
            "step_time": self.step_time.isoformat() if self.step_time else None,
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None,
            "has_vlm_diagnosis": self.has_vlm_diagnosis,
            "vlm_detection_result": self.vlm_detection_result,
            "vlm_process_result": self.vlm_process_result,
            "vlm_decision_result": self.vlm_decision_result,
            "data_summary": self.data_summary,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
        if include_chart:
            result["chart_image_b64"] = self.chart_image_b64
        else:
            result["chart_image_b64"] = None
        return result
