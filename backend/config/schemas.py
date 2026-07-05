"""
配置模型 - 使用 Pydantic 校验配置
"""
from typing import List, Literal, Optional
from pydantic import BaseModel, Field, field_validator


class DatabaseConfig(BaseModel):
    """数据库配置"""
    host: str = Field(default="localhost", description="数据库主机")
    port: int = Field(default=5432, ge=1, le=65535, description="数据库端口")
    name: str = Field(default="tsdb", description="数据库名")
    user: str = Field(default="user", description="数据库用户")
    password: str = Field(default="123456", description="数据库密码")
    pool_size: int = Field(default=10, ge=1, le=100, description="连接池大小")
    max_overflow: int = Field(default=20, ge=0, le=100, description="最大溢出连接数")
    pool_recycle: int = Field(default=3600, ge=0, description="连接回收时间（秒）")


class MinioConfig(BaseModel):
    """MinIO 配置"""
    endpoint: str = Field(default="127.0.0.1:9000", description="MinIO endpoint")
    access_key: str = Field(default="minioadmin", description="Access Key")
    secret_key: str = Field(default="Minio@123456", description="Secret Key")
    bucket_name: str = Field(default="test-bucket", description="Bucket 名称")
    secure: bool = Field(default=False, description="是否使用 HTTPS")


class KafkaConfig(BaseModel):
    """Kafka 配置"""
    bootstrap_servers: str = Field(default="localhost:9092", description="Kafka 地址")
    sensor_topic: str = Field(default="sensor_data", description="传感器数据 topic")
    detection_topic: str = Field(default="detection_data", description="检测数据 topic")
    sensor_producer_interval: float = Field(default=1.0, ge=0.1, le=3600, description="传感器数据生产间隔（秒）")
    detection_producer_interval: float = Field(default=1.0, ge=0.1, le=3600, description="检测数据生产间隔（秒）")
    consumer_poll_timeout_ms: int = Field(default=1000, ge=100, le=60000, description="消费者 poll 超时（毫秒）")


class ApiConfig(BaseModel):
    """API 服务配置"""
    host: str = Field(default="0.0.0.0", description="监听地址")
    port: int = Field(default=8002, ge=1, le=65535, description="监听端口")


class CnnApiConfig(BaseModel):
    """CNN API 配置"""
    host: str = Field(default="0.0.0.0", description="CNN 服务主机")
    port: int = Field(default=8003, ge=1, le=65535, description="CNN 服务端口")
    script: str = Field(default="cnnfast.py", description="CNN 启动脚本")
    project_dir: str = Field(default="cnn_image_classification", description="CNN 项目目录")


class ThicknessMapConfig(BaseModel):
    """膜厚云图生成参数"""
    target_thickness: float = Field(default=0.045, ge=0.001, le=1.0, description="目标膜厚（mm）")
    thickness_range: float = Field(default=0.005, ge=0.0001, le=1.0, description="颜色范围 ±value")
    record_count: int = Field(default=40, ge=10, le=500, description="每次读取最新 N 条检测记录")
    pipeline_interval: int = Field(default=20, ge=5, le=3600, description="云图生成管道执行间隔（秒）")
    mask_preserve_rows: int = Field(default=4, ge=0, le=50, description="mask 永远不遮挡最后 N 行")
    reduce_mask_ratio: float = Field(default=0.1, ge=0.0, le=1.0, description="正常时每次减少 mask 的比例")
    dpi: int = Field(default=600, ge=72, le=2400, description="输出图片 DPI")
    figsize: List[float] = Field(default=[10, 6], description="基础云图尺寸")
    combined_figsize: List[float] = Field(default=[16, 16], description="组合图尺寸")

    @field_validator('figsize', 'combined_figsize')
    @classmethod
    def validate_figsize(cls, v: List[float]) -> List[float]:
        if len(v) != 2:
            raise ValueError("figsize 必须包含两个数值 [width, height]")
        if v[0] <= 0 or v[1] <= 0:
            raise ValueError("figsize 的宽度和高度必须大于 0")
        return v


class CnnDiagnosisConfig(BaseModel):
    """CNN 异常检测参数"""
    enabled: bool = Field(default=True, description="是否启用 CNN 异常检测")
    confidence_threshold: float = Field(default=0.5, ge=0.0, le=1.0, description="置信度阈值")
    fallback_on_error: bool = Field(default=True, description="CNN 失败时默认正常")
    timeout: float = Field(default=30.0, ge=1.0, le=300.0, description="CNN 推理超时（秒）")


class LlmConfig(BaseModel):
    """LLM Agent 参数（OpenAI 兼容格式）"""
    model_name: str = Field(default="qwen3-vl-plus", description="模型名称")
    base_url: str = Field(default="https://dashscope.aliyuncs.com/compatible-mode/v1", description="OpenAI 兼容 API 地址")
    api_key_env: str = Field(default="DASHSCOPE_API_KEY", description="API Key 所在环境变量名")
    temperature: float = Field(default=1.0, ge=0.0, le=2.0, description="生成温度")
    max_tokens: int = Field(default=5000, ge=1, le=100000, description="最大生成 tokens 数")
    timeout: float = Field(default=120.0, ge=1.0, le=600.0, description="LLM 调用超时（秒）")
    use_rag: bool = Field(default=True, description="是否启用 RAG")
    rag_top_k: int = Field(default=3, ge=1, le=20, description="RAG 检索条数")
    rag_query_max_length: int = Field(default=500, ge=100, le=2000, description="RAG 查询最大长度")
    detect_agent_enabled: bool = Field(default=True, description="检测 Agent 开关")
    process_agent_enabled: bool = Field(default=True, description="工艺 Agent 开关")
    decision_agent_enabled: bool = Field(default=True, description="决策 Agent 开关")


class WebSocketConfig(BaseModel):
    """WebSocket 参数"""
    update_interval: int = Field(default=10, ge=1, le=3600, description="云图广播间隔（秒）")
    heartbeat_interval: int = Field(default=30, ge=1, le=3600, description="心跳间隔（秒）")


class SystemConfig(BaseModel):
    """系统参数"""
    timezone: str = Field(default="Asia/Shanghai", description="时区")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO", description="日志级别")


class AppConfig(BaseModel):
    """应用总配置"""
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    minio: MinioConfig = Field(default_factory=MinioConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    cnn_api: CnnApiConfig = Field(default_factory=CnnApiConfig)
    thickness_map: ThicknessMapConfig = Field(default_factory=ThicknessMapConfig)
    cnn_diagnosis: CnnDiagnosisConfig = Field(default_factory=CnnDiagnosisConfig)
    llm: LlmConfig = Field(default_factory=LlmConfig)
    websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
    system: SystemConfig = Field(default_factory=SystemConfig)


# 热生效参数：修改后不需要重启即可生效
HOT_RELOAD_PARAMS = {
    "thickness_map",
    "websocket",
    "cnn_diagnosis",
    "kafka.sensor_producer_interval",
    "kafka.detection_producer_interval",
    "kafka.consumer_poll_timeout_ms",
    "llm.temperature",
    "llm.max_tokens",
    "llm.timeout",
    "llm.use_rag",
    "llm.rag_top_k",
    "llm.rag_query_max_length",
    "llm.detect_agent_enabled",
    "llm.process_agent_enabled",
    "llm.decision_agent_enabled",
    "system.log_level",
}

# 需重启参数：修改后必须重启服务才能生效
RESTART_REQUIRED_PARAMS = {
    "database.host",
    "database.port",
    "database.name",
    "database.user",
    "database.password",
    "database.pool_size",
    "database.max_overflow",
    "database.pool_recycle",
    "minio",
    "api.host",
    "api.port",
    "cnn_api.host",
    "cnn_api.port",
    "kafka.bootstrap_servers",
    "kafka.sensor_topic",
    "kafka.detection_topic",
    "llm.model_name",
    "llm.base_url",
    "llm.api_key_env",
    "system.timezone",
}


def is_hot_reload_param(key_path: str) -> bool:
    """
    判断参数是否为热生效参数
    
    Args:
        key_path: 配置路径，如 "thickness_map.target_thickness"
        
    Returns:
        是否热生效
    """
    # 检查精确匹配
    if key_path in HOT_RELOAD_PARAMS:
        return True
    
    # 检查前缀匹配（section 级别）
    for prefix in HOT_RELOAD_PARAMS:
        if key_path.startswith(prefix + "."):
            return True
    
    return False


def is_restart_required_param(key_path: str) -> bool:
    """
    判断参数是否为需重启参数
    
    Args:
        key_path: 配置路径
        
    Returns:
        是否需重启
    """
    if key_path in RESTART_REQUIRED_PARAMS:
        return True
    
    for prefix in RESTART_REQUIRED_PARAMS:
        if key_path.startswith(prefix + "."):
            return True
    
    return False
