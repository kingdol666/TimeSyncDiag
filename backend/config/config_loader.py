"""
配置加载器 - 从 config.yml 加载、保存、管理配置
支持：
- 启动时从 config.yml 加载
- 运行时通过 API 修改并持久化到 config.yml
- 配置变更回调通知
- 原子写保证并发安全
"""
import os
import threading
import logging
import yaml
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from dotenv import load_dotenv

from pydantic import BaseModel
from .schemas import AppConfig, is_hot_reload_param, is_restart_required_param

logger = logging.getLogger(__name__)


class ConfigLoader:
    """配置加载器"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, config_path: Optional[Path] = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: Optional[Path] = None):
        # 避免重复初始化
        if self._initialized:
            return
        
        self._initialized = True
        self._callbacks: Dict[str, List[Callable[[str, Any], None]]] = {}
        self._callback_lock = threading.Lock()
        
        # 项目根目录
        self.project_root = Path(__file__).parent.parent.parent
        
        # 加载 .env（仅 Token）
        env_path = self.project_root / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        
        # 配置文件路径
        if config_path is None:
            config_path = self.project_root / "config.yml"
        self.config_path = Path(config_path)
        
        # 加载配置
        self._app_config: Optional[AppConfig] = None
        self._raw_config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        """从 config.yml 加载配置"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self._raw_config = yaml.safe_load(f) or {}
        
        # 使用 Pydantic 校验
        self._app_config = AppConfig(**self._raw_config)
    
    def load(self) -> AppConfig:
        """加载并返回当前配置"""
        self._load_config()
        return self._app_config
    
    def reload(self) -> AppConfig:
        """从文件重新加载配置"""
        return self.load()
    
    def save(self):
        """原子方式保存当前配置到 config.yml"""
        # 准备 YAML 内容
        raw_config = self._app_config.model_dump()
        yaml_content = yaml.safe_dump(raw_config, allow_unicode=True, sort_keys=False, default_flow_style=False)
        
        # 原子写：先写入临时文件，再重命名
        temp_path = self.config_path.with_suffix('.yml.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            f.write(yaml_content)
            f.flush()
            os.fsync(f.fileno())
        
        temp_path.replace(self.config_path)
        self._raw_config = raw_config
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        通过点分路径获取配置值
        
        Args:
            key_path: 配置路径，如 "database.host" 或 "llm.temperature"
            default: 默认值
            
        Returns:
            配置值
        """
        try:
            value = self._app_config
            for key in key_path.split('.'):
                if isinstance(value, BaseModel):
                    value = getattr(value, key)
                elif isinstance(value, dict):
                    value = value.get(key)
                else:
                    return default
            return value
        except (AttributeError, KeyError):
            return default
    
    def set(self, key_path: str, value: Any, save: bool = True) -> Dict[str, Any]:
        """
        设置单个配置值
        
        Args:
            key_path: 配置路径，如 "llm.temperature"
            value: 新值
            save: 是否立即保存到文件
            
        Returns:
            变更信息
        """
        return self.update({key_path: value}, save=save)
    
    def update(self, updates: Dict[str, Any], save: bool = True) -> Dict[str, Any]:
        """
        批量更新配置
        
        Args:
            updates: {key_path: new_value} 字典
            save: 是否立即保存到文件
            
        Returns:
            变更摘要
        """
        # 先构建新的 raw config
        new_raw_config = self._deep_copy(self._raw_config)
        
        changed_keys: List[str] = []
        for key_path, value in updates.items():
            self._set_nested_value(new_raw_config, key_path, value)
            changed_keys.append(key_path)
        
        # 用 Pydantic 校验新配置
        new_app_config = AppConfig(**new_raw_config)
        
        # 更新内存配置
        self._app_config = new_app_config
        self._raw_config = new_raw_config
        
        # 持久化
        if save:
            self.save()
        
        # 触发回调
        summary = {
            "changed_keys": changed_keys,
            "hot_reload_keys": [],
            "restart_required_keys": [],
            "requires_restart": False,
        }
        
        for key_path in changed_keys:
            if is_hot_reload_param(key_path):
                summary["hot_reload_keys"].append(key_path)
            elif is_restart_required_param(key_path):
                summary["restart_required_keys"].append(key_path)
                summary["requires_restart"] = True
            
            # 触发回调
            self._notify_callbacks(key_path, self.get(key_path))
        
        return summary
    
    def update_section(self, section: str, section_config: Dict[str, Any], save: bool = True) -> Dict[str, Any]:
        """
        完整替换某个 section 的配置
        
        Args:
            section: section 名称，如 "llm"
            section_config: 新的 section 配置字典
            save: 是否立即保存到文件
            
        Returns:
            变更摘要
        """
        # 构建 flat updates
        updates = {}
        for key, value in section_config.items():
            updates[f"{section}.{key}"] = value
        
        return self.update(updates, save=save)
    
    def reset_to_defaults(self, save: bool = True) -> Dict[str, Any]:
        """重置为默认配置"""
        default_config = AppConfig()
        self._app_config = default_config
        self._raw_config = default_config.model_dump()
        
        if save:
            self.save()
        
        # 触发所有热生效参数的回调
        for key_path in self._flatten_dict(self._raw_config):
            self._notify_callbacks(key_path, self.get(key_path))
        
        return {
            "changed_keys": list(self._flatten_dict(self._raw_config).keys()),
            "requires_restart": True,
            "message": "配置已重置为默认值，部分变更需要重启服务才能生效",
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """返回当前配置的 dict 形式"""
        return self._app_config.model_dump()
    
    @property
    def config(self) -> AppConfig:
        """返回 Pydantic AppConfig 对象"""
        return self._app_config
    
    def register_change_callback(self, key_prefix: str, callback: Callable[[str, Any], None]):
        """
        注册配置变更回调
        
        Args:
            key_prefix: 监听的前缀，如 "thickness_map" 或 "websocket.update_interval"
            callback: 回调函数，签名为 callback(key_path, new_value)
        """
        with self._callback_lock:
            if key_prefix not in self._callbacks:
                self._callbacks[key_prefix] = []
            self._callbacks[key_prefix].append(callback)
    
    def unregister_change_callback(self, key_prefix: str, callback: Callable[[str, Any], None]):
        """注销配置变更回调"""
        with self._callback_lock:
            if key_prefix in self._callbacks:
                self._callbacks[key_prefix] = [
                    cb for cb in self._callbacks[key_prefix] if cb != callback
                ]
    
    def _notify_callbacks(self, key_path: str, value: Any):
        """触发配置变更回调"""
        with self._callback_lock:
            callbacks = []
            for prefix, cbs in self._callbacks.items():
                if key_path == prefix or key_path.startswith(prefix + ".") or prefix.startswith(key_path + "."):
                    callbacks.extend(cbs)
            
            for callback in callbacks:
                try:
                    callback(key_path, value)
                except Exception as e:
                    # 回调异常不应影响主流程
                    logger.warning(f"配置变更回调异常 [{key_path}]: {e}")
    
    def _set_nested_value(self, d: Dict[str, Any], key_path: str, value: Any):
        """在嵌套 dict 中设置值"""
        keys = key_path.split('.')
        current = d
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value
    
    def _deep_copy(self, obj: Any) -> Any:
        """深拷贝"""
        import copy
        return copy.deepcopy(obj)
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '') -> Dict[str, Any]:
        """把嵌套 dict 展平为点分路径"""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self._flatten_dict(v, new_key))
            else:
                items[new_key] = v
        return items
    
    # ----- 便捷属性访问 -----
    
    @property
    def database(self):
        """数据库配置"""
        return self._app_config.database
    
    @property
    def minio(self):
        """MinIO 配置"""
        return self._app_config.minio
    
    @property
    def kafka(self):
        """Kafka 配置"""
        return self._app_config.kafka
    
    @property
    def api(self):
        """API 配置"""
        return self._app_config.api
    
    @property
    def cnn_api(self):
        """CNN API 配置"""
        return self._app_config.cnn_api
    
    @property
    def llm(self):
        """LLM 配置"""
        return self._app_config.llm
    
    @property
    def websocket(self):
        """WebSocket 配置"""
        return self._app_config.websocket
    
    @property
    def thickness_map(self):
        """膜厚云图配置"""
        return self._app_config.thickness_map
    
    @property
    def cnn_diagnosis(self):
        """CNN 诊断配置"""
        return self._app_config.cnn_diagnosis

    @property
    def qdrant(self):
        """Qdrant 配置"""
        return self._app_config.qdrant

    @property
    def custom_analysis(self):
        """自定义分析配置"""
        return self._app_config.custom_analysis

    @property
    def system(self):
        """系统配置"""
        return self._app_config.system
    
    def get_token(self, key: str, default: str = None) -> str:
        """获取 Token（从环境变量）"""
        return os.getenv(key, default)


# 全局配置实例
config = ConfigLoader()
