"""
配置加载器 - 从 config.yml 加载配置
"""
import os
import yaml
from pathlib import Path
from typing import Any, Dict
from dotenv import load_dotenv


class ConfigLoader:
    """配置加载器"""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):
        """加载配置文件"""
        # 项目根目录
        self.project_root = Path(__file__).parent.parent.parent
        
        # 加载 .env（仅 Token）
        env_path = self.project_root / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        
        # 加载 config.yml
        config_path = self.project_root / "config.yml"
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self._config = yaml.safe_load(f)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            key_path: 配置路径，如 "database.host"
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return default
            
            if value is None:
                return default
        
        return value
    
    def get_token(self, key: str, default: str = None) -> str:
        """
        获取 Token（从环境变量）
        
        Args:
            key: 环境变量名
            default: 默认值
            
        Returns:
            Token 值
        """
        return os.getenv(key, default)
    
    @property
    def database(self) -> Dict[str, Any]:
        """数据库配置"""
        return self._config.get('database', {})
    
    @property
    def minio(self) -> Dict[str, Any]:
        """MinIO 配置"""
        return self._config.get('minio', {})
    
    @property
    def kafka(self) -> Dict[str, Any]:
        """Kafka 配置"""
        return self._config.get('kafka', {})
    
    @property
    def api(self) -> Dict[str, Any]:
        """API 配置"""
        return self._config.get('api', {})
    
    @property
    def cnn_api(self) -> Dict[str, Any]:
        """CNN API 配置"""
        return self._config.get('cnn_api', {})
    
    @property
    def llm(self) -> Dict[str, Any]:
        """LLM 配置"""
        return self._config.get('llm', {})
    
    @property
    def websocket(self) -> Dict[str, Any]:
        """WebSocket 配置"""
        return self._config.get('websocket', {})


# 全局配置实例
config = ConfigLoader()
