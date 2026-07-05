"""
配置管理 API
提供运行时配置的查询、修改、重载、重置能力。
"""
from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field
from typing import Any, Dict, List

from backend.config.config_loader import config
from backend.config.schemas import AppConfig


router = APIRouter(tags=["配置管理"])


class ConfigUpdateRequest(BaseModel):
    """配置更新请求"""
    updates: Dict[str, Any] = Field(..., description="配置更新项，key 为点分路径，如 {'llm.temperature': 0.7}")


class ConfigSectionRequest(BaseModel):
    """配置 section 更新请求"""
    config: Dict[str, Any] = Field(..., description="完整的 section 配置字典")


class ConfigUpdateResponse(BaseModel):
    """配置更新响应"""
    success: bool
    changed_keys: List[str]
    hot_reload_keys: List[str]
    restart_required_keys: List[str]
    requires_restart: bool
    message: str


class ConfigReloadResponse(BaseModel):
    """配置重载响应"""
    success: bool
    message: str


@router.get("/config", summary="获取完整配置", response_model=Dict[str, Any])
async def get_config():
    """
    获取当前完整配置（不含敏感信息如密码）
    """
    cfg = config.to_dict()
    # 隐藏敏感字段
    if "database" in cfg and "password" in cfg["database"]:
        cfg["database"]["password"] = "******"
    if "minio" in cfg and "secret_key" in cfg["minio"]:
        cfg["minio"]["secret_key"] = "******"
    return cfg


@router.get("/config/{section}", summary="获取指定 section 配置", response_model=Dict[str, Any])
async def get_config_section(section: str):
    """
    获取指定 section 的配置
    
    Args:
        section: section 名称，如 llm、websocket、thickness_map
    """
    if section not in AppConfig.model_fields:
        raise HTTPException(status_code=404, detail=f"配置 section '{section}' 不存在")
    
    section_config = config.get(section)
    if section_config is None:
        raise HTTPException(status_code=404, detail=f"配置 section '{section}' 未找到")
    
    result = section_config.model_dump() if hasattr(section_config, "model_dump") else section_config
    
    # 隐藏敏感字段
    if section == "database" and "password" in result:
        result["password"] = "******"
    if section == "minio" and "secret_key" in result:
        result["secret_key"] = "******"
    
    return result


@router.patch("/config", summary="批量更新配置", response_model=ConfigUpdateResponse)
async def patch_config(request: ConfigUpdateRequest):
    """
    批量更新配置项，修改后自动持久化到 config.yml。
    
    示例请求体：
    ```json
    {
        "updates": {
            "llm.temperature": 0.7,
            "thickness_map.target_thickness": 0.05,
            "websocket.update_interval": 5
        }
    }
    ```
    """
    if not request.updates:
        raise HTTPException(status_code=400, detail="updates 不能为空")
    
    try:
        summary = config.update(request.updates)
        
        message_parts = []
        if summary["hot_reload_keys"]:
            message_parts.append(f"以下参数已热生效: {', '.join(summary['hot_reload_keys'])}")
        if summary["restart_required_keys"]:
            message_parts.append(f"以下参数需重启服务后生效: {', '.join(summary['restart_required_keys'])}")
        
        message = "; ".join(message_parts) if message_parts else "配置更新成功"
        
        return ConfigUpdateResponse(
            success=True,
            changed_keys=summary["changed_keys"],
            hot_reload_keys=summary["hot_reload_keys"],
            restart_required_keys=summary["restart_required_keys"],
            requires_restart=summary["requires_restart"],
            message=message,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"配置校验失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新配置失败: {str(e)}")


@router.put("/config/{section}", summary="完整替换 section 配置", response_model=ConfigUpdateResponse)
async def put_config_section(section: str, request: ConfigSectionRequest):
    """
    完整替换某个 section 的配置
    
    Args:
        section: section 名称，如 llm、websocket
    """
    if not hasattr(AppConfig, section):
        raise HTTPException(status_code=404, detail=f"配置 section '{section}' 不存在")
    
    try:
        summary = config.update_section(section, request.config)
        
        message_parts = []
        if summary["hot_reload_keys"]:
            message_parts.append(f"以下参数已热生效: {', '.join(summary['hot_reload_keys'])}")
        if summary["restart_required_keys"]:
            message_parts.append(f"以下参数需重启服务后生效: {', '.join(summary['restart_required_keys'])}")
        
        message = "; ".join(message_parts) if message_parts else "配置更新成功"
        
        return ConfigUpdateResponse(
            success=True,
            changed_keys=summary["changed_keys"],
            hot_reload_keys=summary["hot_reload_keys"],
            restart_required_keys=summary["restart_required_keys"],
            requires_restart=summary["requires_restart"],
            message=message,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"配置校验失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新配置失败: {str(e)}")


@router.post("/config/reload", summary="从 config.yml 重新加载配置", response_model=ConfigReloadResponse)
async def reload_config():
    """
    从 config.yml 重新加载配置。注意：此操作会覆盖当前内存中的配置。
    """
    try:
        config.reload()
        return ConfigReloadResponse(
            success=True,
            message="配置已从 config.yml 重新加载",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"重新加载配置失败: {str(e)}")


@router.post("/config/reset", summary="重置为默认配置", response_model=ConfigUpdateResponse)
async def reset_config():
    """
    重置所有配置为默认值，并持久化到 config.yml。
    注意：此操作通常需要重启服务才能完全生效。
    """
    try:
        summary = config.reset_to_defaults()
        return ConfigUpdateResponse(
            success=True,
            changed_keys=summary["changed_keys"],
            hot_reload_keys=[],
            restart_required_keys=summary["changed_keys"],
            requires_restart=True,
            message=summary["message"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"重置配置失败: {str(e)}")
