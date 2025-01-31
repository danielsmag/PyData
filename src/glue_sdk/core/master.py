from __future__ import annotations
from pydantic_settings import BaseSettings,SettingsConfigDict
from pydantic import Field,field_validator
from typing import Optional, TYPE_CHECKING,List,Dict, Any
from ..core.cache.config import CacheConfig
from ..core.base import Sources,Output
from .config_loader import ConfigLoader
from ..core.logging.logger import logger
from .decorators.decorators import singleton

if TYPE_CHECKING:
    from glue_sdk.core.opensearch.config import OpenSearchConfig


@singleton
class MasterConfig(BaseSettings):
    model_config = SettingsConfigDict(
        frozen=False,           
        case_sensitive=False,
        extra="allow"  
    )
    app_name: str = Field(default="default_app_name")
    env: str = Field(default="", description="Environment (e.g., dev, prod, staging, local)")
    version: str = Field(default="", description="Version of the configuration")
    opensearch: Optional["OpenSearchConfig"]=Field(default=None,description="Opensearch settings") 
    aws_region: str = Field(default="eu-west-1", description="AWS region")
    cache: CacheConfig = Field(default=CacheConfig(),description="cache settings")
    sources: Optional[Sources] = Field(default=None)
    output: Optional[Output] = Field(default=None)
    
    
    @field_validator("env", mode="before")
    @classmethod
    def coerce_env(cls, v: str) -> str:
        return v.lower()

    @field_validator("env")
    @classmethod
    def validate_env(cls, v: str) -> str:
        allowed_envs: List[str] = ["dev", "prod", "staging", "local","test"]
        if v not in allowed_envs:
            raise ValueError(f"Invalid env '{v}'. Must be one of: {', '.join(allowed_envs)}")
        return v

      
def get_settings_from_s3(env: str,
                        prefix: str,
                        version: str
                        ) -> MasterConfig:
    logger.debug(f"Loading YAML configuration from S3 with prefix: {prefix}")
    if env == "test":
        config_dict = {}
    else:
        ConfigLoader.load_config_yaml_from_s3(prefix=prefix)
        config_dict: Dict[str, Any] = ConfigLoader.get_config()
    logger.debug(f"Configuration loaded from S3: {config_dict}")

    global _singleton_settings
    with _lock:
        if _singleton_settings is None:
            config_dict['env'] = env
            config_dict['version'] = version
            settings_obj = MasterConfig(**config_dict)
            logger.debug(f"Settings object created: {settings_obj}")
            _singleton_settings = settings_obj

        return _singleton_settings


def get_settings() -> MasterConfig:
    global _singleton_settings
    with _lock:
        if _singleton_settings is None:
            raise RuntimeError("Must call get_settings_from_s3 before get_settings")
        return _singleton_settings

