from pydantic_settings import BaseSettings,SettingsConfigDict
from pydantic import Field
from typing import Optional
from glue_sdk.core.base import RdsConfig

class AuroraConfig(RdsConfig):
    model_config = SettingsConfigDict(
        frozen=False,           
        case_sensitive=False,  
        extra="allow"
    )
    