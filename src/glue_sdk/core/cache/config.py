from pydantic_settings import BaseSettings,SettingsConfigDict
from pydantic import Field
from typing import Optional

class CacheConfig(BaseSettings):
    model_config = SettingsConfigDict(
        frozen=False,           
        case_sensitive=False,  
        extra="allow"
    )
    cache_timeout: Optional[int] = Field(default=None, description="Cache timeout for shared data service")


