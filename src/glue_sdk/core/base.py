from pydantic_settings import BaseSettings,SettingsConfigDict
from pydantic import Field,ConfigDict, BaseModel
from typing import Optional, List

class RdsConfig(BaseSettings):
    model_config = SettingsConfigDict(
        frozen=False,           
        case_sensitive=False,  
        extra="allow"
    )
    db_host: str
    db_name: str
    db_port: int = Field(default=5432)
    connection_name: Optional[str] = Field(default="", description="Connection name in aws glue")
    jdbc_url: Optional[str] = Field(default="", description="Jdbc url")
    secret: Optional[str] = Field(default="", description="Secrets in aws for user password")
    username: Optional[str] = Field(default="", description="Username if not provided secrets")
    password: Optional[str] = Field(default="", description="Password if not provided secrets")

class S3EndPoints(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        frozen=False,
        str_to_lower=True         
    )
    silver: Optional[str] = Field(default=None)
    bronze: Optional[str] = Field(default=None)
    gold: Optional[str] = Field(default=None)
    
    
class DataCatalog(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        frozen=False,
        str_to_lower=True         
    )
    bronze_db: Optional[str] = Field(default=None)
    bronze_tables: Optional[List[str]] = Field(default=None)
    silver_db: Optional[str] =Field(default=None)
    silver_tables:Optional[List[str]] =Field(default=None)
    gold_db: Optional[str] =Field(default=None)
    gold_tables:Optional[List[str]]=Field(default=None)

class Sources(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        frozen=False,
        str_to_lower=True         
    )
    data_catalog: Optional[DataCatalog] = Field(default=None)
    s3_endpoints: Optional[S3EndPoints] = Field(default=None)
    
class Output(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        frozen=False,
        str_to_lower=True         
    )
    data_catalog: Optional[DataCatalog]=Field(default=None)
    s3_endpoints: Optional[S3EndPoints]=Field(default=None)
 
    
