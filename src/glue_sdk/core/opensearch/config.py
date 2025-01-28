from pydantic_settings import BaseSettings,SettingsConfigDict
from pydantic import Field
from typing import Optional


class OpenSearchConfig(BaseSettings):
    model_config = SettingsConfigDict(
        frozen=False,           
        case_sensitive=False,
        extra="allow"
    )
    index_name: str = Field(default="", description="Index name for OpenSearch")
    index_mapping_s3: Optional[str] = Field(default="", description="S3 path for index mapping")
    opensearch_connection_name: str = Field(description="Connection name for OpenSearch")
    opensearch_secret_name: str = Field(description="Secret name for OpenSearch")
    opensearch_host: str = Field(description="OpenSearch host URL")
    opensearch_batch_size_bytes: str = Field(default="10m", description="Batch size in bytes for OpenSearch")
    opensearch_batch_size_entries: int = Field(default=100, description="Batch size in entries for OpenSearch")
    opensearch_mapping_unique_id: Optional[str] = Field(default=None, description="Unique ID field for OpenSearch")
    pushdown: bool = Field(default=True, description="Enable pushdown for queries")
    es_nodes_wan_only: str = "true"
