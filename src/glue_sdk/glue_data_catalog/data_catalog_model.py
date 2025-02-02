from typing import Optional,Dict,List, Any
import datetime
from pydantic import BaseModel,ConfigDict

__all__:List[str] = [
    "DataCatalogDatabase",
    "DataCatalogTable"
]

class DataCatalogDatabase(BaseModel):
    model_config = ConfigDict(frozen=True)
    
    name: str
    description: Optional[str] = None
    location_uri: Optional[str] = None
    parameters: Dict[str, str] = {}
    create_time: Optional[datetime.datetime] = None
    update_time: Optional[datetime.datetime] = None
    catalog_id: Optional[str] = None
    owner: Optional[str] = None
    version_id: Optional[str] = None


class DataCatalogTable(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    database: DataCatalogDatabase
    description: Optional[str] = None
    table_type: Optional[str] = None
    parameters: Dict[str, str] = {}
    create_time: Optional[datetime.datetime] = None
    update_time: Optional[datetime.datetime] = None
    owner: Optional[str] = None
    last_access_time: Optional[datetime.datetime] = None
    retention: Optional[int] = None
    storage_descriptor: Optional[Dict[str, Any]] = None
    view_original_text: Optional[str] = None
    view_expanded_text: Optional[str] = None
    catalog_id: Optional[str] = None
    version_id: Optional[str] = None
