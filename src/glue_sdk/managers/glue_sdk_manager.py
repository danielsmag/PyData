from pydantic import validate_call
from typing import Any, Dict, Optional

from ..containers import ApplicationContainer
from ..core.decorators.decorators import singelton
from ..core.master import MasterConfig 

@singelton
class SdkManager():
    def __init__(self) -> None:
        self._use_opensearch: bool = False
        self._aurora_pg: bool = False
        self._master_config: Optional[MasterConfig] = None
        self._container = ApplicationContainer()
        
     
    @property
    def container(self) -> 'ApplicationContainer':
        return self._container
        
    @property
    def use_opensearch(self) -> bool:
        return self._use_opensearch
    
    @use_opensearch.setter
    @validate_call
    def use_opensearch(self,v: bool) -> None:
        self._use_opensearch = v
    
    @property
    def aurora_pg(self) -> bool:
        return self._aurora_pg
    
    @aurora_pg.setter
    @validate_call
    def aurora_pg(self,v: bool) -> None:
        self._aurora_pg = v
    
    @validate_call
    def set_config_application(self,config_data: Dict = {}):
        """Set main config for sdk """
        validate_config = MasterConfig(**config_data)
        self._master_config = validate_config
        validated_dict: Dict = validate_config.model_dump()
        self._container.config.from_dict(validated_dict)