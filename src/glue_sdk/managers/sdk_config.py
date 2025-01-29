from ..core.decorators.decorators import singelton
from ..core.shared import AwsServicesToUse
from pydantic import validate_call
from typing import Any, Dict, Optional, TYPE_CHECKING
from ..core.master import MasterConfig 
 
if TYPE_CHECKING:
    from ..containers import ApplicationContainer

class SdkConfigError(Exception):
    pass

@singelton
class SdkConfig():
    def __init__(self,container,aws_services_to_use) -> None:
        self._opensearch = None
        self.aws_services_to_use: 'AwsServicesToUse' = aws_services_to_use
        self.container:'ApplicationContainer'=container
        
    @validate_call
    def set_config_application(self,config_data: Dict = {}) -> None:
        """Set main config for sdk """
        validate_config = MasterConfig(**config_data)
        self._master_config = validate_config
        validated_dict: Dict = validate_config.model_dump()
        self.container.config.from_dict(validated_dict)
    
    @validate_call
    def set_spark_conf(self,config_data: Dict = {}) -> None:
        """
        set all spark config before load spark client
        """
        
        self.container.spark.dynamic_configs_spark_client.override(provider=config_data)

    @property
    def use_opensearch(self) -> bool:
        return self.aws_services_to_use.use_opensearch
    
    @use_opensearch.setter
    @validate_call
    def use_opensearch(self,v: bool) -> None:
        from .sdk_opensearch import SdkOpenSearch
        self.aws_services_to_use.use_opensearch = v
        if self.aws_services_to_use.use_opensearch:
            self.aws_services_to_use.use_opensearch=True
            self._opensearch = SdkOpenSearch(container=self.container)
    
    @property
    def use_aurora_pg(self) -> bool:
        return self.aws_services_to_use.use_aurora_pg
    
    @use_aurora_pg.setter
    @validate_call
    def use_aurora_pg(self,v: bool) -> None:
        self.aws_services_to_use.use_aurora_pg  = v
    
    @property
    def use_data_catalog(self) -> bool:
        return self.aws_services_to_use.use_data_catalog
    
    @use_data_catalog.setter
    @validate_call
    def use_data_catalog(self,v: bool) -> None:
        self.aws_services_to_use.use_data_catalog = v
    