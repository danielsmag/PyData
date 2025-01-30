from pydantic import validate_call
from typing import Any, Dict, Optional, TYPE_CHECKING


from ..core.decorators.decorators import singelton
from ..core.shared import AwsServicesToUse
from .sdk_config import SdkConfig

if TYPE_CHECKING:
    from ..containers import ApplicationContainer
    from .sdk_opensearch import SdkOpenSearch
    from ..core.master import MasterConfig 
    
class SdkManagerError(Exception):
    pass
    
@singelton
class SdkManager:
    _master_config: Optional['MasterConfig']
    _container: Optional['ApplicationContainer']
    _opensearch: Optional['SdkOpenSearch']
    aws_services_to_use: 'AwsServicesToUse'
    
    
    def __init__(self,
                 config:Optional[SdkConfig] = None
                 ) -> None:
        self._master_config = None
        self._container = None
        self._opensearch = None
        self.aws_services_to_use = AwsServicesToUse()
        self._config: Optional[SdkConfig] = None
       
        if isinstance(config, SdkConfig):
            self._config = config
        else:
            self._config = SdkConfig()        
        
    def initialize(self) -> None:
        from ..containers import ApplicationContainer
        self._container = ApplicationContainer()
        self.container.spark.dynamic_configs_spark_client.override(provider=self.config._spark_conf)
        
    @property
    def config(self) -> SdkConfig:
        if not self._config:
            self._config = SdkConfig()
        if not self._config:    
            raise SdkManagerError("Cant intialize SdkConfig")
        return self._config
    
   
    @config.setter
    def config(self, v: Optional[SdkConfig]) ->None:
        print(f"Debug: SdkConfig Type: {type(SdkConfig)}")  
        print(f"Debug: Config Value: {v}, Type: {type(v)}") 
        if isinstance(v, SdkConfig): 
            self._config = v
        else:
            raise SdkManagerError("Invalid configuration type")
        
    @property
    def container(self) -> 'ApplicationContainer':
        if not self._container:
            raise SdkManagerError("U must call initialize func firstlly")
        return self._container
        
    @property
    def opensearch(self) -> 'SdkOpenSearch':
        if not self.aws_services_to_use.USE_OPENSEARCH:
            raise SdkManagerError("U have to enable OpenSearch resource")
        if not self._opensearch:
            self._opensearch = SdkOpenSearch(container=self.container)
        if not self._opensearch:
            raise SdkManagerError("opensearch sdk is not initialized")
        return self._opensearch