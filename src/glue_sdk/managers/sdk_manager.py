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
class SdkManager():
    def __init__(self) -> None:
        self._master_config: Optional[MasterConfig] = None
        self._container: Optional['ApplicationContainer'] = None
        self._opensearch: Optional['SdkOpenSearch'] = None
        self.aws_services_to_use = AwsServicesToUse()
        self._config_sdk: Optional['SdkConfig'] = None
        
        
    def initialize(self) -> None:
        from ..containers import ApplicationContainer
        self._container = ApplicationContainer()
        self._config_sdk = SdkConfig(container=self.container,
                                     aws_services_to_use=self.aws_services_to_use)
    
    @property
    def config(self) -> 'SdkConfig':
        if not self._config_sdk:
            raise SdkManagerError("U must call initialize func firstlly")
        return self._config_sdk
    
    @property
    def container(self) -> 'ApplicationContainer':
        if not self._container:
            raise SdkManagerError("U must call initialize func firstlly")
        return self._container
        
    @property
    def opensearch(self) -> 'SdkOpenSearch':
        if not self.aws_services_to_use.use_opensearch:
            raise SdkManagerError("U have to enable OpenSearch resource")
        if not self._opensearch:
            self._opensearch = SdkOpenSearch(container=self.container)
        if not self._opensearch:
            raise SdkManagerError("opensearch sdk is not initialized")
        return self._opensearch