from pydantic import validate_call
from typing import Any, Dict, Optional, TYPE_CHECKING
from glue_sdk.containers.opensearch_container import OpenSearchContainer
from ..core.decorators.decorators import singleton


if TYPE_CHECKING:
    from ..containers import ApplicationContainer
    from ..containers.opensearch_container import OpenSearchContainer
    from opensearchpy import OpenSearch

class SdkOpenSearchError(Exception):
    pass

@singleton
class SdkOpenSearch():
    def __init__(self,container: 'ApplicationContainer') -> None:
        self.container: 'ApplicationContainer' = container
        self._opensearch_container: Optional['OpenSearchContainer']= None
        
    @property
    def opensearch_container(self)->'OpenSearchContainer':
        if not self._opensearch_container:
            opc: Optional['OpenSearchContainer'] = self.container.opensearch() # type: ignore
            if opc:
                self._opensearch_container=opc
            raise SdkOpenSearchError("Cant intialize container.opensearch()")
        return self._opensearch_container
        
    @property    
    def client(self)->'OpenSearch':
        return self.opensearch_container.opensearch_client()