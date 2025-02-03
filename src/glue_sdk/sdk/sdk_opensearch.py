from __future__ import annotations
from typing import Optional, TYPE_CHECKING
from ..core.decorators.decorators import singleton

if TYPE_CHECKING:
    from ..containers import ApplicationContainer
    from ..containers.opensearch_container import OpenSearchContainer
    from opensearchpy import OpenSearch
    from ..opensearch.opensearch_service import OpenSearchService


class SdkOpenSearchError(Exception):
    pass


@singleton
class SdkOpenSearch:
    def __init__(self, container: "ApplicationContainer") -> None:
        self.container: "ApplicationContainer" = container
        self._opensearch_container: Optional["OpenSearchContainer"] = None

    @property
    def opensearch_container(self) -> OpenSearchContainer:
        if not self._opensearch_container:
            print("DEBUG: self.container.opensearch:", self.container.opensearch)
            opc: Optional[OpenSearchContainer] = self.container.opensearch()  # type: ignore
            if not opc:
                raise SdkOpenSearchError("Cant intialize container.opensearch()")
            self._opensearch_container = opc

        return self._opensearch_container

    @property
    def client(self) -> OpenSearch:
        return self.container.opensearch.client()

    @property
    def client_factory(self) -> OpenSearch:
        return self.container.opensearch.client_factory()

    @property
    def opensearch_service(self) -> OpenSearchService:
        return self.container.opensearch.service()

    @property
    def config(self):
        config = self.container.opensearch.config()
        if not config:
            raise SdkOpenSearchError("Cant get config from OpenSerach")
        return self.container.opensearch.config()
