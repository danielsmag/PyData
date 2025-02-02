from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Optional
from ..core.utils.utils import SingletonMeta

if TYPE_CHECKING:
    from ..containers import ApplicationContainer
    from ..core.shared import ServicesEnabled
    from ..glue_data_catalog.data_catalog_service import DataCatalogService
    
    
class SDKDataCatalogError(Exception):
    pass


class SDKDataCatalog(metaclass=SingletonMeta):
    def __init__(self,
                services_enabled: ServicesEnabled,
                container: ApplicationContainer 
                ) -> None:
        self._services_enabled: ServicesEnabled = services_enabled
        self._container: ApplicationContainer = container

    @cached_property
    def data_catalog_service(self) -> DataCatalogService:
        return self._container.data_catalog.data_catalog_service()
