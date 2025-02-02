from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Optional
from ..core.utils.utils import SingletonMeta

if TYPE_CHECKING:
    from ..cache.shared_data_service import SharedDataService
    from ..containers import ApplicationContainer
    from ..containers.cache_container import CacheContainer


class SDKDataCatalogError(Exception):
    pass


class SDKDataCatalog(metaclass=SingletonMeta):
    def __init__(self, container: ApplicationContainer) -> None:
        self.container: ApplicationContainer = container
        self._cache_container: Optional[CacheContainer] = None

    @cached_property
    def cache_obj(self) -> "SharedDataService":
        return self.container.cache.cache()
