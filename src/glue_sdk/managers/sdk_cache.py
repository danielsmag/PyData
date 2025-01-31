from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Optional

from ..core.decorators.decorators import singleton

if TYPE_CHECKING:
    from ..cache.services.shared_data_service import SharedDataService
    from ..containers import ApplicationContainer
    from ..containers.cache_container import CacheContainer


class SdkCacheError(Exception):
    pass


@singleton
class SdkCache:
    def __init__(self, container: ApplicationContainer) -> None:
        self.container: ApplicationContainer = container
        self._cache_container: Optional[CacheContainer] = None

    @cached_property
    def cache_obj(self) -> "SharedDataService":
        return self.container.cache.cache()
