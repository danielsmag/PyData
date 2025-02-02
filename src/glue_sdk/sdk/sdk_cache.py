from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Optional

from ..core.utils.utils import SingletonMeta
from ..core.shared import ServicesEnabled,SharedUtilsSettings

if TYPE_CHECKING:
    from ..cache.shared_data_service import SharedDataService
    from ..containers import ApplicationContainer
    from ..containers.cache_container import CacheContainer
    from ..core.shared import ServicesEnabled

class SdkCacheError(Exception):
    pass


class SdkCache(metaclass=SingletonMeta):
    def __init__(self, services_enabled: SharedUtilsSettings) -> None:
        self.services_enabled: ApplicationContainer = container
        self._cache_container: Optional[CacheContainer] = None

    @cached_property
    def cache_obj(self) -> "SharedDataService":
        return self.container.cache.cache()

    @classmethod
    def reset(cls) -> None:
        """
        Reset the singleton instance.

        This method is thread-safe and is useful for testing or when a full reset
        of the singleton is required.
        """
        with SingletonMeta._lock:
            if cls in SingletonMeta._instances:
                del SingletonMeta._instances[cls]