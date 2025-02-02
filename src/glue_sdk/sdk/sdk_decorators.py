from __future__ import annotations
from typing import Any, Dict, Optional, TYPE_CHECKING, Callable
from ..core.utils.utils import SingletonMeta
from ..core.shared import ServicesEnabled
from functools import cached_property

if TYPE_CHECKING:
    pass


class SDKdDecoratorsError(Exception):
    pass


class SDKdDecorators(metaclass=SingletonMeta):
    __services_enabled: Optional[ServicesEnabled] = None

    def __init__(self) -> None:
        self._services_enabled = None

    def __get_services_enabled(self) -> ServicesEnabled:
        """Private helper to lazily initialize and return the ServicesEnabled instance."""
        if self.__services_enabled is None:
            self.__services_enabled = ServicesEnabled()
        return self.__services_enabled

    @cached_property
    def cache_obj(self) -> Callable:
        from ..decorators import cache_obj

        if not self.__get_services_enabled().USE_CACHE:
            raise SDKdDecoratorsError("U must enable cache resource")
        return cache_obj
