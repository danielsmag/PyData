from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, ParamSpec, TypeVar

from ..core.shared import ServicesEnabled
from ..core.utils.utils import SingletonMeta

if TYPE_CHECKING:
    pass

P = ParamSpec("P")
R = TypeVar("R")

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
    def cache_obj(self) -> Callable[[Callable[P, R]], Callable[P, R]]:
        from ..decorators import cache_obj

        if not self.__get_services_enabled().USE_CACHE:
            raise SDKdDecoratorsError("U must enable cache resource")
        return cache_obj

    @cached_property
    def spark_context(self) -> Callable[[Callable[P, R]], Callable[P, R]]:
        from ..decorators import spark_context

        if not self.__get_services_enabled().USE_SPARK:
            raise SDKdDecoratorsError("U must enable cache resource")
        return spark_context
