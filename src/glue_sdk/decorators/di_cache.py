from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Callable, ParamSpec, TypeVar

from glue_sdk.containers.application_container import ApplicationContainer

from ..cache.i_cache import ICache

if TYPE_CHECKING:
    pass

P = ParamSpec("P")
R = TypeVar("R")

container = ApplicationContainer()


def cache_obj(func: Callable[P, R]) -> Callable[P, R]:
    """
    Decorator that injects the cache into the decorated function.
    Sets the 'cache' keyword argument.
    The injected object must implement ICache (from interfaces.i_cache import ICache).
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        container.cache.cache.reset_override()
        cache_obj: ICache = container.cache.cache()
        container.core.dynamic_configs_spark_client.reset_override()
        print(container.core.dynamic_configs_spark_client())
        kwargs["cache_obj"] = cache_obj
        # kwargs["cache_obj"] = Provide[ApplicationContainer.cache.cache]()
        return func(*args, **kwargs)

    return wrapper
