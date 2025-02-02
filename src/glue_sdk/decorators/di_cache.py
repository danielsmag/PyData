from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Callable, ParamSpec, TypeVar

from ..core.shared import SharedUtilsSettings

from ..cache.i_cache import ICache

if TYPE_CHECKING:
    pass

P = ParamSpec("P")
R = TypeVar("R")

shared_settings = SharedUtilsSettings()


def cache_obj(func: Callable[P, R]):
    """
    Decorator that injects the cache into the decorated function.
    Sets the 'cache' keyword argument.
    The injected object must implement ICache (from interfaces.i_cache import ICache).
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> R:
        cache_obj: ICache = shared_settings.container.cache.cache()
        kwargs["cache_obj"] = cache_obj
        # kwargs["cache_obj"] = Provide[ApplicationContainer.cache.cache]()
        return func(*args, **kwargs)

    return wrapper
