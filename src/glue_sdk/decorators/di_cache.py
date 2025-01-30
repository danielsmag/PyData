from dependency_injector.wiring import inject, Provide
from typing import List,Callable, TYPE_CHECKING, Any
import functools
from glue_sdk.containers.application_container import ApplicationContainer

if TYPE_CHECKING:
    from glue_sdk.interfaces.i_cache import ICache
    

def cache(func: Callable):
    """
    Decorator that injects the cache into the decorated function.
    Sets the 'cache' keyword argument.
    The injected object must implement ICache (from interfaces.i_cache import ICache).
    """
   
    @functools.wraps(func)
    @inject
    def wrapper(
        *args,
        cache: "ICache" = Provide[ApplicationContainer.cache.cache](),
        **kwargs
    ) -> Any:
        kwargs["cache"] = cache
        return func(*args, **kwargs)

    return wrapper

