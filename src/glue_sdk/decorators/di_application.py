
from dependency_injector.wiring import inject, Provide
from typing import List,Callable, TYPE_CHECKING, Any
import functools
from glue_sdk.containers.application_container import ApplicationContainer
if TYPE_CHECKING:
    from glue_sdk.interfaces.i_cache import ICache
    from glue_sdk.core.app_settings import Settings

__all__:List[str] = [
    "cache",
    "config"
]

def config(func: Callable):
    """
    Decorator that injects the app config (Settings) into the decorated function.
    Sets the 'config' keyword argument.
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @functools.wraps(func)
    @inject
    def wrapper(
        *args,
        config: "Settings" = Provide[ApplicationContainer.app_settings],
        **kwargs
    ) -> Any:
        kwargs["config"] = config
        return func(*args, **kwargs)

    return wrapper