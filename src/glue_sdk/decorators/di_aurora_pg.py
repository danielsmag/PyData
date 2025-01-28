from typing import Callable, Any,TYPE_CHECKING
import functools
from dependency_injector.wiring import Provide, inject
from dependency_injector.providers import Callable
from glue_sdk.containers.application_container import ApplicationContainer

if TYPE_CHECKING:
    from glue_sdk.interfaces.i_aurora_pg_client import IAuroraPgClient
    from glue_sdk.interfaces.i_aurora_pg_service import IAuroraPgService
    from glue_sdk.interfaces.i_client import IClient
    
    
def aurora_pg_secrets(func: Callable):
    """
    Decorator that injects the AuroraPgSecrets service.
    Passes the service as `aurora_pg_secrets` kwarg.
    """
    @inject
    @functools.wraps(func)
    def wrapper(
        *args,
        aurora_pg_secrets: "IClient" = Provide[ApplicationContainer.aurora_pg.aurora_pg_secrets],  # Replace 'Any' with actual type
        **kwargs
    ) -> Any:
        kwargs["aurora_pg_secrets"] = aurora_pg_secrets
        return func(*args, **kwargs)

    return wrapper

# Decorator for `aurora_pg_client`
def aurora_pg_client(func: Callable):
    """
    Decorator that injects the AuroraPgClient service.
    Passes the service as `aurora_pg_client` kwarg.
    """
    @inject
    @functools.wraps(func)
    def wrapper(
        *args,
        aurora_pg_client: "IAuroraPgClient" = Provide[ApplicationContainer.aurora_pg.aurora_pg_client],  # Replace 'Any' with actual type
        **kwargs
    ) -> Any:
        kwargs["aurora_pg_client"] = aurora_pg_client
        return func(*args, **kwargs)

    return wrapper

def aurora_pg_service(func: Callable):
    """
    Decorator that injects the AuroraPgService.
    Passes the service as `aurora_pg_service` kwarg.
    """
    @inject
    @functools.wraps(func)
    def wrapper(
        *args,
        aurora_pg_service: "IAuroraPgService" = Provide[ApplicationContainer.aurora_pg.aurora_pg_service],  # Replace 'Any' with actual type
        **kwargs
    ) -> Any:
        kwargs["aurora_pg_service"] = aurora_pg_service
        return func(*args, **kwargs)

    return wrapper