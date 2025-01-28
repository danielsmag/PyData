
from dependency_injector.wiring import inject, Provide
from typing import Callable, TYPE_CHECKING, Any
import functools


if TYPE_CHECKING:
    from glue_sdk.interfaces.i_opensearch_service import IOpenSearchService


def opensearch_service(func: Callable):
    """
    Decorator that injects the opensearch_service into the decorated function.
    The parameter will be added to kwargs as 'opensearch_service'.
    The type of the object is "IOpenSearchService" from interfaces.i_opensearch_service.
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @functools.wraps(func)
    @inject  # Automatically inject dependencies
    def wrapper(
        *args, 
        opensearch_service: "IOpenSearchService" = Provide[ApplicationContainer.opensearch.opensearch_service], 
        **kwargs
    ) -> Any:
        kwargs["opensearch_service"] = opensearch_service
        return func(*args, **kwargs)
    return wrapper

def opensearch_client(func: Callable):
    """Decorator that injects the opensearch_client into the decorated function.
        The param will set to param 'opensearch_client'.
        The type of Object is "OpenSearch"
        from opensearchpy import OpenSearch
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @functools.wraps(func)
    def wrapper(*args, **kwargs)->Any:
        opensearch_client: "IOpenSearchService" = Provide[ApplicationContainer.opensearch.opensearch_client]()
        kwargs["opensearch_client"] = opensearch_client
        return func(*args, **kwargs)
    return wrapper