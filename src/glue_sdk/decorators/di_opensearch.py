from typing import List, Callable, TYPE_CHECKING, Any, ParamSpec, TypeVar
import functools

from ..core.shared import SharedUtilsSettings

if TYPE_CHECKING:
    from glue_sdk.containers.application_container import ApplicationContainer
    from ..opensearch.services.opensearch_service import OpenSearchService
    from opensearchpy import OpenSearch

P = ParamSpec("P")
R = TypeVar("R")

__all__: List[str] = [
    "opensearch_service",
    "opensearch_service_pyspark",
    "opensearch_service_glue",
]

shared_settings = SharedUtilsSettings()
container: ApplicationContainer = shared_settings.container


def opensearch_service(func: Callable):
    """
    Decorator that injects the opensearch_service into the decorated function.
    The parameter will be added to kwargs as 'opensearch_service'.
    The type of the object is "IOpenSearchService" from interfaces.i_opensearch_service.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        opensearch_service = container.opensearch.opensearch_service()
        kwargs["opensearch_service"] = opensearch_service
        return func(*args, **kwargs)

    return wrapper


def opensearch_service_pyspark(func: Callable):
    """
    Decorator that injects the opensearch_service into the decorated function.
    The parameter will be added to kwargs as 'opensearch_service'.
    The type of the object is "IOpenSearchService" from interfaces.i_opensearch_service.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        opensearch_service_pyspark: OpenSearchService = (
            container.opensearch.opensearch_service()
        )
        opensearch_service_pyspark.worker_mode = "pyspark"
        kwargs["opensearch_service_pyspark"] = opensearch_service_pyspark
        return func(*args, **kwargs)

    return wrapper


def opensearch_service_glue(func: Callable):
    """
    Decorator that injects the opensearch_service into the decorated function.
    The parameter will be added to kwargs as 'opensearch_service'.
    The type of the object is "IOpenSearchService" from interfaces.i_opensearch_service.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        opensearch_service_glue: OpenSearchService = (
            container.opensearch.opensearch_service()
        )
        opensearch_service_glue.worker_mode = "glue"
        kwargs["opensearch_service_glue"] = opensearch_service_glue
        return func(*args, **kwargs)

    return wrapper


def opensearch_client(func: Callable):
    """Decorator that injects the opensearch_client into the decorated function.
    The param will set to param 'opensearch_client'.
    The type of Object is "OpenSearch"
    from opensearchpy import OpenSearch
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        opensearch_client: OpenSearch = container.opensearch.opensearch_client()
        kwargs["opensearch_client"] = opensearch_client
        return func(*args, **kwargs)

    return wrapper
