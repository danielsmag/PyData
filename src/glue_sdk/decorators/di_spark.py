
from dependency_injector.wiring import Provide, inject
from typing import List,Callable, TYPE_CHECKING, Any
import functools

if TYPE_CHECKING:
    from glue_sdk.services import SparkBaseService

__all__:List[str] = [
    "spark_baes_service",
   
]

def spark_baes_service(func: Callable):
    """
    Spark service
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @inject
    @functools.wraps(wrapped=func)
    def wrapper(
        *args,
        spark_service_obj: "SparkBaseService" = Provide[ApplicationContainer.spark.spark_base_service],
        **kwargs
    ) -> Any:
        kwargs["spark_baes_service"] = spark_service_obj
        return func(*args, **kwargs)

    return wrapper
