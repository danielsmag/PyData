from glue_sdk.containers.application_container import ApplicationContainer
from dependency_injector.wiring import Provide, inject
from typing import List,Callable, TYPE_CHECKING, Any
import functools


if TYPE_CHECKING:
    from glue_sdk.interfaces.i_s3_service import IS3Service 
    from glue_sdk.interfaces.i_secret_service import ISecretService
    
__all__:List[str] = [
    "s3_service",
    "secret_service"
]

def s3_service(func: Callable):
    """
    Decorator that injects the SparkSession (from Core.spark_session) into the function.
    Passes the SparkSession as `spark_session` kwarg.
    """
    @inject
    @functools.wraps(wrapped=func)
    def wrapper(
        *args,
        s3_service_obj: "IS3Service" = Provide[ApplicationContainer.general.s3_service],
        **kwargs
    ) -> Any:
        kwargs["s3_service"] = s3_service_obj
        return func(*args, **kwargs)

    return wrapper


def secret_service(func: Callable):
    """
    Decorator that injects the SecretService (from Core.secret_service).
    Passes the service as `secret_service` kwarg.
    """
    @inject
    @functools.wraps(wrapped=func)
    def wrapper(
        *args,
        secret_service_obj: "ISecretService" = Provide[ApplicationContainer.general.secret_service],
        **kwargs
    ) -> Any:
        kwargs["secret_service"] = secret_service_obj
        return func(*args, **kwargs)

    return wrapper


def s3_client(func: Callable):
    """
    Decorator that injects the S3 client (from Core.s3_client).
    Passes the client as `s3_client` kwarg.
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @inject
    @functools.wraps(func)
    def wrapper(
        *args,
        s3_client: "BaseClient" = Provide[ApplicationContainer.clients.s3_client],
        **kwargs
    ) -> Any:
        kwargs["s3_client"] = s3_client
        return func(*args, **kwargs)

    return wrapper

def glue_client(func: Callable):
    """
    Decorator that injects the Glue client (from Core.glue_client).
    Passes the client as `glue_client` kwarg.
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @inject
    @functools.wraps(func)
    def wrapper(
        *args,
        glue_client: "BaseClient" = Provide[ApplicationContainer.clients.glue_client],
        **kwargs
    ) -> Any:
        kwargs["glue_client"] = glue_client
        return func(*args, **kwargs)

    return wrapper



def secret_client(func: Callable):
    """
    Decorator that injects the Secrets Manager client (from Core.secret_client).
    Passes the client as `secret_client` kwarg.
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @inject
    @functools.wraps(func)
    def wrapper(
        *args,
        secret_client: "BaseClient" = Provide[ApplicationContainer.clients.secret_client],
        **kwargs
    ) -> Any:
        kwargs["secret_client"] = secret_client
        return func(*args, **kwargs)

    return wrapper
