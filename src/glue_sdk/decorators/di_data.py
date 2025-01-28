
from dependency_injector.wiring import inject, Provide

from typing import List,Callable, TYPE_CHECKING, Any
import functools

if TYPE_CHECKING:
    from glue_sdk.interfaces.i_data_loader import IDataLoader
    from glue_sdk.interfaces.i_data_writer import IDataWriter

__all__:List[str] = [
    "data_loader"
]

def data_loader(func: Callable):
    """Decorator that injects the cadata_catalog_loader into the decorated function.
        The param will set to param 'data_catalog_loader'.
        The type of Object is "IDataCatalogLoader"
       from interfaces.i_data_catalog_loader import IDataCatalogLoader
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @functools.wraps(func)
    @inject
    def wrapper(*args,
                data_loader: "IDataLoader" = Provide[ApplicationContainer.data_builders.data_loader],
                **kwargs)-> Any:
        return func(*args, data_loader=data_loader, **kwargs)
    return wrapper

def data_writer(func: Callable):
    """Decorator that injects the data_writer into the decorated function.
        The param will set to param 'data_catdata_writeralog_loader'.
        The type of Object is "IDataCatalogLoader"
       from interfaces.i_data_catalog_loader import IDataCatalogLoader
    """
    from glue_sdk.containers.application_container import ApplicationContainer
    @functools.wraps(func)
    @inject
    def wrapper(*args, 
                data_writer: "IDataWriter" = Provide[ApplicationContainer.data_builders.data_writer],
                **kwargs)-> Any:
        return func(*args, data_writer=data_writer, **kwargs)
    return wrapper

