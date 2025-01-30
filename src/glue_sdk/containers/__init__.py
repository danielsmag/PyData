from typing import List, TYPE_CHECKING
import importlib
from .application_container import ApplicationContainer

if TYPE_CHECKING:
    from .application_container import ApplicationContainer
    
__all__:List[str] = ["ApplicationContainer"]

def __getattr__(name):
    if name in __all__:
        match name:
            case "ApplicationContainer":
                submod = importlib.import_module('.application_container', __name__)
                attr = submod.ApplicationContainer
            case _:
                raise AttributeError(f"Module {__name__} has no attribute {name}")
            
        globals()[name] = attr
        return attr
    
    raise AttributeError(f"module {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__