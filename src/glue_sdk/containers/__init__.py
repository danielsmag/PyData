from typing import List, TYPE_CHECKING
import importlib

if TYPE_CHECKING:
    from ..containers.application_container import ApplicationContainer

__all__:List[str] = ["ApplicationContainer"]

def __getattr__(name):
    if name in __all__:
        submod = importlib.import_module(f'.{name}',__name__)
        globals()[name] = submod
        return submod
    
    raise AttributeError(f"mudule {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__