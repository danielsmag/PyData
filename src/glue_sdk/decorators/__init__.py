from typing import List, TYPE_CHECKING
import importlib

if TYPE_CHECKING:
    from .decorators import singelton
# from ..general.decorators.decorators import validate_processed



__all__:List[str] = [
    "validate_processed",
    "singelton"
]

def __getattr__(name):
    if name in __all__:
        submod = importlib.import_module(f'.{name}',__name__)
        globals()[name] = submod
        return submod
    
    raise AttributeError(f"mudule {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__