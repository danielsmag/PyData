from typing import List, TYPE_CHECKING
import importlib

if TYPE_CHECKING:
    from .data_loader import DataLoader
    from .data_writer import DataWriter

__all__: List[str] = ["DataLoader", 
                      "DataWriter"]

def __getattr__(name):
    if name in __all__:
        match name:
            case "DataLoader":
                submod = importlib.import_module('.data_loader', __name__)
                attr = submod.DataLoader
            case "DataWriter":
                submod = importlib.import_module('.data_writer', __name__)
                attr = submod.DataWriter
            case _:
                raise AttributeError(f"Module {__name__} has no attribute {name}")

        globals()[name] = attr
        return attr
    
    raise AttributeError(f"mudule {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__