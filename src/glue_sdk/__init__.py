from typing import List, TYPE_CHECKING
import importlib

if TYPE_CHECKING:
    from .managers.sdk_manager import SdkManager
    from .managers.sdk_config import SdkConfig

# sdkManager = SdkManager()

__all__:List[str] = ["SdkManager","SdkConfig"]

def __getattr__(name):
    if name in __all__:
        match name:
            case "SdkManager":
                submod = importlib.import_module('.managers.sdk_manager', __name__)
                attr = submod.SdkManager
            case "SdkConfig":
                submod = importlib.import_module('.managers.sdk_config', __name__)
                attr = submod.SdkConfig

            case _:
                raise AttributeError(f"Module {__name__} has no attribute {name}")
            
        globals()[name] = attr
        return attr
    
    raise AttributeError(f"mudule {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__