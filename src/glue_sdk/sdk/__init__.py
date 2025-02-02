from __future__ import annotations
from typing import List, TYPE_CHECKING

from importlib import import_module

__all__: List[str] = ["SdkManager", "SdkConf", "SdkOpenSearch", "SDKdDecorators"]


if TYPE_CHECKING:
    from .sdk_manager import SdkManager
    from .sdk_config import SdkConf
    from .sdk_opensearch import SdkOpenSearch
    from .sdk_decorators import SDKdDecorators
# A mapping of {<member name>: (package, <module name>)} defining dynamic imports
_dynamic_imports: dict[str, tuple[str, str]] = {
    "SdkManager": (__spec__.parent, ".sdk_manager"),
    "SdkConf": (__spec__.parent, ".sdk_config"),
    "SdkOpenSearch": (__spec__.parent, ".sdk_opensearch"),
    "SDKdDecorators": (__spec__.parent, ".sdk_decorators"),
}
_deprecated_dynamic_imports = set()


def _getattr_migration(attr_name: str) -> object:
    raise AttributeError(f"Module {__name__} has no attribute {attr_name}")


def __getattr__(attr_name: str) -> object:
    dynamic_attr = _dynamic_imports.get(attr_name)
    if dynamic_attr is None:
        return _getattr_migration(attr_name)

    package, module_name = dynamic_attr

    if module_name == "__module__":

        result = import_module(f".{attr_name}", package=package)
        globals()[attr_name] = result
        return result
    else:
        module = import_module(module_name, package=package)
        result = getattr(module, attr_name)
        g = globals()
        for k, (_, v_module_name) in _dynamic_imports.items():
            if v_module_name == module_name and k not in _deprecated_dynamic_imports:
                g[k] = getattr(module, k)
        return result


def __dir__() -> list[str]:
    return list(__all__)
