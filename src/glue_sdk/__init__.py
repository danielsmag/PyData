from __future__ import annotations
from importlib import import_module
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from .sdk import SdkManager
    from .sdk import SdkConf

__all__: List[str] = ["SdkManager", "SdkConf"]

_dynamic_imports: dict[str, tuple[str, str]] = {
    "SdkManager": (__spec__.parent, ".sdk"),
    "SdkConf": (__spec__.parent, ".sdk"),
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
        # Import the entire module as an attribute.
        result = import_module(f".{attr_name}", package=package)
        globals()[attr_name] = result  # Cache for subsequent accesses.
        return result
    else:
        # Import the specified module and extract the desired attribute.
        module = import_module(module_name, package=package)
        result = getattr(module, attr_name)
        # Cache all non-deprecated attributes that come from this module.
        g = globals()
        for k, (_, v_module_name) in _dynamic_imports.items():
            if v_module_name == module_name and k not in _deprecated_dynamic_imports:
                g[k] = getattr(module, k)
        return result


def __dir__() -> list[str]:
    return list(__all__)
