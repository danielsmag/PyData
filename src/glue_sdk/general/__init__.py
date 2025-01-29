from typing import List, TYPE_CHECKING
import importlib

if TYPE_CHECKING:
    from .services.s3_service import S3Service
    from .services.secret_service import SecretService

    from .clients.s3_client import S3Client
    from .clients.secret_client import SecretClient
    from .clients.glue_client import GlueClient



__all__: List[str] = [
    "S3Service",
    "S3Client",
    "SecretClient",
    "SecretService",
    "GlueClient"
]

def __getattr__(name):
    if name in __all__:
        submod = importlib.import_module(f'.{name}',__name__)
        globals()[name] = submod
        return submod
    
    raise AttributeError(f"mudule {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__