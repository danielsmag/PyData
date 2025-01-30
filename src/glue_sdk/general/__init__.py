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
        match name:
            case "S3Client":
                submod = importlib.import_module('.clients.s3_client', __name__)
                attr = submod.S3Client
            case "S3Service":
                submod = importlib.import_module('.services.s3_service', __name__)
                attr = submod.S3Service
            case "SecretClient":
                submod = importlib.import_module('.clients.secret_client', __name__)
                attr = submod.SecretClient
            case "SecretService":
                submod = importlib.import_module('.services.secret_service', __name__)
                attr = submod.SecretService
            case "GlueClient":
                submod = importlib.import_module('.clients.glue_client', __name__)
                attr = submod.GlueClient
            case _:
                raise AttributeError(f"Module {__name__} has no attribute {name}")
            
        globals()[name] = attr
        return attr
    
    raise AttributeError(f"mudule {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__