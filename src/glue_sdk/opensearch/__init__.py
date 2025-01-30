from typing import List, TYPE_CHECKING
import importlib


import importlib
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from .clients.opensearch_client import OpenSearchClient
    from .services.opensearch_service import OpenSearchService
    from .workers.opensearch_glue_worker import OpenSearchGlueWorker
    from .workers.opensearch_pyspark_worker import OpenSearchPySparkWorker

__all__: List[str] = [
    "OpenSearchClient",
    "OpenSearchService",
    "OpenSearchPySparkWorker",
    "OpenSearchGlueWorker"
]

def __getattr__(name: str):
    if name in __all__:
        match name:
            case "OpenSearchClient":
                submod = importlib.import_module('.clients.opensearch_client', __name__)
                attr = submod.OpenSearchClient
            case "OpenSearchService":
                submod = importlib.import_module('.services.opensearch_service', __name__)
                attr = submod.OpenSearchService
            case "OpenSearchPySparkWorker" | "OpenSearchGlueWorker":
                if name == "OpenSearchPySparkWorker":
                    submod = importlib.import_module('.workers.opensearch_pyspark_worker', __name__)
                    attr = submod.OpenSearchPySparkWorker
                else:
                    submod = importlib.import_module('.workers.opensearch_glue_worker', __name__)
                    attr = submod.OpenSearchGlueWorker
            case _:
                raise AttributeError(f"Module {__name__} has no attribute {name}")

        globals()[name] = attr
        return attr

    raise AttributeError(f"Module {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__
