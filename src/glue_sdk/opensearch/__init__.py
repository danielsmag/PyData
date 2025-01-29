from typing import List, TYPE_CHECKING
import importlib


if TYPE_CHECKING:
    from .clients.opensearch_client import OpenSearchClient

    from .services.opensearch_service import OpenSearchService

    from .workers.opensearch_glue_worker import OpenSearchGlueWorker
    from .workers.opensearch_pyspark_worker import OpenSearchPySparkWorker


__all__:List[str] = [
    "OpenSearchClient",
    "OpenSearchService",
    "OpenSearchPySparkWorker",
    "OpenSearchGlueWorker"
]

def __getattr__(name):
    if name in __all__:
        submod = importlib.import_module(f'.{name}',__name__)
        globals()[name] = submod
        return submod
    
    raise AttributeError(f"mudule {__name__} has no attribute {name}")

def __dir__() -> List[str]:
    return list(globals().keys()) + __all__