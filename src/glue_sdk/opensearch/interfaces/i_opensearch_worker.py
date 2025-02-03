from abc import ABC, abstractmethod
from typing import Optional, Literal, TYPE_CHECKING


if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class IOpenSearchWorker(ABC):

    @abstractmethod
    def load_data(
        self,
        df: "DataFrame",
        index: str,
        mode: Literal["overwrite", "append", "ignore", "errorifexists"] = "overwrite",
        opensearch_batch_size_entries: Optional[int] = None,
        opensearch_batch_size_bytes: Optional[str] = None,
        opensearch_nodes_wan_only: Optional[str] = "true",
        opensearch_mapping_id: Optional[str] = None,
    ) -> bool:
        pass
