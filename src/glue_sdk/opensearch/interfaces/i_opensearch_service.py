from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union,Literal

from pyspark.sql import DataFrame

class IOpenSearchService(ABC):
    @abstractmethod
    def load_data(
        self,
        df:DataFrame,
        index: str,
        worker_mode: Literal["glue","pyspark"] = "pyspark",
        opensearch_mapping_id: Optional[str] = None,
        mode: Literal["overwrite", "append", "ignore", "errorifexists"] = "overwrite"
    ) -> bool:
        pass
    
    @abstractmethod
    def delete_indices(self, indices: List[str]) -> None:
        pass
    

    @abstractmethod
    def delete_alias_from_indices(self, alias_name: str, indices: List[str]) -> None:
       
        pass

    @abstractmethod
    def get_indices_by_alias(self, alias_name: str) -> List[str]:
       
        pass

    @abstractmethod
    def create_alias(self, index: str, alias_name: str) -> bool:
       
        pass

