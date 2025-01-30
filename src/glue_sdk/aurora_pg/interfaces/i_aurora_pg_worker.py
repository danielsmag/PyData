from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Literal, TYPE_CHECKING


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    
class IAuroraPgWorker(ABC):
    @abstractmethod
    def fetch_data(self, table_name: str, push_down_predicate: Optional[str] = None) -> 'DataFrame':
        pass

    @abstractmethod
    def load_data(self,
                    spark_df: 'DataFrame',
                    table_name: str,
                    db_name: Optional[str] = None,
                    schema: Optional[str] = None,
                    mode:Literal['overwrite','error','ignore','append'] = 'overwrite' 
                  ) -> bool:
        pass
