from abc import ABC, abstractmethod
from typing import Optional, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

class ISparkBaseService(ABC):
    @abstractmethod
    def flatten_df(self,
                   df:"DataFrame",
                sep: str = ".",
                   lower_case: bool = True
                   )-> "DataFrame":
        """
     
        Flatten a nested DataFrame schema and include levels in headers.

        :param df: PySpark DataFrame to flatten.
        :param sep: Separator for nested levels in column names.
        :return: Flattened DataFrame.
     
        """
        pass