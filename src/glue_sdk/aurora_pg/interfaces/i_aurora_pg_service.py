from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING, Literal


__all__:List[str] = ["IAuroraPgService"]

if TYPE_CHECKING:
    from ...core.interfaces.i_client import IClient
    from ..interfaces.i_aurora_pg_worker import IAuroraPgWorker
    from pyspark.sql import DataFrame


class IAuroraPgService(ABC):
    __slots__: Tuple = ("glue_context", "client", "config")

    @abstractmethod
    def __init__(
        self,
        config: Dict,
        aurora_pg_client: "IClient",
        connection_name: Optional[str] = None,
        python_worker: Optional["IAuroraPgWorker"] = None,
        pyspark_worker: Optional["IAuroraPgWorker"] = None,
        glue_worker: Optional["IAuroraPgWorker"] = None,
    ) -> None:
        pass

    @abstractmethod
    def _get_worker(self, worker_type: str) -> "IAuroraPgWorker":
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[tuple]]:
        """
        Executes a query against the Aurora PostgreSQL database.

        Args:
            query (str): SQL query to execute.
            params (Optional[tuple]): Parameters for the SQL query.

        Returns:
            Optional[List[tuple]]: Query result if applicable, else None.
        """
        pass

    @abstractmethod
    def fetch_data(
        self,
        table_name: str,
        push_down_predicate: Optional[str] = None,
        worker_type: Literal['python', 'pyspark', 'glue'] = "glue",
    ) -> 'DataFrame':
        """
        Fetches data from the Aurora PostgreSQL database as a Spark DataFrame.

        Args:
            table_name (str): Name of the table to fetch data from.
            push_down_predicate (Optional[str]): SQL WHERE clause for filtering data.
            worker_type (str): Worker type ('python', 'pyspark', 'glue').

        Returns:
            DataFrame: The fetched data as a Spark DataFrame.
        """
        pass

    @abstractmethod
    def load_data(
        self, 
        spark_df: 'DataFrame',
        table_name: str, 
        db_name:Optional[str] = None,
        schema: Optional[str] = None,
        worker_type: Literal['python', 'pyspark', 'glue'] = "glue",
        mode:Literal['overwrite','error','ignore','append'] = 'overwrite' 
    ) -> bool:
        """
        Loads data into the Aurora PostgreSQL database from a Spark DataFrame.

        Args:
            spark_df (DataFrame): Data to load into the database.
            table_name (str): Name of the target table.
            worker_type (str): Worker type ('python', 'pyspark', 'glue').

        Returns:
            bool: True if data is successfully loaded, else False.
        """
        pass
