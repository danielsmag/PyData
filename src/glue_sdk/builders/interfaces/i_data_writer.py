from abc import ABC, abstractmethod
from typing import Optional, List, Literal,TYPE_CHECKING
from typing_extensions import Self


from ..interfaces.i_data_builder_base import IDataBuilder

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

class IDataWriter(IDataBuilder,ABC):
    """
    Interface for writing data (Spark DataFrame or Glue DynamicFrame) 
    to various targets (e.g., Iceberg, S3, Data Catalog) and optionally 
    caching/retrieving data from a cache layer.
    """

    @abstractmethod
    def write(self, data: 'DataFrame') -> Self:
        """
        Store the provided data internally (Spark DataFrame or Glue DynamicFrame).

        :param data: The data to be written, either a Spark DataFrame or a Glue DynamicFrame.
        :return: Self for fluent chaining.
        """
        pass

    @abstractmethod
    def write_iceberg(
        self,
        db_name: str,
        table_name: str,
        partition_by: Optional[str] = None,
        mode: Literal['overwrite','ignore','error','errorifexists','append'] = "append",
        merge_schema: Literal['true','false'] = "true"
    ) -> Self:
        """
        Write the data to an Iceberg table.

        :param db_name: The Iceberg database name.
        :param table_name: The Iceberg table name.
        :param partition_by: Optional partitioning column or expression.
        :param mode: Write mode (overwrite, ignore, etc.).
        :param merge_schema: Whether to merge schema or not ("true" or "false").
        :return: Self for fluent chaining.
        """
        pass

    @abstractmethod
    def write_to_s3(
        self,
        target_path: str,
        format: str = "parquet",
        partition_keys: Optional[List[str]] = None,
        transformation_ctx: Optional[str] = None,
        mode: Literal['overwrite','append','error','ignore'] = "overwrite"
    ) -> Self:
        """
        Write the data to S3.

        :param target_path: The S3 path where the data should be written.
        :param format: The output format (e.g., "parquet", "csv").
        :param partition_keys: Optional list of partition keys.
        :param transformation_ctx: Optional Glue transformation context.
        :param mode: Write mode ("overwrite", "append", etc.).
        :return: Self for fluent chaining.
        """
        pass

    @abstractmethod
    def to_data_catalog(self, db_name: str, table_name: str) -> Self:
        """
        Write the data to the Data Catalog.

        :param db_name: The database name in the Data Catalog.
        :param table_name: The table name in the Data Catalog.
        :return: Self for fluent chaining.
        :raises DataWriterError: If there is no data to write.
        """
        pass

    @abstractmethod
    def load_data_aurora_pg(self, 
                            table_name: str,
                            db_name: Optional[str] = None,
                            schema: Optional[str] = None,
                            worker_type: Literal['python', 'pyspark', 'glue']  = "glue",
                            mode:Literal['overwrite','error','ignore','append'] = 'overwrite'
                            ) -> Self:
        """
        Loads data into Aurora PostgreSQL using a specified worker type.

        Args:
            table_name (str): Name of the target table.
            worker_type (WorkerType): Worker type ('python', 'pyspark', 'glue').

        Returns:
            IAuroraPgService: The instance of the service for method chaining.

        Raises:
            DataWriterError: If the Aurora service is not established.
        """
        pass
  
   
