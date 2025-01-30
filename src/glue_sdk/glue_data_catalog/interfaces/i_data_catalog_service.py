from abc import ABC, abstractmethod
from typing import Optional, Any, List, Literal, Union,TYPE_CHECKING
from pyspark.sql import DataFrame
from ...core.interfaces.i_base_service import IBaseService


if TYPE_CHECKING:
    from ..models.data_catalog_model import DataCatalogDatabase,DataCatalogTable
    from awsglue import DynamicFrame
class IDataCatalogService(IBaseService,ABC):
    
    @abstractmethod
    def load_dynamic_frame(self,database_name: str, 
                           table_name: str) -> 'DynamicFrame':
        pass
    
    @abstractmethod
    def fetch_table(self,
                    table_name: str,
                    db: Union[str,"DataCatalogDatabase"],
                    )-> "DataCatalogTable":
        pass
    
    @abstractmethod
    def write_iceberg_s3(self,
        df: DataFrame,
        db_name: str,
        table_name: str,
        partition_by: Optional[Union[str, List[str]]] = None,
        mode: Literal['overwrite','ignore','error','errorifexists','append'] = "append",
        overwrite_mode: Literal['static','dynamic'] = "dynamic",
        merge_schema: Literal['true','false'] = "true",
        snapshot_id:Optional[str] = None
    ) -> None:
        pass
    
    @abstractmethod
    def fetch_db(self,
                db_name: str           
                ) -> "DataCatalogDatabase":
        pass
    
    @abstractmethod
    def write_to_s3(self,
        df: 'DynamicFrame | DataFrame',
        target_path: str,
        format: str = "parquet",
        partition_keys: Optional[List[str]] = None,
        transformation_ctx: Optional[str] = None,
        mode: Literal['overwrite','append','error','ignore'] ="overwrite"
        ) -> None:
        pass
    
    @abstractmethod
    def write_to_catalog(self,
            data: 'DynamicFrame | DataFrame | None',
            db_name: str,
            table_name: str,
            partition_keys: Optional[list] = None,
            mode: str = "append"
            ) -> None:
        
        pass
    
    @abstractmethod
    def load_from_s3(self,
        source_path: str,
        format: str = "parquet",
        recurse: bool = True,
        transformation_ctx: Optional[str] = None
        ) -> 'DataFrame':
        """
            Loads data from S3 into a DataFrame.

            :param source_path: S3 path from where data will be read.
            :param format: Data format (default 'parquet').
            :param partition_keys: List of partition keys to read.
            :param transformation_ctx: Optional context name for job bookmarking.
            :return: Instance of DataWriter with loaded data.
            :raises DataWriterError: If loading fails.
        """
        pass