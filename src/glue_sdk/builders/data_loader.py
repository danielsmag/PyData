from typing import Optional, List, Literal,TYPE_CHECKING, Union, Any
from typing_extensions import Self
from pyspark.sql.dataframe import DataFrame
from pydantic import validate_call
from glue_sdk.builders.data_builder_base import DataBuilderBase
from glue_sdk.interfaces.i_data_loader import IDataLoader
from pydantic import validate_call

if TYPE_CHECKING:
    from glue_sdk.interfaces.i_cache import ICache
    from glue_sdk.interfaces.i_data_catalog_service import IDataCatalogService
    from glue_sdk.interfaces.i_aurora_pg_service import IAuroraPgService
    from awsglue.dynamicframe import DynamicFrame
    from glue_sdk.services import ISparkBaseService
    
    
__all__:List[str] = ["DataLoader"]

class DataLoaderError(Exception):
    pass


class DataLoader(DataBuilderBase,IDataLoader):
    """
        Loader class responsible for:
        - Fetching a DynamicFrame from AWS Glue Data Catalog
        - Converting it to a Spark DataFrame
        - Handling optional caching
        - Providing methods to retrieve and persist data
    """
    def __init__(
        self,
        data_catalog_service: Optional["IDataCatalogService"]=None,
        aurora_pg_service: Optional["IAuroraPgService"]= None,
        cache: Optional["ICache"] = None,
        spark_base_service: Optional["ISparkBaseService"]= None
    ) -> None:
        """
        :param data_catalog_service: Service to interact with Glue (fetch DB/table metadata, load data)
        :param cache: Optional shared cache for storing and retrieving Spark data
        """
        super().__init__(data_catalog_service=data_catalog_service,
                         cache=cache,
                        aurora_pg_service=aurora_pg_service,
                        spark_base_service=spark_base_service
                        )
    @validate_call          
    def load(self,
            db_name: str, 
            table_name: str,
            keep_dynamicframe: bool = False
            ) -> Self:
        """    
            :param database_name: Glue database name
            :param table_name: Glue table name
            :param use_cache: Whether to use caching
            :param cache_key: Optional override for cache key
            :return: self
        """
        
        self.db_name = db_name
        self.table_name = table_name
        
        if not self.data:
            self._load_dynamic_frame()
        if not keep_dynamicframe:
            self._set_df_from_data()
        return self
        
    def _load_dynamic_frame(self) -> Self:
        assert isinstance(self.table_name,str) and isinstance(self.db_name,str), "table or db is not str" 
        assert isinstance(self.data_catalog_service,"IDataCatalogService")
        self.data = self.data_catalog_service.load_dynamic_frame(
            database_name=self.db_name,
            table_name=self.table_name
        )
        if not self.data:
            error_msg: str = f"Failed to load data from {self.db_name}.{self.table_name}"
            self.log_error(message=error_msg)
            raise DataLoaderError(error_msg)
        return self
       
    def load_from_s3(self,
                    source_path: str,
                    format="parquet",
                    recurse: bool = True,
                    transformation_ctx: Optional[str] = None,
                    ) ->Self:
        """Loads data from S3 into a DataFrame."""
        if not self.data_catalog_service:
            error_msg:str = "U try use data_catalog_service service but nut establish"
            self.log_error(message=error_msg)
            raise DataLoaderError(error_msg )
        
        self.data = self.data_catalog_service.load_from_s3(
            source_path=source_path,
            format=format,
            recurse=recurse,
            transformation_ctx=transformation_ctx
        )
        self._set_df_from_data()
        self._ensure_df()
        return self
    
    def to_persist(self) -> Self:
        """
            Persist (cache) the Spark DataFrame in memory/disk for reuse.

            :return: self
            :raises DataCatalogLoaderError: if self.df is invalid
        """
        if not isinstance(self.df, "DataFrame"):
            error_msg = "to_persist() requires a Spark DataFrame in self.df."
            self.log_error(message=error_msg)
            raise DataLoaderError(error_msg)
        self.df.persist()
        self.log_debug(message=f"Df {self.table_name} persist ")
        return self

    def get(self) -> Union["DynamicFrame", "DataFrame"]:
        """
        Get the loaded data (DynamicFrame or DataFrame).

        :return: The loaded data
        :raises DataCatalogLoaderError: if self.data is not loaded
        """
        if not self.data:
            error_msg = "No data to get()."
            self.log_error(message=error_msg)
            raise DataLoaderError(error_msg)
        
        self.log_info(message="Returning loaded data")
        return self.data
        
    def get_df(self) -> "DataFrame":
        """
            Get the loaded Spark DataFrame.

            :return: Spark DataFrame
            :raises DataCatalogLoaderError: if self.df is not valid
        """
        self._set_df_from_data()
        self._ensure_df()
        self.log_debug(message="Returning Spark DataFrame")
        return self.df # type: ignore

    
    