from typing import Optional, Union, List, TYPE_CHECKING
from pyspark.sql import DataFrame
from glue_sdk.interfaces.i_data_builder_base import IDataBuilder
from glue_sdk.services.base_service import BaseService
from glue_sdk.utils.utils import to_camel_case
from typing_extensions import Self
from pydantic import validate_call

if TYPE_CHECKING:
    from glue_sdk.interfaces.i_cache import ICache
    from glue_sdk.interfaces.i_data_catalog_service import IDataCatalogService
    from glue_sdk.interfaces.i_data_builder_base import IDataBuilder
    from glue_sdk.interfaces.i_aurora_pg_service import IAuroraPgService
    from awsglue.dynamicframe import DynamicFrame
    from glue_sdk.services import ISparkBaseService
        
__all__:List[str] = ['DataBuilderBase']

class DataBuilderBaseError(Exception):
    """Base exception for DataCatalog operations."""
    pass


class DataBuilderBase(IDataBuilder,BaseService):
    """
    Base class for DataCatalog operations, providing shared methods and attributes.
    """
    def __init__(
        self,
        data_catalog_service: Optional["IDataCatalogService"]=None,
        aurora_pg_service: Optional["IAuroraPgService"]= None,
        spark_base_service: Optional["ISparkBaseService"] = None,
        cache: Optional["ICache"] = None
    ) -> None:
        """
        Initialize the base DataCatalog class.

        :param data_catalog_service: Service to interact with Glue (fetch DB/table metadata, load/write data)
        :param cache: Optional shared cache for storing and retrieving Spark data
        """
        super().__init__()
        self.aurora_pg_service: Optional[IAuroraPgService] = aurora_pg_service
        self.data_catalog_service: Optional["IDataCatalogService"] = data_catalog_service
        self.spark_base_service: Optional["ISparkBaseService"] = spark_base_service
        self.cache: Optional["ICache"] = cache
        self.data: Optional[Union["DynamicFrame", DataFrame]] = None
        self.df: Optional[DataFrame] = None
        self.db_name: Optional[str] = None
        self.table_name: Optional[str] = None

    def _set_df_from_data(self) -> None:
        """
        Internal helper to ensure self.data is stored as a Spark DataFrame in self.df.
        """
        if isinstance(self.data, "DynamicFrame"):
            self.df = self.data.toDF()
        elif isinstance(self.data, DataFrame):
            self.df = self.data
        else:
            error_msg: str = f"Data is neither DynamicFrame nor DataFrame. Found type: {type(self.data)}"
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)

    def _ensure_df(self) -> None:
        """
        Ensures that self.df is set. Raises DataCatalogError if not.
        """
        if self.df is None:
            error_msg = "DataFrame is not set. Use the appropriate method to set data first."
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)
        if not isinstance(self.df, DataFrame):
            error_msg = f"Expected self.df to be a DataFrame, but got {type(self.df)}."
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)

    def to_camel_case_headers(self) -> Self:
        """
        Convert DataFrame column headers to camelCase with separator '_'.

        :return: self
        :raises DataCatalogError: if self.df is not a valid Spark DataFrame
        """
        if not isinstance(self.df, DataFrame):
            error_msg = "to_camel_case_headers() requires a Spark DataFrame in self.df."
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)
        
        camel_case_cols: List[str] = to_camel_case(columns=self.df.columns)
        self.df = self.df.toDF(*camel_case_cols)
        self.log_info("Converted DataFrame headers to camelCase.")
        return self

    @validate_call
    def cache_data(self, key: Optional[str] = None) -> Self:
        """
        Cache the current Spark DataFrame.

        :param key: Optional cache key override
        :return: self
        :raises DataCatalogError: if no cache is configured or self.df is invalid
        """
        if not (key or self.table_name):
            error_msg = "no key and not table_name exist"
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)
        
        if not isinstance(self.df, DataFrame):
            error_msg = "cache_data() requires a Spark DataFrame in self.df."
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)
        
        if not self.cache:
            error_msg = "Caching requested but no cache is configured."
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)

        self._ensure_df()
        cache_key: str = key or self.table_name # type: ignore
        
        self.cache.set(key=cache_key, value=self.df)
        self.log_info(message=f"Cached DataFrame with key: {cache_key}")
        return self

    @validate_call
    def load_from_cache(self, key: str) -> Self:
        """
        Load data from cache using the provided key.

        :param key: Cache key
        :return: self
        :raises DataCatalogError: if cache is not configured or data not found
        """
        if not self.cache:
            error_msg = "Attempted to load from cache, but no cache is configured."
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)
        
        cached_data = self.cache.get(key=key)
        if cached_data is None:
            error_msg = f"No data found in cache for key: {key}"
            self.log_error(message=error_msg)
            raise DataBuilderBaseError(error_msg)
        
        self.data = cached_data
        self.log_debug(message=f"Loaded data from cache with key: {key}")
        return self

    @validate_call
    def flatten_df(self,
                   sep: str = ".",
                   lower_case: bool = True
                   )-> Self:
        if not self.spark_base_service:
            raise DataBuilderBaseError("U have to set up spark_base_service")
        self._ensure_df()
        self.spark_base_service.flatten_df(
            df=self.df, # type: ignore
            sep=sep,
            lower_case=lower_case
        )
        return self
    