from pyspark.sql import DataFrame
from .data_builder_base import DataBuilderBase
from typing import Optional, Union, TYPE_CHECKING, Literal, List
from typing_extensions import Self
from .interfaces.i_data_writer import IDataWriter
from ..core.models.s3path_model import S3Path
from pydantic import validate_call
from ..spark.interfaces.i_spark_base_service import ISparkBaseService

if TYPE_CHECKING:
    from ..glue_data_catalog.interfaces.i_data_catalog_service import IDataCatalogService   
    from ..cache.interfaces.i_cache import ICache
    from ..aurora_pg.interfaces.i_aurora_pg_service import IAuroraPgService

class DataWriterError(Exception):
    pass


class DataWriter(IDataWriter,DataBuilderBase,):
    
    def __init__(self,
                data_catalog_service: Optional["IDataCatalogService"] = None,
                cache: Optional["ICache"] = None,
                aurora_pg_service: Optional["IAuroraPgService"]= None,
                spark_base_service: Optional["ISparkBaseService"] = None 
                ) -> None:
        super().__init__(data_catalog_service=data_catalog_service,
                cache=cache,
                aurora_pg_service=aurora_pg_service,
                spark_base_service=spark_base_service
                )
            
    def write(self,
            data: 'DataFrame'
            ) -> Self:
        """set data to be wirreten and convert to DF"""
        if data is not None:
            self.data= data
            self._set_df_from_data()
     
        return self
     
    def write_iceberg(self,
                    db_name: str,
                    table_name: str,
                    partition_by: Optional[str] = None,
                    mode: Literal['overwrite','ignore','error','errorifexists','append'] = "append",
                    merge_schema: Literal['true','false'] = "true"
                    ) -> Self:
        """write the dataframe to an iceberg table """
        self._set_df_from_data()
        self._ensure_df()
        self.data_catalog_service.write_iceberg_s3(
            df=self.df, # type: ignore
            db_name=db_name,
            table_name=table_name,
            partition_by=partition_by,
            mode=mode,
            merge_schema=merge_schema
            
        )
        return self

    def write_to_s3(self,
                    target_path: str,
                    format: str = "parquet",
                    partition_keys: Optional[List[str]] = None,
                    transformation_ctx: Optional[str] = None,
                    mode: Literal['overwrite','append','error','ignore'] ="overwrite"
                    
                    ) ->Self:
        """write the DF to s3 """
        target_path_s3 = S3Path(url=target_path)
        self._set_df_from_data()  
        self._ensure_df()   
        self.data_catalog_service.write_to_s3(
            df=self.df, # type: ignore
            format=format,
            target_path=target_path_s3.url,
            partition_keys=partition_keys,
            transformation_ctx=transformation_ctx,
            mode=mode    
        )
        
        return self
    
    def to_data_catalog(self,db_name: str,table_name:str) -> Self:
        """
        Writes the data to the data catalog.
        """
        if self.data is None:
            error_msg = "No data available to write to the data catalog."
            self.log_error(message=error_msg)
            raise DataWriterError(error_msg)
        
        self.data_catalog_service.write_to_catalog(
            data=self.data,
            db_name=db_name,
            table_name=table_name
        )
        self.log_info("Data successfully written to the data catalog")
        return self
    
    @validate_call
    def load_data_aurora_pg(self, 
                            table_name: str,
                            db_name: Optional[str] = None,
                            schema: Optional[str] = None,
                            worker_type: Literal['python', 'pyspark', 'glue']  = "glue",
                            mode:Literal['overwrite','error','ignore','append'] = 'overwrite' 
                            ) -> Self:
        if not self.aurora_pg_service:
            error_msg:str = "U try use aurora service but nut establish"
            self.log_error(message=error_msg)
            raise DataWriterError(error_msg )

        self._set_df_from_data()
        self._ensure_df
        self.aurora_pg_service.load_data(
            spark_df=self.df, # type: ignore
            table_name=table_name,
            db_name=db_name,
            worker_type=worker_type,
            schema=schema,
            mode=mode
        )
        return self
    