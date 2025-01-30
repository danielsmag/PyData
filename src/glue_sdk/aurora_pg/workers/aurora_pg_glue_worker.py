from ..interfaces.i_aurora_pg_worker import IAuroraPgWorker
from typing import Dict, Optional, Tuple,TYPE_CHECKING, Literal
from pyspark.sql import DataFrame
from ...core.services.base_service import BaseService

if TYPE_CHECKING:
    from ..interfaces.i_aurora_pg_client import IAuroraPgClient
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    
class GlueAuroraPgWorker(IAuroraPgWorker, BaseService):
    __slots__: Tuple = ("config", "client", "connection_name", "spark")
    
    def __init__(
        self,
        glue_context: 'GlueContext',
        config: Dict,
        aurora_pg_client: "IAuroraPgClient",
        connection_name: Optional[str] = None
    ) -> None:
        self.glue_context: 'GlueContext' = glue_context
        self.config: Dict = config
        self.client: "IAuroraPgClient" = aurora_pg_client
        self.connection_name = connection_name or self.config.get("connection_name", "")
    
    def fetch_data(self, table_name: str, push_down_predicate: Optional[str] = None) -> DataFrame:
        try:
            self.log_debug(f"Fetching data from table '{table_name}' with predicate: {push_down_predicate}")
            if not self.connection_name:
                raise Exception("No connection name configured.")
            
            options: Dict[str, str] = {
                "dbtable": table_name,
                "connectionName": self.connection_name
            }
            if push_down_predicate:
                options["pushDownPredicate"] = push_down_predicate
            
            dynamic_frame: 'DynamicFrame' = self.glue_context.create_dynamic_frame.from_options(
                connection_type="postgresql",
                connection_options=options,
                transformation_ctx=f"fetch_data_{table_name}"
            )
            spark_df: 'DataFrame' = dynamic_frame.toDF()
            self.log_debug(f"Successfully fetched data from table '{table_name}'")
            return spark_df
        except Exception as e:
            self.log_error(f"Error fetching data from table '{table_name}': {e}")
            raise
    
    def load_data(self, 
                spark_df: 'DataFrame',
                table_name: str,
                db_name: Optional[str] = None,
                schema: Optional[str] = None,
                mode:Literal['overwrite','error','ignore','append'] = 'overwrite' 
                ) -> bool:
        
        from awsglue.dynamicframe import DynamicFrame
        try:
            self.log_debug(f"Loading data into table '{table_name}'")
            if not self.connection_name:
                raise Exception("No connection name configured.")
            
            dynamic_frame: 'DynamicFrame' = DynamicFrame.fromDF(
                dataframe=spark_df,
                glue_ctx=self.glue_context,
                name=f"dynamic_frame_{table_name}"
            )
            options: Dict[str, str] = {
                "dbtable": table_name,
                "connectionName": self.connection_name
            }
            self.glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="postgresql",
                connection_options=options,
                transformation_ctx=f"load_data_{table_name}"
            )
            self.log_info(f"Successfully loaded data into table '{table_name}'")
            return True
        except Exception as e:
            self.log_error(f"Error loading data into table '{table_name}': {e}")
            return False
    
    
