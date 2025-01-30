from ..interfaces.i_opensearch_worker import IOpenSearchWorker
from pyspark.sql import DataFrame
from typing import List,Literal, Dict, Any, TYPE_CHECKING
from ...core.logging.logger import logger
from pyspark.sql import DataFrame
from typing import Optional, Literal

__all__:List[str] = ["OpenSearchGlueWorker"]

if TYPE_CHECKING:
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.context import GlueContext
    
class OpenSearchGlueWorkerError(Exception):
    pass

class OpenSearchGlueWorker(IOpenSearchWorker):
    """
    Worker to load data from Glue DataFrame to OpenSearch.
    """
    def __init__(self,
                opensearch_config: Dict,
                glue_context: "GlueContext"
                ) -> None:
        """
        Initialize the OpenSearchPySparkWorker.

        Args:
            connection_name: name of connection in glue env
        """
        
        super().__init__()
        self.opensearch_config: Dict =opensearch_config
        self.glue_context: "GlueContext" = glue_context
        
    def load_data(
        self,
        df: DataFrame,
        index: str,
        mode: Literal["overwrite", "append", "ignore", "errorifexists"] = "overwrite",
        es_batch_size_entries: Optional[int] = None,
        es_batch_size_bytes: Optional[str] = None,
        es_nodes_wan_only: Optional[str] = "true",
        opensearch_mapping_id: Optional[str] = None,
        pushdown: Optional[bool] = None
        ) -> bool:
        """
        Load data into OpenSearch index from PySpark DataFrame.

        Args:
            df (DataFrame): The PySpark DataFrame to write to OpenSearch.
            index (str): The OpenSearch index to write data to.
            connection_name (Optional[str]): Connection name (if applicable) for use glue connection.
            es_batch_size_entries (Optional[int]): Batch size (entries).
            es_batch_size_bytes (Optional[str]): Batch size (bytes).
            es_nodes_wan_only (Optional[str]): WAN-only mode. Defaults to "true".
            opensearch_mapping_id (Optional[str]): Mapping ID for OpenSearch.
            mode (Literal["overwrite", "append", "ignore", "errorifexists"]): Save mode. Defaults to "overwrite".
            pushdown (Optional[bool]): Enable pushdown. Defaults to None.

        Returns:
            bool: True if data is successfully written, False otherwise.
        """
        from awsglue.dynamicframe import DynamicFrame
        try:
            es_batch_size_entries = es_batch_size_entries or self.opensearch_config.get("opensearch_batch_size_entries")
            es_batch_size_bytes= es_batch_size_bytes or self.opensearch_config.get("opensearch_batch_size_bytes")
            opensearch_mapping_id= opensearch_mapping_id or self.opensearch_config.get("opensearch_mapping_unique_id")
            pushdown =  self.opensearch_config.get("pushdown","true")
            connection_name: Optional[str] = self.opensearch_config.get("connection_name")
            es_nodes_wan_only= es_nodes_wan_only or self.opensearch_config.get("es_nodes_wan_only","true")
            
            if not connection_name:
                logger.error("no connection name fro opensearch ")
                raise OpenSearchGlueWorkerError("no connection name fro opensearch")
            
            if isinstance(df, DataFrame):
                dynamic_frame: "DynamicFrame" = DynamicFrame.fromDF(
                    dataframe=df,
                    glue_ctx=self.glue_context,
                    name="dynamic_frame_temp"
                )
            else:
                dynamic_frame = df

            connection_options: Dict[str, Any] = {
                "opensearch.resource": index,
                "pushdown": pushdown,
                "connectionName": connection_name,
            }

            if opensearch_mapping_id:
                connection_options["opensearch.mapping.id"] = opensearch_mapping_id
            if es_nodes_wan_only:
                connection_options["es.nodes.wan.only"] = es_nodes_wan_only
            if es_batch_size_entries:
                connection_options["es.batch.size.entries"] = es_batch_size_entries
            if es_batch_size_bytes:
                connection_options["es.batch.size.bytes"] = es_batch_size_bytes
            if pushdown:
                connection_options["pushdown"] = pushdown
            
            
            logger.info(f"Starting upload to index '{index}' with connection '{connection_name}'.")
            logger.debug(f"open search connection options: {connection_options}")
            self.glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="opensearch",
                connection_options=connection_options,
                transformation_ctx=index
            )
            logger.info(f"Upload to OpenSearch index '{index}' successful.")
            return True
        except Exception as e:
            logger.error(f"Exception during loading process to index '{index}': {e}")
            return False