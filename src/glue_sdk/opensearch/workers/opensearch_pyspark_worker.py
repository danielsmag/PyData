from ..interfaces.i_opensearch_worker import IOpenSearchWorker
from pyspark.sql import DataFrame
from typing import List,Literal, Dict
from ...core.logging.logger import logger

from pyspark.sql import DataFrame
from typing import Optional, Literal

__all__:List[str] = ["OpenSearchPySparkWorker"]

class OpenSearchPySparkWorker(IOpenSearchWorker):
    """
    Worker to load data from PySpark DataFrame to OpenSearch.
    """
    def __init__(self,
                 opensearch_config: Dict,
                 host: str,
                 username: str,
                 password: str,
                 port: int = 9200) -> None:
        """
        Initialize the OpenSearchPySparkWorker.

        Args:
            host (str): OpenSearch host.
            username (str): Authentication username.
            password (str): Authentication password.
            port (int, optional): OpenSearch port. Defaults to 9200.
        """
        super().__init__()
        self.host = host
        self._username = username
        self._password = password
        self.port = port
        self.opensearch_config: Dict =opensearch_config
        
    def load_data(
        self,
        df: DataFrame,
        index: str,
        mode: Literal["overwrite", "append", "ignore", "errorifexists"] = "overwrite",
        es_batch_size_entries: Optional[int] = None,
        es_batch_size_bytes: Optional[str] = None,
        es_nodes_wan_only: Optional[str] = "true",
        opensearch_mapping_id: Optional[str] = None,

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
        try:
            assert isinstance(df,DataFrame)
            es_batch_size_entries = self.opensearch_config.get("opensearch_batch_size_entries")
            es_batch_size_bytes = self.opensearch_config.get("opensearch_batch_size_bytes")
            es_nodes_wan_only= self.opensearch_config.get("es_nodes_wan_only","true")
            pushdown: str =  self.opensearch_config.get("pushdown","true")

            
            options: Dict = {
                "es.nodes": self.host,
                "es.port": str(self.port),
                "es.net.http.auth.user": self._username,
                "es.net.http.auth.pass": self._password,
                "es.resource": index,
            }

           
            if es_batch_size_entries:
                options["es.batch.size.entries"] = str(es_batch_size_entries)
            if es_batch_size_bytes:
                options["es.batch.size.bytes"] = es_batch_size_bytes
            if pushdown is not None:
                options["es.pushdown"] = str(pushdown).lower()
            if es_nodes_wan_only:
                options["es.nodes.wan.only"] = es_nodes_wan_only
            if opensearch_mapping_id:
                options['es.mapping.id'] =opensearch_mapping_id    
                
            logger.debug(f"open search connection options: {options}")
            (df.write 
                .format("org.elasticsearch.spark.sql") 
                .options(**options) 
                .mode(mode) 
                .save())

            return True

        except Exception as e:
            logger.error(f"Failed to load data to OpenSearch: {e}")
            return False
