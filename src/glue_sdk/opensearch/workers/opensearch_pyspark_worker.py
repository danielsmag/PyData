from __future__ import annotations
from typing import List, Literal, Dict, Optional, Any, TYPE_CHECKING

from ..interfaces.i_opensearch_worker import IOpenSearchWorker
from ...core.logging.logger import logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__: List[str] = ["OpenSearchPySparkWorker"]


class OpenSearchPySparkWorker(IOpenSearchWorker):
    """
    Worker to load data from a PySpark DataFrame to OpenSearch.
    """

    def __init__(
        self,
        opensearch_config: Dict[str, Any],
        host: str,
        username: str,
        password: str,
        port: int = 9200,
    ) -> None:
        """
        Initialize the OpenSearchPySparkWorker.

        Args:
            opensearch_config (Dict[str, Any]): Extra settings for the connector.
            host (str): OpenSearch host.
            username (str): Authentication username.
            password (str): Authentication password.
            port (int, optional): OpenSearch port. Defaults to 9200.
        """
        super().__init__()
        self.host: str = host
        self._username: str = username
        self._password: str = password
        self.port: int = port
        self.opensearch_config: Dict[str, Any] = opensearch_config

    def load_data(
        self,
        df: DataFrame,
        index: str,
        mode: Literal["overwrite", "append", "ignore", "errorifexists"] = "overwrite",
        opensearch_batch_size_entries: Optional[int] = None,
        opensearch_batch_size_bytes: Optional[str] = None,
        opensearch_nodes_wan_only: Optional[str] = "true",
        opensearch_mapping_id: Optional[str] = None,
    ) -> bool:
        """
        Load data into an OpenSearch index from a PySpark DataFrame.

        Args:
            df (DataFrame): The PySpark DataFrame to write to OpenSearch.
            index (str): The target OpenSearch index.
            mode (Literal[...]): Save mode (defaults to "overwrite").
            opensearch_batch_size_entries (Optional[int]): Maximum number of documents per bulk request.
            opensearch_batch_size_bytes (Optional[str]): Maximum batch size in bytes per bulk request.
            opensearch_nodes_wan_only (Optional[str]): WAN-only mode setting (defaults to "true").
            opensearch_mapping_id (Optional[str]): Field name to be used as the document ID.

        Returns:
            bool: True if data is successfully written, False otherwise.
        """
        try:
            from pyspark.sql import DataFrame

            assert isinstance(df, DataFrame)

            full_host: str = f"{self.host}:{self.port}"
            if not full_host.startswith("https://"):
                full_host = "https://" + full_host

            options: Dict[str, Any] = {
                "opensearch.nodes": full_host,
                "opensearch.username": self._username,
                "opensearch.password": self._password,
                "opensearch.resource": index,
            }

            if self.opensearch_config:
                options.update(self.opensearch_config)

            if opensearch_batch_size_entries is not None:
                options["opensearch.batch.size.entries"] = str(
                    opensearch_batch_size_entries
                )
            if opensearch_batch_size_bytes is not None:
                options["opensearch.batch.size.bytes"] = opensearch_batch_size_bytes
            if opensearch_nodes_wan_only is not None:
                options["opensearch.nodes.wan.only"] = opensearch_nodes_wan_only
            if opensearch_mapping_id:
                options["opensearch.mapping.id"] = opensearch_mapping_id

            logger.debug(f"OpenSearch connection options: {options}")

            df.write.format(source="opensearch").options(**options).mode(
                saveMode=mode
            ).save()

            return True

        except Exception as e:
            logger.error(f"Failed to load data to OpenSearch: {e}")
            return False
