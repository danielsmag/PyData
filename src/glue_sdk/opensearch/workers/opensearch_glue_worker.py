from __future__ import annotations
from ..interfaces.i_opensearch_worker import IOpenSearchWorker
from typing import List, Literal, Dict, Any, TYPE_CHECKING, Optional
from ...core.logging.logger import logger

__all__: List[str] = ["OpenSearchGlueWorker"]

if TYPE_CHECKING:
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.context import GlueContext
    from pyspark.sql import DataFrame


class OpenSearchGlueWorkerError(Exception):
    pass


class OpenSearchGlueWorker(IOpenSearchWorker):
    """
    Worker to load data from a Glue DataFrame to OpenSearch.
    """

    def __init__(
        self, opensearch_config: Dict[str, Any], glue_context: "GlueContext"
    ) -> None:
        """
        Initialize the OpenSearchGlueWorker.

        Args:
            opensearch_config (Dict[str, Any]): Extra settings for the OpenSearch connection.
            glue_context (GlueContext): The Glue context instance.
        """
        super().__init__()
        self.opensearch_config: Dict[str, Any] = opensearch_config
        self.glue_context: "GlueContext" = glue_context

    def load_data(
        self,
        df: DataFrame,
        index: str,
        mode: Literal["overwrite", "append", "ignore", "errorifexists"] = "overwrite",
        opensearch_batch_size_entries: Optional[int] = None,
        opensearch_batch_size_bytes: Optional[str] = None,
        opensearch_nodes_wan_only: Optional[str] = "true",
        opensearch_mapping_id: Optional[str] = None,
        pushdown: Optional[bool] = None,
    ) -> bool:
        """
        Load data into an OpenSearch index from a Glue DataFrame.

        Args:
            df (DataFrame): The PySpark DataFrame to write to OpenSearch.
            index (str): The target OpenSearch index.
            mode (Literal["overwrite", "append", "ignore", "errorifexists"]): Save mode. Defaults to "overwrite".
            opensearch_batch_size_entries (Optional[int]): Maximum number of documents per bulk request.
            opensearch_batch_size_bytes (Optional[str]): Maximum batch size in bytes per bulk request.
            opensearch_nodes_wan_only (Optional[str]): WAN-only mode setting. Defaults to "true".
            opensearch_mapping_id (Optional[str]): Field name to be used as the document ID.
            pushdown (Optional[bool]): Enable pushdown. If not provided, the value from opensearch_config is used.

        Returns:
            bool: True if the upload is successful, False otherwise.
        """
        from awsglue.dynamicframe import DynamicFrame
        from pyspark.sql import DataFrame

        try:

            opensearch_batch_size_entries = (
                opensearch_batch_size_entries
                or self.opensearch_config.get("opensearch_batch_size_entries")
            )
            opensearch_batch_size_bytes = (
                opensearch_batch_size_bytes
                or self.opensearch_config.get("opensearch_batch_size_bytes")
            )
            opensearch_mapping_id = opensearch_mapping_id or self.opensearch_config.get(
                "opensearch_mapping_unique_id"
            )

            pushdown = (
                pushdown
                if pushdown is not None
                else self.opensearch_config.get("pushdown", True)
            )
            connection_name: Optional[str] = self.opensearch_config.get(
                "connection_name"
            )
            opensearch_nodes_wan_only = (
                opensearch_nodes_wan_only
                or self.opensearch_config.get("opensearch_nodes_wan_only", "true")
            )

            if not connection_name:
                logger.error("No connection name provided for OpenSearch.")
                raise OpenSearchGlueWorkerError(
                    "No connection name provided for OpenSearch."
                )

            if isinstance(df, DataFrame):
                dynamic_frame: "DynamicFrame" = DynamicFrame.fromDF(
                    dataframe=df, glue_ctx=self.glue_context, name="dynamic_frame_temp"
                )
            else:
                dynamic_frame = df

            connection_options: Dict[str, Any] = {
                "opensearch.resource": index,
                "connectionName": connection_name,
                "pushdown": pushdown,
            }

            connection_options.update(self.opensearch_config)

            if opensearch_mapping_id:
                connection_options["opensearch.mapping.id"] = opensearch_mapping_id
            if opensearch_nodes_wan_only:
                connection_options["opensearch.nodes.wan.only"] = (
                    opensearch_nodes_wan_only
                )
            if opensearch_batch_size_entries:
                connection_options["opensearch.batch.size.entries"] = (
                    opensearch_batch_size_entries
                )
            if opensearch_batch_size_bytes:
                connection_options["opensearch.batch.size.bytes"] = (
                    opensearch_batch_size_bytes
                )

            logger.info(
                f"Starting upload to index '{index}' with connection '{connection_name}'."
            )
            logger.debug(f"OpenSearch connection options: {connection_options}")

            self.glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="opensearch",
                connection_options=connection_options,
                transformation_ctx=index,
            )
            logger.info(f"Upload to OpenSearch index '{index}' successful.")
            return True

        except Exception as e:
            logger.error(f"Exception during loading process to index '{index}': {e}")
            return False
