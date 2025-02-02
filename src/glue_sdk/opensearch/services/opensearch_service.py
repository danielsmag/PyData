from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Literal, TYPE_CHECKING
from pyspark.sql import DataFrame
from ...core.services.base_service import BaseService
from ..interfaces.i_opensearch_service import IOpenSearchService
from ...core.logging.logger import logger
from glue_sdk.core.shared import ServicesEnabled


if TYPE_CHECKING:
    from ..interfaces.i_opensearch_worker import IOpenSearchWorker
    from opensearchpy import OpenSearch

aws_services_to_use = ServicesEnabled()


class OpenSearchServiceError(Exception):
    pass


class OpenSearchService(IOpenSearchService, BaseService):

    def __init__(
        self,
        opensearch_config: Dict,
        opensearch_client: OpenSearch,
        opensearch_pyspark_worker: Optional[IOpenSearchWorker] = None,
        opensearch_glue_worker: Optional[IOpenSearchWorker] = None,
    ) -> None:

        self.opensearch_config: Dict = opensearch_config
        self.client: OpenSearch = opensearch_client
        self._worker_mode: Literal["glue", "pyspark"] = "pyspark"

        if aws_services_to_use.USE_GLUE and opensearch_glue_worker is None:
            raise OpenSearchServiceError(
                "AWS Glue is enabled but no OpenSearch Glue worker is provided."
            )

        if aws_services_to_use.USE_SPARK and opensearch_pyspark_worker is None:
            raise OpenSearchServiceError(
                "AWS Spark is enabled but no OpenSearch PySpark worker is provided."
            )

        self._worker_instances: Dict[str, Optional[IOpenSearchWorker]] = {
            "glue": opensearch_glue_worker if aws_services_to_use.USE_GLUE else None,
            "pyspark": (
                opensearch_pyspark_worker if aws_services_to_use.USE_SPARK else None
            ),
        }

    @property
    def worker_mode(self) -> Literal["glue", "pyspark"]:
        return self._worker_mode

    @worker_mode.setter
    def worker_mode(self, v: Literal["glue", "pyspark"]) -> None:
        if v not in ["glue", "pyspark"]:
            raise OpenSearchServiceError(
                "U provide illegal value to worker mode , value can be in [glue,pyspark]"
            )
        self._worker_mode = v

    def load_data(
        self,
        df: DataFrame,
        index: str,
        worker_mode: Optional[Literal["glue", "pyspark"]] = None,
        opensearch_mapping_id: Optional[str] = None,
        mode: Literal["overwrite", "append", "ignore", "errorifexists"] = "overwrite",
    ) -> bool:

        if worker_mode is None:
            worker_mode = self.worker_mode

        worker: IOpenSearchWorker | None = self._worker_instances.get(worker_mode)

        if not worker:
            raise ValueError(f"Unsupported mode: {worker_mode}")

        res: bool = worker.load_data(
            df=df, index=index, opensearch_mapping_id=opensearch_mapping_id, mode=mode
        )
        return res

    def delete_indices(self, indices: List[str]) -> None:
        for index in indices:
            try:
                if self.client.indices.exists(index=index):
                    self.client.indices.delete(index=index)
                    logger.info(f"Successfully deleted index '{index}'.")
                else:
                    logger.warning(f"Index '{index}' does not exist.")
            except Exception as e:
                logger.error(f"Error deleting index '{index}': {e}")

    def delete_alias_from_indices(self, alias_name: str, indices: List[str]) -> None:
        for index in indices:
            try:
                self.client.indices.delete_alias(index=index, name=alias_name)
                logger.info(f"Deleted alias {alias_name} from index {index}")
            except Exception as e:
                logger.warning(
                    f"Error deleting alias {alias_name} from index {index} {e}"
                )

    def get_indices_by_alias(self, alias_name: str) -> List[str]:
        try:
            response: Dict = self.client.indices.get_alias(name=alias_name)
            return list(response.keys())
        except Exception as e:
            logger.warning(f"Could not find indices for alias {alias_name} {e}")
            return []

    def create_alias(self, index: str, alias_name: str) -> bool:
        try:
            existing_indices: List[str] = self.get_indices_by_alias(
                alias_name=alias_name
            )
            if existing_indices:
                logger.info(
                    f"Alias {alias_name} already exists and points to indices: {existing_indices}"
                )
                return False
            self.client.indices.put_alias(index=index, name=alias_name)
            logger.info(
                f"Alias '{alias_name}' created and associated with index '{index}'."
            )
            return True
        except Exception as e:
            logger.error(f"Error creating alias {alias_name} for index {index} {e}")
            return False
