from botocore.client import BaseClient
from glue_sdk.clients.base_aws_client import BaseAWSClient
from typing import Optional, TYPE_CHECKING
from glue_sdk.core.logger import logger


if TYPE_CHECKING:
    from botocore.client import BaseClient

class GlueClientError(Exception):
    pass

class GlueClient(BaseAWSClient):
    def __init__(
        self,
        region_name: str = 'eu-west-1',
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        test: bool = False
    ) -> None:
        super().__init__(
            service_name='glue',
            region_name=region_name,
            profile_name=profile_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            test=test
        )

    @property
    def client(self) -> "BaseClient":
        self._client = self.create_client()
        if not self._client:
            msg = "Error return Glue client"
            logger.error(msg)
            raise GlueClientError(msg)
        return self._client