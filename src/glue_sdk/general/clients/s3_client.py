from glue_sdk.clients.base_aws_client import BaseAWSClient
from typing import Optional,TYPE_CHECKING
from glue_sdk.core.logger import logger

if TYPE_CHECKING:
    from botocore.client import BaseClient


class S3ClientError(Exception):
    pass

class S3Client(BaseAWSClient):
    def __init__(
        self,
        region_name: str = 'eu-west-1',
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None
    ) -> None:
        super().__init__(
            service_name='s3',
            region_name=region_name,
            profile_name=profile_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

    @property
    def client(self) -> "BaseClient":
        self._client  = self.create_client()
        if not self._client:
            msg = "Error return S3 client"
            logger.error(msg)
            raise S3ClientError(msg)
        return self._client