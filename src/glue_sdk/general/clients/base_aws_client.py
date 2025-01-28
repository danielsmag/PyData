import boto3
from typing import Any, Optional, Dict, TYPE_CHECKING
from glue_sdk.interfaces.i_client import IClient
from glue_sdk.core.logger import logger

if TYPE_CHECKING:
    from botocore.client import BaseClient

class BaseAWSClientError(Exception):
    pass

class BaseAWSClient(IClient):
    def __init__(
        self,
        service_name: str,
        region_name: str = 'eu-west-1',
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        test: bool = False
        
    ) -> None:
        self.service_name: str = service_name
        self.region_name: str = region_name
        self.profile_name: str | None = profile_name
        self.endpoint_url: str | None = endpoint_url
        self.aws_access_key_id: str | None = aws_access_key_id
        self.aws_secret_access_key: str | None = aws_secret_access_key
        self._client: Optional["BaseClient"] = None
        self.test: bool=test
        
    @property
    def client(self) -> "BaseClient":
        self._client = self.create_client()
        if not self._client:
            msg = "Error return client"
            logger.error(msg)
            raise BaseAWSClientError(msg)
        return self._client
        
    def create_client(self) -> "BaseClient":
        from botocore.exceptions import BotoCoreError, ClientError
        from botocore.stub import Stubber
        try:
            if self.test:
                client = boto3.client(self.service_name,region_name=self.region_name)
                stubber = Stubber(client)
                stubber.activate()
                return client
            
            if self.profile_name:
                session = boto3.Session(profile_name=self.profile_name)
                logger.debug(f"Initialized boto3 session with profile: {self.profile_name}")
            else:
                session = boto3.Session()
                logger.debug("Initialized boto3 session with default credentials")

            client_properties: Dict[str, Any] = {
                "service_name": self.service_name,
                "region_name": self.region_name,
            }

            if self.endpoint_url:
                client_properties["endpoint_url"] = self.endpoint_url
                logger.debug(f"using custom endpoint url: {self.endpoint_url}")

            if self.aws_access_key_id and self.aws_secret_access_key:
                client_properties["aws_access_key_id"] = self.aws_access_key_id
                client_properties["aws_secret_access_key"] = self.aws_secret_access_key
                logger.debug("using provided aws access key id and secret access key")

            client = session.client(**client_properties)
            logger.info(f"Successfully created {self.service_name} client")
            return client

        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to create {self.service_name} client: {e}")
            raise BaseAWSClientError(f"Failed to create {self.service_name} client: {e}")
        
        except Exception as e:
            msg = f"unexpected error while creating {self.service_name} client: {e}"
            logger.error(msg)
            raise BaseAWSClientError(msg)
        
a = BaseAWSClient('glue', test=True,region_name='eu-west-1')
c = a.client
print(c)