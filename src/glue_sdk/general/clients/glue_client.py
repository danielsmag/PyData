from .base_aws_client import BaseAWSClient
from typing import Optional, TYPE_CHECKING
from ...core.logging.logger import logger


if TYPE_CHECKING:
    from botocore.client import BaseClient

class GlueClientError(Exception):
    """
    Exception raised for errors in the GlueClient.
    """
    pass

class GlueClient(BaseAWSClient):
    """
    A client for interacting with AWS Glue using Boto3.

    Inherits from BaseAWSClient and initializes a Glue service client.

    Attributes:
        region_name (str): The AWS region to connect to (default: 'eu-west-1').
        profile_name (Optional[str]): The AWS profile name to use for authentication.
        endpoint_url (Optional[str]): A custom endpoint URL for AWS Glue.
        aws_access_key_id (Optional[str]): AWS access key ID for authentication.
        aws_secret_access_key (Optional[str]): AWS secret access key for authentication.
        test (bool): Whether to use a stubbed client for testing.

    Methods:
        client() -> BaseClient:
            Returns the Boto3 client for AWS Glue.
            Raises GlueClientError if client creation fails.
    """
    def __init__(
        self,
        region_name: str = 'eu-west-1',
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        test: bool = False
    ) -> None:
        """
        Initializes the GlueClient with AWS authentication and configuration parameters.

        Args:
            region_name (str, optional): The AWS region to connect to (default: 'eu-west-1').
            profile_name (Optional[str], optional): The AWS profile name for authentication.
            endpoint_url (Optional[str], optional): A custom endpoint URL for AWS Glue.
            aws_access_key_id (Optional[str], optional): AWS access key ID.
            aws_secret_access_key (Optional[str], optional): AWS secret access key.
            test (bool, optional): If True, uses a stubbed client for testing (default: False).

        Returns:
            None
        """
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
        """
        Retrieves the AWS Glue client, creating it if necessary.

        Raises:
            GlueClientError: If client creation fails.

        Returns:
            BaseClient: The Boto3 client for AWS Glue.
        """
        self._client = self.create_client()
        if not self._client:
            msg = "Error return Glue client"
            logger.error(msg)
            raise GlueClientError(msg)
        return self._client