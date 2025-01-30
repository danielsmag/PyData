import boto3
from typing import Any, Optional, Dict, TYPE_CHECKING
from ...core.interfaces.i_client import IClient
from ...core.logging.logger import logger

if TYPE_CHECKING:
    from botocore.client import BaseClient

class BaseAWSClientError(Exception):
    pass

class BaseAWSClient(IClient):
    """
    A base class for AWS clients using Boto3, providing session management and client creation.

    Attributes:
        service_name (str): The AWS service name (e.g., 's3', 'ec2', 'dynamodb').
        region_name (str): The AWS region to connect to (default: 'eu-west-1').
        profile_name (Optional[str]): The AWS profile name to use for authentication.
        endpoint_url (Optional[str]): A custom endpoint URL for the AWS service.
        aws_access_key_id (Optional[str]): AWS access key ID for authentication.
        aws_secret_access_key (Optional[str]): AWS secret access key for authentication.
        test (bool): Whether to use a stubbed client for testing.
        _client (Optional[BaseClient]): The instantiated AWS service client.

    Methods:
        client() -> BaseClient:
            Returns the Boto3 client for the AWS service.
            Raises BaseAWSClientError if client creation fails.

        create_client() -> BaseClient:
            Creates and returns a Boto3 client for the specified AWS service.
            Uses profile-based or access-key authentication.
            Logs and raises BaseAWSClientError on failure.
    """

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
        """
        Initializes the BaseAWSClient with AWS service and authentication parameters.

        Args:
            service_name (str): The AWS service name (e.g., 's3', 'ec2').
            region_name (str, optional): The AWS region to connect to (default: 'eu-west-1').
            profile_name (Optional[str], optional): The AWS profile name for authentication.
            endpoint_url (Optional[str], optional): A custom endpoint URL for the AWS service.
            aws_access_key_id (Optional[str], optional): AWS access key ID.
            aws_secret_access_key (Optional[str], optional): AWS secret access key.
            test (bool, optional): If True, uses a stubbed client for testing (default: False).

        Returns:
            None
        """
        self.service_name: str = service_name
        self.region_name: str = region_name
        self.profile_name: Optional[str] = profile_name
        self.endpoint_url: Optional[str] = endpoint_url
        self.aws_access_key_id: Optional[str] = aws_access_key_id
        self.aws_secret_access_key: Optional[str] = aws_secret_access_key
        self._client: Optional["BaseClient"] = None
        self.test: bool = test

    @property
    def client(self) -> "BaseClient":
        """
        Retrieves the AWS service client, creating it if necessary.

        Raises:
            BaseAWSClientError: If client creation fails.

        Returns:
            BaseClient: The Boto3 client for the specified AWS service.
        """
        self._client = self.create_client()
        if not self._client:
            msg = "Error returning client"
            logger.error(msg)
            raise BaseAWSClientError(msg)
        return self._client

    def create_client(self) -> "BaseClient":
        """
        Creates and returns a Boto3 client for the specified AWS service.

        - Uses profile-based authentication if `profile_name` is provided.
        - Uses access-key-based authentication if `aws_access_key_id` and `aws_secret_access_key` are provided.
        - Uses default credentials if neither is provided.
        - If `test` is True, creates a stubbed client for unit testing.

        Raises:
            BaseAWSClientError: If client creation fails due to BotoCoreError, ClientError, or any unexpected error.

        Returns:
            BaseClient: The Boto3 client for the AWS service.
        """
        from botocore.exceptions import BotoCoreError, ClientError
        from botocore.stub import Stubber

        try:
            if self.test:
                client = boto3.client(self.service_name, region_name=self.region_name)
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
                logger.debug(f"Using custom endpoint URL: {self.endpoint_url}")

            if self.aws_access_key_id and self.aws_secret_access_key:
                client_properties["aws_access_key_id"] = self.aws_access_key_id
                client_properties["aws_secret_access_key"] = self.aws_secret_access_key
                logger.debug("Using provided AWS access key ID and secret access key")

            client = session.client(**client_properties)
            logger.info(f"Successfully created {self.service_name} client")
            return client

        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to create {self.service_name} client: {e}")
            raise BaseAWSClientError(f"Failed to create {self.service_name} client: {e}")

        except Exception as e:
            msg = f"Unexpected error while creating {self.service_name} client: {e}"
            logger.error(msg)
            raise BaseAWSClientError(msg)
