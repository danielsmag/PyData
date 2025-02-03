from __future__ import annotations

import json
from typing import Dict, Optional

from botocore.client import BaseClient

from ...core.logging.logger import logger
from ...core.shared import SharedUtilsSettings
from ..interfaces.i_secret_service import ISecretService
from ..models.secret_model import Secrets


shared_settings = SharedUtilsSettings()


class SecretService(ISecretService):
    """
    A service for retrieving and parsing secrets from AWS Secrets Manager.

    This service fetches secrets stored in AWS Secrets Manager and extracts
    relevant credentials such as usernames and passwords.

    Attributes:
        secret_client (BaseClient): The AWS Secrets Manager client for fetching secrets.

    Methods:
        fetch_secrets(secret_name: str) -> Optional[Secrets]:
            Fetches and parses the secret stored in AWS Secrets Manager.
            Extracts credentials such as username and password.
            Returns a Secrets object if successful, otherwise None.
    """

    def __init__(self, secret_client: BaseClient) -> None:
        """
        Initializes the SecretService with a Secrets Manager client.

        Args:
            secret_client (BaseClient): The AWS Secrets Manager client instance.

        Returns:
            None
        """
        if shared_settings.test_mode:
            self.secret_client: Optional[BaseClient] = None
        else:
            self.secret_client = secret_client

    def fetch_secrets(self, secret_name: str) -> Optional[Secrets]:
        """
        Fetches a secret from AWS Secrets Manager and extracts credentials.

        The method retrieves the secret value for the given secret name,
        parses it as JSON, and extracts relevant credentials such as
        `username` and `password`. If the secret cannot be retrieved or
        parsed, it logs an error and returns None.

        Args:
            secret_name (str): The name of the secret to fetch.

        Returns:
            Optional[Secrets]: A Secrets object containing the extracted
            credentials if successful, otherwise None.

        Raises:
            json.JSONDecodeError: If the secret value cannot be parsed as JSON.
            Exception: If fetching the secret fails.
        """
        try:
            if shared_settings.test_mode:
                data: Dict[str, str] = {
                    "username": "fake_user",
                    "secret_name": "fake_name",
                    "password": "fake_pass",
                }
                return Secrets(**data)

            assert isinstance(
                self.secret_client, BaseClient
            ), "Secret_client must be BaseClient type"
            valid_data: Dict[str, str] = {}
            response: Dict = self.secret_client.get_secret_value(SecretId=secret_name)
            secret_string: str = response.get("SecretString", "")
            if not secret_string:
                logger.error(f"No SecretString found for secret '{secret_name}'")
                return None
            secret_data = json.loads(secret_string)
            valid_data["username"] = (
                secret_data.get("opensearch.net.http.auth.user")
                or secret_data.get("username")
                or secret_data.get("USERNAME")
            )
            valid_data["password"] = (
                secret_data.get("opensearch.net.http.auth.pass")
                or secret_data.get("password")
                or secret_data.get("PASSWORD")
            )
            logger.debug(f"the response for secret {secret_name} is {secret_data}")
            secrets = Secrets(secret_name=secret_name, **valid_data)
            return secrets
        except json.JSONDecodeError as e:
            logger.error(
                f"cant parse secret manager string to JSON for {secret_name}: {e}"
            )
        except Exception as e:
            logger.error(f"fail to retrieve secret {secret_name} {e}")
        return None
