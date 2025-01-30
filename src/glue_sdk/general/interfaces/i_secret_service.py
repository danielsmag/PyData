from abc import ABC, abstractmethod
from typing import Optional, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.secret_model import Secrets

class ISecretService(ABC):
    """
    Interface for SecretService to define the required methods.
    """

    @abstractmethod
    def fetch_secrets(self, secret_name: str) -> Optional["Secrets"]:
        """
        Fetch secrets from the secret manager.

        :param secret_name: The name of the secret to fetch.
        :return: A dictionary containing the secrets, or None if an error occurs.
        """
        pass