from typing import Dict, Any, Optional, TYPE_CHECKING
import json
from botocore.client import BaseClient
from glue_sdk.models.secret_model import Secrets
from glue_sdk.core.logger import logger
from glue_sdk.interfaces.i_secret_service import ISecretService

if TYPE_CHECKING:
   from botocore.client import BaseClient
    
class SecretService(ISecretService):
    
    def __init__(self, 
                secret_client: "BaseClient"
                ) -> None:
        self.secret_client: "BaseClient" = secret_client

    def fetch_secrets(self, secret_name: str) -> Optional[Secrets]:
        try:
            valide_data:Dict[str,str] = {}
            response: Dict = self.secret_client.get_secret_value(SecretId=secret_name)
            secret_string: str = response.get("SecretString","")
            if not secret_string:
                logger.error(f"No SecretString found for secret '{secret_name}'")
                return None
            secret_data = json.loads(secret_string)
            valide_data['username'] = (
                secret_data.get("opensearch.net.http.auth.user") 
                or secret_data.get("username")
                or secret_data.get("USERNAME")
            )
            valide_data['password'] = (
                secret_data.get("opensearch.net.http.auth.pass") 
                or secret_data.get("password")
                or secret_data.get("PASSWORD")
            )
            logger.debug(f"the response for secret {secret_name} is {secret_data}")
            secrets = Secrets(secret_name=secret_name, **valide_data)
            return secrets
        except json.JSONDecodeError as e:
            logger.error(f"cant parse secret manager string to JSON for {secret_name}: {e}")
        except Exception as e:
            logger.error(f"fail to retrieve secret {secret_name} {e}")
        return None
