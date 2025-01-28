from typing import Any, Optional, Dict
from glue_sdk.clients.base_aws_client import BaseAWSClient  


class SecretClient(BaseAWSClient):
    def __init__(
        self,
        region_name: str = 'eu-west-1',
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None
    ) -> None:
        super().__init__(
            service_name='secretsmanager',
            region_name=region_name,
            profile_name=profile_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
