
from typing import Optional,Dict,List, Any
import json
from botocore.client import BaseClient
from glue_sdk.core.logger import logger
from pydantic import BaseModel, model_validator, ConfigDict

class Secrets(BaseModel):
    model_config = ConfigDict(frozen=True)
    
    secret_name: str
    username: str = ""
    password: str = ""

    @model_validator(mode="after")
    def validate_required(self) -> "Secrets":
        missing: List[str] = []
        required_fields: List[str] = [
            "username",
            "password",
        ]
        for field in required_fields:
            if not getattr(self, field):
                missing.append(field)
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

        return self 