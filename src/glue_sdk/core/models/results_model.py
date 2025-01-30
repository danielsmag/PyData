from dataclasses import dataclass, field
from pydantic import BaseModel,ConfigDict


class FuncResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    processed: bool
    name: str = ""
    data: dict = {}
