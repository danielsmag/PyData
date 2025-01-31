from dataclasses import dataclass,field
from .decorators.decorators import singleton

@singleton
@dataclass
class AwsServicesToUse:
    USE_OPENSEARCH: bool = False
    USE_AURORA_PG: bool = False
    USE_DATA_CATALOG: bool = False
    USE_CACHE: bool = True
    USE_DATA_BUILDERS: bool = True
    USE_GLUE: bool = True
    USE_SPARK: bool = True
    USE_EMR: bool = False