from dataclasses import dataclass,field
from .decorators.decorators import singelton

@singelton
@dataclass
class AwsServicesToUse:
    use_opensearch: bool = False
    use_aurora_pg: bool = False
    use_data_catalog: bool = False
    