


class GlueSdkManager():
    def __init__(self) -> None:
        pass
    
    from typing import Optional,Dict

from ..containers import ApplicationContainer
from ..core.master import MasterConfig

def configure_application(config_data: dict = {}) -> ApplicationContainer:
    """
    Instantiate and configure the ApplicationContainer.

    :param config_data: A dictionary of config overrides (e.g., {"env": "dev"}).
    :return: An ApplicationContainer instance, fully configured.
    """
    container = ApplicationContainer()

    # 1. If the user provided a dict, validate it via MasterConfig
    #    This ensures we have correct types, no invalid env, etc.
    if config_data is not None:
        validated_config = MasterConfig(**config_data)
        # Dump the validated model back to dict
        validated_dict = validated_config.model_dump()
        # 2. Override container's config with validated fields
        container.config.from_dict(validated_dict)

    # 3. Return the container, which can now produce a validated MasterConfig
    return container
