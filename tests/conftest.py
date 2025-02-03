import pytest
from glue_sdk import SdkManager, SdkConf
from glue_sdk.containers import ApplicationContainer


@pytest.fixture(scope="session", autouse=True)
def sdk():
    conf = SdkConf()
    conf.set_services_to_use(USE_CACHE=True, USE_DATA_CATALOG=True, USE_OPENSEARCH=True)
    conf.set_config_app_from_dict(
        config_data={
            "env": "test",
            "opensearch": {
                "opensearch_host": "localhost",
                "opensearch_port": 9200,
                "test": 124,
                "opensearch_secret_name": "my_secret",
            },
        }
    )
    sdk = SdkManager(config=conf)
    sdk.initialize()
    sdk.set_test_mode()
    return sdk
