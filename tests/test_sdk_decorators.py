from glue_sdk import SDKdDecorators, SdkConf, SdkManager
from glue_sdk.cache.i_cache import ICache
from glue_sdk.decorators.di_cache import cache_obj

SdkConf.reset()
config = SdkConf()
config.set_spark_conf({"test": 123})
config.set_services_to_use(USE_CACHE=True, USE_SPARK=True)
sdk_main = SdkManager(config=config)
sdk_main.initialize()
container = sdk_main.container
# container.wire(modules=[di_cache])

d = SDKdDecorators()

# cache_obj = d.cache_obj
# Set up configuration


# Decorated inner function that will receive the injected cache_obj.
@cache_obj
def inner_test(cache_obj: ICache):
    cache_obj.set(key="test", value=123)
    number = cache_obj.get("test")
    return number


def test_sdk_decorators():
    # Call the decorated function manually. The injection should occur inside the decorator.
    for i in range(100):
        print(i)
        result = inner_test()
        assert result == 123
