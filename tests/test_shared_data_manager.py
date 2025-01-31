from threading import Thread
import pytest
from glue_sdk.cache.services.shared_data_service import SharedDataService

@pytest.fixture(autouse=True)
def reset_manager():

    manager = SharedDataService()
    manager.reset()

def test_set_and_get():
    manager = SharedDataService()

    manager.set("key1", "value1")
    assert manager.get("key1") == "value1", "Failed to retrieve the correct value for key1"
    assert manager.get("nonexistent", default="default") == "default", "Default value not returned for nonexistent key"


def test_concurrent_writes():
    manager = SharedDataService()
    import threading

    def writer(thread_id):
        manager.set(f"key{thread_id}", f"value{thread_id}")

    threads: list[Thread] = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Verify all keys were written
    for i in range(10):
        assert manager.get(f"key{i}") == f"value{i}", f"Key key{i} was not written correctly"


def test_timeout():
    manager = SharedDataService()
    manager.set("key_timeout", "initial_value")

    import threading
    import time

    def delayed_set():
        lock = manager._get_lock("key_timeout")
        with lock:
            time.sleep(2)  # Simulate a long operation
            manager.set("key_timeout", "updated_value")

    thread = threading.Thread(target=delayed_set)
    thread.start()

    # Attempt to acquire the same lock with a timeout
    with pytest.raises(TimeoutError, match="Failed to acquire lock for key 'key_timeout'"):
        manager.set("key_timeout", "new_value", timeout=1)

    thread.join()


def test_delete():
    manager = SharedDataService()
    manager.set("key_to_delete", "value_to_delete")

    assert manager.get("key_to_delete") == "value_to_delete", "Failed to retrieve the key before deletion"

    manager.delete("key_to_delete")

    assert manager.get("key_to_delete", None) is None, "Failed to delete the key"


def test_update() -> None:
    manager = SharedDataService()
    updates = {"key1": "value1", "key2": "value2", "key3": "value3"}
    manager.update(updates)

    for key, value in updates.items():
        assert manager.get(key) == value, f"Failed to update key {key}"


def test_all():
    manager = SharedDataService()
    manager.set("key1", "value1")
    manager.set("key2", "value2")

    all_data = manager.all()
    assert all_data == {"key1": "value1", "key2": "value2"}, "all() did not return correct data"
