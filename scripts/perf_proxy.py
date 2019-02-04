import multiprocessing
from multiprocessing.managers import (
    BaseManager,
    BaseProxy,
)
import pathlib
import time

# This benchmark uses native Python proxy objects to communicate between two processes.
# With default settings, this should be roughly compareable to:
# python scripts/perf_benchmark.py --num-processes 1 --num-events 100000 --payload-bytes 1

NUM_CALL_TO_REMOTE = 10000
PAYLOAD = b'\x00' * 1
IPC_PATH = 'test.ipc'

class TestThingy:

    def __init__(self):
        self.i = 0

    def send_bytes(self, val: bytes):
        # We are doing nothing with the bytes. Just trying to be compatible
        # to the other benchmark (perf_benchmark.py)
        self.i += 1

    def get_val(self):
        return self.i

class ProxyTestThingy(BaseProxy):

    def send_bytes(self, val: bytes) -> None:
        return self._callmethod('send_bytes', (val,))

    def get_val(self) -> None:
        return self._callmethod('get_val')

def create_db_server_manager() -> BaseManager:

    class DummyManager(BaseManager):
        pass

    DummyManager.register(
        'get_thingy', callable=lambda: TestThingy(), proxytype=ProxyTestThingy)

    manager = DummyManager(address=IPC_PATH)  # type: ignore
    return manager


def create_db_consumer_manager() -> BaseManager:
    class DummyManager(BaseManager):
        pass

    DummyManager.register('get_thingy', proxytype=ProxyTestThingy)

    manager = DummyManager(address=IPC_PATH)  # type: ignore
    manager.connect()
    return manager


def server_proc_launch():
    manager = create_db_server_manager()
    print("server process launched")
    server = manager.get_server()
    server.serve_forever()

def proc_launch():
    manager = create_db_consumer_manager()
    print("consumer process launched")
    thingy = manager.get_thingy()

    back_then = time.perf_counter()
    for i in range(NUM_CALL_TO_REMOTE):
        thingy.send_bytes(PAYLOAD)
    now = time.perf_counter()
    duration = now - back_then
    print(f"we are at {thingy.get_val()}, duration: {duration}")

if __name__ == "__main__":

    print("starting proxy server")
    
    server_proc = multiprocessing.Process(
        target=server_proc_launch,
    )
    server_proc.start()

    print("starting consumer process")

    consumer_proc = multiprocessing.Process(
        target=proc_launch,
    )
    consumer_proc.start()