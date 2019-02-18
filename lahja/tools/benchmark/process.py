import asyncio
import itertools
import logging
import multiprocessing
import time
from typing import (  # noqa: F401
    NamedTuple,
    Optional,
    Tuple,
)

from lahja import (
    BroadcastConfig,
    ConnectionConfig,
    Endpoint,
)
from lahja.tools.benchmark.constants import (
    DRIVER_ENDPOINT,
    REPORTER_ENDPOINT,
    ROOT_ENDPOINT,
)
from lahja.tools.benchmark.stats import (
    GlobalStatistic,
    LocalStatistic,
)
from lahja.tools.benchmark.typing import (
    PerfMeasureEvent,
    RawMeasureEntry,
    ShutdownEvent,
    TotalRecordedEvent,
)
from lahja.tools.benchmark.utils.reporting import (
    print_full_report,
)


class DriverProcessConfig(NamedTuple):
    num_events: int
    connected_endpoints: Tuple[ConnectionConfig, ...]
    throttle: float
    payload_bytes: int


class DriverProcess:

    def __init__(self, config: DriverProcessConfig) -> None:
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._config,)
        )
        self._process.start()

    @staticmethod
    def launch(config: DriverProcessConfig) -> None:
        loop = asyncio.get_event_loop()
        event_bus = Endpoint()
        event_bus.start_serving_nowait(ConnectionConfig.from_name(DRIVER_ENDPOINT))
        event_bus.connect_to_endpoints_blocking(*config.connected_endpoints)
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        loop.run_until_complete(DriverProcess.worker(event_bus, config))

    @staticmethod
    async def worker(event_bus: Endpoint, config: DriverProcessConfig) -> None:
        payload = b'\x00' * config.payload_bytes
        for n in range(config.num_events):
            await asyncio.sleep(config.throttle)
            event_bus.broadcast(
                PerfMeasureEvent(payload, n, time.time())
            )

        event_bus.stop()


class ConsumerProcess:

    def __init__(self, name: str, num_events: int) -> None:
        self._name = name
        self._num_events = num_events
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._name, self._num_events)
        )
        self._process.start()

    @staticmethod
    async def worker(event_bus: Endpoint, num_events: int) -> None:
        counter = itertools.count(1)
        stats = LocalStatistic()
        async for event in event_bus.stream(PerfMeasureEvent):
            stats.add(RawMeasureEntry(
                sent_at=event.sent_at,
                received_at=time.time()
            ))

            if next(counter) == num_events:
                event_bus.broadcast(
                    TotalRecordedEvent(stats.crunch(event_bus.name)),
                    BroadcastConfig(filter_endpoint=REPORTER_ENDPOINT)
                )
                event_bus.stop()
                break

    @staticmethod
    def launch(name: str, num_events: int) -> None:
        loop = asyncio.get_event_loop()
        event_bus = Endpoint()
        event_bus.start_serving_nowait(ConnectionConfig.from_name(name))
        event_bus.connect_to_endpoints_blocking(
            ConnectionConfig.from_name(REPORTER_ENDPOINT)
        )
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)

        loop.run_until_complete(ConsumerProcess.worker(event_bus, num_events))


class ReportingProcessConfig(NamedTuple):
    num_processes: int
    num_events: int
    throttle: float
    payload_bytes: int


class ReportingProcess:

    def __init__(self, config: ReportingProcessConfig) -> None:
        self._name = REPORTER_ENDPOINT
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._config,)
        )
        self._process.start()

    @staticmethod
    async def worker(event_bus: Endpoint,
                     logger: logging.Logger,
                     config: ReportingProcessConfig) -> None:

        global_statistic = GlobalStatistic()
        async for event in event_bus.stream(TotalRecordedEvent):

            global_statistic.add(event.total)
            if len(global_statistic) == config.num_processes:
                print_full_report(logger, config.num_processes, config.num_events, global_statistic)
                event_bus.broadcast(ShutdownEvent(), BroadcastConfig(filter_endpoint=ROOT_ENDPOINT))
                event_bus.stop()
                break

    @staticmethod
    def launch(config: ReportingProcessConfig) -> None:
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        logger = logging.getLogger('reporting')

        loop = asyncio.get_event_loop()
        event_bus = Endpoint()
        event_bus.start_serving_nowait(ConnectionConfig.from_name(REPORTER_ENDPOINT))
        event_bus.connect_to_endpoints_blocking(ConnectionConfig.from_name(ROOT_ENDPOINT))

        loop.run_until_complete(ReportingProcess.worker(event_bus, logger, config))
