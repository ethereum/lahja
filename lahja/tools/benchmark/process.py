import asyncio
import itertools
import logging
import multiprocessing
import time
from typing import (  # noqa: F401
    NamedTuple,
    Optional,
)

from lahja import (
    BroadcastConfig,
    Endpoint,
)
from lahja.tools.benchmark.constants import (
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
    event_bus: Endpoint
    num_events: int
    throttle: float
    payload_bytes: int


class DriverProcess:

    def __init__(self, config: DriverProcessConfig) -> None:
        self._name = config.event_bus.name
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
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        loop = asyncio.get_event_loop()
        config.event_bus.connect_no_wait()
        loop.run_until_complete(DriverProcess.worker(config))

    @staticmethod
    async def worker(config: DriverProcessConfig) -> None:
        payload = b'\x00' * config.payload_bytes
        event_bus = config.event_bus
        for n in range(config.num_events):
            await asyncio.sleep(config.throttle)
            event_bus.broadcast(
                PerfMeasureEvent(payload, n, time.time())
            )

        event_bus.stop()


class ConsumerProcess:

    def __init__(self, num_events: int, event_bus: Endpoint) -> None:
        self._name = event_bus.name
        self._event_bus = event_bus
        self._num_events = num_events
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._event_bus, self._num_events)
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
    def launch(event_bus: Endpoint, num_events: int) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)

        loop = asyncio.get_event_loop()
        event_bus.connect_no_wait()

        loop.run_until_complete(ConsumerProcess.worker(event_bus, num_events))


class ReportingProcessConfig(NamedTuple):
    event_bus: Endpoint
    num_processes: int
    num_events: int
    throttle: float
    payload_bytes: int


class ReportingProcess:

    def __init__(self, config: ReportingProcessConfig) -> None:
        self._name = config.event_bus.name
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._config,)
        )
        self._process.start()

    @staticmethod
    async def worker(logger: logging.Logger, config: ReportingProcessConfig) -> None:
        global_statistic = GlobalStatistic()
        event_bus = config.event_bus
        async for event in event_bus.stream(TotalRecordedEvent):

            global_statistic.add(event.total)
            if len(global_statistic) == config.num_processes:
                print_full_report(logger, config.num_processes, config.num_events, global_statistic)
                event_bus.broadcast(ShutdownEvent(), BroadcastConfig(filter_endpoint=ROOT_ENDPOINT))
                event_bus.stop()

    @staticmethod
    def launch(config: ReportingProcessConfig) -> None:
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        logger = logging.getLogger('reporting')

        loop = asyncio.get_event_loop()
        config.event_bus.connect_no_wait()

        loop.run_until_complete(ReportingProcess.worker(logger, config))
