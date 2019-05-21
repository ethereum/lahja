import itertools
import logging
import multiprocessing
import time
from typing import Any, AsyncGenerator, List, NamedTuple, Optional, Tuple  # noqa: F401

from lahja import BroadcastConfig, ConnectionConfig
from lahja.tools.benchmark.backends import BaseBackend
from lahja.tools.benchmark.constants import (
    DRIVER_ENDPOINT,
    REPORTER_ENDPOINT,
    ROOT_ENDPOINT,
)
from lahja.tools.benchmark.stats import GlobalStatistic, LocalStatistic
from lahja.tools.benchmark.typing import (
    PerfMeasureEvent,
    RawMeasureEntry,
    ShutdownEvent,
    TotalRecordedEvent,
)
from lahja.tools.benchmark.utils.reporting import print_full_report


class DriverProcessConfig(NamedTuple):
    num_events: int
    connected_endpoints: Tuple[ConnectionConfig, ...]
    throttle: float
    payload_bytes: int
    backend: BaseBackend


class DriverProcess:
    def __init__(self, config: DriverProcessConfig) -> None:
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch, args=(self._config,), daemon=True
        )
        self._process.start()

    def stop(self) -> None:
        assert self._process is not None
        self._process.terminate()
        self._process.join(1)

    @staticmethod
    def launch(config: DriverProcessConfig) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        config.backend.run(DriverProcess.worker, config)

    @staticmethod
    async def worker(config: DriverProcessConfig) -> None:
        conn_config = ConnectionConfig.from_name(DRIVER_ENDPOINT)
        async with config.backend.Endpoint.serve(conn_config) as event_bus:
            await event_bus.connect_to_endpoints(*config.connected_endpoints)
            await event_bus.wait_until_all_remotes_subscribed_to(PerfMeasureEvent)

            counter = itertools.count()
            payload = b"\x00" * config.payload_bytes
            while True:
                await config.backend.sleep(config.throttle)
                await event_bus.broadcast(
                    PerfMeasureEvent(payload, next(counter), time.time())
                )


class ConsumerProcess:
    def __init__(self, name: str, num_events: int, backend: BaseBackend) -> None:
        self._name = name
        self._num_events = num_events
        self._process: Optional[multiprocessing.Process] = None
        self._backend = backend

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch, args=(self._name, self._num_events, self._backend)
        )
        self._process.start()

    @staticmethod
    def launch(name: str, num_events: int, backend: BaseBackend) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        backend.run(ConsumerProcess.worker, backend, name, num_events)

    @staticmethod
    async def worker(backend: BaseBackend, name: str, num_events: int) -> None:
        config = ConnectionConfig.from_name(name)
        async with backend.Endpoint.serve(config) as event_bus:
            await event_bus.connect_to_endpoints(
                ConnectionConfig.from_name(REPORTER_ENDPOINT)
            )
            await event_bus.wait_until_all_remotes_subscribed_to(TotalRecordedEvent)

            stats = LocalStatistic()
            events = event_bus.stream(PerfMeasureEvent, num_events=num_events)
            async for event in events:
                stats.add(
                    RawMeasureEntry(sent_at=event.sent_at, received_at=time.time())
                )

            await event_bus.broadcast(
                TotalRecordedEvent(stats.crunch(event_bus.name)),
                BroadcastConfig(filter_endpoint=REPORTER_ENDPOINT),
            )


class ReportingProcessConfig(NamedTuple):
    num_processes: int
    num_events: int
    throttle: float
    payload_bytes: int
    backend: BaseBackend


class ReportingProcess:
    def __init__(self, config: ReportingProcessConfig) -> None:
        self._name = REPORTER_ENDPOINT
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch, args=(self._config,)
        )
        self._process.start()

    @staticmethod
    def launch(config: ReportingProcessConfig) -> None:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        logger = logging.getLogger("reporting")

        config.backend.run(ReportingProcess.worker, logger, config)

    @staticmethod
    async def worker(logger: logging.Logger, config: ReportingProcessConfig) -> None:
        conn_config = ConnectionConfig.from_name(REPORTER_ENDPOINT)
        async with config.backend.Endpoint.serve(conn_config) as event_bus:
            await event_bus.connect_to_endpoints(
                ConnectionConfig.from_name(ROOT_ENDPOINT)
            )

            global_statistic = GlobalStatistic()
            events = event_bus.stream(
                TotalRecordedEvent, num_events=config.num_processes
            )
            async for event in events:
                global_statistic.add(event.total)

            print_full_report(
                logger,
                config.backend,
                config.num_processes,
                config.num_events,
                global_statistic,
            )
            await event_bus.broadcast(
                ShutdownEvent(), BroadcastConfig(filter_endpoint=ROOT_ENDPOINT)
            )
