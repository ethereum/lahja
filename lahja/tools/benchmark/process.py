from abc import ABC, abstractmethod
import itertools
import logging
import multiprocessing
import os
import signal
import time
from typing import Any, AsyncGenerator, List, NamedTuple, Optional, Tuple  # noqa: F401

from lahja import BroadcastConfig, ConnectionConfig
from lahja.base import BaseEndpoint
from lahja.tools.benchmark.backends import BaseBackend
from lahja.tools.benchmark.constants import (
    DRIVER_ENDPOINT,
    REPORTER_ENDPOINT,
    ROOT_ENDPOINT,
)
from lahja.tools.benchmark.logging import setup_stderr_lahja_logging
from lahja.tools.benchmark.stats import GlobalStatistic, LocalStatistic
from lahja.tools.benchmark.typing import (
    PerfMeasureEvent,
    PerfMeasureRequest,
    PerfMeasureResponse,
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
    debug_logging: bool


class BaseDriverProcess(ABC):
    logger = logging.getLogger("lahja.tools.benchmark.process.DriverProcess")

    def __init__(self, config: DriverProcessConfig) -> None:
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch, args=(self._config,), daemon=True
        )
        self._process.start()

    def stop(self) -> None:
        if self._process is None:
            raise Exception("no process")
        elif self._process.pid is not None:
            os.kill(self._process.pid, signal.SIGINT)
        else:
            self._process.terminate()

        try:
            self._process.join(1)
        except TimeoutError:
            self._process.terminate()
            self._process.join(1)

    @classmethod
    def launch(cls, config: DriverProcessConfig) -> None:
        if config.debug_logging:
            setup_stderr_lahja_logging()

        try:
            config.backend.run(cls.worker, config)
        except KeyboardInterrupt:
            return

    @classmethod
    async def worker(cls, config: DriverProcessConfig) -> None:
        conn_config = ConnectionConfig.from_name(DRIVER_ENDPOINT)
        async with config.backend.Endpoint.serve(conn_config) as event_bus:
            await event_bus.connect_to_endpoints(*config.connected_endpoints)

            await cls.do_driver(event_bus, config)

    @staticmethod
    @abstractmethod
    async def do_driver(event_bus: BaseEndpoint, config: DriverProcessConfig) -> None:
        ...


class BroadcastDriver(BaseDriverProcess):
    @staticmethod
    async def do_driver(event_bus: BaseEndpoint, config: DriverProcessConfig) -> None:
        for consumer in config.connected_endpoints:
            await event_bus.wait_until_remote_subscribed_to(
                consumer.name, PerfMeasureEvent
            )

        counter = itertools.count()
        payload = b"\x00" * config.payload_bytes
        while True:
            await config.backend.sleep(config.throttle)
            await event_bus.broadcast(
                PerfMeasureEvent(payload, next(counter), time.time())
            )


class RequestDriver(BaseDriverProcess):
    @classmethod
    async def do_driver(
        cls, event_bus: BaseEndpoint, config: DriverProcessConfig
    ) -> None:
        for consumer in config.connected_endpoints:
            await event_bus.wait_until_remote_subscribed_to(
                consumer.name, PerfMeasureRequest
            )

        counter = itertools.count()
        payload = b"\x00" * config.payload_bytes
        while True:
            await config.backend.sleep(config.throttle)
            await event_bus.request(
                PerfMeasureRequest(payload, next(counter), time.time())
            )


class ConsumerConfig(NamedTuple):
    num_events: int
    backend: BaseBackend
    debug_logging: bool


class BaseConsumerProcess(ABC):
    logger = logging.getLogger("lahja.tools.benchmark.process.ConsumerProcess")

    def __init__(self, name: str, config: ConsumerConfig) -> None:
        self._name = name
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch, args=(self._name, self._config)
        )
        self._process.start()

    @classmethod
    def launch(cls, name: str, config: ConsumerConfig) -> None:
        if config.debug_logging:
            setup_stderr_lahja_logging()

        config.backend.run(cls.worker, name, config)

    @classmethod
    async def worker(cls, name: str, config: ConsumerConfig) -> None:
        conn_config = ConnectionConfig.from_name(name)
        async with config.backend.Endpoint.serve(conn_config) as event_bus:
            await event_bus.connect_to_endpoints(
                ConnectionConfig.from_name(REPORTER_ENDPOINT)
            )
            await event_bus.wait_until_connected_to(DRIVER_ENDPOINT)
            stats = await cls.do_consumer(event_bus, config)

            await event_bus.wait_until_remote_subscribed_to(
                REPORTER_ENDPOINT, TotalRecordedEvent
            )

            await event_bus.broadcast(
                TotalRecordedEvent(stats.crunch(event_bus.name)),
                BroadcastConfig(filter_endpoint=REPORTER_ENDPOINT),
            )

    @staticmethod
    @abstractmethod
    async def do_consumer(
        event_bus: BaseEndpoint, config: ConsumerConfig
    ) -> LocalStatistic:
        ...


class BroadcastConsumer(BaseConsumerProcess):
    @staticmethod
    async def do_consumer(
        event_bus: BaseEndpoint, config: ConsumerConfig
    ) -> LocalStatistic:
        stats = LocalStatistic()
        events = event_bus.stream(PerfMeasureEvent, num_events=config.num_events)
        async for event in events:
            stats.add(RawMeasureEntry(sent_at=event.sent_at, received_at=time.time()))
        return stats


class RequestConsumer(BaseConsumerProcess):
    @staticmethod
    async def do_consumer(
        event_bus: BaseEndpoint, config: ConsumerConfig
    ) -> LocalStatistic:
        stats = LocalStatistic()
        events = event_bus.stream(PerfMeasureRequest, num_events=config.num_events)
        async for event in events:
            await event_bus.broadcast(PerfMeasureResponse(), event.broadcast_config())
            stats.add(RawMeasureEntry(sent_at=event.sent_at, received_at=time.time()))
        return stats


class ReportingProcessConfig(NamedTuple):
    num_processes: int
    num_events: int
    throttle: float
    payload_bytes: int
    backend: BaseBackend
    debug_logging: bool


class ReportingProcess:
    logger = logging.getLogger("lahja.tools.benchmark.process.ReportingProcess")

    def __init__(self, config: ReportingProcessConfig) -> None:
        self._name = REPORTER_ENDPOINT
        self._config = config
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch, args=(self._config,)
        )
        self._process.start()

    @classmethod
    def launch(cls, config: ReportingProcessConfig) -> None:
        if config.debug_logging:
            setup_stderr_lahja_logging()

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
