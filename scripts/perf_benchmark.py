import argparse
import multiprocessing
import os
import tempfile

from lahja import ConnectionConfig
from lahja.tools.benchmark.backends import AsyncioBackend, BaseBackend
from lahja.tools.benchmark.constants import (
    DRIVER_ENDPOINT,
    REPORTER_ENDPOINT,
    ROOT_ENDPOINT,
)
from lahja.tools.benchmark.process import (
    BroadcastConsumer,
    BroadcastDriver,
    DriverProcessConfig,
    ReportingProcess,
    ReportingProcessConfig,
    RequestConsumer,
    RequestDriver,
)
from lahja.tools.benchmark.typing import ShutdownEvent
from lahja.tools.benchmark.utils.config import (
    create_consumer_endpoint_configs,
    create_consumer_endpoint_name,
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--num-processes",
    type=int,
    default=10,
    help="The number of processes listening for events",
)
parser.add_argument(
    "--num-events", type=int, default=100, help="The number of events propagated"
)
parser.add_argument(
    "--throttle",
    type=float,
    default=0.0,
    help="The time to wait between propagating events",
)
parser.add_argument(
    "--payload-bytes", type=int, default=1, help="The payload of each event in bytes"
)
parser.add_argument("--backend", action="append", help="The endpoint backend to use")
parser.add_argument(
    "--mode",
    default="broadcast",
    choices=("broadcast", "request"),
    help="benchmarks request/response round trip",
)


async def run(args: argparse.Namespace, backend: BaseBackend):
    if args.mode == "broadcast":
        DriverClass = BroadcastDriver
        ConsumerClass = BroadcastConsumer
    elif args.mode == "request":
        DriverClass = RequestDriver
        ConsumerClass = RequestConsumer
    else:
        raise Exception(f"Unknown mode: '{args.mode}'")

    consumer_endpoint_configs = create_consumer_endpoint_configs(args.num_processes)

    (
        config.path.unlink()
        for config in consumer_endpoint_configs
        + tuple(
            ConnectionConfig.from_name(name)
            for name in (ROOT_ENDPOINT, REPORTER_ENDPOINT, DRIVER_ENDPOINT)
        )
    )

    root_config = ConnectionConfig.from_name(ROOT_ENDPOINT)
    async with backend.Endpoint.serve(root_config) as root:
        # The reporter process is collecting statistical events from all consumer processes
        # For some reason, doing this work in the main process didn't end so well which is
        # why it was moved into a dedicated process. Notice that this will slightly skew results
        # as the reporter process will also receive events which we don't account for
        reporting_config = ReportingProcessConfig(
            num_events=args.num_events,
            num_processes=args.num_processes,
            throttle=args.throttle,
            payload_bytes=args.payload_bytes,
            backend=backend,
        )
        reporter = ReportingProcess(reporting_config)
        reporter.start()

        for n in range(args.num_processes):
            consumer_process = ConsumerClass(
                create_consumer_endpoint_name(n), args.num_events, backend=backend
            )
            consumer_process.start()

        # In this benchmark, this is the only process that is flooding events
        driver_config = DriverProcessConfig(
            connected_endpoints=consumer_endpoint_configs,
            num_events=args.num_events,
            throttle=args.throttle,
            payload_bytes=args.payload_bytes,
            backend=backend,
        )
        driver = DriverClass(driver_config)
        driver.start()

        await root.wait_for(ShutdownEvent)
        driver.stop()


if __name__ == "__main__":
    args = parser.parse_args()

    # WARNING: The `fork` method does not work well with asyncio yet.
    # This might change with Python 3.8 (See https://bugs.python.org/issue22087#msg318140)
    multiprocessing.set_start_method("spawn")

    for backend_str in args.backend or ["asyncio"]:
        if backend_str == "asyncio":
            backend = AsyncioBackend()
        else:
            raise Exception(f"Unrecognized backend: {args.backend}")

        original_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as base_dir:
            os.chdir(base_dir)
            try:
                backend.run(run, args, backend)
            finally:
                os.chdir(original_dir)
