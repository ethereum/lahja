import argparse
import asyncio
import multiprocessing
import os
import signal

from lahja import (
    ConnectionConfig,
    Endpoint,
)

from lahja.tools.benchmark.constants import (
    REPORTER_ENDPOINT,
    ROOT_ENDPOINT,
)
from lahja.tools.benchmark.process import (
    ConsumerProcess,
    DriverProcess,
    DriverProcessConfig,
    ReportingProcess,
    ReportingProcessConfig,
)
from lahja.tools.benchmark.typing import (
    ShutdownEvent,
)
from lahja.tools.benchmark.utils.config import (
    create_consumer_endpoint_configs,
    create_consumer_endpoint_name,
)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--num-processes', type=int, default=10,
                        help='The number of processes listening for events')
    parser.add_argument('--num-events', type=int, default=100,
                        help='The number of events propagated')
    parser.add_argument('--throttle', type=float, default=0.0,
                        help='The time to wait between propagating events')
    parser.add_argument('--payload-bytes', type=int, default=1,
                        help='The payload of each event in bytes')
    args = parser.parse_args()

    # WARNING: The `fork` method does not work well with asyncio yet.
    # This might change with Python 3.8 (See https://bugs.python.org/issue22087#msg318140)
    multiprocessing.set_start_method('spawn')

    consumer_endpoint_configs = create_consumer_endpoint_configs(args.num_processes)

    root = Endpoint()
    root.start_serving_nowait(ConnectionConfig.from_name(ROOT_ENDPOINT))

    # In this benchmark, this is the only process that is flooding events
    driver_config = DriverProcessConfig(
        connected_endpoints=consumer_endpoint_configs,
        num_events=args.num_events,
        throttle=args.throttle,
        payload_bytes=args.payload_bytes,
    )
    driver = DriverProcess(driver_config)

    # The reporter process is collecting statistical events from all consumer processes
    # For some reason, doing this work in the main process didn't end so well which is
    # why it was moved into a dedicated process. Notice that this will slightly skew results
    # as the reporter process will also receive events which we don't account for
    reporting_config = ReportingProcessConfig(
        num_events=args.num_events,
        num_processes=args.num_processes,
        throttle=args.throttle,
        payload_bytes=args.payload_bytes,
    )
    reporter = ReportingProcess(reporting_config)

    for n in range(args.num_processes):
        consumer_process = ConsumerProcess(
            create_consumer_endpoint_name(n),
            args.num_events,
        )
        consumer_process.start()

    async def shutdown():
        await root.wait_for(ShutdownEvent)
        root.stop()
        asyncio.get_event_loop().stop()

    reporter.start()
    driver.start()
    asyncio.get_event_loop().run_until_complete(shutdown())
