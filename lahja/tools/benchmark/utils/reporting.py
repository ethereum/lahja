import datetime
from logging import Logger
import time

from ..backends import BaseBackend
from ..stats import GlobalStatistic, Total


def print_global_header(logger: Logger, backend: BaseBackend) -> None:

    logger.info(f'{"+++" + backend.name.upper() + "+++":^150}')
    logger.info(f'{"+++Globals+++":^150}')
    logger.info(
        "|{:^19}|{:^16}|{:^23}|{:^20}|{:^16}|{:^16}|{:^16}|".format(
            "Consumer processes",
            "Total time",
            "Total aggegated time",
            "Propagated events",
            "Received events",
            "Propagated EPS",
            "Received EPS",
        )
    )


def print_global_entry(
    logger: Logger,
    num_consumer_processes: int,
    num_events: int,
    global_statistic: GlobalStatistic,
) -> None:

    total_propagates_events = num_events
    total_duration = time.time() - global_statistic._created_at
    total_aggregated_time = sum(
        n.total_aggregated_time for n in global_statistic._entries
    )
    total_received_events = sum(n.num_total for n in global_statistic._entries)
    propagated_eps = total_propagates_events / total_duration
    received_eps = total_received_events / total_duration

    logger.info(
        "|{:^19}|{:^16.5f}|{:^23.5f}|{:^20}|{:^16}|{:^16.3f}|{:^16.3f}|".format(
            num_consumer_processes,
            total_duration,
            total_aggregated_time,
            total_propagates_events,
            total_received_events,
            propagated_eps,
            received_eps,
        )
    )


def print_entry_header(logger: Logger) -> None:
    logger.info(f'{"+++Process Details+++":^150}')
    logger.info(
        "|{:^19}|{:^16}|{:^16}|{:^16}|{:^16}|{:^16}|{:^16}|{:^16}|{:^23}|".format(
            "Process",
            "Processed Events",
            "First sent",
            "Last received",
            "Fastest",
            "Slowest",
            "AVG",
            "Total duration",
            "Total aggregated time",
        )
    )


def to_readable_timestamp(timestamp: float) -> str:
    return datetime.datetime.utcfromtimestamp(timestamp).strftime("%H:%M:%S.%f")[:-3]


def print_entry_line(logger: Logger, total: Total) -> None:
    logger.info(
        "|{:^19}|{:^16}|{:^16}|{:^16}|{:^16.5f}|{:^16.5f}|{:^16.5f}|{:^16.5f}|{:^23.5f}|".format(  # noqa: E501
            total.caption,
            total.num_total,
            to_readable_timestamp(total.first_sent),
            to_readable_timestamp(total.last_received),
            total.duration_fastest,
            total.duration_slowest,
            total.duration_avg,
            total.total_duration,
            total.total_aggregated_time,
        )
    )


def print_full_report(
    logger: Logger,
    backend: BaseBackend,
    num_consumer_processes: int,
    num_events: int,
    global_statistic: GlobalStatistic,
) -> None:

    print_global_header(logger, backend)
    print_global_entry(logger, num_consumer_processes, num_events, global_statistic)

    print_entry_header(logger)
    for total in global_statistic._entries:
        print_entry_line(logger, total)
