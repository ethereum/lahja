from typing import Tuple

from lahja import ConnectionConfig


def create_consumer_endpoint_configs(
    num_processes: int
) -> Tuple[ConnectionConfig, ...]:
    return tuple(
        ConnectionConfig.from_name(create_consumer_endpoint_name(i))
        for i in range(num_processes)
    )


def create_consumer_endpoint_name(id: int) -> str:
    return f"consumer_{id}"
