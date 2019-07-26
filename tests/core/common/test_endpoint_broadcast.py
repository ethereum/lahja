from lahja import BaseEvent, ConnectionConfig
from lahja.tools import drivers as d


class Event(BaseEvent):
    pass


def test_endpoint_broadcast_from_client_to_server(ipc_base_path, runner):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server = d.driver(
        d.serve_endpoint(server_config),
        d.wait_for(Event),
    )

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_any_then_broadcast(Event()),
    )

    runner(server, client)


def test_endpoint_broadcast_from_server_to_client(ipc_base_path, runner):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server = d.driver(
        d.serve_endpoint(server_config),
        d.wait_any_then_broadcast(Event()),
    )

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_for(Event),
    )

    runner(server, client)
