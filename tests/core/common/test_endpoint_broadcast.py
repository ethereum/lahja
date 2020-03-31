from lahja import BaseEvent, BroadcastConfig, ConnectionConfig
from lahja.exceptions import NoSubscribers
from lahja.tools import drivers as d


class Event(BaseEvent):
    pass


def test_endpoint_broadcast_from_client_to_server(ipc_base_path, runner):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server = d.driver(d.serve_endpoint(server_config), d.wait_for(Event))

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_any_then_broadcast(Event()),
    )

    runner(server, client)


def test_endpoint_broadcast_from_server_to_client(ipc_base_path, runner):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server = d.driver(
        d.serve_endpoint(server_config), d.wait_any_then_broadcast(Event())
    )

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_for(Event),
    )

    runner(server, client)


def test_broadcast_without_listeners_throws(ipc_base_path, runner):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)
    server_done, client_done = d.checkpoint("done")

    server = d.driver(d.serve_endpoint(server_config), server_done)

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_until_connected_to("server"),
        d.throws(d.broadcast(Event()), NoSubscribers),
        client_done,
    )

    runner(server, client)


def test_broadcast_without_listeners_explicitly_allowed(ipc_base_path, runner):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)
    server_done, client_done = d.checkpoint("done")

    server = d.driver(d.serve_endpoint(server_config), server_done)

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_until_connected_to("server"),
        d.broadcast(Event(), BroadcastConfig(require_subscriber=False)),
        client_done,
    )

    runner(server, client)
