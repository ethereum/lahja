from lahja import BaseEvent, ConnectionConfig
from lahja.tools import drivers as d


class Event(BaseEvent):
    pass


def test_endpoint_broadcast_from_client_to_server(ipc_base_path, engine):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    received = engine.Event()

    server = d.driver(
        d.serve_endpoint(server_config),
        d.wait_for(Event, on_event=lambda endpoint, event: received.set()),
    )

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_any_then_broadcast(Event()),
    )

    engine.run(server, client)
    assert received.is_set()


def test_endpoint_broadcast_from_server_to_client(ipc_base_path, engine):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    received = engine.Event()

    server = d.driver(
        d.serve_endpoint(server_config),
        d.wait_any_then_broadcast(Event()),
    )

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_for(Event, on_event=lambda endpoint, event: received.set()),
    )

    engine.run(server, client)
    assert received.is_set()
