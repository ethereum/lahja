from lahja import ConnectionConfig
from lahja.exceptions import ConnectionAttemptRejected
from lahja.tools import drivers as d


def test_connecting_to_server_endpoint(ipc_base_path, engine):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server = d.driver(d.serve_endpoint(server_config))

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_until_connected_to("server"),
    )

    engine.run(server, client)


def test_duplicate_connection_throws_exception(ipc_base_path, engine):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server = d.driver(d.serve_endpoint(server_config))

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_until_connected_to("server"),
        d.throws(d.connect_to_endpoints(server_config), ConnectionAttemptRejected),
    )

    engine.run(server, client)


def test_server_establishes_reverse_connection(ipc_base_path, engine):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server = d.driver(
        d.serve_endpoint(server_config), d.wait_until_connected_to("client")
    )

    client = d.driver(d.run_endpoint("client"), d.connect_to_endpoints(server_config))

    engine.run(server, client)
