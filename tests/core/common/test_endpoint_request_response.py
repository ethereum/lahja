import multiprocessing

from lahja import BaseEvent, BaseRequestResponseEvent, ConnectionConfig
from lahja.exceptions import NoSubscribers
from lahja.tools import drivers as d


class Response(BaseEvent):
    pass


class Request(BaseRequestResponseEvent[Response]):
    @staticmethod
    def expected_response_type():
        return Response


def test_request_response(runner, ipc_base_path):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    received = multiprocessing.Event()

    server = d.driver(
        d.serve_endpoint(server_config),
        d.serve_request(Request, get_response=lambda endpoint, event: Response()),
    )

    client = d.driver(
        d.run_endpoint("client"),
        d.connect_to_endpoints(server_config),
        d.wait_until_any_endpoint_subscribed_to(Request),
        d.request(Request(), on_response=lambda endpoint, event: received.set()),
    )

    runner(server, client)
    assert received.is_set()


def test_request_without_subscriber_throws(runner, ipc_base_path):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server = d.driver(d.serve_endpoint(server_config))

    client = d.driver(
        d.run_endpoint("client"), d.throws(d.request(Request()), NoSubscribers)
    )

    runner(server, client)
