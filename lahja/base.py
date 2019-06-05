from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import (  # noqa: F401
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from .common import (
    BaseEvent,
    BaseRequestResponseEvent,
    Broadcast,
    BroadcastConfig,
    ConnectionConfig,
    Message,
    Msg,
    Subscription,
    SubscriptionsUpdated,
)
from .exceptions import RemoteDisconnected
from .typing import ConditionAPI, EventAPI, LockAPI

TResponse = TypeVar("TResponse", bound=BaseEvent)
TWaitForEvent = TypeVar("TWaitForEvent", bound=BaseEvent)
TSubscribeEvent = TypeVar("TSubscribeEvent", bound=BaseEvent)
TStreamEvent = TypeVar("TStreamEvent", bound=BaseEvent)


class ConnectionAPI(ABC):
    @classmethod
    @abstractmethod
    async def connect_to(cls, path: Path) -> "ConnectionAPI":
        ...

    @abstractmethod
    async def send_message(self, message: Msg) -> None:
        pass

    @abstractmethod
    async def read_message(self) -> Message:
        pass


class RemoteEndpointAPI(ABC):
    """
    Represents a connection to another endpoint.  Connections *can* be
    bi-directional with messages flowing in either direction.

    A 'message' can be any of:

    - ``SubscriptionsUpdated``
            broadcasting the subscriptions that the endpoint on the other side
            of this connection is interested in.
    - ``SubscriptionsAck``
            acknowledgedment of a ``SubscriptionsUpdated``
    - ``Broadcast``
            an event meant to be processed by the endpoint.
    """

    name: Optional[str]
    conn: ConnectionAPI

    subscribed_messages: Set[Type[BaseEvent]]

    _running: EventAPI
    _stopped: EventAPI

    _notify_lock: LockAPI

    _received_response: ConditionAPI
    _received_subscription: ConditionAPI

    subscribed_events: Set[Type[BaseEvent]]

    _subscriptions_initialized: EventAPI

    #
    # Object run lifecycle
    #
    @abstractmethod
    async def wait_started(self) -> None:
        ...

    @abstractmethod
    async def wait_stopped(self) -> None:
        ...

    @abstractmethod
    def is_running(self) -> bool:
        ...

    @abstractmethod
    def is_stopped(self) -> bool:
        ...

    @abstractmethod
    async def start(self) -> None:
        ...

    @abstractmethod
    async def stop(self) -> None:
        ...

    @abstractmethod
    async def _run(self) -> None:
        ...

    #
    # Core external API
    #
    @abstractmethod
    async def notify_subscriptions_updated(
        self, subscriptions: Set[Type[BaseEvent]], block: bool = True
    ) -> None:
        """
        Alert the endpoint on the other side of this connection that the local
        subscriptions have changed. If ``block`` is ``True`` then this function
        will block until the remote endpoint has acknowledged the new
        subscription set. If ``block`` is ``False`` then this function will
        return immediately after the send finishes.
        """
        ...

    @abstractmethod
    def can_send_item(self, item: BaseEvent, config: Optional[BroadcastConfig]) -> bool:
        ...

    @abstractmethod
    async def send_message(self, message: Msg) -> None:
        ...

    @abstractmethod
    async def wait_until_subscription_received(self) -> None:
        ...


class BaseRemoteEndpoint(RemoteEndpointAPI):
    """
    This base class implements common logic that can be shared across different
    implementations of the `RemoteEndpointAPI`

    Represents a connection to another endpoint.  Connections *can* be
    bi-directional with messages flowing in either direction.
    """

    logger = logging.getLogger("lahja.endpoint.asyncio.RemoteEndpoint")

    def __init__(
        self,
        name: Optional[str],
        conn: ConnectionAPI,
        new_msg_func: Callable[[Broadcast], Awaitable[Any]],
    ) -> None:
        self.name = name
        self.conn = conn
        self.new_msg_func = new_msg_func

        self.subscribed_messages: Set[Type[BaseEvent]] = set()

    def __str__(self) -> str:
        return f"RemoteEndpoint[{self.name if self.name is not None else id(self)}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    async def wait_started(self) -> None:
        await self._running.wait()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    def is_running(self) -> bool:
        return not self.is_stopped and self.running.is_set()

    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    async def notify_subscriptions_updated(
        self, subscriptions: Set[Type[BaseEvent]], block: bool = True
    ) -> None:
        """
        Alert the endpoint on the other side of this connection that the local
        subscriptions have changed. If ``block`` is ``True`` then this function
        will block until the remote endpoint has acknowledged the new
        subscription set. If ``block`` is ``False`` then this function will
        return immediately after the send finishes.
        """
        # The extra lock ensures only one coroutine can notify this endpoint at any one time
        # and that no replies are accidentally received by the wrong
        # coroutines. Without this, in the case where `block=True`, this inner
        # block would release the lock on the call to `wait()` which would
        # allow the ack from a different update to incorrectly result in this
        # returning before the ack had been received.
        async with self._notify_lock:
            async with self._received_response:
                try:
                    await self.conn.send_message(
                        SubscriptionsUpdated(subscriptions, block)
                    )
                except RemoteDisconnected:
                    return
                if block:
                    await self._received_response.wait()

    def can_send_item(self, item: BaseEvent, config: Optional[BroadcastConfig]) -> bool:
        if config is not None:
            if self.name is not None and not config.allowed_to_receive(self.name):
                return False
            elif config.filter_event_id is not None:
                # the item is a response to a request.
                return True

        return type(item) in self.subscribed_messages

    async def send_message(self, message: Msg) -> None:
        await self.conn.send_message(message)

    async def wait_until_subscription_received(self) -> None:
        async with self._received_subscription:
            await self._received_subscription.wait()


class EndpointAPI(ABC):
    """
    The :class:`~lahja.endpoint.Endpoint` enables communication between different processes
    as well as within a single process via various event-driven APIs.
    """

    __slots__ = ("name",)

    name: str

    @property
    @abstractmethod
    def is_running(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_serving(self) -> bool:
        ...

    #
    # Running and Server API
    #
    @abstractmethod
    def run(self) -> AsyncContextManager["BaseEndpoint"]:
        """
        Context manager API for running endpoints.

        .. code-block::

            async with endpoint.run() as endpoint:
                ... # endpoint running within context
            ... # endpoint stopped after

        test
        """
        ...

    @classmethod
    @abstractmethod
    def serve(cls, config: ConnectionConfig) -> AsyncContextManager["BaseEndpoint"]:
        """
        Context manager API for running and endpoint server.

        .. code-block::

            async with endpoint.serve():
                ... # server running within context
            ... # server stopped

        """
        ...

    #
    # Connection API
    #
    @abstractmethod
    async def connect_to_endpoint(self, config: ConnectionConfig) -> None:
        """
        Establish a new connection to an endpoint.
        """
        ...

    @abstractmethod
    def is_connected_to(self, endpoint_name: str) -> bool:
        """
        Return whether this endpoint is connected to another endpoint with the given name.
        """
        ...

    @abstractmethod
    async def wait_until_connected_to(self, endpoint_name: str) -> None:
        """
        Return once a connection exists to an endpoint with the given name.
        """
        ...

    @abstractmethod
    def get_connected_endpoints_and_subscriptions(
        self
    ) -> Tuple[Tuple[Optional[str], Set[Type[BaseEvent]]], ...]:
        """
        Return 2-tuples for all all connected endpoints containing the name of
        the endpoint (which might be ``None``) coupled with the set of messages
        the endpoint subscribes to
        """
        ...

    @abstractmethod
    async def wait_until_connections_change(self) -> None:
        """
        Block until the set of connected remote endpoints changes.
        """
        ...

    #
    # Event API
    #
    @abstractmethod
    async def broadcast(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        Broadcast an instance of :class:`~lahja.common.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.common.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        ...

    @abstractmethod
    def broadcast_nowait(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        A sync compatible version of :meth:`~lahja.base.EndpointAPI.broadcast`

        .. warning::

            Heavy use of :meth:`~lahja.base.EndpointAPI.broadcast_nowait` in
            contiguous blocks of code without yielding to the `async`
            implementation should be expected to cause problems.

        """
        pass

    @abstractmethod
    async def request(
        self,
        item: BaseRequestResponseEvent[TResponse],
        config: Optional[BroadcastConfig] = None,
    ) -> TResponse:
        """
        Broadcast an instance of
        :class:`~lahja.common.BaseRequestResponseEvent` on the event bus and
        immediately wait on an expected answer of type
        :class:`~lahja.common.BaseEvent`. Optionally pass a second parameter of
        :class:`~lahja.common.BroadcastConfig` to decide where the request
        should be broadcasted to. By default, requests are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        ...

    @abstractmethod
    def subscribe(
        self,
        event_type: Type[TSubscribeEvent],
        handler: Callable[[TSubscribeEvent], Union[Any, Awaitable[Any]]],
    ) -> Subscription:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.common.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        ...

    @abstractmethod
    async def stream(
        self, event_type: Type[TStreamEvent], num_events: Optional[int] = None
    ) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        yield  # type: ignore  # yield statemen convinces mypy this is a generator function

    @abstractmethod
    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        ...

    #
    # Subscription API
    #
    @abstractmethod
    async def wait_until_remote_subscriptions_change(self) -> None:
        """
        Block until any subscription change occurs on any remote endpoint or
        the set of remote endpoints changes
        """
        ...

    @abstractmethod
    def is_remote_subscribed_to(
        self, remote_endpoint: str, event_type: Type[BaseEvent]
    ) -> bool:
        """
        Return ``True`` if the specified remote endpoint is subscribed to the specified event type
        from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    def is_any_remote_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if at least one of the connected remote endpoints is subscribed to the
        specified event type from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    def are_all_remotes_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if every connected remote endpoint is subscribed to the specified event
        type from this endpoint. Otherwise return ``False``.
        """
        ...

    @abstractmethod
    async def wait_until_remote_subscribed_to(
        self, remote_endpoint: str, event: Type[BaseEvent]
    ) -> None:
        """
        Block until the specified remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        ...

    @abstractmethod
    async def wait_until_any_remote_subscribed_to(self, event: Type[BaseEvent]) -> None:
        """
        Block until any other remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        ...

    @abstractmethod
    async def wait_until_all_remotes_subscribed_to(
        self, event: Type[BaseEvent]
    ) -> None:
        """
        Block until all currently connected remote endpoints are subscribed to the specified
        event type from this endpoint.
        """
        ...


class BaseEndpoint(EndpointAPI):
    """
    Base class for endpoint implementations that implements shared/common logic
    """

    logger = logging.getLogger("lahja.endpoint.Endpoint")

    def __str__(self) -> str:
        return f"Endpoint[{self.name}]"

    def __repr__(self) -> str:
        return f"<{self.name}>"

    #
    # Common implementations
    #
    def __reduce__(self) -> None:  # type: ignore
        raise NotImplementedError("Endpoints cannot be pickled")

    #
    # Connection API
    #
    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Naive asynchronous implementation.  Subclasses should consider
        connecting concurrently.
        """
        for config in endpoints:
            await self.connect_to_endpoint(config)

    async def wait_until_connected_to(self, endpoint_name: str) -> None:
        """
        Return once a connection exists to an endpoint with the given name.
        """
        while True:
            if self.is_connected_to(endpoint_name):
                return
            await self.wait_until_connections_change()

    #
    # Event API
    #
    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        agen = self.stream(event_type, num_events=1)
        event = await agen.asend(None)
        await agen.aclose()
        return event

    #
    # Subscription API
    #
    def is_remote_subscribed_to(
        self, remote_endpoint: str, event_type: Type[BaseEvent]
    ) -> bool:
        """
        Return ``True`` if the specified remote endpoint is subscribed to the specified event type
        from this endpoint. Otherwise return ``False``.
        """
        for endpoint, subscriptions in self.get_connected_endpoints_and_subscriptions():
            if endpoint != remote_endpoint:
                continue

            for subscribed_event_type in subscriptions:
                if subscribed_event_type is event_type:
                    return True

        return False

    def is_any_remote_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if at least one of the connected remote endpoints is subscribed to the
        specified event type from this endpoint. Otherwise return ``False``.
        """
        for endpoint, subscriptions in self.get_connected_endpoints_and_subscriptions():
            for subscribed_event_type in subscriptions:
                if subscribed_event_type is event_type:
                    return True

        return False

    def are_all_remotes_subscribed_to(self, event_type: Type[BaseEvent]) -> bool:
        """
        Return ``True`` if every connected remote endpoint is subscribed to the specified event
        type from this endpoint. Otherwise return ``False``.
        """
        for endpoint, subscriptions in self.get_connected_endpoints_and_subscriptions():
            if event_type not in subscriptions:
                return False

        return True

    async def wait_until_remote_subscribed_to(
        self, remote_endpoint: str, event: Type[BaseEvent]
    ) -> None:
        """
        Block until the specified remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        while True:
            if self.is_remote_subscribed_to(remote_endpoint, event):
                return
            await self.wait_until_remote_subscriptions_change()

    async def wait_until_any_remote_subscribed_to(self, event: Type[BaseEvent]) -> None:
        """
        Block until any other remote endpoint has subscribed to the specified event type
        from this endpoint.
        """

        while True:
            if self.is_any_remote_subscribed_to(event):
                return
            await self.wait_until_remote_subscriptions_change()

    async def wait_until_all_remotes_subscribed_to(
        self, event: Type[BaseEvent]
    ) -> None:
        """
        Block until all currently connected remote endpoints are subscribed to the specified
        event type from this endpoint.
        """
        while True:
            if self.are_all_remotes_subscribed_to(event):
                return
            await self.wait_until_remote_subscriptions_change()
