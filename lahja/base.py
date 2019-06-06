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
    Hello,
    Message,
    Msg,
    Subscription,
    SubscriptionsAck,
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
    """

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    #
    # Object run lifecycle
    #
    @abstractmethod
    async def wait_started(self) -> None:
        ...

    @abstractmethod
    async def wait_ready(self) -> None:
        ...

    @abstractmethod
    async def wait_stopped(self) -> None:
        ...

    @abstractmethod
    def is_running(self) -> bool:
        ...

    @abstractmethod
    def is_ready(self) -> bool:
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
    def get_subscribed_events(self) -> Set[Type[BaseEvent]]:
        ...

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
    async def send_message(self, message: Msg) -> None:
        ...

    @abstractmethod
    async def wait_until_subscription_initialized(self) -> None:
        ...


class BaseRemoteEndpoint(RemoteEndpointAPI):
    """
    This base class implements common logic that can be shared across different
    implementations of the `RemoteEndpointAPI`

    Represents a connection to another endpoint.  Connections *can* be
    bi-directional with messages flowing in either direction.
    """

    logger = logging.getLogger("lahja.base.RemoteEndpoint")

    conn: ConnectionAPI

    _local_name: str
    _running: EventAPI
    _ready: EventAPI
    _stopped: EventAPI

    _notify_lock: LockAPI

    _received_response: ConditionAPI
    _received_subscription: ConditionAPI

    _subscribed_events: Set[Type[BaseEvent]]

    _subscriptions_initialized: EventAPI

    def __init__(
        self,
        local_name: str,
        conn: ConnectionAPI,
        new_msg_func: Callable[[Broadcast], Awaitable[Any]],
    ) -> None:
        self._local_name = local_name
        self.conn = conn
        self.new_msg_func = new_msg_func

        self._subscribed_events = set()

    def __str__(self) -> str:
        return f"RemoteEndpoint[{self._name if self._name is not None else id(self)}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    _name: Optional[str] = None

    @property
    def name(self) -> str:
        if self._name is None:
            raise AttributeError("Endpoint name has not yet been established")
        else:
            return self._name

    async def wait_started(self) -> None:
        await self._running.wait()

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    def is_running(self) -> bool:
        return not self.is_stopped and self.running.is_set()

    def is_ready(self) -> bool:
        return self.is_running and self._ready.is_set()

    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    async def _run(self) -> None:
        self._running.set()

        # Send the hello message
        await self.send_message(Hello(self._local_name))

        # Wait for the other endpoint to identify itself.
        hello = await self.conn.read_message()
        if isinstance(hello, Hello):
            self._name = hello.name
            self.logger.debug(
                "RemoteEndpoint connection established: %s <-> %s",
                self._local_name,
                self.name,
            )
            self._ready.set()
        else:
            self.logger.debug(
                "Invalid message: First message must be `Hello`, Got: %s", hello
            )
            self._stopped.set()
            return

        while self.is_running:
            try:
                message = await self.conn.read_message()
            except RemoteDisconnected:
                async with self._received_response:
                    self._received_response.notify_all()
                return

            if isinstance(message, Broadcast):
                await self.new_msg_func(message)
            elif isinstance(message, SubscriptionsUpdated):
                self._subscriptions_initialized.set()
                async with self._received_subscription:
                    self._subscribed_events = message.subscriptions
                    self._received_subscription.notify_all()
                # The ack is sent after releasing the lock since we've already
                # exited the code which actually updates the subscriptions and
                # we are merely responding to the sender to acknowledge
                # receipt.
                if message.response_expected:
                    await self.send_message(SubscriptionsAck())
            elif isinstance(message, SubscriptionsAck):
                async with self._received_response:
                    self._received_response.notify_all()
            else:
                self.logger.error(f"received unexpected message: {message}")

    def get_subscribed_events(self) -> Set[Type[BaseEvent]]:
        return self._subscribed_events

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

    async def send_message(self, message: Msg) -> None:
        await self.conn.send_message(message)

    async def wait_until_subscription_initialized(self) -> None:
        await self._subscriptions_initialized.wait()


class EndpointAPI(ABC):
    """
    The :class:`~lahja.endpoint.Endpoint` enables communication between different processes
    as well as within a single process via various event-driven APIs.
    """

    __slots__ = ("name",)

    name: str

    _remote_connections_changed: ConditionAPI
    _remote_subscriptions_changed: ConditionAPI

    _connections: Set[RemoteEndpointAPI]

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
    ) -> Tuple[Tuple[str, Set[Type[BaseEvent]]], ...]:
        """
        Return 2-tuples for all all connected endpoints containing the name of
        the endpoint coupled with the set of messages the endpoint subscribes
        to
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

    def is_connected_to(self, endpoint_name: str) -> bool:
        return any(endpoint_name == remote.name for remote in self._connections)

    async def wait_until_connected_to(self, endpoint_name: str) -> None:
        """
        Return once a connection exists to an endpoint with the given name.
        """
        async with self._remote_connections_changed:
            while True:
                if self.is_connected_to(endpoint_name):
                    return
                await self._remote_connections_changed.wait()

    def get_connected_endpoints_and_subscriptions(
        self
    ) -> Tuple[Tuple[str, Set[Type[BaseEvent]]], ...]:
        """
        Return all connected endpoints and their event type subscriptions to this endpoint.
        """
        return tuple(
            (remote.name, remote.get_subscribed_events())
            for remote in self._connections
        )

    async def wait_until_connections_change(self) -> None:
        """
        Block until the set of connected remote endpoints changes.
        """
        async with self._remote_connections_changed:
            await self._remote_connections_changed.wait()

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
    async def wait_until_remote_subscriptions_change(self) -> None:
        async with self._remote_subscriptions_changed:
            await self._remote_subscriptions_changed.wait()

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
        async with self._remote_subscriptions_changed:
            while True:
                if self.is_remote_subscribed_to(remote_endpoint, event):
                    return
                await self._remote_subscriptions_changed.wait()

    async def wait_until_any_remote_subscribed_to(self, event: Type[BaseEvent]) -> None:
        """
        Block until any other remote endpoint has subscribed to the specified event type
        from this endpoint.
        """
        async with self._remote_subscriptions_changed:
            while True:
                if self.is_any_remote_subscribed_to(event):
                    return
                await self._remote_subscriptions_changed.wait()

    async def wait_until_all_remotes_subscribed_to(
        self, event: Type[BaseEvent]
    ) -> None:
        """
        Block until all currently connected remote endpoints are subscribed to the specified
        event type from this endpoint.
        """
        async with self._remote_subscriptions_changed:
            while True:
                if self.are_all_remotes_subscribed_to(event):
                    return
                await self._remote_subscriptions_changed.wait()
