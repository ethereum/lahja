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
    BroadcastConfig,
    ConnectionConfig,
    Message,
    Msg,
    RemoteSubscriptionChanged,
    Subscription,
)

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
    def get_connected_endpoints_and_subscriptions(
        self
    ) -> Tuple[Tuple[Optional[str], Set[Type[BaseEvent]]], ...]:
        """
        Return all connected endpoints and their event type subscriptions to this endpoint.
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
    async def subscribe(
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


class BaseEndpoint(EndpointAPI):
    """
    Base class for endpoint implementations that implements shared/common logic
    """

    logger = logging.getLogger("lahja.endpoint.Endpoint")

    #
    # Common implementations
    #
    def __reduce__(self) -> None:  # type: ignore
        raise NotImplementedError("Endpoints cannot be pickled")

    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Naive asynchronous implementation.  Subclasses should consider
        connecting concurrently.
        """
        for config in endpoints:
            await self.connect_to_endpoint(config)

    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        agen = self.stream(event_type, num_events=1)
        event = await agen.asend(None)
        await agen.aclose()
        return event

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

        if self.is_remote_subscribed_to(remote_endpoint, event):
            return

        async for _ in self.stream(RemoteSubscriptionChanged):  # noqa: F841
            if self.is_remote_subscribed_to(remote_endpoint, event):
                return

    async def wait_until_any_remote_subscribed_to(self, event: Type[BaseEvent]) -> None:
        """
        Block until any other remote endpoint has subscribed to the specified event type
        from this endpoint.
        """

        if self.is_any_remote_subscribed_to(event):
            return

        async for _ in self.stream(RemoteSubscriptionChanged):  # noqa: F841
            if self.is_any_remote_subscribed_to(event):
                return

    async def wait_until_all_remotes_subscribed_to(
        self, event: Type[BaseEvent]
    ) -> None:
        """
        Block until all currently connected remote endpoints are subscribed to the specified
        event type from this endpoint.
        """

        if self.are_all_remotes_subscribed_to(event):
            return

        async for _ in self.stream(RemoteSubscriptionChanged):  # noqa: F841
            if self.are_all_remotes_subscribed_to(event):
                return
