import asyncio
import functools
import logging
import multiprocessing
from typing import Any, Awaitable, Callable, Optional, Sequence, Tuple, Type, Union

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
    ConnectionConfig,
    EndpointAPI,
)
from lahja.base import EventAPI
from lahja.exceptions import LahjaError
from lahja.tools.engine import EngineAPI

logger = logging.getLogger("lahja.driver")


class DidNotThrow(LahjaError):
    pass


class SyncAction:
    def __str__(self) -> str:
        return self._fn.__name__

    def __init__(self, fn: Callable[..., None], **kwargs: Any) -> None:
        functools.update_wrapper(self, fn)
        self._fn = fn
        self._kwargs = kwargs

    def __call__(self, engine: EngineAPI, endpoint: EndpointAPI) -> None:
        self._fn(engine, endpoint, **self._kwargs)


class AsyncAction:
    def __str__(self) -> str:
        return self._coro.__name__

    def __init__(self, coro: Callable[..., Awaitable[None]], **kwargs: Any) -> None:
        functools.update_wrapper(self, coro)
        self._coro = coro
        self._kwargs = kwargs

    async def __call__(self, engine: EngineAPI, endpoint: EndpointAPI) -> None:
        await self._coro(engine, endpoint, **self._kwargs)


Action = Union[SyncAction, AsyncAction]
EventHandlerFn = Callable[[EndpointAPI, BaseEvent], Any]


#
# EndpointAPI.wait_for
#
async def _wait_for(
    engine: EngineAPI,
    endpoint: EndpointAPI,
    event_type: Type[BaseEvent],
    on_event: Optional[EventHandlerFn],
) -> None:
    logger.debug("[%s].wait_for(%s)", endpoint, event_type)
    result = await endpoint.wait_for(event_type)
    logger.debug("[%s].wait_for(%s) RECEIVED", endpoint, event_type)
    if on_event is not None:
        if asyncio.iscoroutinefunction(on_event):
            await on_event(endpoint, result)
        else:
            on_event(endpoint, result)


def wait_for(
    event_type: Type[BaseEvent], on_event: Optional[EventHandlerFn] = None
) -> AsyncAction:
    """
    Wait for an event of the provided ``request_type`` and call
    response event returned by the provide ``get_response`` function.
    """
    return AsyncAction(_wait_for, event_type=event_type, on_event=on_event)


#
# Serve request
#
GetResponseFn = Callable[[EndpointAPI, BaseRequestResponseEvent[BaseEvent]], BaseEvent]


def serve_request(
    request_type: Type[BaseRequestResponseEvent[BaseEvent]], get_response: GetResponseFn
) -> AsyncAction:
    """
    Wait for an event of the provided ``request_type`` and respond using the
    response event returned by the provide ``get_response`` function.
    """

    async def _serve_response(
        endpoint: EndpointAPI, request: BaseRequestResponseEvent[BaseEvent]
    ) -> None:
        response = get_response(endpoint, request)
        await endpoint.broadcast(response, config=request.broadcast_config())
        logger.debug("[%s] sent response: %s", endpoint, response)

    return AsyncAction(_wait_for, event_type=request_type, on_event=_serve_response)


#
# EndpointAPI.request
#
async def _request(
    engine: EngineAPI,
    endpoint: EndpointAPI,
    event: BaseRequestResponseEvent[BaseEvent],
    config: Optional[BroadcastConfig],
    on_response: Optional[EventHandlerFn],
) -> None:
    logger.debug("[%s].request(%s, config=%s)", endpoint, event, config)
    response = await endpoint.request(event, config=config)
    logger.debug("[%s].request(%s) GOT RESPONSE: %s", endpoint, event, response)
    if on_response is not None:
        if asyncio.iscoroutinefunction(on_response):
            await on_response(endpoint, response)
        else:
            on_response(endpoint, response)


def request(
    event: BaseRequestResponseEvent[BaseEvent],
    config: Optional[BroadcastConfig] = None,
    on_response: Optional[EventHandlerFn] = None,
) -> AsyncAction:
    """
    See ``EndpointAPI.connect_to_endpoints``

    Optionally provide a callback ``on_response`` that will be run upon receipt
    of the response.
    """
    return AsyncAction(_request, event=event, config=config, on_response=on_response)


#
# EndpointAPI.connect_to_endpoints
#
async def _connect_to_endpoints(
    engine: EngineAPI, endpoint: EndpointAPI, configs: Sequence[ConnectionConfig]
) -> None:
    logger.debug(
        "[%s].connect_to_endpoints(%s)",
        endpoint,
        ",".join((str(config) for config in configs)),
    )
    await endpoint.connect_to_endpoints(*configs)


def connect_to_endpoints(*configs: ConnectionConfig) -> AsyncAction:
    """
    See ``EndpointAPI.connect_to_endpoints``
    """
    return AsyncAction(_connect_to_endpoints, configs=configs)


#
# EndpointAPI.wait_until_any_endpoint_subscribed_to
#
async def _wait_until_any_endpoint_subscribed_to(
    engine: EngineAPI, endpoint: EndpointAPI, event_type: Type[BaseEvent]
) -> None:
    logger.debug("[%s].wait_until_any_endpoint_subscribed_to(%s)", endpoint, event_type)
    await endpoint.wait_until_any_endpoint_subscribed_to(event_type)


def wait_until_any_endpoint_subscribed_to(event_type: Type[BaseEvent]) -> AsyncAction:
    """
    See ``EndpointAPI.wait_until_any_endpoint_subscribed_to``
    """
    return AsyncAction(_wait_until_any_endpoint_subscribed_to, event_type=event_type)


#
# EndpointAPI.wait_until_connected_to
#
async def _wait_until_connected_to(
    engine: EngineAPI, endpoint: EndpointAPI, name: str
) -> None:
    logger.debug("[%s].wait_until_connected_to(%s)", endpoint, name)
    await endpoint.wait_until_connected_to(name)


def wait_until_connected_to(name: str) -> AsyncAction:
    """
    See ``EndpointAPI.wait_until_connected_to``
    """
    return AsyncAction(_wait_until_connected_to, name=name)


#
# EndpointAPI.broadcast
#
async def _broadcast(
    engine: EngineAPI,
    endpoint: EndpointAPI,
    event: BaseEvent,
    config: Optional[BroadcastConfig],
) -> None:
    logger.debug("[%s].broadcast(%s, config=%s)", endpoint, event, config)
    await endpoint.broadcast(event, config=config)


def broadcast(
    event: BaseEvent, config: Optional[BroadcastConfig] = None
) -> AsyncAction:
    """
    See ``EndpointAPI.broadcast``
    """
    return AsyncAction(_broadcast, event=event, config=config)


#
# EndpointAPI wait then broadcast
#
async def _wait_any_then_broadcast(
    engine: EngineAPI,
    endpoint: EndpointAPI,
    event: BaseEvent,
    config: Optional[BroadcastConfig],
) -> None:
    # TODO: these could be abstracted into a single `compose` action which
    # combines multiple actions.
    await _wait_until_any_endpoint_subscribed_to(engine, endpoint, type(event))
    await _broadcast(engine, endpoint, event, config)


def wait_any_then_broadcast(
    event: BaseEvent, config: Optional[BroadcastConfig] = None
) -> AsyncAction:
    """
    Combination of ``wait_until_any_endpoint_subscribed_to`` and ``broadcast``
    """
    return AsyncAction(_wait_any_then_broadcast, event=event, config=config)


#
# Throws
#
async def _throws(
    engine: EngineAPI, endpoint: EndpointAPI, action: Action, exc_type: Type[Exception]
) -> None:
    logger.debug("[%s](%s) - Expecting Error: %s", endpoint, action, exc_type)
    if not isinstance(action, (SyncAction, AsyncAction)):
        # We have to do this up here to ensure we don't end up catching in the
        # try/except below.
        raise TypeError(f"Unsupported action: {action}")

    try:
        if isinstance(action, AsyncAction):
            await action(engine, endpoint)
        elif isinstance(action, SyncAction):
            action(engine, endpoint)
        else:
            raise Exception("Unreachable code path")
    except exc_type:
        logger.debug("[%s](%s) - Got Error: %s", endpoint, action, exc_type)
    else:
        logger.debug("[%s](%s) - Did Not Error: %s", endpoint, action, exc_type)
        raise DidNotThrow(f"Action `{action}` did not throw expected error: {exc_type}")


def throws(action: Action, exc_type: Type[Exception]) -> Action:
    """
    Checks that the provided *Action* throws the provided exception type.
    """
    return AsyncAction(_throws, action=action, exc_type=exc_type)


#
# Synchronization Point
#
async def _checkpoint(
    engine: EngineAPI,
    endpoint: EndpointAPI,
    name: str,
    my_event: EventAPI,
    other_event: EventAPI,
) -> None:
    logger.debug("[%s] at checkpoint: %s", endpoint, name)
    my_event.set()
    while not other_event.is_set():
        await engine.sleep(0.01)


def checkpoint(name: str) -> Tuple[AsyncAction, AsyncAction]:
    """
    Generates a pair of actions that can be used in separate drivers to
    synchronize their action execution.  Each driver will wait until this
    checkpoint has been hit before proceeding.
    """
    left = multiprocessing.Event()
    right = multiprocessing.Event()
    return (
        AsyncAction(_checkpoint, name=name, my_event=left, other_event=right),
        AsyncAction(_checkpoint, name=name, my_event=right, other_event=left),
    )
