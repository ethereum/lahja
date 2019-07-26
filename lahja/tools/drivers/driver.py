import asyncio
import functools
import logging
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    Optional,
    Sequence,
    Type,
    Union,
)

from lahja import BroadcastConfig, ConnectionConfig, BaseEvent, EndpointAPI
from lahja.exceptions import LahjaError

from lahja.tools.engine import EngineAPI


logger = logging.getLogger('lahja.driver')


class DidNotThrow(LahjaError):
    pass


class Initializer:
    def __str__(self) -> str:
        return self._fn.__name__

    def __init__(self, fn: Callable[..., AsyncContextManager[EndpointAPI]], **kwargs: Any) -> None:
        self._fn = fn
        self._kwargs = kwargs

    def __call__(self, engine: EngineAPI) -> AsyncContextManager[EndpointAPI]:
        return self._fn(engine, **self._kwargs)


class SyncAction:
    def __str__(self) -> str:
        return self._fn.__name__

    def __init__(self, fn: Callable[..., None], **kwargs: Any) -> None:
        self._fn = fn
        self._kwargs = kwargs

    def __call__(self, endpoint: EndpointAPI) -> None:
        self._fn(endpoint, **self._kwargs)


class AsyncAction:
    def __str__(self) -> str:
        return self._coro.__name__

    def __init__(self, coro: Callable[..., Awaitable[None]], **kwargs: Any) -> None:
        self._coro = coro
        self._kwargs = kwargs

    async def __call__(self, endpoint: EndpointAPI) -> None:
        await self._coro(endpoint, **self._kwargs)


Action = Union[SyncAction, AsyncAction]
Driver = Callable[[EngineAPI], Awaitable[None]]
EventHandlerFn = Callable[[EndpointAPI, BaseEvent], Any]


def driver(initializer: Initializer,
           *actions: Action,
           ) -> Driver:
    return functools.partial(drive, initializer=initializer, actions=actions)


async def drive(engine: EngineAPI,
                initializer: Initializer,
                actions: Sequence[Action],
                action_timeout: int = 1,
                ) -> None:
    logger.debug('STARTING DRIVER')
    async with initializer(engine) as endpoint:
        for idx, action in enumerate(actions):
            logger.debug('RUNNING ACTION[%d]: %s', idx, action)
            if isinstance(action, AsyncAction):
                await engine.run_with_timeout(action, endpoint, timeout=action_timeout)
            elif isinstance(action, SyncAction):
                action(endpoint)
            else:
                raise TypeError(f"Unsupported action: {action}")


#
# EndpointAPI.serve
#
def _serve_endpoint(engine: EngineAPI,
                    config: ConnectionConfig) -> AsyncContextManager[EndpointAPI]:
    logger.debug(
        '[%s(%s)].serve(%s)',
        engine.endpoint_class.__name__,
        config.name,
        config.path,
    )
    return engine.endpoint_class.serve(config)


def serve_endpoint(config: ConnectionConfig) -> Initializer:
    return Initializer(_serve_endpoint, config=config)


#
# EndpointAPI.run
#
def _run_endpoint(engine: EngineAPI, name: str) -> AsyncContextManager[EndpointAPI]:
    logger.debug('[%s(%s)].run()', engine.endpoint_class.__name__, name)
    return engine.endpoint_class(name).run()


def run_endpoint(name) -> Initializer:
    return Initializer(_run_endpoint, name=name)


#
# EndpointAPI.wait_for
#
async def _wait_for(endpoint: EndpointAPI,
                    event_type: Type[BaseEvent],
                    on_event: Optional[EventHandlerFn]) -> None:
    logger.debug('[%s].wait_for(%s)', endpoint, event_type)
    result = await endpoint.wait_for(event_type)
    logger.debug('[%s].wait_for(%s) RECEIVED', endpoint, event_type)
    if on_event is not None:
        if asyncio.iscoroutinefunction(on_event):
            await on_event(endpoint, result)
        else:
            on_event(endpoint, result)


def wait_for(event_type: Type[BaseEvent], on_event: EventHandlerFn = None) -> AsyncAction:
    return AsyncAction(_wait_for, event_type=event_type, on_event=on_event)


#
# EndpointAPI.connect_to_endpoints
#
async def _connect_to_endpoints(endpoint: EndpointAPI, configs: Sequence[ConnectionConfig]):
    logger.debug(
        '[%s].connect_to_endpoints(%s)',
        endpoint,
        ','.join((str(config) for config in configs)),
    )
    await endpoint.connect_to_endpoints(*configs)


def connect_to_endpoints(*configs: ConnectionConfig) -> AsyncAction:
    return AsyncAction(_connect_to_endpoints, configs=configs)


#
# EndpointAPI.wait_until_any_endpoint_subscribed_to
#
async def _wait_until_any_endpoint_subscribed_to(endpoint: EndpointAPI,
                                                 event_type: Type[BaseEvent]) -> None:
    logger.debug('[%s].wait_until_any_endpoint_subscribed_to(%s)', endpoint, event_type)
    await endpoint.wait_until_any_endpoint_subscribed_to(event_type)


def wait_until_any_endpoint_subscribed_to(event_type: Type[BaseEvent]) -> AsyncAction:
    return AsyncAction(_wait_until_any_endpoint_subscribed_to, event_type=event_type)


#
# EndpointAPI.wait_until_connected_to
#
async def _wait_until_connected_to(endpoint: EndpointAPI,
                                   name: str) -> None:
    logger.debug('[%s].wait_until_connected_to(%s)', endpoint, name)
    await endpoint.wait_until_connected_to(name)


def wait_until_connected_to(name: str) -> AsyncAction:
    return AsyncAction(_wait_until_connected_to, name=name)


#
# EndpointAPI.broadcast
#
async def _broadcast(endpoint: EndpointAPI, event: BaseEvent, config: Optional[BroadcastConfig]):
    logger.debug('[%s].broadcast(%s, config=%s)', endpoint, event, config)
    await endpoint.broadcast(event, config=config)


def broadcast(event: BaseEvent, config: BroadcastConfig = None) -> AsyncAction:
    return AsyncAction(_broadcast, event=event, config=config)


#
# EndpointAPI wait then broadcast
#
async def _wait_any_then_broadcast(endpoint: EndpointAPI,
                                   event: BaseEvent, config: Optional[BroadcastConfig]):
    await _wait_until_any_endpoint_subscribed_to(endpoint, type(event))
    await _broadcast(endpoint, event, config)


def wait_any_then_broadcast(event: BaseEvent, config: BroadcastConfig = None) -> AsyncAction:
    return AsyncAction(_wait_any_then_broadcast, event=event, config=config)


#
# Throws
#
async def _throws(endpoint: EndpointAPI, action: Action, exc_type: Type[Exception]) -> None:
    logger.debug('[%s](%s) - Expecting Error: %s', endpoint, action, exc_type)
    if not isinstance(action, (SyncAction, AsyncAction)):
        # We have to do this up here to ensure we don't end up catching in the
        # try/except below.
        raise TypeError(f"Unsupported action: {action}")

    try:
        if isinstance(action, AsyncAction):
            await action(endpoint)
        elif isinstance(action, SyncAction):
            action(endpoint)
        else:
            raise Exception("Unreachable code path")
    except exc_type:
        logger.debug('[%s](%s) - Got Error: %s', endpoint, action, exc_type)
    else:
        logger.debug('[%s](%s) - Did Not Error: %s', endpoint, action, exc_type)
        raise DidNotThrow(f"Action `{action}` did not throw expected error: {exc_type}")


def throws(action: Action, exc_type: [Exception]) -> Action:
    return AsyncAction(_throws, action=action, exc_type=exc_type)
