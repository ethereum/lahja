import functools
import logging
from typing import Any, AsyncContextManager, Callable

from lahja import ConnectionConfig, EndpointAPI
from lahja.tools.engine import EngineAPI

logger = logging.getLogger("lahja.tools.initializers")


class Initializer:
    def __str__(self) -> str:
        return self._fn.__name__

    def __init__(
        self, fn: Callable[..., AsyncContextManager[EndpointAPI]], **kwargs: Any
    ) -> None:
        functools.update_wrapper(self, fn)
        self._fn = fn
        self._kwargs = kwargs

    def __call__(self, engine: EngineAPI) -> AsyncContextManager[EndpointAPI]:
        return self._fn(engine, **self._kwargs)


#
# EndpointAPI.serve
#
def _serve_endpoint(
    engine: EngineAPI, config: ConnectionConfig
) -> AsyncContextManager[EndpointAPI]:
    logger.debug(
        "[%s(%s)].serve(%s)", engine.endpoint_class.__name__, config.name, config.path
    )
    return engine.endpoint_class.serve(config)


def serve_endpoint(config: ConnectionConfig) -> Initializer:
    return Initializer(_serve_endpoint, config=config)


#
# EndpointAPI.run
#
def _run_endpoint(engine: EngineAPI, name: str) -> AsyncContextManager[EndpointAPI]:
    logger.debug("[%s(%s)].run()", engine.endpoint_class.__name__, name)
    # EndpointAPI doesn't specify an __init__ so mypy doesn't understand this.
    return engine.endpoint_class(name).run()  # type: ignore


def run_endpoint(name: str) -> Initializer:
    return Initializer(_run_endpoint, name=name)
