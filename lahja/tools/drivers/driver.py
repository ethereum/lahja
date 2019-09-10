import functools
import logging
from typing import Sequence

from lahja.tools.engine import Driver, EngineAPI

from .actions import Action, AsyncAction, SyncAction
from .initializers import Initializer

logger = logging.getLogger("lahja.driver")


class _Driver:
    def __str__(self) -> str:
        initializer_and_actions = " > ".join(
            (str(self.initializer),) + tuple(str(action) for action in self.actions)
        )
        return f"<Driver {initializer_and_actions}>"

    def __init__(self, initializer: Initializer, actions: Sequence[Action]) -> None:
        functools.update_wrapper(self, drive)
        self.initializer = initializer
        self.actions = tuple(actions)

    async def __call__(self, engine: EngineAPI) -> None:
        await drive(engine, self.initializer, self.actions)


def driver(initializer: Initializer, *actions: Action) -> Driver:
    """
    Construct a *Driver*.  Should contain a single *Initializer* followed by a
    variadic number of *Actions*.
    """
    return _Driver(initializer=initializer, actions=actions)


async def drive(
    engine: EngineAPI,
    initializer: Initializer,
    actions: Sequence[Action],
    action_timeout: int = 5,
) -> None:
    """
    Use the provide *Engine* to initialize and drive an endpoint
    implementation.
    """
    logger.debug("STARTING DRIVER")
    async with initializer(engine) as endpoint:
        for idx, action in enumerate(actions):
            logger.debug("RUNNING ACTION[%d]: %s", idx, action)
            if isinstance(action, AsyncAction):
                try:
                    await engine.run_with_timeout(
                        action, engine, endpoint, timeout=action_timeout
                    )
                except TimeoutError as err:
                    raise TimeoutError(
                        f"[{endpoint}] Timeout running action #{idx}:{action}"
                    ) from err
            elif isinstance(action, SyncAction):
                action(engine, endpoint)
            else:
                raise TypeError(f"Unsupported action: {action}")
