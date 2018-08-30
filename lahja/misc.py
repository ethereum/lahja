from typing import (  # noqa: F401
    Any,
    Callable,
    Optional,
)


class Subscription:

    def __init__(self, unsubscribe_fn: Callable[[], Any]) -> None:
        self._unsubscribe_fn = unsubscribe_fn

    def unsubscribe(self) -> None:
        self._unsubscribe_fn()


class BroadcastConfig:

    def __init__(self,
                 filter_endpoint: Optional[str] = None,
                 filter_event_id: Optional[str] = None) -> None:

        self.filter_endpoint = filter_endpoint
        self.filter_event_id = filter_event_id

    def allowed_to_receive(self, endpoint: str) -> bool:
        return self.filter_endpoint is None or self.filter_endpoint == endpoint


class BaseEvent:

    def __init__(self) -> None:
        self._origin = ''
        self._id: Optional[str] = None
        self._config: Optional[BroadcastConfig] = None

    def broadcast_config(self) -> BroadcastConfig:
        return BroadcastConfig(
            filter_endpoint=self._origin,
            filter_event_id=self._id
        )


class TransparentEvent(BaseEvent):
    """
    This event is used to create artificial activity so that code that
    blocks on a :meth:`~multiprocessing.queues.Queue.get` unblocks and
    gets a chance to revalidate if it should continue to block for reading.
    """
    pass


TRANSPARENT_EVENT = TransparentEvent()
