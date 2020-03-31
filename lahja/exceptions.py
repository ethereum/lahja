class LahjaError(Exception):
    """
    Base class for all lahja errors
    """

    pass


class BindError(LahjaError):
    """
    Raise when an attempt was made to bind an event that is already bound.
    """


class ConnectionAttemptRejected(LahjaError):
    """
    Raised when an attempt was made to connect to an endpoint that is already connected.
    """

    pass


class LifecycleError(LahjaError):
    """
    Raised when attempting to violate the lifecycle of an endpoint such as
    starting an already started endpoint or starting an endpoint that has
    already stopped.
    """

    pass


class NoSubscribers(LahjaError):
    """
    Raised when attempting to send an event or make a request while there are no listeners for the
    specific type of event or request.
    This is a safety check, set ``require_subscriber`` of :class:`~lahja.base.BroadcastConfig`
    to ``False`` to allow propagation without listeners.
    """

    pass


class UnexpectedResponse(LahjaError):
    """
    Raised when the type of a response did not match the ``expected_response_type``.
    """

    pass


class RemoteDisconnected(LahjaError):
    """
    Raise when a remote disconnects while we attempting to read a message.
    """

    pass
