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


class NotServing(LahjaError):
    """
    Raised when an API call was made before :meth:`~lahja.endpoint.Endpoint.start_serving`
    was called.
    """


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
