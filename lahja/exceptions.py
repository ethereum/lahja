class LahjaError(Exception):
    """
    Base class for all lahja errors
    """

    pass


class ConnectionAttemptRejected(Exception):
    """
    Raised when an attempt was made to connect to an endpoint that is already connected.
    """

    pass


class UnexpectedResponse(LahjaError):
    """
    Raised when the type of a response did not match the ``expected_response_type``.
    """

    pass


class RemoteDisconnected(LahjaError):
    pass
