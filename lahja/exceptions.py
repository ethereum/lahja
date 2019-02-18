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
