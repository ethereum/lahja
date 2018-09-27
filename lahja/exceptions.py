class LahjaError(Exception):
    """
    Base class for all lahja errors
    """
    pass


class UnexpectedResponse(LahjaError):
    """
    Raised when the type of a response did not match the ``expected_response_type``.
    """
    pass


class NoConnection(LahjaError):
    """
    Raised when an API call was made prior to connecting the ``Endpoint```
    """
    pass
