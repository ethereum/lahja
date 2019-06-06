import logging
import sys


def setup_stderr_lahja_logging() -> None:
    logger = logging.getLogger("lahja")
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
