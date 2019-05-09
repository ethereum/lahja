is_snappy_available: bool

try:
    import snappy  # noqa: F401
    is_snappy_available = True
except ModuleNotFoundError:
    is_snappy_available = False
