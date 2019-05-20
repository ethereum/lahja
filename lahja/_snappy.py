def check_has_snappy_support() -> bool:
    try:
        import snappy  # noqa: F401

        return True
    except ModuleNotFoundError:
        return False
