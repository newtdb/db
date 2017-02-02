import contextlib

@contextlib.contextmanager
def closing(thing):
    """Like contextlib.closing except hides errors raised by close
    """
    try:
        yield thing
    finally:
        try:
            thing.close()
        except Exception:
            pass
