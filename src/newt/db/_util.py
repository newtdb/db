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

def table_exists(cursor, name):
    cursor.execute(
        "select from information_schema.tables "
        "where table_schema = 'public' AND table_name = %s",
        (name, ))
    return bool(list(cursor))

def trigger_exists(cursor, name):
    cursor.execute(
        "select from pg_catalog.pg_trigger "
        "where tgname = %s",
        (name, ))
    return bool(list(cursor))
