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

def table_exists(conn, name):
    with closing(conn.cursor()) as cursor:
        cursor.execute("""
        select from information_schema.tables
        where table_schema = 'public' AND table_name = %s
        """, (name, ))
        return bool(list(cursor))

        
