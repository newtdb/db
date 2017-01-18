"""Search API.

It's assumed that the API is used with an object stored in a
RelStorage with a Postgres back end.
"""
from ZODB.utils import p64

def _try_to_close_cursor(cursor):
    try:
        cursor.close()
    except Exception:
        pass

def search(conn, query, *args, **kw):
    if kw:
        if args:
            raise TypeError("Only positional or keyword arguments can be used,"
                            " not both")
        args = kw
    get = conn.ex_get
    cursor = conn._storage.ex_cursor()
    try:
        cursor.execute("select zoid, ghost_pickle from (%s)_" % query,
                       args or kw)
        return [get(p64(zoid), ghost_pickle) for (zoid, ghost_pickle) in cursor]
    finally:
        _try_to_close_cursor(cursor)

def search_batch(conn, query, args, batch_start, batch_size):
    query = """
    select zoid, ghost_pickle, count(*) over()
    from (%s) _
    offset %s limit %s
    """ % (query, batch_start, batch_size)
    get = conn.ex_get
    cursor = conn._storage.ex_cursor()
    try:
        cursor.execute(query, args)
        count = 0
        result = []
        for zoid, ghost_pickle, count in cursor:
            result.append(get(p64(zoid), ghost_pickle))
        return count, result
    finally:
        _try_to_close_cursor(cursor)
