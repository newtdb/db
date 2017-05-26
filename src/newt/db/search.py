"""Search API.

It's assumed that the API is used with an object stored in a
RelStorage with a Postgres back end.
"""

import re
from ZODB.utils import p64

def _try_to_close_cursor(cursor):
    try:
        cursor.close()
    except Exception:
        pass

def search(conn, query, *args, **kw):
    """Search for newt objects using an SQL query.

    Query parameters may be provided as either positional
    arguments or keyword arguments.  They are inserted into the
    query where there are placeholders of the form ``%s`` for
    positional arguments or ``%(NAME)s`` for keyword arguments.

    The query results must contain the columns ``zoid`` and
    ``ghost_pickle``.  It's simplest and costs nothing to simply
    select all columns (using ``*``) from the ``newt`` table.

    A sequence of newt objects is returned.
    """
    if kw:
        if args:
            raise TypeError("Only positional or keyword arguments can be used,"
                            " not both")
        args = kw
    get = conn.ex_get
    cursor = read_only_cursor(conn)
    try:
        cursor.execute(b"select zoid, ghost_pickle from (" + query + b")_"
                       if isinstance(query, bytes) else
                       "select zoid, ghost_pickle from (" + query + ")_",
                       args or None)
        return [get(p64(zoid), ghost_pickle) for (zoid, ghost_pickle) in cursor]
    finally:
        _try_to_close_cursor(cursor)

def search_batch(conn, query, args, batch_start, batch_size=None):
    """Query for a batch of newt objects.

    Query parameters are provided using the ``args``
    argument, which may be a tuple or a dictionary.  They are
    inserted into the query where there are placeholders of the
    form ``%s`` for an arguments tuple or ``%(NAME)s`` for an
    arguments dict.

    The ``batch_size`` and ``batch_size`` arguments are used to
    specify the result batch.  An ``ORDER BY`` clause should be
    used to order results.

    The total result count and sequence of batch result objects
    are returned.

    The query parameters, ``args``, may be omitted. (In this case,
    ``batch_size`` will be None and the other arguments will be
    re-arranged appropriately. ``batch_size`` *is required*.)  You
    might use this feature if you pre-inserted data using a database
    cursor `mogrify
    <http://initd.org/psycopg/docs/cursor.html#cursor.mogrify>`_
    method.
    """
    if not batch_size:
        if isinstance(args, int):
            batch_size = batch_start
            batch_start = args
            args = None
        else:
            raise AssertionError("Invalid batch size %r" % batch_size)

    if isinstance(query, str):
        query = """select zoid, ghost_pickle, count(*) over()
        from (%s) _
        offset %d limit %d
        """ % (query, batch_start, batch_size)
    else:
        # Python 3.4, whimper
        query = (
            b"select zoid, ghost_pickle, count(*) over()\nfrom (" +
            query +
            (") _\noffset %s limit %d" % (batch_start, batch_size)
             ).encode('ascii')
            )

    get = conn.ex_get
    cursor = read_only_cursor(conn)
    try:
        cursor.execute(query, args or None)
        count = 0
        result = []
        for zoid, ghost_pickle, count in cursor:
            result.append(get(p64(zoid), ghost_pickle))
        return count, result
    finally:
        _try_to_close_cursor(cursor)


text_extraction_template = """\
create or replace function %s(state jsonb) returns tsvector as $$
declare
  text text;
  result tsvector;
begin
  if state is null then return null; end if;
""", """\
  return result;
end
$$ language plpgsql immutable;
"""

def _texts(texts, exprs, weight=None, config=None):
    if not exprs:
        return

    if isinstance(exprs, str):
        exprs = (exprs, )

    first_block = not texts

    first = True
    for expr in exprs:
        if identifier(expr):
            expr = "state ->> '%s'" % expr

        text = "coalesce(%s, '')" % expr
        if first:
            first = False
        else:
            text = "text || " + text
        texts.append("  text = %s;" % text)

    if config:
        tsvector = 'to_tsvector(%r, text)' % config
    else:
        tsvector = 'to_tsvector(text)'

    if weight:
        tsvector = "setweight(%s, '%s')" % (tsvector, weight)

    if not first_block:
        tsvector = "result || " + tsvector

    texts.append("  result := %s;\n" % tsvector)


identifier = re.compile(r'\w+$').match
def create_text_index_sql(fname, D=None, C=None, B=None, A=None, config=None):
    """Compute and return SQL to set up a newt text index.

    The resulting SQL contains a statement to create a
    `PL/pgSQL <https://www.postgresql.org/docs/current/static/plpgsql.html>`_
    function and an index-creation function that uses it.

    The first argument is the name of the function to be generated.  The
    second argument is a single expression or property name or a
    sequence of expressions or property names.  If expressions are
    given, they will be evaluated against the newt JSON ``state``
    column.  Values consisting of alphanumeric characters (including
    underscores) are threaded as names, and other values are treated
    as expressions.

    Additional arguments, ``C``, ``B``, and ``A`` can be used to
    supply expressions and/or names for text to be extracted with
    different weights for ranking.  See:
    https://www.postgresql.org/docs/current/static/textsearch-controls.html#TEXTSEARCH-RANKING

    The ``config`` argument may be used to specify which `text search
    configuration
    <https://www.postgresql.org/docs/current/static/textsearch-intro.html#TEXTSEARCH-INTRO-CONFIGURATIONS>`_
    to use. If not specified, the server-configured default
    configuration is used.
    """
    texts = []
    _texts(texts, D, config=config)
    _texts(texts, C, 'C', config=config)
    _texts(texts, B, 'B', config=config)
    _texts(texts, A, 'A', config=config)

    if not texts:
        raise TypeError("No text expressions were specified")

    texts.insert(0, text_extraction_template[0] % fname)
    texts.append(text_extraction_template[1])
    texts.append("create index newt_%s_idx on newt using gin (%s(state));\n"
                 % (fname, fname))
    return '\n'.join(texts)

def create_text_index(conn, fname, D, C=None, B=None, A=None, config=None):
    """Set up a newt full-text index.

    The ``create_text_index_sql`` method is used to compute SQL, which
    is then executed to set up the index.  (This can take a long time
    on an existing database with many records.)

    The SQL is executed against the database associated with the given
    connection, but a separate connection is used, so it's execution
    is independent of the current transaction.
    """
    conn, cursor = _storage(conn).ex_connect()
    sql = create_text_index_sql(fname, D, C, B, A, config)
    try:
        cursor.execute(sql)
        conn.commit()
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

def query_data(conn, query, *args, **kw):
    """Query the newt Postgres database for raw data.

    Query parameters may be provided as either positional
    arguments or keyword arguments. They are inserted into the
    query where there are placeholders of the form: ``%s`` for
    positional arguments, or ``%(NAME)s`` for keyword arguments.

    A sequence of data tuples is returned.
    """
    if kw:
        if args:
            raise TypeError("Only positional or keyword arguments"
                            " may be provided, not both.")
        args = kw

    cursor = read_only_cursor(conn)
    try:
        cursor.execute(query, args)
        result = list(cursor)
    finally:
        try:
            cursor.close()
        except Exception:
            pass # whatever :)

    return result

def where(conn, query_tail, *args, **kw):
    """Query for objects satisfying criteria.

    This is a convenience wrapper for the ``search`` method.  The
    first arument is SQL text for query criteria to be included in
    an SQL where clause.

    This mehod simply appends it's first argument to::

      select * from newt where

    and so may also contain code that can be included after a
    where clause, such as an ``ORDER BY`` clause.

    Query parameters may be provided as either positional
    arguments or keyword arguments.  They are inserted into the
    query where there are placeholders of the form: ``%s`` for
    positional arguments, or ``%(NAME)s`` for keyword arguments.

    A sequence of newt objects is returned.
    """
    return search(conn,
                  (b"select * from newt where "
                   if isinstance(query_tail, bytes) else
                   "select * from newt where "
                   ) +
                  query_tail,
                  *args, **kw)

def where_batch(conn, query_tail, args, batch_start, batch_size=None):
    """Query for batch of objects satisfying criteria

    Like the ``where`` method, this is a convenience wrapper for
    the ``search_batch`` method.

    Query parameters are provided using the second, ``args``
    argument, which may be a tuple or a dictionary.  They are
    inserted into the query where there are placeholders of the
    form ``%s`` for an arguments tuple or ``%(NAME)s`` for an
    arguments dict.

    The ``batch_size`` and ``batch_size`` arguments are used to
    specify the result batch.  An ``ORDER BY`` clause should be
    used to order results.

    The total result count and sequence of batch result objects
    are returned.

    The query parameters, ``args``, may be omitted. (In this case,
    ``batch_size`` will be None and the other arguments will be
    re-arranged appropriately. ``batch_size`` *is required*.)  You
    might use this feature if you pre-inserted data using a database
    cursor `mogrify
    <http://initd.org/psycopg/docs/cursor.html#cursor.mogrify>`_
    method.
    """

    return search_batch(conn,
                        (b"select * from newt where "
                         if isinstance(query_tail, bytes) else
                         "select * from newt where ")
                        + query_tail,
                        args, batch_start, batch_size)


def _storage(conn):
    try:
        return conn._storage
    except AttributeError:
        return conn._p_jar._storage

def read_only_cursor(conn):
    """Get a database cursor for reading.

    The returned `cursor
    <http://initd.org/psycopg/docs/cursor.html>`_ can be used to
    make PostgreSQL queries and to perform safe SQL generation
    using the `cursor's mogrify method
    <http://initd.org/psycopg/docs/cursor.html#cursor.mogrify>`_.

    The caller must close the returned cursor after use.
    """
    return _storage(conn).ex_cursor()
