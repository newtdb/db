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
    cursor = conn._storage.ex_cursor()
    try:
        cursor.execute("select zoid, ghost_pickle from (%s)_" % query,
                       args or kw)
        return [get(p64(zoid), ghost_pickle) for (zoid, ghost_pickle) in cursor]
    finally:
        _try_to_close_cursor(cursor)

def search_batch(conn, query, args, batch_start, batch_size):
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
    """
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

def _texts(texts, exprs, weight=None):
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

    tsvector = 'to_tsvector(text)'
    if weight:
        tsvector = "setweight(%s, '%s')" % (tsvector, weight)

    if not first_block:
        tsvector = "result || " + tsvector

    texts.append("  result := %s;\n" % tsvector)


identifier = re.compile(r'\w+$').match
def create_text_index_sql(fname, D=None, C=None, B=None, A=None):
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
    """
    texts = []
    _texts(texts, D)
    _texts(texts, C, 'C')
    _texts(texts, B, 'B')
    _texts(texts, A, 'A')

    if not texts:
        raise TypeError("No text expressions were specified")

    texts.insert(0, text_extraction_template[0] % fname)
    texts.append(text_extraction_template[1])
    texts.append("create index newt_%s_idx on newt using gin (%s(state));\n"
                 % (fname, fname))
    return '\n'.join(texts)

def create_text_index(conn, fname, D, C=None, B=None, A=None):
    """Set up a newt full-text index.

    The ``create_text_index_sql`` method is used to compute SQL, which
    is then executed to set up the index.  (This can take a long time
    on an existing database with many records.)

    The SQL is executed against the database associated with the given
    connection, but a separate connection is used, so it's execution
    is independent of the current transaction.
    """
    conn, cursor = conn._storage.ex_connect()
    sql = create_text_index_sql(fname, D, C, B, A)
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
