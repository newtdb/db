"""Search API.

It's assumed that the API is used with an object stored in a
RelStorage with a Postgres back end.
"""
import psycopg2
import re
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
    conn, cursor = conn._storage.ex_connect()
    sql = create_text_index_sql(fname, D, C, B, A)
    try:
        cursor.execute(sql)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass
