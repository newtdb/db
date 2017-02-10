"""URL resolver for zodburi

http://docs.pylonsproject.org/projects/zodburi/en/latest/api.html
"""
from six.moves.urllib import parse

bool_values = dict(
    true = True,
    yes = True,
    false = False,
    no = False,
    **{'1': True, '0': False})

def parse_bool(v):
    r = bool_values.get(v.lower())
    if r is None:
        raise ValueError("Invalid boolean value", v)
    return r

storage_options = dict(
    keep_history = parse_bool,
    driver = str,
    )

db_options = dict(
    connection_pool_size = int,
    connection_cache_size = int,
    database_name=str,
)

def resolve_uri(uri):
    scheme, netloc, path, params, query, fragment = parse.urlparse(uri)
    if params:
        raise ValueError("Unexpected URI params", params)
    if fragment:
        raise ValueError("Unexpected URI fragment", fragment)

    dbkw = {}
    options = {}
    pgq = ''

    if query:
        pgq = []
        for name, value in parse.parse_qsl(query):
            if name in storage_options:
                options[name] = storage_options[name](value)
            elif name in db_options:
                dbkw[name] = db_options[name](value)
            else:
                pgq.append(name + '=' + value)
        pgq = '?' + '&'.join(pgq) if pgq else ''

    dsn = "postgresql://" + netloc + path + pgq

    def factory():
        import newt.db
        return newt.db.storage(dsn, **options)

    return factory, dbkw
