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
    pool_size = int,
    pool_timeout = int,
    cache_size = int,
    cache_size_bytes = int,
    historical_pool_size=int,
    historical_cache_size=int,
    historical_cache_size_bytes=int,
    historical_timeout=int,
    database_name=str,
    xrefs=parse_bool,
    large_record_size=int,
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
        pgq = '?' + '&'.join(pgq)

    dsn = "postgresql://" + netloc + path + pgq

    def factory():
        import newt.db
        return newt.db.storage(dsn, **options)

    return factory, dbkw
