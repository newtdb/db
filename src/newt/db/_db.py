import relstorage.adapters.postgresql
import relstorage.storage
import relstorage.options
import ZODB

from . import search as _search
from ._adapter import Adapter

class NewtDB:
    """Wrapper for a ZODB.DB object that provides newt.db-specific connections
    """

    def __init__(self, db):
        self._db = db

    def open(self, *args, **kw):
        return Connection(self._db.open(*args, **kw))

    def __getattr__(self, name):
        return getattr(self._db, name)

class Connection:
    """Wrapper for ZODB.Connection.Connection objects

    ``newt.db.Connection`` objects provide extra helper methods for
    searching and for transaction management.
    """

    def search(self, query, *args, **kw):
        return _search.search(self, query, *args, **kw)
    search.__doc__ = _search.search.__doc__

    def search_batch(self, query, args, batch_start, batch_size):
        return _search.search_batch(self, query, args, batch_start, batch_size)
    search_batch.__doc__ = _search.search_batch.__doc__

    def create_text_index(self, fname, D=None, C=None, B=None, A=None):
        return _search.create_text_index(self, fname, D, C, B, A)
    create_text_index.__doc__ = _search.create_text_index.__doc__

    def create_text_index_sql(fname, D=None, C=None, B=None, A=None):
        return _search.create_text_index_sql(fname, D, C, B, A)
    create_text_index_sql.__doc__ = _search.create_text_index_sql.__doc__
    create_text_index_sql = staticmethod(create_text_index_sql)

    def query_data(self, query, *args, **kw):
        return _search.query_data(self, query, *args, **kw)
    query_data.__doc__ = _search.query_data.__doc__

    def where(self, query_tail, *args, **kw):
        return _search.where(self, query_tail, *args, **kw)
    where.__doc__ = _search.where.__doc__

    def where_batch(self, query_tail, args, batch_start, batch_size):
        return _search.where_batch(
            self, query_tail, args, batch_start, batch_size)
    where_batch.__doc__ = _search.where_batch.__doc__

    def __init__(self, connection):
        self._connection = connection # A ZODB connection

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def abort(self, ignore=None):
        """Abort the current transaction
        """
        if ignore is None:
            self._connection.transaction_manager.abort()
        else:
            self._connection.abort(ignore)

    def commit(self, ignore=None):
        """Commit the current transaction
        """
        if ignore is None:
            self._connection.transaction_manager.commit()
        else:
            self._connection.commit(ignore)

def _split_options(
    # DB options
    pool_size=7,
    pool_timeout=1<<31,
    cache_size=400,
    cache_size_bytes=0,
    historical_pool_size=3,
    historical_cache_size=1000,
    historical_cache_size_bytes=0,
    historical_timeout=300,
    database_name='unnamed',
    databases=None,
    xrefs=True,
    large_record_size=1<<24,
    **storage_options):
    return dict(
        pool_size=pool_size,
        pool_timeout=pool_timeout,
        cache_size=cache_size,
        cache_size_bytes=cache_size_bytes,
        historical_pool_size=historical_pool_size,
        historical_cache_size=historical_cache_size,
        historical_cache_size_bytes=historical_cache_size_bytes,
        historical_timeout=historical_timeout,
        database_name=database_name,
        databases=databases,
            xrefs=xrefs,
        large_record_size=large_record_size,
        ), storage_options

def storage(dsn, keep_history=False, **kw):
    """Create a RelStorage storage using the newt PostgresQL adapter.

    Keyword options can be used to provide either `ZODB.DB
    <http://www.zodb.org/en/latest/reference/zodb.html#databases>`_
    options or `RelStorage
    <http://relstorage.readthedocs.io/en/latest/relstorage-options.html>`_
    options.
    """
    options = relstorage.options.Options(keep_history=keep_history, **kw)
    return relstorage.storage.RelStorage(Adapter(dsn, options), options=options)

def DB(dsn, **kw):
    """Create a Newt DB database object.

    Keyword options can be used to provide either `ZODB.DB
    <http://www.zodb.org/en/latest/reference/zodb.html#databases>`_
    options or `RelStorage
    <http://relstorage.readthedocs.io/en/latest/relstorage-options.html>`_
    options.

    A Newt DB object is a thin wrapper around ``ZODB.DB``
    objects. When it's ``open`` method is called, it returns
    :py:class:`newt.db.Connection` objects.
    """
    db_options, storage_options = _split_options(**kw)
    return NewtDB(ZODB.DB(storage(dsn, **storage_options), **db_options))

def connection(dsn, **kw):
    """Create a newt :py:class:`newt.db.Connection`.

    Keyword options can be used to provide either `ZODB.DB
    <http://www.zodb.org/en/latest/reference/zodb.html#databases>`_
    options or `RelStorage
    <http://relstorage.readthedocs.io/en/latest/relstorage-options.html>`_
    options.
    """
    db_options, storage_options = _split_options(**kw)
    return Connection(
        ZODB.connection(storage(dsn, **storage_options), **db_options)
        )

def pg_connection(dsn, driver_name='auto'):
    """Create a PostgreSQL (not newt) database connection

    This function should be used rather than, for example, calling
    ``psycopg2.connect``, because it can use other Postgres drivers
    depending on the Python environment and available modules.
    """
    options = relstorage.options.Options(driver=driver_name)
    driver = relstorage.adapters.postgresql.select_driver(options)
    return driver.connect(dsn)
