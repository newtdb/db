import relstorage.storage
import relstorage.options
import ZODB

from ._adapter import Adapter

class NewtDB:

    def __init__(self, db):
        self._db = db

    def open(self, *args, **kw):
        return Connection(self._db.open(*args, **kw))

    def __getattr__(self, name):
        return getattr(self._db, name)

class Connection:

    from .search import search, search_batch
    from .search import create_text_index, create_text_index_sql
    create_text_index_sql = staticmethod(create_text_index_sql)

    def __init__(self, connection):
        self._connection = connection # A ZODB connection

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def abort(self):
        self._connection.transaction_manager.abort()

    def commit(self):
        self._connection.transaction_manager.commit()

    def query_data(self, query, *args, **kw):
        if kw:
            if args:
                raise TypeError("Only positional or keyword arguments"
                                " may be provided, not both.")
            args = kw
        cursor = self._connection._storage.ex_cursor()
        try:
            cursor.execute(query, args)
            result = list(cursor)
        finally:
            try:
                cursor.close()
            except Exception:
                pass # whatever :)

        return result

    def where(self, query_tail, *args, **kw):
        return self.search("select * from newt where " + query_tail,
                           *args, **kw)

    def batch_where(self, query_tail, args, batch_start, batch_size):
        return self.search_batch("select * from newt where " + query_tail,
                                 args, batch_start, batch_size)

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
    options = relstorage.options.Options(keep_history=keep_history, **kw)
    return relstorage.storage.RelStorage(Adapter(dsn, options), options=options)

def DB(dsn, **kw):
    db_options, storage_options = _split_options()
    return NewtDB(ZODB.DB(storage(dsn, **storage_options), **db_options))

def connection(dsn, **kw):
    db_options, storage_options = _split_options()
    return Connection(
        ZODB.connection(storage(dsn, **storage_options), **db_options)
        )
