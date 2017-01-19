import relstorage.storage
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

    from ._search import search, search_batch
    from ._search import create_text_index, create_text_index_sql
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

def storage(dsn):
    return relstorage.storage.RelStorage(Adapter(dsn))

def DB(dsn, **kw):
    return NewtDB(ZODB.DB(storage(dsn), **kw))

def connection(dsn, **kw):
    return Connection(ZODB.connection(storage(dsn), **kw))


