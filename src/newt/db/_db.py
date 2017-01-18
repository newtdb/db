import relstorage.storage
import ZODB

from ._adapter import Adapter

class Connection:

    def __init__(self, connection):
        self._connection = connection # A ZODB connection
        self.root = connection.root

    def abort(self):
        self._connection.transaction_manager.abort()

    def commit(self):
        self._connection.transaction_manager.commit()

    def close(self):
        self._connection.close()

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

def storage(dsn):
    return relstorage.storage.RelStorage(Adapter(dsn))

def connection(dsn, **kw):
    return Connection(ZODB.connection(storage(dsn), **kw))


