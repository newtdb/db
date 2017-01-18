import relstorage.storage
import ZODB.Connection

# Monkey patches, ook
def _ex_cursor(self, name=None):
    if self._stale_error is not None:
        raise self._stale_error

    with self._lock:
        self._before_load()
        return self._load_conn.cursor(name)

relstorage.storage.RelStorage.ex_cursor = _ex_cursor

def _ex_get(self, oid, class_pickle):
    """Return the persistent object with oid 'oid'."""
    if self.opened is None:
        raise ConnectionStateError("The database connection is closed")

    obj = self._cache.get(oid, None)
    if obj is not None:
        return obj
    obj = self._added.get(oid, None)
    if obj is not None:
        return obj
    obj = self._pre_cache.get(oid, None)
    if obj is not None:
        return obj

    # if class_pickle is None:
    #     p, _ = self._storage.load(oid)
    # else:
    #     p = class_pickle
    obj = self._reader.getGhost(class_pickle) # New code

    # Avoid infiniate loop if obj tries to load its state before
    # it is added to the cache and it's state refers to it.
    # (This will typically be the case for non-ghostifyable objects,
    # like persistent caches.)
    self._pre_cache[oid] = obj
    self._cache.new_ghost(oid, obj)
    self._pre_cache.pop(oid)
    return obj

ZODB.Connection.Connection.ex_get = _ex_get
