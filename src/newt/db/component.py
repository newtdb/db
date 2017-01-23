import relstorage.storage
import relstorage.adapters.postgresql

class Adapter:

    def __init__(self, config):
        self.config = config.adapter.config

    def create(self, options):
        from ._adapter import Adapter
        return Adapter(dsn=self.config.dsn, options=options)

class DB:

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self, databases=None):
        db = self.config.db
        db.name = db.name or self.name
        db = db.open(databases)
        assert (
            isinstance(db.storage,
                       relstorage.storage.RelStorage)
            and
            isinstance(db.storage._adapter,
                       relstorage.adapters.postgresql.PostgreSQLAdapter)
            ), "Invalid storage"
        from ._db import NewtDB
        return NewtDB(db)
