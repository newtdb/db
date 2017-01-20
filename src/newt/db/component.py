from relstorage.options import Options

class Adapter:

    def __init__(self, config):
        self.config = config.adapter.config

    def create(self, options):
        from ._adapter import Adapter
        return Adapter(dsn=self.config.dsn, options=options)

