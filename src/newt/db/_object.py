import persistent

class Object(persistent.Persistent):

    def __init__(self, **kw):
        self.__dict__.update(kw)
