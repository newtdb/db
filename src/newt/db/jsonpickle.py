"""Convert pickles to JSON

The goal of the conversion is to produce JSON that is useful for
indexing, querying and reporting in external systems like Postgres and
Elasticsearch.
"""
import binascii
from six import BytesIO
from six.moves import copyreg
import _codecs
import datetime
import json
import logging
import pickletools
import re
from struct import unpack

logger = logging.getLogger(__name__)

class Bytes(object):

    def __init__(self, data):
        self.data = data

    def json_reduce(self):
        return {'::': 'hex', 'hex': binascii.b2a_hex(self.data)}

def u64(v):
    """Unpack an 8-byte string into a 64-bit long integer."""
    try:
        return unpack(">Q", v)[0]
    except Exception:
        if isinstance(v, Bytes):
            v = v.data
        elif not isinstance(v, bytes):
            v = v.encode('latin1')
        return unpack(">Q", v)[0]

bytes_types = (Bytes, bytes, str)
class Persistent(object):

    def __init__(self, id):
        if isinstance(id, bytes_types):
            id = u64(id)
        else:
            assert (len(id) == 2 and
                    isinstance(id[0], bytes_types) and
                    isinstance(id[1], Global))
            id = u64(id[0]), id[1].name
        self.id = id

    def json_reduce(self):
        return {'::': 'persistent', 'id': self.id}

class Global(object):

    def __init__(self, module, name):
        self.name = module + '.' + name

    def json_reduce(self):
        return {'::': 'global', 'name': self.name}

class Instance(object):

    id = None

    def __init__(self, class_name, args, state=None):
        self.class_name = class_name
        self.args = args
        self.state = state

    def __setstate__(self, state):
        self.state = state

    def json_reduce(self):
        state = self.state
        if not isinstance(state, dict):
            state = dict(state=state) if state else {}

        state['::'] = self.class_name
        if self.args:
            state['::()'] = self.args
        if self.id is not None:
            state['::id'] = self.id

        return state

class Get(object):

    def __init__(self, unpickler, id, v):
        self.unpickler = unpickler
        self.id = id
        self.v = v

    def json_reduce(self):
        if self.unpickler.cyclic:
            return {'::': 'ref', 'id': self.id}
        else:
            return self.v

class Put(Get):

    got = False

    def __setstate__(self, state):
        self.v.__setstate__(state)

    def __setitem__(self, k, v):
        self.v[k] = v

    def append(self, i):
        self.v.append(i)

    def update(self, v):
        self.v.update(v)

    def extend(self, v):
        self.v.extend(v)

    def __bool__(self):
        return bool(self.v)
    __nonzero__ = __bool__

    def json_reduce(self):
        v = self.v
        if self.got and self.unpickler.cyclic:
            if isinstance(v, Instance):
                v.id = self.id
            else:
                v = {'::': 'shared', 'id': self.id, 'value': v}
        return v

def dt_bytes(v):
    if isinstance(v, Bytes):
        v = v.data
    return v

def datetime_(data, tz=None):
    result = datetime.datetime(dt_bytes(data)).isoformat()
    if tz is not None:
        result = Instance("datetime", (), dict(value=result, tz=tz))
    return result

def reconstruct(args):
    (cls, base, state) = args
    ob = Instance(cls.name, ())
    if state is not None:
        ob.__setstate__(state)
    return ob

def handle_set(args):
    [arg] = args
    if isinstance(arg, Put):
        arg = arg.v
    return sorted(arg)

special_classes = {
    'datetime.datetime':
    lambda args: datetime_(*args),
    'datetime.date': lambda args: datetime.date(dt_bytes(*args)).isoformat(),
    '_codecs.encode': lambda args: _codecs.encode(*args),
    'copy_reg._reconstructor': reconstruct,
    '__builtin__.frozenset': handle_set,
    '__builtin__.set': handle_set,
    'builtins.frozenset': handle_set,
    'builtins.set': handle_set,
    }

def instance(global_, args):
    name = global_.name
    if name in special_classes:
        return special_classes[name](args)

    return Instance(name, args)

basic_types = (float, int, type(1<<99), type(b''), type(u''),
               tuple, Global, Bytes, frozenset)

def default(ob):
    try:
        return ob.json_reduce()
    except AttributeError:
        if isinstance(ob, bytes):
            try:
                return ob.decode('ascii')
            except Exception:
                return {'::': 'hex', 'hex': binascii.b2a_hex(ob)}

class JsonUnpickler:
    """Unpickler that returns JSON

    Usage::

      >>> apickle = pickle.dumps([1,2])
      >>> unpickler = JsonUnpickler(apickle)
      >>> json_string = unpickler.load()
      >>> unpickler.pos == len(apickle)
      True
    """

    cyclic = False

    def __init__(self, pickle):
        self.stack = []
        self.append = self.stack.append
        self.marks = []
        self.memo = {}
        self.set_pickle(pickle)

    def set_pickle(self, pickle):
        self.pickle = pickle
        self.ops = pickletools.genops(BytesIO(pickle))

    def load(self, **json_args):
        for op, arg, pos in self.ops:
            # print(op.name, arg, pos, self.stack) # uncomment to trace
            if op.name == 'STOP':
                self.pos = pos + 1
                self.set_pickle(self.pickle[self.pos:])
                try:
                    return json.dumps(self.stack[-1],
                                      default=default,
                                      **json_args)
                except ValueError:
                    self.cyclic = True
                    return json.dumps(self.stack[-1],
                                      default=default,
                                      **json_args)

            getattr(self, op.name)(arg)

    def push_arg(self, arg):
        self.append(arg)

    INT = BININT = BININT1 = BININT2 = push_arg
    LONG = LONG1 = LONG4 = push_arg

    def STRING(self, v):
        if isinstance(v, bytes):
            return self.BINBYTES(v)
        self.append(v)
    BINSTRING = SHORT_BINSTRING = STRING

    def BINBYTES(self, v):
        try:
            v.decode('ascii')
        except UnicodeDecodeError:
            v = Bytes(v)
        self.append(v)
    SHORT_BINBYTES = BINBYTES8 = BINBYTES

    UNICODE = BINUNICODE = SHORT_BINUNICODE = BINUNICODE8 = push_arg
    FLOAT = BINFLOAT = push_arg
    STOP = None # for checking completeness

    def        NONE(self, _): self.push_arg( None )
    def     NEWTRUE(self, _): self.push_arg( True )
    def    NEWFALSE(self, _): self.push_arg( False )
    def  EMPTY_LIST(self, _): self.push_arg( [] )
    def EMPTY_TUPLE(self, _): self.push_arg( () )
    def  EMPTY_DICT(self, _): self.push_arg( {} )
    def   EMPTY_SET(self, _): self.push_arg( [] )

    def APPEND(self, _):
        ob = self.stack.pop()
        self.stack[-1].append(ob)

    def pop(self, count):
        result = self.stack[-count:]
        self.stack[-count:] = []
        return result

    def pop_marked(self):
        mark = self.marks.pop()
        marked = self.stack[mark:]
        self.stack[mark:] = []
        return marked

    def APPENDS(self, _):
        marked = self.pop_marked()
        self.stack[-1].extend(marked)

    def LIST(self, _):
        marked = self.pop_marked()
        self.append(marked)

    def TUPLE(self, _):
        marked = self.pop_marked()
        self.append(tuple(marked))

    def TUPLE1(self, _): self.stack[-1] = (self.stack[-1], )
    def TUPLE2(self, _): self.stack[-2:] = [tuple(self.stack[-2:])]
    def TUPLE3(self, _): self.stack[-3:] = [tuple(self.stack[-3:])]

    def DICT(self, _):
        marked = self.pop_marked()
        self.append({marked[i]: marked[i+1]
                             for i in range(0, len(marked), 2)
                             })

    def SETITEM(self, _):
        k, v = self.pop(2)
        self.stack[-1][k] = v

    def SETITEMS(self, _):
        marked = self.pop_marked()
        self.stack[-1].update({marked[i]: marked[i+1]
                                 for i in range(0, len(marked), 2)
                                 })

    def ADDITEMS(self, _):
        marked = self.pop_marked()
        self.stack[-1].extend(sorted(marked))

    def FROZENSET(self, _):
        marked = self.pop_marked()
        self.append(sorted(marked))

    def POP(self, _): self.stack.pop()
    def DUP(self, _): self.append(self.stack[-1])
    def MARK(self, _): self.marks.append(len(self.stack))
    def POP_MARK(self, _): self.pop_marked()

    def GET(self, k):
        v = self.memo[k]
        if not isinstance(v, basic_types):
            v.got = True
            v = Get(self, k, v.v)
        self.append(v)
    BINGET = LONG_BINGET = GET

    def PUT(self, k):
        v = self.stack.pop()
        if not isinstance(v, basic_types):
            v = Put(self, k, v)
        self.append(v)
        self.memo[k] = v
    BINPUT = LONG_BINPUT = PUT

    def MEMOIZE(self, _):
        self.PUT(len(self.memo))

    def EXT1(self, code):
        self.append(Global(*copyreg._inverted_registry[code]))
    EXT2 = EXT4 = EXT1

    def GLOBAL(self, arg):
        self.append(Global(*arg.split()))

    def STACK_GLOBAL(self, _):
        args = self.pop(2)
        self.append(Global(*args))

    def REDUCE(self, _):
        f, args = self.pop(2)
        self.append(instance(f,args))

    def BUILD(self, _):
        state = self.stack.pop()
        self.stack[-1].__setstate__(state)

    def INST(self, arg):
        self.append(instance(Global(*arg.split()), tuple(self.pop_marked())))

    def OBJ(self, _):
        args = self.pop_marked()
        self.append(instance(args[0], tuple(args[1:])))

    def NEWOBJ(self, _):
        self.append(instance(*self.pop(2)))

    def NEWOBJ_EX(self, _):
        cls, args, kw = self.pop(3)
        if kw:
            if args:
                args = args, kw
            else:
                args = kw
        self.append(instance(cls, args))

    def PROTO(self, _): pass
    def FRAME(self, _): pass

    def PERSID(self, id):           # pragma: no cover
        self.append(Persistent(id)) # Not used because only proto 0
                                    # and ZODB uses 1 or 3 (or greater later).

    def BINPERSID(self, _):
        self.stack[-1] = Persistent(self.stack[-1])


unicode_surrogates = re.compile(r'\\ud[89a-f][0-9a-f]{2,2}', flags=re.I)
NoneNoneNone = None, None, None

class Jsonifier:

    skip_class = re.compile('BTrees[.]|ZODB.blob').match

    def __init__(self, skip_class=None):
        """Create a callable for converting database data to Newt JSON

        Parameters:

        skip_class
          A callable that will be called with the class name extracted
          from the data.  If the callable returns a true value, then data
          won't be converted to JSON and ``(None, None, None)`` are
          returned.  The default skips objects from the ``BTrees`` package and
          blobs.
        """
        if skip_class is not None:
            self.skip_class = skip_class

    def __call__(self, id, data):
        """Convert data from a ZODB data record to data used by newt.

        The data returned is a class name, ghost pickle, and state triple.
        The state is a JSON-formatted string.  The ghost pickle is a
        binary string that can be used to create a ZODB ghost object.

        If there is an error converting data, if the data is empty, or
        if the skip_class function returns a true value, then
        ``(None, None, None)`` is returned.

        Parameters:

        id
          A data identifier (e.g. an object id) used when logging errors.

        data
          Pickle data to be converted.
        """
        if not data:
            return NoneNoneNone
        unpickler = JsonUnpickler(data)
        try:
            klass = json.loads(unpickler.load())

            if isinstance(klass, list):
                klass, args = klass
                if isinstance(klass, list):
                    class_name = '.'.join(klass)
                else:
                    class_name = klass['name']
            else:
                class_name = klass['name']

            if self.skip_class(class_name):
                return NoneNoneNone

            ghost_pickle = data[:unpickler.pos]
            state = unpickler.load()

            # xstate = xform(zoid, class_name, state)
            # if xstate is not state:
            #     state = xstate
            #     if not isinstance(state, bytes):
            #         state = json.dumps(state)

            # Remove unicode surrogate strings, as postgres utf-8
            # will reject them.

            state = unicode_surrogates.sub(' ', state).replace('\\u0000', ' ')
        except Exception:
            logger.error("Failed pickle load, oid: %r, pickle starts: %r",
                         id, data[:50], exc_info=True)
            return NoneNoneNone

        return class_name, ghost_pickle, state
