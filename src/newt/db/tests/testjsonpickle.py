##############################################################################
#
# Copyright (c) Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
from six.moves import copyreg
import datetime
import doctest
import json
import persistent
from persistent.mapping import PersistentMapping
import pickle
from pprint import pprint
from six import BytesIO, PY3
import textwrap
import unittest
import ZODB
from ZODB.utils import z64, p64, maxtid

from ..jsonpickle import JsonUnpickler, dumps

class C(object):
    def __init__(self, **attrs):
        self.__dict__.update(attrs)

class I:
    def __init__(self, **attrs):
        self.__dict__.update(attrs)

class P(persistent.Persistent):

    def __init__(self, **kw):
        self.__dict__.update(kw)

class E190(object): pass
class E60190(object): pass
class E600000190(object): pass
copyreg.add_extension(__name__, 'E190', 190)
copyreg.add_extension(__name__, 'E60190', 60190)
copyreg.add_extension(__name__, 'E600000190', 600000190)

test_many_pickle_expect = """\
{
  "l1": [
    "spam ",
    "spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam ",
    {
      "::": "hex",
      "hex": "dd"
    },
    1.23,
    "2017-01-02",
    {
      "f": false,
      "l": [],
      "l1": [
        1
      ],
      "n": null,
      "t": true
    },
    "2017-01-02T04:05:06",
    9,
    99,
    1073741824,
    1152921504606846976,
    1237940039285380274899124224,
    [
      "spam ",
      "spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam ",
      {
        "::": "hex",
        "hex": "dd"
      },
      1.23,
      "2017-01-02",
      {
        "f": false,
        "l": [],
        "l1": [
          1
        ],
        "n": null,
        "t": true
      },
      "2017-01-02T04:05:06",
      9,
      99,
      1073741824,
      1152921504606846976,
      1237940039285380274899124224
    ]
  ],
  "l2": [
    "spam ",
    "spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam ",
    {
      "::": "hex",
      "hex": "dd"
    },
    1.23,
    "2017-01-02",
    {
      "f": false,
      "l": [],
      "l1": [
        1
      ],
      "n": null,
      "t": true
    },
    "2017-01-02T04:05:06",
    9,
    99,
    1073741824,
    1152921504606846976,
    1237940039285380274899124224,
    [
      "spam ",
      "spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam spam ",
      {
        "::": "hex",
        "hex": "dd"
      },
      1.23,
      "2017-01-02",
      {
        "f": false,
        "l": [],
        "l1": [
          1
        ],
        "n": null,
        "t": true
      },
      "2017-01-02T04:05:06",
      9,
      99,
      1073741824,
      1152921504606846976,
      1237940039285380274899124224
    ]
  ]
}"""

special = object()
class SpecialPickler(pickle.Pickler):

    def persistent_id(self, obj):
        if obj is special:
            return b'\xff' * 8


class JsonUnpicklerProtocol0Tests(unittest.TestCase):

    maxDiff = None

    proto = 0

    def test_many_pickle(self):
        s = 'spam '
        S = s*99
        b = b'\xdd'
        f = 1.23
        d = datetime.date(2017, 1, 2)
        di = dict(n=None, t=True, f=False, l=[], l1=[1])
        t = datetime.datetime(2017, 1, 2, 4, 5, 6)
        tup = s, S, b, f, d, di, t, 9, 99, 1 << 30, 1 << 60, 1 << 90
        lst = list(tup)
        lst.append(tup)
        data = dict(l1 = lst, l2=lst)
        got = (
            JsonUnpickler(pickle.dumps(data, self.proto))
            .load(sort_keys=True, indent=2)
            .replace(' \n', '\n')
            )
        self.assertEqual(test_many_pickle_expect, got)

    def test_unicode(self):
        data = u'\ua000', u'\ua000\n\ua000'
        got = JsonUnpickler(pickle.dumps(data, self.proto)).load(sort_keys=True)
        self.assertEqual(got, '["\\ua000", "\\ua000\\n\\ua000"]')

    def test_cyclic_list(self):
        data = [1, 2]
        data.append((3, data))
        got = JsonUnpickler(pickle.dumps(data, self.proto)).load(sort_keys=True)
        self.assertEqual(
            got,
            '{"::": "shared", "::id": 0, "value":'
            ' [1, 2, [3, {"::->": 0}]]}'
            )

    def test_cyclic_object(self, cls=C):
        c = cls(); c.name = 'c'; c.c = c
        got = json.loads(JsonUnpickler(pickle.dumps(c, self.proto)).load())
        self.assertEqual(got.pop('::id'), got['c'].pop('::->'))
        self.assertEqual(
            {"::": "newt.db.tests.testjsonpickle." + cls.__name__,
             "c": {}, "name": "c"},
            got)

    def test_cyclic_instance(self):
        self.test_cyclic_object(I)

    def test_sets(self):
        data = set((1,2,3)), frozenset((4, 5, 6))
        got = JsonUnpickler(pickle.dumps(data, self.proto)).load()
        self.assertEqual(got, '[[1, 2, 3], [4, 5, 6]]')

    def test_Decimal(self):
        import decimal
        data = decimal.Decimal(6)/decimal.Decimal(5)
        got = JsonUnpickler(pickle.dumps(data, self.proto)).load()
        self.assertEqual(got, '1.2')

    def test_reducer_in_dumps(self):

        def reducer(name, v):
            if (name == 'datetime.date'):
                [v] = v
                if hasattr(v, 'data'):
                    v = v.data
                date = datetime.date(v)
                return date.year, date.month, date.day

        self.assertEqual(
            '[\n  2017,\n  2,\n  27\n]',
            dumps(datetime.date(2017, 2, 27), reducer, self.proto))

    def test_non_empty_instance(self):
        i = I(a=1)
        self.assertEqual('{"::": "newt.db.tests.testjsonpickle.I", "a": 1}',
                         dumps(i, indent=None))
        i = C(a=1)
        self.assertEqual('{"::": "newt.db.tests.testjsonpickle.C", "a": 1}',
                         dumps(i, indent=None))


class TZ(datetime.tzinfo):
    pass

class JsonUnpicklerProtocol1Tests(JsonUnpicklerProtocol0Tests):

    def test_scalar_persistent_id(self):
        data = (special, special)
        f = BytesIO()
        pickler = SpecialPickler(f, protocol=self.proto)
        pickler.dump(data)
        got = JsonUnpickler(f.getvalue()).load(sort_keys=True)
        self.assertEqual(
            got,
            '[{"::": "persistent", "::=>": 18446744073709551615,'
            ' "id": 18446744073709551615},'
            ' {"::": "persistent", "::=>": 18446744073709551615,'
            ' "id": 18446744073709551615}]')

    def test_dt_with_tz(self):
        data = datetime.datetime(1, 2, 3, 4, 5, 6, 7, TZ())
        got = JsonUnpickler(pickle.dumps(data, self.proto)).load(sort_keys=True)
        self.assertEqual(
            '{"::": "datetime",'
            ' "tz": {"::": "newt.db.tests.testjsonpickle.TZ"},'
            ' "value": "0001-02-03T04:05:06.000007"}',
            got)

    proto = 1

class JsonUnpicklerProtocol2Tests(JsonUnpicklerProtocol0Tests):

    proto = 2

    def test_ext(self):
        data = E190(), E60190(), E600000190()
        got = JsonUnpickler(pickle.dumps(data, self.proto)).load()
        self.assertEqual(
            got,
            '[{"::": "newt.db.tests.testjsonpickle.E190"},'
            ' {"::": "newt.db.tests.testjsonpickle.E60190"},'
            ' {"::": "newt.db.tests.testjsonpickle.E600000190"}]')

if pickle.HIGHEST_PROTOCOL >=3:

    class JsonUnpicklerProtocol3Tests(JsonUnpicklerProtocol0Tests):

        proto = 3

    if pickle.HIGHEST_PROTOCOL >=4:

        class JsonUnpicklerProtocol4Tests(JsonUnpicklerProtocol0Tests):

            proto = 4

            def test_newobj_ex(self):
                class X:

                    def __reduce_ex__(self, proto):
                        assert proto >= 4
                        return copyreg.__newobj_ex__, (C, (1,2), dict(c=3))

                got = JsonUnpickler(pickle.dumps(X(), self.proto)
                                    ).load(sort_keys=True)
                self.assertEqual(
                    got,
                    '{"::": "newt.db.tests.testjsonpickle.C",'
                    ' "::()": [[1, 2], {"c": 3}]}')

class Complete(unittest.TestCase):

    def test_handle_all_opcodes(self):
        import pickletools
        for opcode in pickletools.opcodes:
            self.assertTrue(hasattr(JsonUnpickler, opcode.name))

class JsonUnpicklerDBTests(unittest.TestCase):

    maxDiff = None

    def setUp(self):
        self.db = ZODB.DB(None)
        self.conn = self.db.open()
        self.root = self.conn.root

    def tearDown(self):
        self.db.close()

    def commit(self, p=None):
        if p is None:
            self.conn.transaction_manager.commit()
            p, _, _ = self.db.storage.loadBefore(z64, maxtid)
        self.unpickler = JsonUnpickler(p)
        return p

    def load(self):
        return json.loads(self.unpickler.load())

    def pprint(self):
        pprint(self.load())

    def check(self, expected):
        self.assertEqual(self.load(), expected)

    def test_basics(self):
        root = self.root
        root.numbers = 0, 123456789, 1 << 70, 1234.56789
        root.time = datetime.datetime(2001, 2, 3, 4, 5, 6, 7)
        root.date = datetime.datetime(2001, 2, 3)
        root.delta = datetime.timedelta(1, 2, 3)
        root.name = u'root'
        root.data = b'\xff'
        root.list = [1, 2, 3, root.name, root.numbers]
        root.first = PersistentMapping()
        p = self.commit()
        self.check({"::": "global",
                    "name": "persistent.mapping.PersistentMapping"})

        # pos points just past end of pickle:
        self.assertEqual(p[self.unpickler.pos-1:self.unpickler.pos], b'.')

        self.check(
            {u'data':
              {u'data': {u'::': u'hex', u'hex': u'ff'},
               u'date': u'2001-02-03T00:00:00',
               u'delta': {u'::': u'datetime.timedelta',
                          u'::()': [1, 2, 3]},
               u'first': {u'::': u'persistent', u'::=>': 1,
                          u'id': [1, u'persistent.mapping.PersistentMapping']},
               u'list': [1, 2, 3, u'root',
                         [0, 123456789, 1180591620717411303424, 1234.56789]],
               u'name': u'root',
               u'numbers': [0, 123456789, 1180591620717411303424, 1234.56789],
               u'time': u'2001-02-03T04:05:06.000007'}}
            )

    def test_non_ascii_zoid(self):
        root = self.root
        for i in range(200):
            self.conn.add(PersistentMapping())
        root.x = PersistentMapping()
        self.commit()
        _ = self.load()
        _ = self.load()

    def test_put_append(self):
        root = self.root
        self.root.x = self.root.y = [1]
        self.commit()
        _ = self.load()
        _ = self.load()

    def test_put_setitem(self):
        root = self.root
        self.root.x = self.root.y = dict(x=1)
        self.commit()
        _ = self.load()
        _ = self.load()

    def test_put_persistent_id(self):
        self.commit(
           b'cBTrees.OOBTree\nOOBTree\nq\x01.((((U\x07100x100q\x02(U\x08'
           b'\x00\x00\x00\x00\x00\x92s\x11q\x03ccontent.models.files\n'
           b'Thumbnail\nq\x04tq\x05QU\x0550x50q\x06(U\x08\x00\x00\x00'
           b'\x00\x00\x9cV_q\x07h\x04tq\x08QU\x0675x100q\t(U\x08\x00'
           b'\x00\x00\x00\x00\x92s\x0eq\nh\x04tq\x0bQU\x0585x85q\x0c'
           b'(U\x08\x00\x00\x00\x00\x00\x9cV]q\rh\x04tq\x0eQttttq\x0f.')

        _ = self.load()
        _ = self.load()

    def test_jsonifier(self):
        from zope.testing.loggingsupport import InstalledHandler
        handler = InstalledHandler('newt.db.jsonpickle')
        from ..jsonpickle import Jsonifier

        jsonifier = Jsonifier()

        p, tid = self.conn._storage.load(z64)
        class_name, ghost_pickle, state = jsonifier('0', p)
        self.assertEqual('persistent.mapping.PersistentMapping', class_name)
        self.assertEqual('{"data": {}}', state)
        self.assertTrue(p.startswith(ghost_pickle) and
                        ghost_pickle[-1:] == b'.' and
                        b'persistent.mapping' in ghost_pickle)

        # custom skip_class
        jsonifier2 = Jsonifier(skip_class=lambda c: 1)
        self.assertEqual((None, None, None), jsonifier2('0', p))

        # empty records are skipped:
        self.assertEqual((None, None, None), jsonifier('0', ''))

        # BTrees are skipped by default
        from BTrees.OOBTree import BTree
        self.root.x = BTree()
        self.conn.transaction_manager.commit()
        p, tid = self.conn._storage.load(self.root.x._p_oid)
        self.assertEqual((None, None, None), jsonifier('0', p))

        # errors are logged, and return Nones:
        self.assertEqual(handler.records, [])
        self.assertEqual((None, None, None), jsonifier('foo', b'badness'))
        self.assertEqual(
            [r.getMessage().replace("b'", "'") for r in handler.records],
            ["Failed pickle load, oid: 'foo', pickle starts: 'badness'"])

        handler.uninstall()

    def test_jsonifier_reducer(self):
        from ..jsonpickle import Jsonifier
        self.conn.root.x = MySpecialString('test')
        self.conn.transaction_manager.commit()
        p, tid = self.conn._storage.load(z64)

        def reducer(name, data):
            if name.endswith('MySpecialString'):
                return data if isinstance(data, str) else data[0]

        jsonifier = Jsonifier(reducer=reducer)
        class_name, ghost_pickle, state = jsonifier('0', p)
        self.assertEqual('persistent.mapping.PersistentMapping', class_name)
        self.assertEqual('{"data": {"x": "test"}}', state)


    def test_jsonifier_transform(self):
        self.conn.root.x = P(test=1)
        self.conn.transaction_manager.commit()

        p, _ = self.conn._storage.load(self.conn.root.x._p_oid)

        # baseline
        from ..jsonpickle import Jsonifier
        self.assertEqual(Jsonifier()('0', p)[2], '{"test": 1}')

        # with transform:
        def transform(class_name, state):
            if class_name.endswith('.P'):
                return '"Tests were here"'
        self.assertEqual(Jsonifier(transform=transform)('0', p)[2],
                         '"Tests were here"')

        # None return have no effect:
        self.assertEqual(Jsonifier(transform = lambda *a: None)('0', p)[2],
                         '{"test": 1}')

        # A transform can return an empty string, signifying a skip/delete:
        def veto(class_name, state):
            if class_name.endswith('.P'):
                return ''

        self.assertEqual(Jsonifier(transform=veto)('0', p), (None, None, None))

class MySpecialString(str):
    pass
