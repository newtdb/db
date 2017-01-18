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
import persistent.mapping
import pickle
import psycopg2
from relstorage.tests import hftestbase
from relstorage.tests import hptestbase
from relstorage.tests import reltestbase

import unittest

from .. import Object

from ZODB.utils import u64

class DBSetup(object):

    dbname = 'newt_test_database'

    def setUp(self):
        self.base_conn = psycopg2.connect('')
        self.base_conn.autocommit = True
        self.base_cursor = self.base_conn.cursor()
        self.drop_db()
        self.base_cursor.execute('create database ' + self.dbname)
        super(DBSetup, self).setUp()

    def drop_db(self):
        self.base_cursor.execute('drop database if exists ' + self.dbname)

    def tearDown(self):
        super(DBSetup, self).tearDown()
        self.drop_db()
        self.base_conn.close()

class AdapterTests(DBSetup, unittest.TestCase):

    def test_basic(self):
        import newt.db
        conn = newt.db.connection('postgresql://localhost/' + self.dbname)

        # Add an object:
        conn.root.x = o = Object(a=1)
        conn.commit()

        # We should see the json data:
        [(class_name, ghost_pickle, state)] = conn.query_data("""\
            select class_name, ghost_pickle, state
            from object_json where zoid = %s""",
            u64(o._p_oid))
        self.assertEqual(class_name, 'newt.db._object.Object')
        self.assertEqual(pickle.loads(ghost_pickle.tobytes()), Object)
        self.assertEqual(state, {'a': 1})

        [(class_name, ghost_pickle, state)] = conn.query_data("""\
            select class_name, ghost_pickle, state
            from object_json where zoid = 0""")
        self.assertEqual(class_name, 'persistent.mapping.PersistentMapping')
        self.assertEqual(pickle.loads(ghost_pickle.tobytes()),
                         persistent.mapping.PersistentMapping)
        self.assertEqual(
            state,
            {'data': {'x': {'id': [1, 'newt.db._object.Object'],
                            '::': 'persistent'}}})

        conn.close()

# Make sure we didn't break anything:

class UseAdapter(DBSetup):

    def make_adapter(self, options):
        from .._adapter import Adapter
        return Adapter(
            dsn='postgresql://localhost/' + self.dbname,
            options=options,
        )

# class HPDestZODBConvertTests(UseAdapter, reltestbase.
#                              AbstractRSDestZodbConvertTests):
#     pass

class HPTests(UseAdapter, hptestbase.HistoryPreservingRelStorageTests):
    pass

class HPToFile(UseAdapter, hptestbase.HistoryPreservingToFileStorage):
    pass

class HFTests(UseAdapter, hftestbase.HistoryFreeRelStorageTests):
    pass

class HFToFile(UseAdapter, hftestbase.HistoryFreeToFileStorage):
    pass


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(AdapterTests))
    suite.addTest(unittest.makeSuite(HPTests, "check"))
    suite.addTest(unittest.makeSuite(HPToFile, "check"))
    suite.addTest(unittest.makeSuite(HFTests, "check"))
    suite.addTest(unittest.makeSuite(HFToFile, "check"))
    return suite
