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
import unittest

from .. import Object

from ZODB.utils import u64

class AdapterTests(unittest.TestCase):

    dbname = 'newt_test_database'

    def setUp(self):
        self.base_conn = psycopg2.connect('')
        self.base_conn.autocommit = True
        self.base_cursor = self.base_conn.cursor()
        self.drop_db()
        self.base_cursor.execute('create database ' + self.dbname)
        # self.conn = psycopg2.connect('dbname=' + self.dbname)
        # self.cursor = self.conn.cursor()
        # self.ex = self.cursor.execute

    def tearDown(self):
        # self.conn.close()
        self.drop_db()
        self.base_conn.close()

    def drop_db(self):
        self.base_cursor.execute('drop database if exists ' + self.dbname)

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
