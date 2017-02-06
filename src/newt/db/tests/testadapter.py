import persistent.mapping
import os
import pickle
import relstorage.tests
from relstorage.tests import hftestbase
from relstorage.tests import hptestbase
from relstorage.tests import reltestbase
import ZODB
from ZODB.tests.util import MininalTestLayer
from ZODB.utils import u64

import unittest

from .. import Object



from .base import DBSetup

class AdapterTests(DBSetup, unittest.TestCase):

    layer = MininalTestLayer('AdapterTests')
    keep_history = False

    def __assertBasicData(self, conn, o):
        # We should see the json data:
        [(class_name, ghost_pickle, state)] = conn.query_data("""\
            select class_name, ghost_pickle, state
            from newt where zoid = %s""",
            u64(o._p_oid))
        self.assertEqual(class_name, 'newt.db._object.Object')
        self.assertEqual(pickle.loads(ghost_pickle), Object)
        self.assertEqual(state, {'a': 1})

        [(class_name, ghost_pickle, state)] = conn.query_data("""\
            select class_name, ghost_pickle, state
            from newt where zoid = 0""")
        self.assertEqual(class_name, 'persistent.mapping.PersistentMapping')
        self.assertEqual(pickle.loads(ghost_pickle),
                         persistent.mapping.PersistentMapping)
        self.assertEqual(
            state,
            {'data': {'x': {'id': [1, 'newt.db._object.Object'],
                            '::': 'persistent'}}})

    def test_basic(self):
        import newt.db
        conn = newt.db.connection(self.dsn, keep_history=self.keep_history)

        # Add an object:
        conn.root.x = o = Object(a=1)
        conn.commit()

        self.__assertBasicData(conn, o)

        conn.close()

    def test_restore(self):
        source_db = ZODB.DB(None)
        source_conn = source_db.open()
        source_conn.root.x = o = Object(a=1)
        source_conn.transaction_manager.commit()

        import newt.db

        storage = newt.db.storage(self.dsn, keep_history=self.keep_history)
        storage.copyTransactionsFrom(source_db.storage)
        storage.close()

        conn = newt.db.connection(self.dsn, keep_history=self.keep_history)
        self.__assertBasicData(conn, o)

        conn.close()
        source_db.close()

    def test_pack(self, upgrade=False):
        import newt.db
        if upgrade:
            conn = newt.db.connection(self.dsn, keep_history=self.keep_history)
            conn.close()
            # emulate an upgrade by dropping the delete trigger
            from relstorage.adapters.postgresql.adapter import select_driver
            pg_conn = select_driver().connect(self.dsn)
            cursor = pg_conn.cursor()
            cursor.execute(
                "drop function if exists newt_delete_on_state_delete() cascade")
            pg_conn.commit()
            cursor.close()
            pg_conn.close()
        conn = newt.db.connection(self.dsn, keep_history=self.keep_history)
        conn.root.a = Object(a=1)
        conn.root.b = Object(a=2)
        conn.commit()
        conn.root.a.a = 3
        del conn.root.b
        conn.commit()
        conn.db().pack()
        conn.commit()
        self.assertEqual(
            [(dict(a=3), )],
            conn.query_data("select state from newt where state ? 'a'"),
            )
        conn.close()

    def test_pack_upgrade(self):
        self.test_pack(True)

    def test_drop_all(self):
        import newt.db
        storage = newt.db.storage(self.dsn, keep_history=self.keep_history)
        storage.close()
        storage._adapter.schema.drop_all()
        from relstorage.adapters.postgresql.adapter import select_driver
        pg_conn = select_driver().connect(self.dsn)
        cursor = pg_conn.cursor()
        cursor.execute(
            "select from pg_catalog.pg_trigger "
            "where tgname = 'newt_delete_on_state_delete_trigger'")
        self.assertEqual([], list(cursor))
        cursor.execute("""
        select from information_schema.tables
        where table_schema = 'public' AND table_name = 'newt'
        """)
        self.assertEqual([], list(cursor))

class HPAdapterTests(AdapterTests):

    layer = MininalTestLayer('HPAdapterTests')
    keep_history = True


# Make sure we didn't break anything:

class UseAdapter(DBSetup):

    def make_adapter(self, options):
        from .._adapter import Adapter
        return Adapter(dsn=self.dsn, options=options)

    def _relstorage_contents(self):
        return """\
        %%import newt.db
        <newt>
          <postgresql>
            dsn %s
          </postgresql>
        </newt>
        """ % self.dsn

class ZConfigTests(object):

    def checkConfigureViaZConfig(self):
        replica_conf = os.path.join(os.path.dirname(relstorage.tests.__file__),
                                    'replicas.conf')
        dsn = 'dbname=' + self.dbname
        conf = u"""
        %%import relstorage
        %%import newt.db
        <zodb main>
            <relstorage>
            name xyz
            read-only false
            keep-history %s
            replica-conf %s
            blob-chunk-size 10MB
            <newt>
            <postgresql>
                driver auto
                dsn %s
            </postgresql>
            </newt>
            </relstorage>
        </zodb>
        """ % (
            self.keep_history and 'true' or 'false',
            replica_conf,
            dsn,
            )

        schema_xml = u"""
        <schema>
        <import package="ZODB"/>
        <section type="ZODB.database" name="main" attribute="database"/>
        </schema>
        """
        import ZConfig
        from io import StringIO
        schema = ZConfig.loadSchemaFile(StringIO(schema_xml))
        config, _ = ZConfig.loadConfigFile(schema, StringIO(conf))

        db = config.database.open()
        try:
            storage = db.storage
            self.assertEqual(storage.isReadOnly(), False)
            self.assertEqual(storage.getName(), "xyz")
            adapter = storage._adapter
            from relstorage.adapters.postgresql import PostgreSQLAdapter
            self.assertIsInstance(adapter, PostgreSQLAdapter)
            self.assertEqual(adapter._dsn, dsn)
            self.assertEqual(adapter.keep_history, self.keep_history)
            self.assertEqual(
                adapter.connmanager.replica_selector.replica_conf,
                replica_conf)
            self.assertEqual(storage._options.blob_chunk_size, 10485760)

            from .._adapter import Adapter
            self.assertEqual(Adapter, storage._adapter.__class__)
        finally:
            db.close()


class HPDestZODBConvertTests(UseAdapter,
                             reltestbase.AbstractRSDestZodbConvertTests):
    layer = MininalTestLayer('HPDestZODBConvertTests')


class HPSrcZODBConvertTests(UseAdapter,
                            reltestbase.AbstractRSSrcZodbConvertTests):
    layer = MininalTestLayer('HPSrcZODBConvertTests')

class HPTests(UseAdapter,
              hptestbase.HistoryPreservingRelStorageTests,
              ZConfigTests):
    layer = MininalTestLayer('HPTests')

class HPToFile(UseAdapter, hptestbase.HistoryPreservingToFileStorage):
    layer = MininalTestLayer('HPToFile')

class HPFromFile(UseAdapter, hptestbase.HistoryPreservingFromFileStorage):
    layer = MininalTestLayer('HPFromFile')

class HFTests(UseAdapter, hftestbase.HistoryFreeRelStorageTests, ZConfigTests):
    layer = MininalTestLayer('HFTests')

class HFToFile(UseAdapter, hftestbase.HistoryFreeToFileStorage):
    layer = MininalTestLayer('HFToFile')

class HFFromFile(UseAdapter, hftestbase.HistoryFreeFromFileStorage):
    layer = MininalTestLayer('HFFromFile')


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(AdapterTests))
    suite.addTest(unittest.makeSuite(HPAdapterTests))
    suite.addTest(unittest.makeSuite(HPTests, "check"))
    suite.addTest(unittest.makeSuite(HPToFile, "check"))
    suite.addTest(unittest.makeSuite(HPFromFile, "check"))
    suite.addTest(unittest.makeSuite(HFTests, "check"))
    suite.addTest(unittest.makeSuite(HFToFile, "check"))
    suite.addTest(unittest.makeSuite(HFFromFile, "check"))
    suite.addTest(unittest.makeSuite(HPDestZODBConvertTests))
    suite.addTest(unittest.makeSuite(HPSrcZODBConvertTests))
    return suite
