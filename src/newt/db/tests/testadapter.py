import persistent.mapping
import pickle
from relstorage.tests import hftestbase
from relstorage.tests import hptestbase
from relstorage.tests import reltestbase

import unittest

from .. import Object

from ZODB.utils import u64

from .base import DBSetup

class AdapterTests(DBSetup, unittest.TestCase):

    def test_basic(self):
        import newt.db
        conn = newt.db.connection(self.dsn)

        # Add an object:
        conn.root.x = o = Object(a=1)
        conn.commit()

        # We should see the json data:
        [(class_name, ghost_pickle, state)] = conn.query_data("""\
            select class_name, ghost_pickle, state
            from newt where zoid = %s""",
            u64(o._p_oid))
        self.assertEqual(class_name, 'newt.db._object.Object')
        self.assertEqual(pickle.loads(ghost_pickle.tobytes()), Object)
        self.assertEqual(state, {'a': 1})

        [(class_name, ghost_pickle, state)] = conn.query_data("""\
            select class_name, ghost_pickle, state
            from newt where zoid = 0""")
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
