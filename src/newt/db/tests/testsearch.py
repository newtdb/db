from ZODB.utils import u64
import unittest


from .. import Object
from .base import DBSetup

class SearchTests(DBSetup, unittest.TestCase):

    def setUp(self):
        super(SearchTests, self).setUp()
        import newt.db
        self.db = newt.db.DB(self.dsn)
        self.conn = self.db.open()

    def tearDown(self):
        self.db.close()
        super(SearchTests, self).tearDown()

    def store(self, index, **data):
        self.conn.root()[index] = o = Object(**data)
        self.conn.transaction_manager.commit()
        return u64(o._p_serial)

    def test_search(self):
        for i in range(9):
            tid = self.store(i, i=i)

        obs = self.conn.search(
            "select * from newt "
            "where state->>'i' >= %s and state->>'i' <= %s "
            "order by zoid ", '2', '5')

        self.assertEqual([2, 3, 4, 5], [o.i for o in obs])

        # separate conn (to make sure we get new ghosts, and
        # ``where``` api and keyword args

        conn2 = self.db.open()
        obs2 = self.conn.where("state->>'i' >= %(a)s and state->>'i' <= %(b)s",
                               a='2', b='5')
        self.assertEqual([2, 3, 4, 5], sorted(o.i for o in obs2))

        self.assertEqual(set(o._p_oid for o in obs),  # yes, these are
                         set(o._p_oid for o in obs2)) #  persistent objects :)


    def test_search_batch(self):
        for i in range(99):
            tid = self.store(i, i=i)

        conn2 = self.db.open()

        total, batch = conn2.search_batch(
            "select * from newt "
            "where (state->>'i')::int >= %(a)s and (state->>'i')::int <= %(b)s "
            "order by zoid ",
            dict(a=2, b=90),
            10, 20
            )
        self.assertEqual(total, 89)

        self.assertEqual(list(range(12, 32)), [o.i for o in batch])

        # We didn't end up with all of the objects getting loaded:
        self.assertEqual(len(conn2._cache), 20)
