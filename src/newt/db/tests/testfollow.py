import unittest
from zope.testing.wait import wait

from .. import pg_connection
from . import base

class FollowTests(base.DBSetup, unittest.TestCase):

    history_preserving = False

    def setUp(self):
        super(FollowTests, self).setUp()
        self.conn = pg_connection(self.dsn)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.ex = self.cursor.execute
        if self.history_preserving:
            self.ex("create table if not exists object_state"
                    " (zoid bigint, tid bigint, state bytea)")
            self.ex("create table if not exists current_object"
                    " (zoid bigint primary key, tid bigint)")
        else:
            self.ex("create table if not exists object_state"
                    " (zoid bigint primary key, tid bigint, state bytea)")

    def tearDown(self):
        self.cursor.close()
        self.conn.close()
        super(FollowTests, self).tearDown()

    def mogrify(self, *args):
        return self.cursor.mogrify(*args).decode('ascii')

    def store(self, tid, *oids):
        svalues = ", ".join(self.mogrify("(%s, %s, 'some data')", (oid, tid))
                            for oid in oids)
        cvalues = ", ".join(self.mogrify("(%s, %s)", (oid, tid))
                            for oid in oids)
        self.ex("begin")
        if self.history_preserving:
            self.ex("insert into object_state values {}".format(svalues))
            self.ex("delete from current_object where zoid = any(%s)",
                    (list(oids), ))
            self.ex("insert into current_object values {}".format(cvalues))
        else:
            self.ex("delete from object_state where zoid = any(%s)",
                    (list(oids), ))
            self.ex("insert into object_state values {}".format(svalues))
        self.ex("commit")

    def test_non_empty_generator(self):
        from ..follow import non_empty_generator
        self.assertEqual(non_empty_generator(iter(())), None)
        self.assertEqual(list(non_empty_generator(iter((1, 2, 3)))), [1, 2, 3])

    def test_update_iterator_batching(self):
        t = 0
        for i in range(0, 99, 7):
            t += 1
            self.store(t, *range(i, i+7))
        t = 20
        for i in range(0, 99, 7):
            t += 1
            self.store(t, *range(i, i+7))
        from ..follow import updates
        self.assertEqual(
            [[int(r[1]) for r in b]
             for b in updates(self.conn.dsn, end_tid=999, batch_limit=20)
             ],
            [list(range(0, 21)),
             list(range(21, 42)),
             list(range(42, 63)),
             list(range(63, 84)),
             list(range(84, 105)),
             ])

        self.assertEqual(
            [[int(r[1]) for r in b]
             for b in updates(self.conn.dsn, start_tid=22, end_tid=24)
             ],
            [list(range(14, 28))],
            )

    def wait_equal(self, expect, got):
        try:
            wait(lambda : expect == got)
        except Exception:
            self.assertEqual(expect, got)

    def test_update_iterator_follow(self, poll_timeout=99):
        self.store(1, 1, 2)
        self.store(2, 1, 2)

        from ..follow import updates

        import threading

        data = []
        def collect():
            for batch in updates(self.conn.dsn, poll_timeout=poll_timeout):
                batch = list(batch)
                data.append([(int(r[0]), int(r[1])) for r in batch])

        thread = threading.Thread(target=collect)
        thread.setDaemon(True)
        thread.start()

        wait_equal = self.wait_equal
        try:
            wait_equal([[(2, 1), (2, 2)]], data); del data[:]
            self.store(3, 3, 4)
            wait_equal([[(3, 3), (3, 4)]], data); del data[:]
            self.store(4, 5, 6)
            wait_equal([[(4, 5), (4, 6)]], data); del data[:]
            self.store(5, 7, 8)
            wait_equal([[(5, 7), (5, 8)]], data); del data[:]
            self.store(6, 5, 8)
            wait_equal([[(6, 5), (6, 8)]], data); del data[:]
        finally:
            self.ex("notify newt_object_state_changed, 'STOP'")
            thread.join(9)

    def test_update_iterator_follow_no_timeout(self):
        self.test_update_iterator_follow(None)

    def test_garbage(self):
        self.ex("drop table object_state")
        if self.history_preserving:
            self.ex("drop table current_object")

        import newt.db
        db = newt.db.DB(self.dsn, keep_history=self.history_preserving)
        conn = db.open()
        from .._object import Object
        conn.root.x = Object()
        conn.root.y = Object()
        conn.commit()
        from ZODB.utils import u64
        zoids = set(u64(o._p_oid) for o in (conn.root.x, conn.root.y))

        del conn.root.x
        del conn.root.y
        conn.commit()

        import time
        from ZODB.serialize import referencesf
        db.storage.pack(time.time(), referencesf, prepack_only=True)

        import newt.db.follow
        self.assertEqual(zoids, set(newt.db.follow.garbage(self.dsn)))

        conn.close()
        db.close()

class FollowTestsHP(FollowTests):

    history_preserving = True
