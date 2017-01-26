import psycopg2
import unittest

from . import base

class FollowTests(base.DBSetup, unittest.TestCase):

    history_preserving = False

    def setUp(self):
        super(FollowTests, self).setUp()
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.mogrify = self.cursor.mogrify
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
        self.conn.close()
        super(FollowTests, self).tearDown()

    def store(self, tid, *oids):
        svalues = ", ".join(self.mogrify("(%s, %s, 'some data')", (oid, tid))
                            for oid in oids)
        cvalues = ", ".join(self.mogrify("(%s, %s)", (oid, tid))
                            for oid in oids)
        if self.history_preserving:
            self.ex("insert into object_state values {}".format(svalues))
            self.ex("""\
            insert into current_object values {}
            on conflict (zoid) do update set tid=excluded.tid
            """.format(cvalues))
        else:
            self.ex("""
            insert into object_state values {}
            on conflict (zoid)
            do update set tid=excluded.tid, state=excluded.state
            """.format(svalues))

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
        conn = psycopg2.connect(self.conn.dsn)
        self.assertEqual(
            [[int(r[1]) for r in b]
             for b in updates(conn, end_tid=999, batch_limit=20)
             ],
            [list(range(0, 21)),
             list(range(21, 42)),
             list(range(42, 63)),
             list(range(63, 84)),
             list(range(84, 105)),
             ])

        self.assertEqual(
            [[int(r[1]) for r in b]
             for b in updates(conn, start_tid=22, end_tid=24)
             ],
            [list(range(14, 28))],
            )

    def test_update_iterator_follow(self, poll_timeout=99):
        conn = psycopg2.connect(self.conn.dsn)
        self.store(1, 1, 2)
        self.store(2, 1, 2)
        from ..follow import updates

        import threading

        data = []
        def collect():
            for batch in updates(conn, poll_timeout=poll_timeout):
                data.append([(int(r[0]), int(r[1])) for r in batch])

        thread = threading.Thread(target=collect)
        thread.setDaemon(True)
        thread.start()

        from zope.testing.wait import wait
        def wait_equal(e, g):
            try:
                wait(lambda : e==g)
            except Exception:
                raise

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

class FollowTestsHP(FollowTests):

    history_preserving = True
