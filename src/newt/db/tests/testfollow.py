import psycopg2
import unittest

from . import base

class FollowTests(base.DBSetup, unittest.TestCase):

    def setUp(self):
        super(FollowTests, self).setUp()
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.ex = self.cursor.execute
        self.ex("create table if not exists object_state"
                " (zoid bigint primary key, tid bigint, state bytea)")

    def tearDown(self):
        self.conn.close()
        super(FollowTests, self).tearDown()

    def store(self, tid, oid):
        self.ex("insert into object_state values(%s, %s, 'some data')"
                " on conflict (zoid)"
                " do update set tid=excluded.tid, state=excluded.state",
                (oid, tid))

    def test_non_empty_generator(self):
        from ..follow import non_empty_generator
        self.assertEqual(non_empty_generator(iter(())), None)
        self.assertEqual(list(non_empty_generator(iter((1, 2, 3)))), [1, 2, 3])

    def test_update_iterator_batching(self):
        t = 0
        for i in range(99):
            if i%7 == 0:
                t += 1
            self.store(t, i)
        from ..follow import updates
        conn = psycopg2.connect(self.conn.dsn)
        self.assertEqual(
            [[int(r[1]) for r in b]
             for b in updates(conn, end_tid=99, batch_limit=20)
             ],
            [list(range(0, 21)),
             list(range(21, 42)),
             list(range(42, 63)),
             list(range(63, 84)),
             list(range(84, 99)),
             ])

        self.assertEqual(
            [[int(r[1]) for r in b]
             for b in updates(conn, start_tid=2, end_tid=4)
             ],
            [list(range(14, 28))],
            )

    def test_update_iterator_follow(self):
        conn = psycopg2.connect(self.conn.dsn)
        self.store(1, 1)
        self.store(1, 2)
        from ..follow import updates
        it = iter(updates(conn))
        self.assertEqual([(1, 1), (1, 2)],
                         [(int(r[0]), int(r[1])) for r in next(it)])
        self.store(2, 3)
        self.store(2, 4)
        self.assertEqual([(2, 3), (2, 4)],
                         [(int(r[0]), int(r[1])) for r in next(it)])
        self.store(3, 5)
        self.store(3, 6)
        self.store(4, 7)
        self.store(4, 8)
        self.assertEqual([(3, 5), (3, 6), (4, 7), (4, 8)],
                         [(int(r[0]), int(r[1])) for r in next(it)])
