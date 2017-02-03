import relstorage.adapters.postgresql
import threading
import ZODB.serialize
from zope.testing.wait import wait

from .. import follow
from .. import Object
from .. import updater

from . import base

class UpdaterTests(base.TestCase):

    def setUp(self):
        super(UpdaterTests, self).setUp()
        driver = relstorage.adapters.postgresql.select_driver()
        self.conn = driver.connect(self.dsn)
        self.Binary = driver.Binary
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.mogrify = self.cursor.mogrify
        self.ex = self.cursor.execute
        self.ex("create table if not exists object_state"
                " (zoid bigint primary key, tid bigint, state bytea)")

        # Avoid a race. The first time this is called, a table gets
        # created after trying and failing a select.  The updater
        # calls this on startup.  We also call this to monitor
        # progress.  We can end up with a race when this happens at
        # the same time. We could add a lock, in get_progress_tid, but
        # this only seems to be an issue in tests.
        self.assertEqual(self.last_tid(), -1)

    def tearDown(self):
        self.stop_updater()
        self.cursor.close()
        self.conn.close()
        super(UpdaterTests, self).tearDown()


    def store_obs(self, tid, *obs):
        writer = ZODB.serialize.ObjectWriter()
        values = ', '.join(
            self.mogrify("(%s, %s, %s)",
                         (oid, tid, self.Binary(writer.serialize(ob)))
                         ).decode('ascii')
            for (oid, ob) in obs
            )
        self.ex("insert into object_state(zoid, tid, state) values %s"
                " on conflict (zoid)"
                " do update set tid=excluded.tid, state=excluded.state"
                % values)

    thread = None
    def start_updater(self, *args):
        thread = threading.Thread(
            target=updater.main,
            args=([self.dsn, '-t1', '-m200'] + list(args),))
        thread.daemon = True
        thread.start()
        self.thread = thread

    def stop_updater(self):
        if self.thread is not None:
            for i in range(3):
                follow.stop_updates(self.conn)
                self.thread.join(.2)
                if not self.thread.isAlive():
                    self.thread = None
                    return

    def drop_trigger(self):
        self.ex("drop trigger newt_trigger_notify_object_state_changed"
                " on object_state")

    def last_tid(self, expect=None):
        tid = follow.get_progress_tid(self.conn, updater.__name__)
        if expect is not None:
            return expect == tid
        else:
            return tid

    def wait_tid(self, tid):
        wait((lambda : self.last_tid(tid)), 9, message="waiting for %s" % tid)

    def search(self, where):
        self.ex("select zoid from newt where %s" % where)
        return list(self.cursor.fetchall())

    def test_basic(self):
        self.store_obs(1, (1, Object(a=1)), (2, Object(a=2)))
        self.store_obs(2, (1, Object(a=1, b=1)), (2, Object(a=2, b=2)))

        self.start_updater()
        self.wait_tid(2)
        self.store_obs(3, (2, Object(a=3, b=3)))
        self.wait_tid(3)
        self.store_obs(4, (2, Object(a=4, b=4)))
        self.wait_tid(4)
        self.stop_updater()
        self.store_obs(5, (3, Object(n=3)))
        self.start_updater()
        self.wait_tid(5)

        # Make sure data were stored correctly:
        self.assertEqual(self.search("""state @> '{"b": 1}'::jsonb"""),
                         [(1,)])
        self.assertEqual(self.search("""state @> '{"b": 4}'::jsonb"""),
                         [(2,)])
        self.assertEqual(self.search("""state @> '{"n": 3}'::jsonb"""),
                         [(3,)])

    def test_skip_Uninteresting(self):
        import BTrees.OOBTree
        import ZODB.blob
        self.start_updater()
        self.store_obs(1, (1, BTrees.OOBTree.BTree()))
        self.store_obs(2, (2, ZODB.blob.Blob()))
        self.wait_tid(2)
        self.ex("select zoid from newt")
        self.assertEqual(list(self.cursor), [])
        self.store_obs(3, (1, BTrees.OOBTree.BTree()))
        self.store_obs(4, (2, ZODB.blob.Blob()))
        self.store_obs(5, (3, Object(n=1)))
        self.wait_tid(5)
        self.ex("select zoid from newt")
        self.assertEqual([int(*r) for r in self.cursor], [3])
