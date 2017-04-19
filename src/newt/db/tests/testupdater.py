import relstorage.adapters.postgresql
import sys
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

    def fetch(self, query):
        self.ex(query)
        return list(self.cursor)

    def store_obs(self, tid, *obs):
        writer = ZODB.serialize.ObjectWriter()
        values = ', '.join(
            self.mogrify("(%s, %s, %s)",
                         (oid, tid, self.Binary(writer.serialize(ob)))
                         ).decode('ascii')
            for (oid, ob) in obs
            )
        self.ex("begin")
        self.ex("delete from object_state where zoid = any(%s)",
                ([oid for oid, ob in obs], ))
        self.ex("insert into object_state(zoid, tid, state) values %s"
                % values)
        self.ex("commit")

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


    def test_detect_delete_trigger(self):
        # Cause the delete trigger to be deleted by opening a newt.db
        # connection:
        import newt.db
        self.ex("drop table object_state")
        newt.db.storage(self.dsn).close()

        # Verify the trigger is there:
        from newt.db._util import trigger_exists
        from newt.db._adapter import DELETE_TRIGGER
        self.assertTrue(trigger_exists(self.cursor, DELETE_TRIGGER))

        from zope.testing.loggingsupport import InstalledHandler
        handler = InstalledHandler('newt.db.updater')

        self.assertEqual(1, updater.main([self.dsn]))
        self.assertEqual(
            "newt.db.updater ERROR\n"
            "  The Newt DB delete trigger exists.\n"
            "It is incompatible with the updater.\n"
            "Use -T to remove it.",
            str(handler))
        handler.clear()

        # Now run the updater with the -T option, which causes the
        # trigger to be deleted.  (We also use -g, which causes it to
        # to GC and nothing else, which makes it stop, so we don't
        # have to run in a thread.)
        self.assertEqual(0, updater.main([self.dsn, '-Tg']))
        self.assertEqual("", str(handler))

        # And the trigger is gone.
        self.assertFalse(trigger_exists(self.cursor, DELETE_TRIGGER))

    def test_gc_on_startup(self):
        # Normally, on startup, the updater does GC.

        # Create garbage
        from .._adapter import _newt_ddl
        self.ex(_newt_ddl)
        self.ex("Insert into newt values(99, 'foo', 'boo', '42')")

        # Start the updater and wait for it to do some things:
        self.start_updater()
        self.store_obs(2, (1, Object(a=1, b=1)), (2, Object(a=2, b=2)))
        self.wait_tid(2)

        # The garbage is gone:
        self.assertEqual([], self.fetch("select from newt where zoid = 99"))

    def test_gc_only_no_startup(self):
        # Normally, on startup, the updater does GC.

        # Create garbage
        from .._adapter import _newt_ddl
        self.ex(_newt_ddl)
        self.ex("Insert into newt values(99, 'foo', 'boo', '42')")

        # Start the updater and wait for it to do some things:
        self.assertEqual(0, updater.main([self.dsn, '-g']))

        # The garbage is gone:
        self.assertEqual([], self.fetch("select from newt where zoid = 99"))

    def test_no_gc_on_startup(self):
        # Normally, on startup, the updater does GC.

        # Create garbage
        from .._adapter import _newt_ddl
        self.ex(_newt_ddl)
        self.ex("Insert into newt values(99, 'foo', 'boo', '42')")

        # Start the updater and wait for it to do some things:
        self.start_updater("-G")
        self.store_obs(2, (1, Object(a=1, b=1)), (2, Object(a=2, b=2)))
        self.wait_tid(2)

        # The garbage is gone:
        self.assertEqual([()], self.fetch("select from newt where zoid = 99"))

    def test_nagios(self):
        # Setup newt:
        from .._adapter import _newt_ddl
        self.ex(_newt_ddl)

        # Lose follow_progress:
        self.ex("drop table newt_follow_progress")

        # Main and newt dbs are empty:
        self.assertEqual([], self.fetch("select from object_state"))
        self.assertEqual([], self.fetch("select from newt"))

        # Because the progress table doesn't exist yet, we get an error.
        import mock
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(2, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual('Updater has not run\n', ''.join(writes))

        # Create follower table:
        self.assertEqual(self.last_tid(), -1)

        # Main and newt dbs are empty:
        self.assertEqual([], self.fetch("select from object_state"))
        self.assertEqual([], self.fetch("select from newt"))

        # So we're OK.
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(0, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual('No transactions\n', ''.join(writes))

        from ZODB.TimeStamp import TimeStamp
        from ZODB.utils import u64
        def make_tid(*args):
            return u64(TimeStamp(*args).raw())

        # If somehow the updater has progress, but there's no data:
        tid = make_tid(2017, 1, 2, 3, 4, 5.6)
        follow.set_progress_tid(self.conn, updater.__name__, tid)
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(2, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual('Updater saw data but there was None\n',
                             ''.join(writes))

        # Conversely, if the updater hasn't committed anything, but there's data
        follow.set_progress_tid(self.conn, updater.__name__, -1)
        self.store_obs(tid, (1, Object(a=1)), (2, Object(a=2)))
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(2, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual("Updater hasn't done anything\n",
                             ''.join(writes))

        # It's weird if the updater is ahead:
        follow.set_progress_tid(self.conn, updater.__name__,
                                make_tid(2017, 1, 2, 3, 4, 5.7))
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(2, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual("Updater is ahead\n",
                             ''.join(writes))

        # It's cool if the update has caught up:
        follow.set_progress_tid(self.conn, updater.__name__,
                                make_tid(2017, 1, 2, 3, 4, 5.6))
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(0, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual("OK | 0.000\n", ''.join(writes))
            # Oh look! Metric!

        # Or a tad behind:
        follow.set_progress_tid(self.conn, updater.__name__,
                                make_tid(2017, 1, 2, 3, 4, 5.5))
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(0, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual("OK | 0.100\n", ''.join(writes))

        # Not so cool
        follow.set_progress_tid(self.conn, updater.__name__,
                                make_tid(2017, 1, 2, 3, 4, 4.5))
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(1, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual("Updater is behind | 1.100\n", ''.join(writes))

        # Badness!
        follow.set_progress_tid(self.conn, updater.__name__,
                                make_tid(2017, 1, 2, 3, 2, 4.5))
        writes = []
        with mock.patch('sys.stdout') as stdout:
            stdout.write.side_effect=writes.append
            self.assertEqual(2, updater.main([self.dsn, '--nagios', '1,99']))
            self.assertEqual("Updater is too far behind | 121.100\n",
                             ''.join(writes))


def xform(class_name, state):
    if class_name == 'persistent.mapping.PersistentMapping':
        import json
        return json.dumps(json.loads(state)['data'])

class TransformTests(base.TestCase):

    def test_updater_transform(self):
        # Initialize a database:
        import newt.db
        conn = newt.db.connection(self.dsn)
        conn.root.x = 1
        conn.commit()

        pg = newt.db.pg_connection(self.dsn)
        cursor = pg.cursor()
        cursor.execute("truncate newt"); pg.commit()

        from .. import updater
        updater.main(['--compute-missing', '-x', __name__ + '.xform', self.dsn])
        pg.rollback()
        cursor.execute('select state from newt')
        [[state]] = cursor
        self.assertEqual(dict(x=1), state)

class ComputeMissingTests(base.TestCase):

    def test_compute_missing_wo_updater(self):
        # When redoing, if we aren't running the updater, we shouldn't
        # create the follow table and should use the max(tid) from
        # object state as the end tid.
        import ZODB.config
        from .. import follow, Object, pg_connection, _util, updater

        db = ZODB.config.databaseFromString("""\
        %%import relstorage
        <zodb>
          <relstorage>
            keep-history false
            <postgresql>
              dsn %s
            </postgresql>
          </relstorage>
        </zodb>
        """ % self.dsn)
        with db.transaction() as conn:
            conn.root.x = Object(a=1)
        db.close()

        conn = pg_connection(self.dsn)
        cursor = conn.cursor()
        self.assertFalse(_util.table_exists(cursor, 'newt'))
        self.assertFalse(_util.table_exists(cursor, follow.PROGRESS_TABLE))

        # If we try to run redo now, we'll get an error, because the
        # net table doesn't exist:
        with self.assertRaises(AssertionError):
            updater.main(['--compute-missing', self.dsn])

        # create newt table
        from .. import connection
        connection(self.dsn).close()

        # The table is empty:
        cursor.execute("select count(*) from newt")
        [[c]] = cursor
        self.assertEqual(0, c)

        # Now run the redo:
        updater.main(['--compute-missing', self.dsn])

        # We have rows:
        cursor.execute("select count(*) from newt")
        [[c]] = cursor
        self.assertEqual(2, c)

        # The progres table still doesn't exist
        self.assertFalse(_util.table_exists(cursor, follow.PROGRESS_TABLE))

        cursor.close()
        conn.close()

    def test_catch_up_consistency(self, back=False):
        import newt.db, BTrees.OOBTree
        conn = newt.db.connection(self.dsn)
        conn.root.b = BTrees.OOBTree.BTree()
        N = 300
        for i in range(*((N-1, -1, -1) if back else (N,))):
            conn.root.b[i] = newt.db.Object(i=i, x=0)
            conn.commit()
        pg = newt.db.pg_connection(self.dsn)
        cursor = pg.cursor()
        cursor.execute("truncate newt")
        pg.commit()

        import threading
        ev = threading.Event()

        def update():
            ev.set()
            for o in conn.root.b.values():
                o.x = 1; conn.commit()

        from .. import updater

        t = threading.Thread(target=update)
        t.setDaemon(True)
        t.start()
        ev.wait(9)
        updater.main(['--compute-missing', self.dsn])
        t.join(N/10)
        self.assertEqual(N, len([o for o in conn.root.b.values() if o.x == 1]))
        pg.rollback()
        cursor.execute(
            """select count(*) from newt where state @> '{"x": 1}'""")
        [[n]] = cursor
        self.assertEqual(N, n)

    def test_catch_up_consistency_back(self):
        self.test_catch_up_consistency(True)
