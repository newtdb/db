from relstorage.adapters.postgresql import adapter
import gc
import sys
PYPY = hasattr(sys, 'pypy_version_info')

class DBSetup(object):

    maxDiff = None

    @property
    def dsn(self):
        return 'postgresql://localhost/' + self.dbname

    def setUp(self, call_super=True):
        self.dbname = self.__class__.__name__.lower() + '_newt_test_database'
        self.base_conn = adapter.select_driver().connect('')
        self.base_conn.autocommit = True
        self.base_cursor = self.base_conn.cursor()
        self.drop_db()
        self.base_cursor.execute('create database ' + self.dbname)
        self.call_super = call_super
        if call_super:
            super(DBSetup, self).setUp()

    def drop_db(self):
        self.base_cursor.execute('drop database if exists ' + self.dbname)

    def tearDown(self):
        if self.call_super:
            super(DBSetup, self).tearDown()

        if PYPY:
            # Make sure there aren't any leaked connections around
            # that would keep us from dropping the DB
            # (https://travis-ci.org/newtdb/db/jobs/195267673)
            # This needs a fix from RelStorage post 2.0.0.
            gc.collect()
            gc.collect()
        self.drop_db()
        self.base_cursor.close()
        self.base_conn.close()
