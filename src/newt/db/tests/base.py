import psycopg2

class DBSetup(object):

    maxDiff = None

    @property
    def dsn(self):
        return 'postgresql://localhost/' + self.dbname

    def setUp(self):
        self.dbname = self.__class__.__name__.lower() + '_newt_test_database'
        self.base_conn = psycopg2.connect('')
        self.base_conn.autocommit = True
        self.base_cursor = self.base_conn.cursor()
        self.drop_db()
        self.base_cursor.execute('create database ' + self.dbname)
        super(DBSetup, self).setUp()

    def drop_db(self):
        self.base_cursor.execute('drop database if exists ' + self.dbname)

    def tearDown(self):
        super(DBSetup, self).tearDown()
        self.drop_db()
        self.base_conn.close()
