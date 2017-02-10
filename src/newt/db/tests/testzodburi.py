import mock
import pkg_resources
import unittest

class ZODBURITests(unittest.TestCase):

    def parse(self, uri):
        resolve_uri = pkg_resources.load_entry_point(
            'newt.db', 'zodburi.resolvers', 'newt')
        factory, dbkw = resolve_uri(uri)
        with mock.patch("newt.db.storage") as storage:
            factory()
            (dsn,), options = storage.call_args
            return dsn, options, dbkw

    def test_minimal(self):
        self.assertEqual(('postgresql://', {}, {}), self.parse("newt://"))

    def test_full(self):
        self.assertEqual(
            ('postgresql://jim:123@foo.bar/mydb?params1=value1&param2=value3',
             dict(keep_history=True, driver='psycopg2'),
             dict(connection_pool_size = 1,
                  connection_cache_size = 3,
                  database_name='main',
                  ),
             ),
            self.parse("newt://jim:123@foo.bar/mydb"
                       "?params1=value1"
                       "&keep_history=True"
                       "&driver=psycopg2"
                       "&connection_pool_size=1"
                       "&connection_cache_size=3"
                       "&database_name=main"
                       "&param2=value3"
                       ),
            )

        def test_bool_variations(self):
            self.assertEqual(('postgresql://', dict(keep_history=True), {}),
                              self.parse("newt://?keep_history=True"))
            self.assertEqual(('postgresql://', dict(keep_history=True), {}),
                              self.parse("newt://?keep_history=TRUE"))
            self.assertEqual(('postgresql://', dict(keep_history=True), {}),
                              self.parse("newt://?keep_history=trUe"))
            self.assertEqual(('postgresql://', dict(keep_history=True), {}),
                              self.parse("newt://?keep_history=YeS"))
            self.assertEqual(('postgresql://', dict(keep_history=True), {}),
                              self.parse("newt://?keep_history=1"))

            self.assertEqual(('postgresql://', dict(keep_history=False), {}),
                              self.parse("newt://?keep_history=False"))
            self.assertEqual(('postgresql://', dict(keep_history=False), {}),
                              self.parse("newt://?keep_history=FALSE"))
            self.assertEqual(('postgresql://', dict(keep_history=False), {}),
                              self.parse("newt://?keep_history=fAlse"))
            self.assertEqual(('postgresql://', dict(keep_history=False), {}),
                              self.parse("newt://?keep_history=nO"))
            self.assertEqual(('postgresql://', dict(keep_history=False), {}),
                              self.parse("newt://?keep_history=0"))