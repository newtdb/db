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

        sql = """
        select * from newt
        where state->>'i' >= %s and state->>'i' <= %s
        order by zoid
        """
        obs = self.conn.search(sql, '2', '5')
        self.assertEqual([2, 3, 4, 5], [o.i for o in obs])

        # test stand-alone API:
        from .. import search
        obs = search.search(self.conn, sql, '2', '5')
        self.assertEqual([2, 3, 4, 5], [o.i for o in obs])

        # separate conn (to make sure we get new ghosts, and
        # ``where``` api and keyword args

        conn2 = self.db.open()
        obs2 = self.conn.where("state->>'i' >= %(a)s and state->>'i' <= %(b)s",
                               a='2', b='5')
        self.assertEqual([2, 3, 4, 5], sorted(o.i for o in obs2))

        self.assertEqual(set(o._p_oid for o in obs),  # yes, these are
                         set(o._p_oid for o in obs2)) #  persistent objects :)

        # test stand-alone API:
        obs2 = search.where(self.conn,
                            "state->>'i' >= %(a)s and state->>'i' <= %(b)s",
                            a='2', b='5')
        self.assertEqual([2, 3, 4, 5], sorted(o.i for o in obs2))


    def test_search_batch(self):
        for i in range(99):
            tid = self.store(i, i=i)

        conn2 = self.db.open()

        sql = """
        select * from newt
        where (state->>'i')::int >= %(a)s and (state->>'i')::int <= %(b)s
        order by zoid
        """
        total, batch = conn2.search_batch(sql, dict(a=2, b=90), 10, 20)
        self.assertEqual(total, 89)

        self.assertEqual(list(range(12, 32)), [o.i for o in batch])

        # We didn't end up with all of the objects getting loaded:
        self.assertEqual(len(conn2._cache), 20)

        # test stand-alone API:
        from .. import search
        totalbatch = search.search_batch(
            conn2, sql, dict(a=2, b=90), 10, 20)
        self.assertEqual((total, batch), totalbatch)

    def test_create_text_index_sql(self):
        from .. import search
        self.assertEqual(
            expect_simple_text,
            self.conn.create_text_index_sql('mytext', 'text'),
            )
        self.assertEqual(
            expect_simple_text,
            search.create_text_index_sql('mytext', 'text'),
            )

        self.assertEqual(
            expect_text,
            self.conn.create_text_index_sql('mytext', ['text', 'title']),
            )
        self.assertEqual(
            expect_text,
            search.create_text_index_sql('mytext', ['text', 'title']),
            )

        self.assertEqual(
            expect_weighted_text,
            self.conn.create_text_index_sql(
                'mytext', 'text', ['title', 'description']),
            )
        self.assertEqual(
            expect_weighted_text,
            search.create_text_index_sql(
                'mytext', 'text', ['title', 'description']),
            )

        self.assertEqual(
            expect_more_weighted_text,
            self.conn.create_text_index_sql(
                'mytext',
                'text',
                ['title', 'description'],
                'keywords',
                "state ->> 'really important'"),
            )
        self.assertEqual(
            expect_more_weighted_text,
            search.create_text_index_sql(
                'mytext',
                'text',
                ['title', 'description'],
                'keywords',
                "state ->> 'really important'"),
            )

        self.assertEqual(
            expect_A_text,
            self.conn.create_text_index_sql('mytext', A='text'),
            )
        self.assertEqual(
            expect_A_text,
            search.create_text_index_sql('mytext', A='text'),
            )

        self.assertRaises(TypeError, self.conn.create_text_index_sql, 'mytext')
        self.assertRaises(TypeError, search.create_text_index_sql, 'mytext')

    def test_create_text_index(self):
        self.conn.create_text_index('txt', 'text')
        self.store('a', text='foo bar')
        self.store('b', text='foo baz')
        self.store('c', text='green eggs and spam')
        self.assertEqual(
            set((self.conn.root.a, self.conn.root.b)),
            set(self.conn.where("txt(state) @@ 'foo'")),
            )
        self.assertEqual(
            set((self.conn.root.a, )),
            set(self.conn.where("txt(state) @@ 'foo & bar'")),
            )
        self.assertEqual(
            set((self.conn.root.a, self.conn.root.c)),
            set(self.conn.where("txt(state) @@ 'bar | green'")),
            )

    def test_create_text_index_standalone(self):
        from .. import search
        search.create_text_index(self.conn, 'txt', 'text')
        self.store('a', text='foo bar')
        self.store('b', text='foo baz')
        self.store('c', text='green eggs and spam')
        self.assertEqual(
            set((self.conn.root.a, self.conn.root.b)),
            set(self.conn.where("txt(state) @@ 'foo'")),
            )
        self.assertEqual(
            set((self.conn.root.a, )),
            set(self.conn.where("txt(state) @@ 'foo & bar'")),
            )
        self.assertEqual(
            set((self.conn.root.a, self.conn.root.c)),
            set(self.conn.where("txt(state) @@ 'bar | green'")),
            )

    def test_query_data(self):
        from .. import search
        self.store('a', text='foo bar')
        self.store('b', text='foo baz')
        self.store('c', text='green eggs and spam')

        self.assertEqual(
            [[1]],
            [list(map(int, r)) for r in
             self.conn.query_data(
                 """select zoid from newt
                 where state @> '{"text": "foo bar"}'""")
             ])
        self.assertEqual(
            [[1]],
            [list(map(int, r)) for r in
             search.query_data(
                 self.conn,
                 """select zoid from newt
                 where state @> '{"text": "foo bar"}'""")
             ])

expect_simple_text = """\
create or replace function mytext(state jsonb) returns tsvector as $$
declare
  text text;
  result tsvector;
begin
  if state is null then return null; end if;

  text = coalesce(state ->> 'text', '');
  result := to_tsvector(text);

  return result;
end
$$ language plpgsql immutable;

create index newt_mytext_idx on newt using gin (mytext(state));
"""

expect_text = """\
create or replace function mytext(state jsonb) returns tsvector as $$
declare
  text text;
  result tsvector;
begin
  if state is null then return null; end if;

  text = coalesce(state ->> 'text', '');
  text = text || coalesce(state ->> 'title', '');
  result := to_tsvector(text);

  return result;
end
$$ language plpgsql immutable;

create index newt_mytext_idx on newt using gin (mytext(state));
"""

expect_weighted_text = """\
create or replace function mytext(state jsonb) returns tsvector as $$
declare
  text text;
  result tsvector;
begin
  if state is null then return null; end if;

  text = coalesce(state ->> 'text', '');
  result := to_tsvector(text);

  text = coalesce(state ->> 'title', '');
  text = text || coalesce(state ->> 'description', '');
  result := result || setweight(to_tsvector(text), 'C');

  return result;
end
$$ language plpgsql immutable;

create index newt_mytext_idx on newt using gin (mytext(state));
"""

expect_more_weighted_text = """\
create or replace function mytext(state jsonb) returns tsvector as $$
declare
  text text;
  result tsvector;
begin
  if state is null then return null; end if;

  text = coalesce(state ->> 'text', '');
  result := to_tsvector(text);

  text = coalesce(state ->> 'title', '');
  text = text || coalesce(state ->> 'description', '');
  result := result || setweight(to_tsvector(text), 'C');

  text = coalesce(state ->> 'keywords', '');
  result := result || setweight(to_tsvector(text), 'B');

  text = coalesce(state ->> 'really important', '');
  result := result || setweight(to_tsvector(text), 'A');

  return result;
end
$$ language plpgsql immutable;

create index newt_mytext_idx on newt using gin (mytext(state));
"""

expect_A_text = """\
create or replace function mytext(state jsonb) returns tsvector as $$
declare
  text text;
  result tsvector;
begin
  if state is null then return null; end if;

  text = coalesce(state ->> 'text', '');
  result := setweight(to_tsvector(text), 'A');

  return result;
end
$$ language plpgsql immutable;

create index newt_mytext_idx on newt using gin (mytext(state));
"""
