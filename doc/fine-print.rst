====================================
Fine print -- things you should know
====================================

Up to this point, we've emphasized how Newt DB leverages ZODB and
Postgres to give you the best of both worlds.  We've given some
examples showing how easy working with an object-oriented database can
be, and how Postgres can allow powerful queries to be easily
expressed. Like anything, however, any database has some topics that
have to be mastered to get full advantage and avoid pitfalls.

.. contents::

Highly, but not completely transparent object persistence
=========================================================

Newt and ZODB try to make accessing and updating objects as simple and
natural as working with objects in memory.  This is done in two ways:

1. When an object is accessed or modified, data are loaded
   automatically and saved if a transaction is committed.

   The database keeps track of objects that have been marked as
   changed. If a transaction is committed, changed objects are saved
   to Postgres.  If a transaction is aborted, then changed objects'
   states are discarded and will be reloaded with current state when
   they're accessed next.

2. Object accesses and changes are detected my observing attribute
   access.  This works very well for accesses, but can miss updates. For
   example, consider this class::

     class Tasks(newt.db.Persistent):

        def __init__(self):
            self._data = set()

        def add(self, task):
            self._data.add(task)

   In this example, the ``add`` method updates the object by updating
   a subobject.  It doesn't set an attribute, and the change isn't
   detected automatically.  There are a number of ways we can fix
   this, for example by explicitly marking the object as changed::

        def add(self, task):
            self._data.add(task)
            self._p_changed = True

To learn more about `writing persistent objects
<http://www.zodb.org/en/latest/guide/writing-persistent-objects.html>`_,
see:

  http://www.zodb.org/en/latest/guide/writing-persistent-objects.html

Learn about indexing and querying Postgresql
============================================

By default, Newt creates a JSON index on your data.  Read about
support for querying and indexing JSON data here:

  https://www.postgresql.org/docs/current/static/datatype-json.html

Postgres can index expressions, not just column values. This can
provide a lot of power.  For example, Newt provides helper functions
for setting up full-text indexes.  These helpers generate text
extraction functions and then define indexes on them.  For example, if
we ask for SQL statements to index title fields::

  >>> import newt.db.search
  >>> print(newt.db.search.create_text_index_sql('title_text', 'title'))
  create or replace function title_text(state jsonb) returns tsvector as $$
  declare
    text text;
    result tsvector;
  begin
    if state is null then return null; end if;

    text = coalesce(state ->> 'title', '');
    result := to_tsvector(text);

    return result;
  end
  $$ language plpgsql immutable;

  create index newt_title_text_idx on newt using gin (title_text(state));


A `PL/pgSQL
<https://www.postgresql.org/docs/current/static/plpgsql.html>`_
function is generated that extracts the title from the JSON.  Then an
index is created using the function. To learn more about full-text
search in Postgres, see::

  https://www.postgresql.org/docs/current/static/textsearch.html

To search the index generated in the example above, you use the
function as well::

  select * from newt where title_text(state) @@ 'green'

In this query, the function, ``title_text(state)`` isn't evaluated
but is instead used to match the search term against the
index [#maybe-match]_.

Indexing expressions allows a lot of power, especially when working
with JSON data.

When designing queries for your application, you'll want to experiment
and learn how to use the Postgres `EXPLAIN
<https://www.postgresql.org/docs/current/static/using-explain.html>`_
command.

Postgres is not (really) object oriented
========================================

Using Newt DB, search and indexing use Postgres.  The data to be
indexed have to be in the object state. You can't call object methods
to get data to be indexed.  You can write `database functions
<https://www.postgresql.org/docs/current/static/xfunc.html>`_ to
extract data and these functions can branch based on object class.

Transactions
============

`Transactions <https://en.wikipedia.org/wiki/Database_transaction>`_
are a core feature of Newt, ZODB and Postgres.  Transactions are
extremely important for implementing reliable applications.  At a
high-level, transactions provide:

Atomicity
  Data modified by a transaction is saved in its entirety or not at
  all.  This makes error handling much easier.  If an error occurs in
  your application, the transaction is rolled back and no changes are
  saved. Without atomicity, if there was an error, you the programmer
  would be responsible for rolling back the changes, which is
  difficult and likely to produce inconsistent data.

Isolation
  Transactions provide isolation between concurrently running
  programs. You as a programmer don't need to worry about concurrency
  control yourself.

In the examples in :doc:`Getting started <getting-started>`, a simple
form of transaction interaction was used, which is appropriate for
interactive sessions.  For programs, there are a number of
transaction-execution forms that can be used.  See:

  http://www.zodb.org/en/latest/guide/transactions-and-threading.html

For more information.

.. [#maybe-match] In a more complex query, Postgres might evaluate the
   expression. It depends on what other indexes might be in play.
