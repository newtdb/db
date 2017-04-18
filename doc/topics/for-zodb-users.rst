==========================
Information for ZODB Users
==========================

Newt DB builds on RelStorage and Postgres, adding JSON conversion and
search support.

Newt provides some significant enhancements to ZODB applications:

- Access to data outside of Python.  Data are stored in a JSON
  representation that can be accessed by non-Python tools.

- Fast, powerful search, via Postgres SQL and indexes.

- Because indexes are maintained in Postgres rather than in the app,
  far fewer objects are stored in the database.

- Database writes may be much smaller, again because indexing data
  structures don't have to be updated at the database level, and the
  likelihood of conflict errors is reduced.


It's easy to migrate existing applications to Newt DB. The standard
RelStorage ``zodbconvert`` works with Newt DB.

The next version of Newt will provide a options for batch-computation
of JSON data, which will allow the conversion of existing Postgres
RelStorage databases in place.

Updating an existing PostgreSQL RelStorage ZODB application to use Newt DB
==========================================================================

There are two ways to add Newt DB to an existing PostgreSQL RelStorage
ZODB application.

a. Update your :doc:`database text configuration <text-configuration>`
   to include a ``newt`` tag and optionally a ``newtdb`` tag.  After
   all of your database clients have been updated (and restarted),
   then new database records will be written to the ``newt`` table.
   You'll need to run the :doc:`newt updater <updater>` with the
   ``--compute-missing`` option to write ``newt`` records for your
   older data:

   .. code-block:: console

      newt-updater --compute-missing CONNECTION_STRING

   **Note that this option requires PostgreSQL 9.5 or later.**

b. Use the :doc:`Newt DB updater <updater>` to maintain Newt data
   asynchronously.  This requires no change to your database setup, but
   requires managing a separate process.  Because updates are
   asynchronous, Newt JSON data may be slightly out of date at times.
