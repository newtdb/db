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
