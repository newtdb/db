===================
zodburi URI support
===================

A number of applications, most notably `Pyramid
<http://docs.pylonsproject.org/projects/pyramid/en/latest/>`_, use URI
syntax to define ZODB databases they use.  Newt DB supports this
syntax through the ``newt`` scheme.  For example::

  newt://mydbserver/my_database?keep_history=true&connection_cache_size=100000

.. -> uri

   >>> import newt.db.tests.testzodburi
   >>> newt.db.tests.testzodburi.parse(uri.strip())
   ... # doctest: +NORMALIZE_WHITESPACE
   ('postgresql://mydbserver/my_database',
   {'keep_history': True}, {'connection_cache_size': 100000})

Newt URIs have roughly the same form as :doc:`PostgreSQL URI
connection strings <connection-strings>` except that they use the
``newt`` URI scheme instead of the ``postgresql`` schema and they
support extra query-string parameters:

keep_history
  Boolean (true, false, yes, no, 1 or 0) indicating whether
  non-current database records should be kept.  This is false, by
  default.

driver
  The Postgres driver name (psycopg2 or psycopg2cffi). By default, the
  driver is determined automatically.

connection_cache_size
  The target maximum number of objects to keep in the per-connection
  object cache.

connection_pool_size
  The target maximum number of ZODB connections to keep in the
  connection pool.

Limitations
===========

There are a number of limitations to be aware of when using the URI
syntax.

- Because the `zodburi
  <http://docs.pylonsproject.org/projects/zodburi>`_ framework can only
  be used to set up ordinary ZODB databases, resulting connection
  objects won't have the extra search or transaction convenience
  functions provided by Newt DB.  When searching, you'll use the
  :ref:`Newt search module <search-module-label>` functions, passing
  your database connections as the first arguments.

- `zodburi <http://docs.pylonsproject.org/projects/zodburi>`_ provides
  insufficient control over database configuration. You'll end up
  having to use something else for production deployments.
