=====================
Text Configuration
=====================

Newt DB provides a Python API for creating database connections.  You
can also use `ZODB's text configuration API
<http://www.zodb.org/en/latest/reference/zodb.html#module-ZODB.config>`_.
Text configuration usually provides the easiest way to configure a
database, especially if you need to provide non-default options.
Configuration strings can be included in configuration files by
themselves or as parts of larger configurations.

Here's an example text configuration for Newt DB::

  %import newt.db

  <newtdb foo>
    <zodb>
      <relstorage>
        keep-history false
        <newt>
          <postgresql>
            dsn dbname=''
          </postgresql>
        <newt>
      </relstorage>
    </zodb>
  </newtdb>

The syntax used is based on the syntax used by web servers such as
Apache and NGINX.  Elements in angle brackets identify configuration
objects with name-value pairs inside elements to specify options.
Optional indentation indicates containment relationships and element
start and end tags must appear on their own lines.

Newt DB provides two configuration elements: ``newtdb`` and ``newt``.
These elements augment existing elements to provide extra behavior.

newt
   Wraps a RelStorage ``postgresql`` element to provide a Newt
   Postgres database adapter that stores JSON data in addition to
   normal database data.

newtdb
   Wraps a ``zodb`` element to provide a Newt database rather than a
   normal ZODB database.  The Newt database provides extra APIs for
   searching and transaction management.

Some things to note:

- An ``%import`` directive is used to load the configuration schema for
  Newt DB.

- A ``keep-history`` option is used to request a history-free
  storage. History-free storages only keep current data and discard
  transaction meta data. History-preserving storages keep past data
  records until they are packed away and allow "time-travel" to view
  data in the past.  History-preserving storages are much slower and
  require more maintenance.  Newt DB works with either
  history-preserving or history-free storages, but history-free
  storages are recommended and are the default for the Python API.

The RelStorage documentation provides information on the options for
the `relstorage element
<http://relstorage.readthedocs.io/en/latest/relstorage-options.html>`_
and for the `postgresql element
<http://relstorage.readthedocs.io/en/latest/db-specific-options.html#postgresql-adapter-options>`_.

The ZODB documentation provides information on the options for the
`zodb element <http://www.zodb.org/en/latest/reference/zodb.html#database-text-configuration>`_.
