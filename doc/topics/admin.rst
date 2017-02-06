=======================
Database Administration
=======================

Because Newt stores it's data in PostgreSQL, you'll want to become
familiar with `PostgreSQL database administration
<https://www.postgresql.org/docs/current/static/admin.html>`_.  You
can forego much of this if you use a a `hosted Postgres server
<https://www.google.com/search?q=postgres+as+a+service>`_, especially
for production deployments.

If you decide to install PostgreSQL, consider one of the `binary
distributions <https://www.postgresql.org/download/>`_ which can make
installation and simple operation very easy.  Another option is to use
Docker images, which have self-contained PostgreSQL installations.
Simple binary installations are a good choice for development
environments.

Packing
=======

In addition to administration of the underlying Postgres database, Newt
DB databases need to be packed periodically.  Packing performs 2
functions:

- For history-preserving [#history-preserving]_ databases, packing
  removes non-current records that were written prior to the pack
  time.

- Packing detects and removes "garbage" object records.  Garbage
  objects are objects that are no-longer reachable from the database
  root object.  When you remove an object from a container, making it
  unreachable, it isn't deleted right away, but is removed the next
  time the database is garbage collected.

When you install Newt DB with pip [#nonpip]_::

  pip install newt.db

A `zodbpack script
<http://relstorage.readthedocs.io/en/latest/zodbpack.html>`_ is also
installed.  This is a command-line script used to pack a database,
typically through some sort of scheduled process such as a `cron
<https://en.wikipedia.org/wiki/Cron>`_ job.  The basic usage is::

  zodbpack -d 1 CONFIG_FILE

The ``-d`` option specified the number of days in the past to pack to.
The default is to pack 0 days in the past [#whynot0]_. ``CONFIG_FILE``
is that path to a :doc:`Newt configuration file <text-configuration>`.
For more information, see the `zodbpack documentation
<http://relstorage.readthedocs.io/en/latest/zodbpack.html>`_.


.. [#history-preserving] History-preserving databases allow time
   travel to times and undo of transactions written before the pack
   time. History-preserving databases are slower and require more
   frequent packing than history-free databases. This is why Newt DB
   makes history-free databases its default configuration.

.. [#nonpip] If you use another tool, you may have to make sure
   the scripts from the RelStorage package are installed. For
   example, `Buildout <http://www.buildout.org>`_ won't install
   RelStorage scripts unless RelStorage is explicitly listed.

.. [#whynot0] You may want to pack to a day in the past, rather than 0
   days to guard against an application bug in which an object is
   removed from a container in one transaction and added to another
   container in a separate transaction. In this case, the object is
   temporarily garbage and, if you're unlucky, it could be garbage
   collection while temporarily garbage.




