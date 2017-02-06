============================
Asynchronous JSON conversion
============================

Normally, newt converts data to JSON as it's saved in the database.
In turn, any indexes defined on the JSON data are updated at the same
time.  With the default JSON index, informal tests show Newt DB write
performance to be around 10 percent slower than RelStorage. Adding a
text index brought the performance down to the point where writes took
twice as long, but were still fairly fast, several hundred per second
on a laptop.

If you have a lot of indexes to update or write performance is
critical, you may want to leverage Newt DB's ability to update the
JSON data asynchronously.  Doing so, allows the primary transactions
to execute more quickly.

Updating indexes asynchronously will usually be more efficient,
because Newt DB's asynchronous updater batches updates. When indexes
are updated for many objects in the same transaction, less data has to
be written per transaction.

If you want to try New DB, with an existing RelStorage/PostgreSQL
database, you can use the updater to populate Newt DB without changing
your application and introduce the use of Newt DB's search API
gradually.

There are some caveats however:

- Because updates are asynchronous, search results may not always
  reflect the current data.

- Packing requires some special care, as will be discussed below.

- You'll need to run a separate daemon, ``newt-updater`` in addition
  to your database server.

.. contents::

Using Newt's Asynchronous Updater
=================================

To use Newt's asynchronous updater:

- Omit ``newt`` tag from your database configuration, as in::

    %import newt.db

    <newtdb foo>
      <zodb>
        <relstorage>
          keep-history false
          <postgresql>
            dsn postgresql://localhost/mydb
          </postgresql>
        </relstorage>
      </zodb>
    </newtdb>

- Run the ``newt-updater`` script::

    newt-updater postgresql://localhost/mydb

  You'll want to run this using a daemonizer like `supervisord
  <http://supervisord.org/>`_ or `ZDaemon
  <https://pypi.python.org/pypi/zdaemon>`_.

``newt-updater`` has a number of options:

-l, --logging-configuration
  Logging configuration.

  This can be a log level, like ``INFO`` (the default), or the path to
  a `ZConfig logging configuration file
  <https://pypi.python.org/pypi/ZConfig>`_.

-g, --gc-only
  Collect garbage and exit.

  This removes Newt DB records that don't have corresponding database records.
  This is done by executing::

    delete from newt n where not exists (
      select from object_state s where n.zoid = s.zoid)

  Note that garbage collection is normally performed on startup unless
  the -G option is used.

-G, --no-gc
  Don't perform garbage collection on startup.

--nagios
  Check the status of the updater.

  The status is checked by checking the updater lag, which is the
  difference between the last transaction committed to the database, and
  the last transaction processed by the updater.  The option takes 2
  numbers, separated by commas.  The first number is the lag, in
  seconds, for the updater to be considered to be OK.  The second number
  is the maximum lag for which the updater isn't considered to be in
  error. For example, 1,99 indicates OK if 1 or less, WARNING if more
  than 1 and less than or equal to 99 and ERROR of more than 99 seconds.

-t, --poll-timeout
  Specify a poll timeout, in seconds.

  Normally, the updater is notified to poll for changes.  If it
  doesn't get notified in poll-timeout seconds, it will poll anyway.
  This is a backstop to PostgreSQL's notification. The default timeout
  is 300 seconds.

-T, --remove-delete-trigger
  Remove the Newt DB delete trigger, if it exists.

  The Newt DB delete trigger is incompatible with the updater.  It can cause
  deadlock errors is packed while the updater is running.  This option
  is needed if you set up Newt DB normally, and then decided that you
  wanted update Newt DB asynchronously.

-d, --driver
    Provide an explicit Postgres driver name (psycopg2 or
    psycopg2cffi).  By default, the appropriate driver will be
    selected automatically.

Garbage collection
==================

See the topic on :doc:`packing`.

The asynchronous updater tracks new database inserts and updates.
When a database is packed, records are removed without generating
updates.  Those deletes won't be reflected in the Newt DB.  You can
tell the updater to clean up Newt DB records for which there are
no-longer database records by either restarting it, or running it with
the ``-g`` option::

  newt-updater -g postgresql://localhost/mydb

This tells the updater to just collect garbage.  You'll probably want
to run this right after running `zodbpack
<http://relstorage.readthedocs.io/en/latest/zodbpack.html>`_.

Monitoring
==========

When running an external updater, like ``newt-updater``, you'll want
to have some way to monitor that it's working correctly.  The
``--nagios`` option ``newt-updater`` script can be used to provide a
`Nagios Plugin
<https://assets.nagios.com/downloads/nagioscore/docs/nagioscore/3/en/pluginapi.html>`_::

  newt-updater postgresql://localhost/mydb --nagios 3,99

The argument to the ``--nagios`` option is a pair of numbers giving
limits for OK and warning alerts.  They're based on how far behind the
updater is.  For example, with the example above, the monitor
considers the updater to be OK if it is 3 seconds behind or less, in
error if it is more than 99 seconds behind and of concern otherwise.

Any monitoring system compatible with the Nagios plugin API can be
used.

The monitor output includes the lag, how far behind the updater is, in
seconds as a performance metric.
