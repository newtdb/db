======================================================
Follow changes in ZODB RelStorage PostgreSQL databases
======================================================

The :py:mod:`newt.db.follow` module provides an API for subscribing to
database changes .

It's used by newt.db for asynchronous updates, but it can be used for
other applications, such as:

- creating data indexes, such as Elasticsearch indexes.

- creating alternate representations, such as relational models to
  support other applications.

- monitoring or analytics of data changes.

.. setup

  >>> import newt.db
  >>> c = newt.db.connection(dsn)
  >>> c.root.x = 1
  >>> c.commit()
  >>> c.close()

You can get an iterator of changes by calling the
:py:func:`~newt.db.follow.updates` function::

  >>> import newt.db
  >>> import newt.db.follow
  >>> import pickle
  >>> connection = newt.db.pg_connection(dsn)
  >>> for batch in newt.db.follow.updates(connection):
  ...     for tid, zoid, data in batch:
  ...         print_(zoid, pickle.loads(data).__name__)
  0 PersistentMapping

.. cleanup

   >>> connection.close()

The updates iterator returns batches to facilitate batch processing of
data while processing data as soon as possible.  Batches are as large
as possible, limited loosly by a target batch size with the constraint
that transactions aren't split between batches.  Batches are
themselves iterators.

The interator can run over a range of transactions, or can run
indefinately, returning new batches as new data are committed.

See the :py:func:`updates reference <newt.db.follow.updates>` for
detailed documentation.  One of the parameters is ``end_tid``, an end
transaction id for the iteration. If no ``end_tid`` parameter is
provided, the iterator will iterate forever, blocking when necessary
to wait for new data to be committed.

The data returned by the follower is a pickle, which probably isn't
very useful.  You can convert it to JSON using Newt's JSON conversion.
We can update the example above::

  >>> import newt.db
  >>> import newt.db.follow
  >>> import newt.db.jsonpickle

  >>> connection = newt.db.pg_connection(dsn)
  >>> jsonifier = newt.db.jsonpickle.Jsonifier()
  >>> for batch in newt.db.follow.updates(connection):
  ...     for tid, zoid, data in batch:
  ...         class_name, _, data = jsonifier((zoid, tid), data)
  ...         if data is not None:
  ...             print_(zoid, class_name, data)
  0 persistent.mapping.PersistentMapping {"data": {"x": 1}}

:py:class:`Jsonifiers <newt.db.jsonpickle.Jsonifier>` take a label
(used for logging errors) and data and return a class_name, a ghost
pickle, and object state as JSON data.  The ghost pickle is generally
only useful to Newt itself, so we ignore it here.  If the JSON data
returned is ``None``, we skip processing the data.  The return value may
be ``None`` if:

- the raw data was an empty string, in which case the database record
  deleted the object,

- the object class was one the JSON conversion skipped, or

- There was an error converting the data.

Tracking progress
=================

.. setup

  >>> newt.db.follow.get_progress_tid(connection, 'mypackage.mymodule')
  -1

Often the data returned by the ``updates`` iterator is used to update
some other data.  Often clients will be stopped and later restarted
and need to keep track of where they left off.  The
:py:func:`~newt.db.follow.set_progress_tid` method can be used to save
progress for a client::

  >>> newt.db.follow.set_progress_tid(connection, 'mypackage.mymodule', tid)

The first argument is a PostgreSQL database connection.  The second
argument is a client identifier, typically the dotted name of the
client module.  The third argument is the last transaction id that was
processed.

Later, you can use :py:func:`~newt.db.follow.get_progress_tid` to retrieve
the saved transaction id::

  >>> start_tid = newt.db.follow.get_progress_tid(
  ...   connection, 'mypackage.mymodule')

.. check

   >>> start_tid == tid
   True

You'd then pass the retrieved transaction identifier as the
``start_tid`` argument to :py:func:`~newt.db.follow.updates`.

.. tearDown

   >>> connection.close()
