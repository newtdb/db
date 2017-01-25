======================================================
Follow changes in ZODB RelStorage PostgreSQL databases
======================================================

The ``newt.db.follow`` package provides an API for subscribing to
database changes .

It's used by newt.db for asynchronous updates, but it can be used for
other applications, such as:

- creating data indexes, such as Elasticsearch indexes.

- creating alternate representations, such as relational models to
  support other applications.

- monitoring or analytics of data changes.

Usage::

  >>> import newt.db.follow
  >>> import psycopg2
  >>> connection = psycopg2.connect('')
  >>> for batch in newt.db.follow.updates(connection):
  ...     for tid, zoid, data in batch:
  ...         print(tid, zoid, len(data))

The reason the updater returns batches to facilitate batch processing
of data while processing data as soon as possible.  Batches are as
large as possible, limited loosly by a target batch size with the
constraint that batches aren't split between batches.  Batches are
themselves iterators.

The interator can run over a range of transactions, or can run
indefinately, returning new batches as new data are committed.

See the :py:func:`updates reference <newt.db.follow.updates>` for
detailed documentation.
