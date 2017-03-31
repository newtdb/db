Changes
=======

0.5.2 (unreleased)
==================

- Added another data-transformation option:

  reducer (advanced)
    Callable called during JSON conversion to control how internal
    (non-persistent) objects are converted to JSON.


0.5.1 (2017-03-30)
==================

- Fixed: When arguments are omitted, there were errors because
  psycopg2 was trying to do substitutions anyway and choking on ``%``
  characters in ``like`` queries.

- Fixed: Connection.search_batch didn't allow arguments to be omitted.


0.5.0 (2017-03-30)
==================

- The ``newt.db.search`` module has a new ``read_only_cursor``
  function for obtaining a `database cursor
  <http://initd.org/psycopg/docs/cursor.html>`_ for selecting data and
  for using the `cursor's mogrify method
  <http://initd.org/psycopg/docs/cursor.html#cursor.mogrify>`_.

- The helpers for setting up full-text search indexes now accept a
  config argument to specify the name of a PostgreSQL full-text search
  configuration.

- The batch search methods (``search_batch``, and ``where_batch``) now
  allow the arguments parameter to be omitted, which is useful when
  substitutions have been be applied with a `cursor mogrify method
  <http://initd.org/psycopg/docs/cursor.html#cursor.mogrify>`_ before
  calling them.

- The search methods (``search``, ``search_batch``, ``where`` and
  ``where_batch``) now accept binary queries.  This is allow
  substitutions to be applied with a `cursor mogrify method
  <http://initd.org/psycopg/docs/cursor.html#cursor.mogrify>`_ before
  calling them.


0.4.0 (2017-03-25)
==================

- Serialization of persistent object references and intra-record
  references (used only when there are cycles) was simplified and made
  easier to use.

  Note: The change to intra-object references is backward
  incompatible, however, intra-record cycles, and thus the use of
  intra-record references, are extremely rare and it isn't thought
  that this change will affect anyone.  If this causes problems,
  please `create an issue <https://github.com/newtdb/db/issues/new>`_.

  The change to persistent references was made in a backward-compatible
  way, but the backward compatibility support will be dropped in Newt
  DB version 1.

- Added a data-transformation option:

  transformer
    Callable to transform data records after they've been converted to
    JSON.

0.3.0 (2017-02-10)
==================

- Added an API for following database changes.

- Exposed JSON conversion as an API that can be used to for other
  applications than updating the newt table, like updating external
  indexes.

- Created a JSON conversion object to support conversion customization.

- Added `zodburi
  <http://docs.pylonsproject.org/projects/zodburi/en/latest/index.html>`_,
  making it easier to use Newt DB with applications like `Pyramid
  <http://docs.pylonsproject.org/projects/pyramid/en/latest/>`_ that
  use zodburi.

0.2.2 (2017-02-08)
==================

- Fixed a packaging bug.


0.2.1 (2017-02-06)
==================

- Fixed: Packing wasn't handled correctly. Objects removed during
  packing weren't removed from the ``newt`` table.

0.2.0 (2017-01-30)
==================

- Added PyPy support

- Fixed: ``datetime`` values with time zones weren't handled correctly.

0.1.2 (2017-01-26)
==================

- Fixed a number of documentation errors.

  (Documentation wasn't tested, but now is, thankd to `manuel
  <http://pythonhosted.org/manuel/>`_.)

- Added some missing names to ``newt.db``.

0.1.1 (2017-01-24)
==================

Fixed a small Python 2 bug that prevented import.

0.1.0 (2017-01-24)
==================

Initial release
