Changes
=======

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
