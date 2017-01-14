====================
Newt DB Architecture
====================

Newt builds on `ZODB <www.zodb.org>`_ and `Postgresql
<https://www.postgresql.org/>`_.  Both of these are mature open-source
projects with years of production experience.

ZODB is an object-oriented database for Python.  It provides
transparent object persistence.  ZODB has a pluggable storage layer
and newt leverages `RelStorage
<http://relstorage.readthedocs.io/en/latest/>`_ to store data in
Postgres.

Newt adds conversion of data from the native serialization used by
ZODB to JSON, stored in a Postgres `JSONB
<https://www.postgresql.org/docs/current/static/datatype-json.html>`_
column.  The JSON data supplements the native data to support indexing
search and access from non-Python application.  Because the JSON
format is lossy, compared to the native format, the native format is
still used for loading objects from the database. For this reason, the
JSON data are read-only.

Newt adds a search API for searching the Postgres JSON data and
returning persistent objects.  It also provides a convenience API for
raw data searches.

Finally, newt adds additional convenience APIs to more directy support
it's intended audience.  These are intended to augment but not hide
ZODB and RelStorage.  Some of these are just aliases.  It will be
possible to integrate new with existing ZODB applications.


