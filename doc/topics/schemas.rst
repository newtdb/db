================
Database Schemas
================

Some databases claim to have no schemas.  There is always a schema,
even if it's only in programmers' heads.

In Newt DB, there isn't a server-side schema. Newt DB is
object-oriented on the client and the schema is expressed
semi-formally by Python classes and their data expectations.

Newt Schemas are highly dynamic. It's easy to add and remove data
elements.  See the ZODB documentation for `tips on schema migration
<http://www.zodb.org/en/latest/guide/writing-persistent-objects.html#schema-migration>`_.
