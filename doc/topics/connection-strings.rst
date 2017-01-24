==================
Connection strings
==================

Postgres connection strings are documented in `section 32.1.1 of the
Postgres documentation
<https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING>`_.
They take 2 forms:

1. URL syntax


   ``postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]``

2. Keyword/Value syntax

   ``host=localhost port=5432 dbname=mydb user=sally password=123456``

All of the parameters have defaults and may be excluded.  An empty
string is just an application of the keyword/value syntax with no
parameters specified.

To avoid including passwords in connection strings, you can use a
`Postgres password file
<https://www.postgresql.org/docs/current/static/libpq-pgpass.html>`_.
