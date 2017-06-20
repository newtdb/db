============================
Getting started with Newt DB
============================

.. contents::

You'll need a Postgres Database server. You can `install one yourself
<https://www.postgresql.org/download/>`_ or you can use a `hosted
Postgres server
<https://www.google.com/search?q=postgres+as+a+service>`_. You'll need
Postgres 9.5 or later.

Next, install newt.db::

  pip install newt.db

You'll eventually want to create a dedicated database and database user for
Newt's use, but if you've installed Postgres locally, you can just use
the default database.

From Python, to get started::

  >>> import newt.db
  >>> connection = newt.db.connection('')

.. -> src

   >>> src = [s.replace('>>> ', '') for s in src.strip().split('\n')]
   >>> import six
   >>> six.exec_(src[0])
   >>> six.exec_(src[1].replace("''", "dsn"))

In this example, we've asked newt to connect to the default Postgres
database.  You can also supply a :doc:`connection string
<topics/connection-strings>`.

The connection has a root object::

  >>> connection.root
  <root: >

This is the starting point for adding objects to the database.

To add data, we simply add objects to the root, directly::

  >>> connection.root.first = newt.db.Object(name='My first object')

Or indirectly, as a subobject::

  >>> connection.root.first.child = newt.db.Object(name='First child')

When we're ready to save our data, we need to tell Newt to commit the
changes::

  >>> connection.commit()

.. Double check:

   >>> connection.root
   <root: first>

   >>> connection.root.x = 1
   >>> connection.root
   <root: first x>

Or, if we decide we made a mistake, we can abort any changes made
since the last commit::

  >>> connection.abort()

.. Double check:

   >>> connection.root
   <root: first>

Above, we used the ``newt.db.Object`` class to create new objects.  This
class creates objects that behave a little bit like JavaScript
objects. They're just generic containers for properties.  They're
handy for playing, and when you have a little data to store and you
don't want to bother making a custom class.

Normally, you'd create application-specific objects by subclassing
``Persistent`` [#persistent]_::

  class Task(newt.db.Persistent):

     assigned = None

     def __init__(self, title, description):
         self.title = title
         self.description = description

     def assign(self, user):
         self.assigned = user

.. -> src

   >>> import newt.db.tests.testdocs
   >>> six.exec_(src, newt.db.tests.testdocs.__dict__)
   >>> Task = newt.db.tests.testdocs.Task

The ``Persistent`` base class helps track object changes. When we
modify an object, by setting an attribute, the object is marked as
changed, so that Newt will write it to the database when your
application commits changes.

With a class like the one above, we can add tasks to the database::

   >>> connection.root.task = Task("First task", "Explain collections")
   >>> connection.commit()

Collections
===========

Having all objects in the root doesn't provide much organization.
It's better to create container objects.  For example, we can
create a task list::

  class TaskList(newt.db.Persistent):

    def __init__(self):
        self.tasks = newt.db.List()

    def add(self, task):
        self.tasks.append(task)

.. -> src

   >>> six.exec_(src, newt.db.tests.testdocs.__dict__)
   >>> TaskList = newt.db.tests.testdocs.TaskList

Then when setting up our database, we'd do something like::

  >>> connection.root.tasks = TaskList()
  >>> connection.commit()

In the ``TaskList`` class, we using a ``List`` object. This is similar to
a Python list, except that, like the ``Persistent`` base class, it
tracks changes so they're saved when your application commits changes.

Rather than supporting a single task list, we could create a list
container, perhaps organized by list name::

  class TaskLists(newt.db.Persistent):

      def __init__(self):
          self.lists = newt.db.BTree()

      def add(self, name, list):
          if name in self.lists:
              raise KeyError("There's already a list named", name)
          self.lists[name] = list

      def __getitem__(self, name):
          return self.lists[name]

.. -> src

   >>> six.exec_(src, newt.db.tests.testdocs.__dict__)
   >>> TaskLists = newt.db.tests.testdocs.TaskLists

Here, we used a ``BTree`` as the basis of our container.  BTrees are
mapping objects that keep data sorted on their keys.

BTrees handle very large collections well, because, when they get
large, they spread their data over multiple database records, reducing
the amount of data read and written and allowing collections that
would be too large to keep in memory at once.

With this, building up the database could look like::

    >>> connection.root.lists = TaskLists()
    >>> connection.root.lists.add('docs', TaskList())
    >>> connection.root.lists['docs'].add(
    ...     Task("First task", "Explain collections"))
    >>> connection.commit()

Notice that the database is hierarchical.  We access different parts
of the database by traversing from object to object.

Searching
=========

Newt leverages PostgreSQL's powerful index and search
capabilities. The simplest way to search is with a connection's
``where`` method::

  >>> tasks = connection.where("""state @> '{"title": "First task"}'""")

The search above used a Postgres JSON ``@>`` operator that tests
whether its right side appears in its left side.  This sort of search
is indexed automatically by newt.  You can also use the ``search`` method::

  >>> tasks = connection.search("""
  ...     select * from newt where state @> '{"title": "First task"}'
  ...     """)

When using ``search``, you can compose any SQL you wish, but the
result must contain columns ``zoid`` and ``ghost_pickle``.  When you
first use a database with Newt, it creates a number of tables,
including ``newt``::

        Table "public.newt"
        Column    |  Type  | Modifiers
    --------------+--------+-----------
     zoid         | bigint | not null
     class_name   | text   |
     ghost_pickle | bytea  |
     state        | jsonb  |
    Indexes:
        "newt_pkey" PRIMARY KEY, btree (zoid)
        "newt_json_idx" gin (state)

The ``zoid`` column is the database primary key. Every persistent
object in Newt has a unique zoid.  The ``ghost_pickle`` pickle
contains minimal information to, along with ``zoid`` create newt
objects. The ``class_name`` column contains object's class name, which
can be useful for search.  The ``state`` column contains a JSON
representation of object state suitable for searching and access from
other applications.

You can use PostgreSQL to define more sophisticated or
application-specific indexes, as needed.

Newt has a built-in helper for defining full-text indexes on your data::

  >>> connection.create_text_index('mytext', ['title', 'description', 'text'])

This creates a `PL/pgSQL
<https://www.postgresql.org/docs/current/static/plpgsql.html>`_
text-extraction function named ``mytext`` and uses it to create a text
index.  With the index in place, you can search it like this::

  >>> tasks = connection.where("mytext(state) @@ 'explain'")

The example above finds all of the objects containing the word
"explain" in their title, description, or text.  We've assumed that
these are tasks. If we wanted to make sure, we could add a "class"
restriction::

  >>> tasks = connection.where(
  ...   "mytext(state) @@ 'explain' and class_name = 'newt.demo.Task'")

Rather than creating an index directly, we can ask Newt to just return
the PostgreSQL code to create them::

  >>> sql = connection.create_text_index_sql(
  ...           'mytext', ['title', 'description', 'text'])

You can customize the returned code or just view it to see how it works.


Query errors
------------

If you enter an invalid query and then retry, you may get an error
like: "InternalError: current transaction is aborted, commands ignored
until end of transaction block".  If this happens, you'll need to
abort the current transaction::

  >>> connection.abort()

After that, you should be able to query again.

You can only search committed data
----------------------------------

If you change objects, you won't see the changes in search results
until changes are committed, because data aren't written to Postgres
until the transaction is committed.

Raw queries
-----------

You can query for raw data, rather than objects using the ``query_data``
method. For example, to get a count of the various classes in your
database, you could use::

  >>> counts = connection.query_data("""
  ...     select class_name, count(*)
  ...     from newt
  ...     group by class_name
  ...     order by class_name
  ...     """)

.. cleanup

   >>> connection.close()

Learning more
=============

To learn more about Newt, see the Newt topics and the Newt
:doc:`topics <topics/index>` and :doc:`reference <reference>`.


.. [#persistent] Newt makes ``Persistent`` available as an attribute,
   but it's an alias for ``persistent.Persistent``.  In fact many of
   the classes provided by Newt are just aliases.
