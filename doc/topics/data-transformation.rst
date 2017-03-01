Data transformation
====================

You can have more control over how is JSON is generated from your data
by supplying a transform function. A transform function accepts two
positional arguments:

class_name
  The full dotted name of a persistent object's class.

state
  The object's state as a bytes string in JSON format.

The transform function must return a new state string or ``None``.
If ``None`` is returned, then the original state is used.

A transform function may return an empty string to indicate that
a record should be skipped and not written to the ``newt`` table.

For example, ``persistent.mapping.PersistentMapping`` objects store
their data in a ``data`` attribute, so their JSON representation is
more complex than we might want. Here's a transform that replaces
the JSON representation of a ``PersistentMapping`` with its data::

  import json

  def flatten_persistent_mapping(class_name, state):
      if class_name == 'persistent.mapping.PersistentMapping':
         state = json.loads(state)
         state = state['data']
         return json.dumps(state)

.. -> transform

   >>> exec(transform)

We can supply a transform function to the Python constructor using the
``transform`` keyword argument::

  import newt.db

  conn = newt.db.connection('', transform=flatten_persistent_mapping)

.. -> src

   >>> src = src.replace("''", "'dbname=%s'" % dsn.rsplit('/')[-1])
   >>> exec(src)
   >>> conn.root.x = 1
   >>> conn.transaction_manager.commit()

   >>> conn.query_data("select state from newt order by zoid")
   [({'x': 1},)]

   >>> conn.close()

To specify a transform in text configuration, use a ``transform``
option to supply the dotted name of your transform function in the
``newt`` configuration element:

.. code-block:: xml

  %import newt.db

  <newtdb>
    <zodb>
      <relstorage>
        keep-history false
        <newt>
          transform myproject.flatten_persistent_mapping
          <postgresql>
            dsn dbname=''
          </postgresql>
        </newt>
      </relstorage>
    </zodb>
  </newtdb>

.. -> src

   >>> from newt.db.tests import testdocs
   >>> testdocs.flatten_persistent_mapping = flatten_persistent_mapping
   >>> src = src.replace('myproject', 'newt.db.tests.testdocs')

   >>> src = src.replace("''", dsn.rsplit('/')[-1])
   >>> from ZODB.config import databaseFromString
   >>> db = databaseFromString(src)
   >>> isinstance(db, newt.db._db.NewtDB)
   True
   >>> import newt.db._adapter
   >>> isinstance(db.storage._adapter, newt.db._adapter.Adapter)
   True

   >>> conn = db.open()
   >>> conn.root.x = 2
   >>> conn.transaction_manager.commit()

   >>> conn.query_data("select state from newt order by zoid")
   [({'x': 2},)]

   >>> db.close()
