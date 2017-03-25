=======================
JSON Serialization
=======================

This topic explains some details about how data are serialized to
JSON, beyond the behavior of the standard Python `json module
<https://docs.python.org/3/library/json.html#module-json>`_.

The first thing to note is that JSON serialization is lossy.  This is
why Newt saves data in both `pickle
<https://docs.python.org/3/library/pickle.html#data-stream-format>`_
and JSON format.

The serialization nevertheless preserves class information.

The serialization, like pickle, supports cyclic data structures using a
combination of persistent references and intra-record references.

Non-persistent instances
========================

Non-persistent instances are converted to JSON objects with ``::``
properties giving their dotted class names.  In the common case of
objects with their instance dictionaries used as their pickled state,
the object attributes become properties.

So, for example, given a class ``MyClass`` in module ``mymodule``::

  class MyClass:

      def __init__(self, a, b):
          self.a = a
          self.b = b

.. -> src

    >>> exec(src)
    >>> MyClass.__module__ = 'newt.db.tests'
    >>> import newt.db.tests
    >>> newt.db.tests.MyClass = MyClass
    >>> i = MyClass(1, 2)

The JSON serialization would look like:

.. code-block:: json

  {"::": "mymodule.MyClass", "a": 1, "b": 2}

.. -> expect

    >>> expect = expect.strip().replace('mymodule', MyClass.__module__)
    >>> from newt.db.jsonpickle import dumps
    >>> dumps(i, indent=None) == expect
    True

Non-dictionary state
--------------------

For instances with pickled state that's not a dictionary, a JSON
object is created with a ``state`` property containing the serialized
state and a ``::`` property with the dotted class name.

New arguments
-------------

Objects that take arguments to their ``__new__`` method will have the
arguments serialized in the ``::()`` property.

Intra-object reference ids
--------------------------

If a record has cycles and an object in the record is referenced more
than once, then the object will have an ``::id`` property who's value
is an internal reference id.

For objects like lists and sets, which aren't normally serialized as
objects, when an object is referenced more than once, it's wrapped in
a "shared" object with an ``::id`` property and a ``value`` property.

Intra-record cycles
===================

Cyclic data structures are allowed within persistent object records,
although they are **extremely rare**.  When there's a cycle, then objects
that are referenced more than once:

- have ``::id`` properties that assign them intra-record ids.

  Objects like lists, who's state are not dictionary are wrapped in a
  "shared" objects.

- Are replaced with reference objects in all bit one of the
  references.  Reference objects have a single property, ``::->``
  giving the intra-record id of the object being referenced.

Here's an example:

  >>> from newt.db.tests.testjsonpickle import I
  >>> i = I(a=1)
  >>> d = dict(b=1)
  >>> l = [i, i, d, d]
  >>> l.append(l)

The serialization of the list, ``l`` would be equivalent to:

.. code-block:: json

    {
      "::": "shared",
      "::id": 0,
      "value": [
        {
          "::": "newt.db.tests.testjsonpickle.I",
          "::id": 2,
          "a": 1
        },
        {"::->": 2},
        {
          "::id": 5,
          "b": 1
        },
        {"::->": 5},
        {"::->": 0}
      ]
    }

.. -> expect

   >>> import json
   >>> expect = json.loads(expect)
   >>> json.loads(dumps(l)) == expect
   True

Intra-record references like these are difficult to work with, which
is a good reason to avoid intra-record cycles.

Persistent object
=================

Persistent objects are stored in 4 columns of the ``newt`` table:

============  ======
   Column      Type
============  ======
zoid          bigint
class_name    text
ghost_pickle  bytea
state         jsonb
============  ======

The class name and state are separated and the state doesn't have a
``::`` property containing the dotted class name.

The ``ghost_pickle`` field contains the class name and ``__new__``
arguments if necessary.  It's used to create new objects when searching.

Persistent references
---------------------

When one persistent object references another persistent object, the
reference is serialized with a reference object, having a property
``::=>`` whose value is the object id of the referenced object
[#prefchanged]_. For example, serialization of a sub-task object
containing a reference to a parent task would be equivalent to:

.. code-block:: json

   {
     "title": "Do something",
     "parent": {"::=>": 42}
   }

Note that cycles among persistent objects are common and don't present
any problems for serialization because persistent objects are
serialized separately.

Dates and times
===============

``datetime.date`` objects and ``datetime.datetime`` instances without
time zones are converted strings using their ``isoformat`` methods.

``datetime.datetime`` instances with time zones are serialized as
objects with a ``::`` property of ``datetime``, a ``value`` property
with their ISO formatted value, and a ``tz`` property containing a
JSON serialization of their time zones.

.. [#prefchanged] This is a change from versions of Newt before 0.4.0.
   Earlier versions represented persistent references as objects with
   a ``::`` property with the value ``persistent`` and an ``id``
   property who's value is an integer object id or a list containing
   an integer object id and a dotted class name.  The attributes will
   be retained until Newt DB version 1, at which point they will no
   longer be included.
