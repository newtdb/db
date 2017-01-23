=========
Reference
=========

newt.db module-level functions
==============================

.. autofunction:: newt.db.connection

.. autofunction:: newt.db.DB

.. autofunction:: newt.db.storage

.. autoclass:: newt.db.Connection
   :members: where, search, where_batch, search_batch, query_data,
             commit, abort,
             create_text_index_sql, create_text_index

newt.db.search module-level functions
=====================================

.. automodule:: newt.db.search
   :members: search, search_batch,
             create_text_index_sql, create_text_index
