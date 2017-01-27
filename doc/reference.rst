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
             create_text_index_sql, create_text_index

   .. method:: abort()

      Abort the current transaction

   .. method:: commit()

      Commit the current transaction

newt.db.search module-level functions
=====================================

.. automodule:: newt.db.search
   :members: search, search_batch,
             create_text_index_sql, create_text_index
