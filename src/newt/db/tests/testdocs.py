from __future__ import print_function
import os
import doctest
import unittest
import manuel.capture
import manuel.doctest
import manuel.testing
from zope.testing import setupstack

import newt
from ..follow import updates as follow_updates
from .base import DBSetup

def finite_updates(conn, end_tid=1<<63, **kw):
    return follow_updates(conn, end_tid=end_tid, **kw)

def setUp(test):
    dbsetup = DBSetup()
    dbsetup.setUp(False)
    setupstack.register(test, dbsetup.tearDown)
    test.globs.update(
        dsn = dbsetup.dsn,
        print_ = print,
        )

    setupstack.mock(test, 'newt.db.follow.updates', side_effect=finite_updates)

def test_suite():
    d = os.path.dirname
    doc = os.path.join(d(d(d(d(d(__file__))))), 'doc')
    if not os.path.exists(doc):
        return unittest.TestSuite(())

    p = lambda *names: os.path.join(doc, *names) + '.rst'

    return manuel.testing.TestSuite(
            manuel.doctest.Manuel() + manuel.capture.Manuel(),
            p('fine-print'),
            p('getting-started'),
            p('topics', 'text-configuration'),
            p('topics', 'following'),
            p('topics', 'zodburi'),
            setUp=setUp, tearDown=setupstack.tearDown,
            )


