import os
import doctest
import unittest
import manuel.capture
import manuel.doctest
import manuel.testing
from zope.testing import setupstack

import newt
from .base import DBSetup

def setUp(test):
    dbsetup = DBSetup()
    dbsetup.setUp(False)
    setupstack.register(test, dbsetup.tearDown)
    test.globs.update(
        dsn = dbsetup.dsn,
        )

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
            setUp=setUp, tearDown=setupstack.tearDown,
            )


