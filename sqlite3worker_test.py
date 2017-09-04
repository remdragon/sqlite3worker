#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2014 Palantir Technologies
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""sqlite3worker test routines."""

__author__ = "Shawn Lee"
__email__ = "dashawn@gmail.com"
__license__ = "MIT"

import logging
import os
import tempfile
import threading
import time
import uuid

import unittest

import sqlite3worker


class Sqlite3WorkerTests(unittest.TestCase):  # pylint:disable=R0904
    """Test out the sqlite3worker library."""

    def setUp(self):  # pylint:disable=D0102
        self.tmp_file = tempfile.mktemp(
            suffix="pytest", prefix="sqlite")
        self.sqlite3worker = sqlite3worker.Sqlite3Worker(self.tmp_file)
        # Create sql db.
        self.sqlite3worker.executescript( # using executescript here for code coverage reasons
            "CREATE TABLE tester (timestamp DATETIME, uuid TEXT)")

    def tearDown(self):  # pylint:disable=D0102
        try:
            self.sqlite3worker.close()
        except sqlite3worker.ProgrammingError:
            pass # the test may have already closed the database
        os.unlink(self.tmp_file)

    def test_bad_select(self):
        """Test a bad select query."""
        query = "select THIS IS BAD SQL"
        with self.assertRaises ( sqlite3worker.OperationalError ):
            self.sqlite3worker.execute(query)

    def test_bad_insert(self):
        """Test a bad insert query."""
        query = "insert THIS IS BAD SQL"
        with self.assertRaises ( sqlite3worker.OperationalError ):
            self.sqlite3worker.execute(query)
        # Give it one second to clear the queue.
        if self.sqlite3worker.queue_size != 0: # pragma: no cover - this never happens any more
            time.sleep(1)
        self.assertEqual(self.sqlite3worker.queue_size, 0)
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"), [])

    def test_valid_insert(self):
        """Test a valid insert and select statement."""
        self.sqlite3worker.execute(
            "INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"),
            [("2010-01-01 13:00:00", "bow")])
        self.sqlite3worker.execute(
            "INSERT into tester values (?, ?)", ("2011-02-02 14:14:14", "dog"))
        # Give it one second to clear the queue.
        if self.sqlite3worker.queue_size != 0: # pragma: no cover - this never happens any more
            time.sleep(1)
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"),
            [("2010-01-01 13:00:00", "bow"), ("2011-02-02 14:14:14", "dog")])

    def test_run_after_close(self):
        """Test to make sure all events are cleared after object closed."""
        self.sqlite3worker.close()
        with self.assertRaises ( sqlite3worker.ProgrammingError ):
            self.sqlite3worker.execute(
                "INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))

    def test_double_close(self):
        """Make sure double closing messages properly."""
        self.sqlite3worker.close()
        with self.assertRaises ( sqlite3worker.ProgrammingError ):
            self.sqlite3worker.close()

    def test_db_closed_properly(self):
        """Make sure sqlite object is properly closed out."""
        self.sqlite3worker.close()
        with self.assertRaises ( sqlite3worker.ProgrammingError ):
            self.sqlite3worker.total_changes

    def test_many_threads(self):
        """Make sure lots of threads work together."""
        class threaded(threading.Thread):
            def __init__(self, sqlite_obj):
                threading.Thread.__init__(self, name=__name__)
                self.sqlite_obj = sqlite_obj
                self.daemon = True
                self.failed = False
                self.completed = False
                self.start()

            def run(self):
                for _ in range(5):
                    token = str(uuid.uuid4())
                    self.sqlite_obj.execute(
                        "INSERT into tester values (?, ?)",
                        ("2010-01-01 13:00:00", token))
                    resp = self.sqlite_obj.execute(
                        "SELECT * from tester where uuid = ?", (token,))
                    if resp != [("2010-01-01 13:00:00", token)]: # pragma: no cover ( we don't expect tests to fail )
                        self.failed = True
                        break
                self.completed = True

        threads = []
        for _ in range(5):
            threads.append(threaded(self.sqlite3worker))

        for i in range(5):
            while not threads[i].completed:
                time.sleep(.1)
            self.assertEqual(threads[i].failed, False)
            threads[i].join()

    def test_many_dbapi_threads ( self ):
        """Make sure lots of threads work together with dbapi interface."""
        class threaded ( threading.Thread ):
            def __init__ ( self, id, tmp_file ):
                threading.Thread.__init__ ( self, name='test {}'.format ( id ) )
                self.tmp_file = tmp_file
                self.daemon = True
                self.failed = False
                self.completed = False
                self.start()

            def run ( self ):
                logging.debug ( 'connecting' )
                con = sqlite3worker.connect ( self.tmp_file )
                for i in range ( 5 ):
                    logging.debug ( 'creating cursor #{}'.format ( i ) )
                    c = con.cursor()
                    token = str ( uuid.uuid4() )
                    logging.debug ( 'cursor #{} inserting token {!r}'.format ( i, token ) )
                    c.execute (
                        "INSERT into tester values (?, ?)",
                        ( "2010-01-01 13:00:00", token )
                    )
                    logging.debug ( 'cursor #{} querying token {!r}'.format ( i, token ) )
                    c.execute (
                        "SELECT * from tester where uuid = ?", (token,)
                    )
                    resp = c.fetchone()
                    logging.debug ( 'cursor #{} closing'.format ( i ) )
                    c.close()
                    if resp != ( "2010-01-01 13:00:00", token ): # pragma: no cover ( we don't expect tests to fail )
                        logging.debug ( 'cursor #{} invalid resp {!r}'.format ( i, resp ) )
                        logging.debug ( repr ( resp ) )
                        self.failed = True
                        break
                    else:
                        logging.debug ( 'cursor #{} success'.format ( i ) )
                logging.debug ( 'closing connection' )
                con.close()
                self.completed = True
        
        threads = []
        for id in range ( 5 ):
            threads.append ( threaded ( id, self.tmp_file ) )
        
        con = sqlite3worker.connect ( self.tmp_file )
        con.executescript ( 'pragma foreign_keys=on;' ) # not using this, put here for code coverage reasons
        con.row_factory = sqlite3worker.Row
        con.text_factory = unicode
        
        for i in range ( 5 ):
            while not threads[i].completed:
                time.sleep ( 0.1 )
            self.assertEqual ( threads[i].failed, False )
            threads[i].join()
        
        logging.debug ( 'counting results' ) # yes I could do a count(*) here but I'm doing it this way for code coverage reasons
        con.commit()
        cur = con.execute ( 'select * from tester' )
        count = 0
        for row in cur:
            self.assertEqual ( len ( row['uuid'] ), 36 )
            count += 1
        self.assertEquals ( cur.fetchone(), None ) # make sure all rows retrieved
        con.close()
        self.assertEquals ( count, 25 )
    
    def test_coverage ( self ):
        """ a bunch of miscellaneous things to get code coverage to 100% """
        class Foo ( sqlite3worker.Frozen_object ):
            pass
        foo = Foo()
        with self.assertRaises ( AttributeError ):
            foo.bar = 'bar'
        self.sqlite3worker.set_row_factory ( sqlite3worker.Row )
        self.assertEquals ( self.sqlite3worker.total_changes, 0 )
        self.sqlite3worker.set_text_factory ( unicode )
        with self.assertRaises ( sqlite3worker.OperationalError ):
            self.sqlite3worker.executescript ( 'THIS IS INTENTIONALLY BAD SQL' )
        
        # try to force and catch an assert in the close logic...
        del self.sqlite3worker._threads[self.sqlite3worker._file_name]
        with self.assertRaises ( AssertionError ):
            self.sqlite3worker.close()
        
        self.assertEquals ( sqlite3worker.normalize_file_name ( ':MEMORY:' ), ':memory:' )
        with self.assertRaises ( sqlite3worker.ProgrammingError ):
            self.sqlite3worker.executescript ( 'drop table tester' )
        with self.assertRaises ( sqlite3worker.ProgrammingError ):
            self.sqlite3worker.commit()
        
if __name__ == "__main__": # pragma: no cover ( only executed when running test directly )
    if False:
        import sys
        logging.basicConfig ( stream=sys.stdout, level=logging.DEBUG, format='%(asctime)s [%(threadName)s %(levelname)s] %(message)s' )
    unittest.main()
