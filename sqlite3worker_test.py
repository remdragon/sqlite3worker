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
        self.tmp_file = tempfile.NamedTemporaryFile(
            suffix="pytest", prefix="sqlite").name
        self.sqlite3worker = sqlite3worker.Sqlite3Worker(self.tmp_file)
        # Create sql db.
        self.sqlite3worker.execute(
            "CREATE TABLE tester (timestamp DATETIME, uuid TEXT)")

    def tearDown(self):  # pylint:disable=D0102
        self.sqlite3worker.close()
        os.unlink(self.tmp_file)

    def test_bad_select(self):
        """Test a bad select query."""
        query = "select THIS IS BAD SQL"
        self.assertEqual(
            self.sqlite3worker.execute(query),
            (
                "Query returned error: select THIS IS BAD SQL: "
                "[]: no such column: THIS"))

    def test_bad_insert(self):
        """Test a bad insert query."""
        query = "insert THIS IS BAD SQL"
        self.sqlite3worker.execute(query)
        # Give it one second to clear the queue.
        if self.sqlite3worker.queue_size != 0:
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
        if self.sqlite3worker.queue_size != 0:
            time.sleep(1)
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"),
            [("2010-01-01 13:00:00", "bow"), ("2011-02-02 14:14:14", "dog")])

    def test_run_after_close(self):
        """Test to make sure all events are cleared after object closed."""
        self.sqlite3worker.close()
        self.sqlite3worker.execute(
            "INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"),
            "Exit Called")

    def test_double_close(self):
        """Make sure double closeing messages properly."""
        self.sqlite3worker.close()
        self.assertEqual(self.sqlite3worker.close(), "Already Closed")

    def test_db_closed_propertly(self):
        """Make sure sqlite object is properly closed out."""
        self.sqlite3worker.close()
        with self.assertRaises(
                self.sqlite3worker._sqlite3_conn.ProgrammingError):
            self.sqlite3worker._sqlite3_conn.total_changes

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
                    if resp != [("2010-01-01 13:00:00", token)]:
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


if __name__ == "__main__":
    unittest.main()
