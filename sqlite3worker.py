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

"""Thread safe sqlite3 interface."""

__author__ = "Shawn Lee"
__email__ = "shawnl@palantir.com"
__license__ = "MIT"

import logging
import platform
import os
import sqlite3
import threading
import time
try:
	import queue as Queue # module re-named in Python 3
except ImportError: # pragma: no cover
	import Queue

LOGGER = logging.getLogger('sqlite3worker')

OperationalError = sqlite3.OperationalError
ProgrammingError = sqlite3.ProgrammingError
Row = sqlite3.Row

class Frozen_object ( object ):
	def __setattr__ ( self, key, value ):
		if key not in dir ( self ): # prevent from accidentally creating new attributes
			raise AttributeError ( '{!r} object has no attribute {!r}'.format ( type ( self ).__name__, key ) )
		super ( Frozen_object, self ).__setattr__ ( key, value )

class Sqlite3WorkerRequest ( Frozen_object ):
	def execute ( self ): # pragma: no cover
		raise NotImplementedError ( type ( self ).__name__ + '.execute()' )

class Sqlite3WorkerSetRowFactory ( Sqlite3WorkerRequest ):
	thread = None
	row_factory = None
	
	def __init__ ( self, thread, row_factory ):
		self.thread = thread
		self.row_factory = row_factory
	
	def execute ( self ):
		self.thread._sqlite3_cursor.row_factory = self.row_factory

class Sqlite3WorkerSetTextFactory ( Sqlite3WorkerRequest ):
	thread = None
	text_factory = None
	
	def __init__ ( self, thread, text_factory ):
		self.thread = thread
		self.text_factory = text_factory
	
	def execute ( self ):
		self.thread._sqlite3_conn.text_factory = self.text_factory

class Sqlite3WorkerExecute ( Sqlite3WorkerRequest ):
	thread = None
	query = None
	values = None
	results = None
	
	def __init__ ( self, thread, query, values ):
		self.thread = thread
		self.query = query
		self.values = values
		self.results = Queue.Queue()
	
	def execute ( self ):
		LOGGER.debug ( "run execute: %s", self.query )
		thread = self.thread
		cur = thread._sqlite3_cursor
		try:
			cur.execute ( self.query, self.values )
			result = ( cur.fetchall(), cur.description, cur.lastrowid )
			success = True
		except Exception as err:
			LOGGER.debug (
				"Sqlite3WorkerExecute.execute sending exception back to calling thread: {!r}".format ( err ) )
			result = err
			success = False
		self.results.put ( ( success, result ) )

class Sqlite3WorkerExecuteScript ( Sqlite3WorkerRequest ):
	thread = None
	query = None
	results = None
	
	def __init__ ( self, thread, query ):
		self.thread = thread
		self.query = query
		self.results = Queue.Queue()
	
	def execute ( self ):
		LOGGER.debug ( "run executescript: %s", self.query )
		cur = self._sqlite3_cursor
		try:
			cur.executescript ( self.query )
			result = ( cur.fetchall(), cur.description, cur.lastrowid )
			success = True
		except Exception as err:
			LOGGER.debug (
				"Sqlite3WorkerExecuteScript.execute sending exception back to calling thread: {!r}".format ( err ) )
			result = err
			success = False
		self.results.put ( ( success, result ) )

class Sqlite3WorkerCommit ( Sqlite3WorkerRequest ):
	thread = None
	
	def __init__ ( self, thread ):
		self.thread = thread
	
	def execute ( self ):
		LOGGER.debug("run commit")
		thread = self.thread
		thread._sqlite3_conn.commit()

class Sqlite3WorkerExit ( Exception, Sqlite3WorkerRequest ):
	def execute ( self ):
		raise self

def normalize_file_name ( file_name ):
	if file_name.lower() == ':memory:':
		return ':memory:'
	# lookup absolute path of file_name
	file_name = os.path.abspath ( file_name )
	if platform.system() == 'Windows':
		file_name = file_name.lower() # Windows filenames are not case-sensitive
	return file_name

class Sqlite3WorkerThread ( threading.Thread ):
	_workers = None
	_sqlite3_conn = None
	_sqlite3_cursor = None
	_sql_queue = None
	_max_queue_size = None
	
	def __init__ ( self, file_name, max_queue_size, *args, **kwargs ):
		super ( Sqlite3WorkerThread, self ).__init__ ( *args, **kwargs )
		self.daemon = True
		self._workers = set()
		self._sqlite3_conn = sqlite3.connect (
			file_name, check_same_thread=False,
			#detect_types=sqlite3.PARSE_DECLTYPES
		)
		self._sqlite3_cursor = self._sqlite3_conn.cursor()
		self._sql_queue = Queue.Queue ( maxsize=max_queue_size )
		self._max_queue_size = max_queue_size
		self.name = self.name.replace ( 'Thread-', 'Sqlite3WorkerThread-' )
		self.start()
	
	def run ( self ):
		"""Thread loop.
		This is an infinite loop.  The iter method calls self._sql_queue.get()
		which blocks if there are not values in the queue.  As soon as values
		are placed into the queue the process will continue.
		If many executes happen at once it will churn through them all before
		calling commit() to speed things up by reducing the number of times
		commit is called.
		"""
		LOGGER.debug("run: Thread started")
		while True:
			try:
				x = self._sql_queue.get()
				x.execute()
			except Sqlite3WorkerExit as e:
				if not self._sql_queue.empty(): # pragma: no cover ( TODO FIXME: come back to this )
					LOGGER.debug ( 'requeueing the exit event because there are unfinished actions' )
					self._sql_queue.put ( e ) # push the exit event to the end of the queue
					continue
				LOGGER.debug ( 'closing database connection' )
				self._sqlite3_cursor.close()
				self._sqlite3_conn.commit()
				self._sqlite3_conn.close()
				LOGGER.debug ( 'exiting thread' )
				break

class Sqlite3Worker ( Frozen_object ):
	"""Sqlite thread safe object.
	Example:
		from sqlite3worker import Sqlite3Worker
		sql_worker = Sqlite3Worker("/tmp/test.sqlite")
		sql_worker.execute(
			"CREATE TABLE tester (timestamp DATETIME, uuid TEXT)")
		sql_worker.execute(
			"INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
		sql_worker.execute(
			"INSERT into tester values (?, ?)", ("2011-02-02 14:14:14", "dog"))
		sql_worker.execute("SELECT * from tester")
		sql_worker.close()
	"""
	_file_name = None
	_exit_set = False
	_thread = None
	
	# class shared attributes
	_threads = {}
	_threads_lock = threading.Lock()
	
	def __init__ ( self, file_name, max_queue_size=100 ):
		"""Automatically starts the thread.
		Args:
			file_name: The name of the file.
			max_queue_size: The max queries that will be queued.
		"""
		
		self._file_name = normalize_file_name ( file_name )
		with self._threads_lock:
			self._thread = self._threads.get ( self._file_name )
			if self._thread is None:
				self._thread = Sqlite3WorkerThread ( self._file_name, max_queue_size )
				self._threads[self._file_name] = self._thread
			if self._file_name != ':memory:':
				self._threads[self._file_name] = self._thread
			self._thread._workers.add ( self )
	
	def close ( self ):
		"""If we're the last worker, close down the thread which closes the sqlite3 database file."""
		if self._exit_set: # pragma: no cover
			LOGGER.debug ( "sqlite worker already closed" )
			raise ProgrammingError ( 'sqlite worker already closed' )
		self._exit_set = True
		with self._threads_lock:
			self._thread._workers.remove ( self )
			if not self._thread._workers:
				self._thread._sql_queue.put ( Sqlite3WorkerExit(), timeout=5 )
				# wait for the thread to finish what it's doing and shut down
				self._thread.join()
				try:
					del self._threads[self._file_name]
				except KeyError:
					assert self._file_name == ':memory:'
	
	@property
	def queue_size ( self ): # pragma: no cover
		"""Return the queue size."""
		return self._thread._sql_queue.qsize()
	
	def set_row_factory ( self, row_factory ):
		self._thread._sql_queue.put ( Sqlite3WorkerSetRowFactory ( self._thread, row_factory ), timeout=5 )
	
	def set_text_factory ( self, text_factory ):
		self._thread._sql_queue.put ( Sqlite3WorkerSetTextFactory ( self._thread, text_factory ), timeout=5 )
	
	def execute_ex ( self, query, values=None ):
		"""Execute a query.
		Args:
			query: The sql string using ? for placeholders of dynamic values.
			values: A tuple of values to be replaced into the ? of the query.
		Returns:
			a tuple of ( rows, description, lastrowid ):
				rows is a list of row results returned by fetchall() or [] if no rows
				description is the results of cursor.description after executing the query
				lastrowid is the result of calling cursor.lastrowid after executing the query
		"""
		if self._exit_set: # pragma: no cover
			LOGGER.debug ( "Exit set, not running: %s", query )
			raise ProgrammingError ( 'sqlite worker already closed' )
		LOGGER.debug ( "request execute: %s", query )
		r = Sqlite3WorkerExecute ( self._thread, query, values or [] )
		self._thread._sql_queue.put ( r, timeout=5 )
		success, result = r.results.get()
		if not success:
			raise result
		else:
			return result
	
	def execute ( self, query, values=None ):
		return self.execute_ex ( query, values )[0]
	
	def executescript_ex ( self, query ):
		if self._exit_set: # pragma: no cover
			LOGGER.debug ( "Exit set, not running: %s", query )
			raise ProgrammingError ( 'sqlite worker already closed' )
		LOGGER.debug ( "request executescript: %s", query )
		r = Sqlite3WorkerExecuteScript ( self._thread, query )
		self._thread._sql_queue.put ( r, timeout=5 )
		success, result = r.results.get()
		if not success:
			raise result
		else:
			return result
	
	def executescript ( self, sql ):
		return self.executescript_ex ( sql )[0]
	
	def commit ( self ):
		if self._exit_set: # pragma: no cover
			LOGGER.debug ( "Exit set, not running: %s", query )
			raise ProgrammingError ( 'sqlite worker already closed' )
		LOGGER.debug ( "request commit" )
		self._thread._sql_queue.put ( Sqlite3WorkerCommit ( self._thread ), timeout=5 )
	
	@property
	def total_changes ( self ):
		if self._exit_set: # pragma: no cover
			LOGGER.debug ( "Exit set, not querying total_changes" )
			raise ProgrammingError ( 'sqlite worker already closed' )
		return self._thread._sqlite3_conn.total_changes

class Sqlite3worker_dbapi_cursor ( Frozen_object ):
	con = None
	rows = None
	description = None
	lastrowid = None
	
	def __init__ ( self, con ):
		self.con = con
	
	def close ( self ):
		pass
	
	def execute ( self, sql, values=None ):
		self.rows, self.description, self.lastrowid = self.con.worker.execute_ex ( sql, values )
	
	def executescript ( self, sql_script ):
		self.rows, self.description, self.lastrowid = self.con.worker.executescript_ex ( sql_script )
	
	def fetchone ( self ):
		try:
			return self.rows.pop ( 0 )
		except IndexError:
			return None
	
	def __iter__ ( self ):
		while self.rows:
			yield self.fetchone()

class Sqlite3worker_dbapi_connection ( Frozen_object ):
	worker = None
	
	def __init__ ( self, worker ):
		self.worker = worker
	
	def commit ( self ):
		self.worker.commit()
	
	def cursor ( self ):
		return Sqlite3worker_dbapi_cursor ( self )
	
	def execute ( self, sql, values=None ):
		cur = self.cursor()
		cur.execute ( sql, values )
		return cur
	
	def executescript ( self, sql_script ):
		cur = self.cursor()
		cur.executescript ( sql_script )
		return cur
	
	def close ( self ):
		self.worker.close()
		self.worker = None
	
	@property
	def row_factory ( self ):
		raise NotImplementedError ( type ( self ).__name__ + '.row_factory' )
	
	@row_factory.setter
	def row_factory ( self, row_factory ):
		self.worker.set_row_factory ( row_factory )
	
	@property
	def text_factory ( self ):
		raise NotImplementedError ( type ( self ).__name__ + '.text_factory' )
	
	@text_factory.setter
	def text_factory ( self, text_factory ):
		self.worker.set_text_factory ( text_factory )

def connect ( file_name ):
	return Sqlite3worker_dbapi_connection ( Sqlite3Worker ( file_name ) )
