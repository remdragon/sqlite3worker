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
try:
	import queue as Queue # module re-named in Python 3
except ImportError: # pragma: no cover
	import Queue
import pathlib # pip install pathlib
import platform
import sqlite3
import threading
import time

LOGGER = logging.getLogger('sqlite3worker')

workers = {}

OperationalError = sqlite3.OperationalError
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
	worker = None
	row_factory = None
	
	def __init__ ( self, worker, row_factory ):
		self.worker = worker
		self.row_factory = row_factory
	
	def execute ( self ):
		self.worker.sqlite3_cursor.row_factory = self.row_factory

class Sqlite3WorkerSetTextFactory ( Sqlite3WorkerRequest ):
	worker = None
	text_factory = None
	
	def __init__ ( self, worker, text_factory ):
		self.worker = worker
		self.text_factory = text_factory
	
	def execute ( self ):
		self.worker._sqlite3_conn.text_factory = self.text_factory

class Sqlite3WorkerExecute ( Sqlite3WorkerRequest ):
	worker = None
	query = None
	values = None
	results = None
	
	def __init__ ( self, worker, query, values ):
		self.worker = worker
		self.query = query
		self.values = values
		self.results = Queue.Queue()
	
	def execute ( self ):
		LOGGER.debug ( "run execute: %s", self.query )
		worker = self.worker
		cur = worker.sqlite3_cursor
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
	worker = None
	query = None
	results = None
	
	def __init__ ( self, worker, query ):
		self.worker = worker
		self.query = query
		self.results = Queue.Queue()
	
	def execute ( self ):
		LOGGER.debug ( "run executescript: %s", self.query )
		worker = self.worker
		cur = worker.sqlite3_cursor
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
	worker = None
	
	def __init__ ( self, worker ):
		self.worker = worker
	
	def execute ( self ):
		LOGGER.debug("run commit")
		worker = self.worker
		worker._sqlite3_conn.commit()

class Sqlite3WorkerExit ( Exception, Sqlite3WorkerRequest ):
	def execute ( self ):
		raise self

def normalize_file_name ( file_name ):
	if file_name.lower() == ':memory:':
		return ':memory:'
	# lookup absolute path of file_name
	file_name = str ( pathlib.Path ( file_name ).absolute() )
	if platform.system() == 'Windows':
		file_name = file_name.lower() # Windows filenames are not case-sensitive
	return file_name

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
	file_name = None
	_sqlite3_conn = None
	sqlite3_cursor = None
	sql_queue = None
	max_queue_size = None
	exit_set = False
	_thread = None
	
	def __init__ ( self, file_name, max_queue_size=100 ):
		"""Automatically starts the thread.
		Args:
			file_name: The name of the file.
			max_queue_size: The max queries that will be queued.
		"""
		self._thread = threading.Thread ( target=self.run )
		self._thread.daemon = True
		
		self.file_name = normalize_file_name ( file_name )
		if self.file_name != ':memory:':
			global workers
			assert self.file_name not in workers, 'attempted to create two different Sqlite3Worker objects that reference the same database'
			workers[self.file_name] = self
		
		self._sqlite3_conn = sqlite3.connect (
			file_name, check_same_thread=False,
			#detect_types=sqlite3.PARSE_DECLTYPES
		)
		self.sqlite3_cursor = self._sqlite3_conn.cursor()
		self.sql_queue = Queue.Queue ( maxsize=max_queue_size )
		self.max_queue_size = max_queue_size
		self._thread.name = self._thread.name.replace ( 'Thread-', 'sqlite3worker-' )
		self._thread.start()
	
	def run ( self ):
		"""Thread loop.
		This is an infinite loop.  The iter method calls self.sql_queue.get()
		which blocks if there are not values in the queue.  As soon as values
		are placed into the queue the process will continue.
		If many executes happen at once it will churn through them all before
		calling commit() to speed things up by reducing the number of times
		commit is called.
		"""
		LOGGER.debug("run: Thread started")
		while True:
			try:
				self.sql_queue.get().execute()
			except Sqlite3WorkerExit as e:
				if not self.sql_queue.empty(): # pragma: no cover ( TODO FIXME: come back to this )
					self.sql_queue.put ( e ) # push the exit event to the end of the queue
					continue
				self._sqlite3_conn.commit()
				self._sqlite3_conn.close()
				if self.file_name != ':memory:':
					global workers
					try:
						del workers[self.file_name]
					except KeyError:
						print ( 'file_name {!r} not found in workers {!r}'.format ( self.file_name, workers ) )
				return
	
	def close ( self ):
		"""Close down the thread and close the sqlite3 database file."""
		if self.exit_set: # pragma: no cover
			LOGGER.debug ( "sqlite worker thread already shutting down" )
			raise OperationalError ( 'sqlite worker thread already shutting down' )
		self.exit_set = True
		self.sql_queue.put ( Sqlite3WorkerExit(), timeout=5 )
		# Sleep and check that the thread is done before returning.
		self._thread.join()
	
	@property
	def queue_size ( self ): # pragma: no cover
		"""Return the queue size."""
		return self.sql_queue.qsize()
	
	def set_row_factory ( self, row_factory ):
		self.sql_queue.put ( Sqlite3WorkerSetRowFactory ( self, row_factory ), timeout=5 )
	
	def set_text_factory ( self, text_factory ):
		self.sql_queue.put ( Sqlite3WorkerSetTextFactory ( self, text_factory ), timeout=5 )
	
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
		if self.exit_set: # pragma: no cover
			LOGGER.debug ( "Exit set, not running: %s", query )
			raise OperationalError ( 'sqlite worker thread already shutting down' )
		LOGGER.debug ( "request execute: %s", query )
		r = Sqlite3WorkerExecute ( self, query, values or [] )
		self.sql_queue.put ( r, timeout=5 )
		success, result = r.results.get()
		if not success:
			raise result
		else:
			return result
	
	def execute ( self, query, values=None ):
		return self.execute_ex ( query, values )[0]
	
	def executescript_ex ( self, query ):
		if self.exit_set: # pragma: no cover
			LOGGER.debug ( "Exit set, not running: %s", query )
			raise OperationalError ( 'sqlite worker thread already shutting down' )
		LOGGER.debug ( "request executescript: %s", query )
		r = Sqlite3WorkerExecuteScript ( self, query )
		self.sql_queue.put ( r, timeout=5 )
		success, result = r.results.get()
		if not success:
			raise result
		else:
			return result
	
	def executescript ( self, sql ):
		return self.executescript_ex ( sql )[0]
	
	def commit ( self ):
		if self.exit_set: # pragma: no cover
			LOGGER.debug ( "Exit set, not running: %s", query )
			raise OperationalError ( 'sqlite worker thread already shutting down' )
		LOGGER.debug ( "request commit" )
		self.sql_queue.put ( Sqlite3WorkerCommit ( self ), timeout=5 )

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
	file_name = normalize_file_name ( file_name )
	global workers
	try:
		worker = workers[file_name]
	except KeyError:
		worker = Sqlite3Worker ( file_name )
	return Sqlite3worker_dbapi_connection ( worker )
